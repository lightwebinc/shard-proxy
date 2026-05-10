// Command bitcoin-shard-proxy accepts BSV transaction datagrams on a UDP
// IPv6 socket, derives a multicast group address from the transaction ID's
// top N bits, and retransmits each datagram verbatim to the derived group.
//
// Multiple worker goroutines — one per CPU by default — each bind an
// independent SO_REUSEPORT socket to the listen port. The kernel distributes
// incoming datagrams across them, providing CPU-local processing with no
// userspace coordination on the ingress path.
//
// # Quick start
//
//	bitcoin-shard-proxy -iface eth0,eth1 -shard-bits 16 -scope site
//
// # Configuration
//
// All flags have environment variable equivalents; see [config.Load] for the
// full mapping. The most important parameters:
//
//   - -shard-bits (SHARD_BITS): controls how many bits of the txid prefix
//     are used as the multicast group key. Range 1–24.
//     8  →   256 groups (fits any managed switch)
//     16 → 65536 groups (standard deployment)
//     24 → 16M   groups (maximum precision)
//
//   - -scope (MC_SCOPE): multicast scope. Use "site" for closed subscriber
//     fabrics; "global" only if subscribers span BGP domains.
//
//   - -iface (MULTICAST_IF): comma-separated NIC names over which multicast
//     datagrams are sent (e.g. eth0,eth1). Each datagram is forwarded to all
//     listed interfaces in order. All names must exist on the host; the proxy
//     exits immediately if any are not found.
//
// # Graceful shutdown
//
// The proxy catches SIGINT (Ctrl-C) and SIGTERM (sent by systemd, container
// orchestrators, etc.). Shutdown proceeds in two phases:
//
//  1. Draining: /readyz immediately returns 503, then the process sleeps
//     -drain-timeout (DRAIN_TIMEOUT) to allow load-balancer health checks to
//     propagate and stop sending new connections. Defaults to 0 (disabled).
//
//  2. Quiescing: the done channel is closed, each worker's ingress socket is
//     closed (unblocking ReadFrom), and main waits for all goroutines to exit
//     before the process returns.
package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/lightwebinc/bitcoin-shard-common/shard"
	"github.com/lightwebinc/bitcoin-shard-proxy/config"
	"github.com/lightwebinc/bitcoin-shard-proxy/forwarder"
	"github.com/lightwebinc/bitcoin-shard-proxy/metrics"
	"github.com/lightwebinc/bitcoin-shard-proxy/worker"
)

func main() {
	// Load and validate configuration from flags / environment variables.
	cfg, err := config.Load()
	if err != nil {
		// Use plain stderr before the structured logger is initialised.
		slog.Error("configuration error", "err", err)
		os.Exit(1)
	}

	// Initialise the structured logger. Debug level enables per-packet output.
	logLevel := slog.LevelInfo
	if cfg.Debug {
		logLevel = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: logLevel,
	})))

	// Resolve all egress interfaces once; workers share the []*net.Interface slice.
	ifaces := make([]*net.Interface, 0, len(cfg.EgressIfaces))
	for _, name := range cfg.EgressIfaces {
		iface, err := net.InterfaceByName(name)
		if err != nil {
			slog.Error("multicast interface not found", "iface", name, "err", err)
			os.Exit(1)
		}
		ifaces = append(ifaces, iface)
	}

	// Initialise the metrics recorder (Prometheus + optional OTLP).
	rec, err := metrics.New(cfg.InstanceID, cfg.NumWorkers, cfg.OTLPEndpoint, cfg.OTLPInterval)
	if err != nil {
		slog.Error("metrics init failed", "err", err)
		os.Exit(1)
	}

	// Construct the shard engine. It is immutable and safe for concurrent use.
	engine := shard.New(cfg.MCPrefix, cfg.MCMiddleBytes, cfg.ShardBits)

	slog.Info("bitcoin-shard-proxy starting",
		"workers", cfg.NumWorkers,
		"shard_bits", cfg.ShardBits,
		"num_groups", engine.NumGroups(),
		"scope", cfg.MCScope,
		"udp_listen_port", cfg.UDPListenPort,
		"tcp_listen_port", cfg.TCPListenPort,
		"egress_port", cfg.EgressPort,
		"ifaces", cfg.EgressIfaces,
		"debug", cfg.Debug,
		"metrics_addr", cfg.MetricsAddr,
		"instance_id", cfg.InstanceID,
		"version", metrics.Version,
	)

	// Construct the shared forwarder.
	fwd := forwarder.New(engine, cfg.MCPrefix, cfg.MCMiddleBytes, cfg.EgressPort, cfg.Debug, rec)

	// done is closed to signal all workers to stop their receive loops.
	done := make(chan struct{})
	var wg sync.WaitGroup

	// Start the metrics HTTP server (blocks on done; shuts down gracefully).
	go rec.Serve(cfg.MetricsAddr, done)

	for i := range cfg.NumWorkers {
		w := worker.New(i, fwd, ifaces, rec)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.Run(cfg.ListenAddr, cfg.UDPListenPort, done); err != nil {
				slog.Error("worker exited with error", "worker", i, "err", err)
			}
		}()
	}

	// Start TCP ingress if configured.
	if cfg.TCPListenPort > 0 {
		tcpIngress := worker.NewTCPIngress(fwd, ifaces, rec)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := tcpIngress.Run(cfg.ListenAddr, cfg.TCPListenPort, done); err != nil {
				slog.Error("TCP ingress exited with error", "err", err)
			}
		}()
	}

	// ── Signal handling ───────────────────────────────────────────────────
	//
	// sig is a buffered channel of capacity 1. The buffer is intentional:
	// if a signal arrives in the brief window between signal.Notify and the
	// <-sig receive below, the runtime deposits it into the buffer rather
	// than dropping it. Without the buffer, that race would cause the signal
	// to be silently lost and the proxy would never shut down.
	//
	// signal.Notify registers sig with the Go runtime's signal dispatcher.
	// From this point, any SIGINT (Ctrl-C) or SIGTERM sent to the process
	// causes the runtime to write the signal value into sig.
	//
	// <-sig is a blocking channel receive. It suspends the main goroutine
	// here — the proxy is running, workers are processing packets — until
	// a value arrives in the channel. The received value is captured (not
	// discarded) so it can be included in the shutdown log line.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	received := <-sig // block until SIGINT or SIGTERM

	slog.Info("received signal, starting drain",
		"signal", received,
		"signal_number", int(received.(syscall.Signal)),
		"drain_timeout", cfg.DrainTimeout,
	)

	// Phase 1: mark draining so /readyz returns 503 immediately, then wait for
	// the load balancer's health-check interval to propagate before we close
	// any sockets. Workers continue processing in-flight packets during this
	// window. If DrainTimeout is 0 the sleep is skipped.
	rec.SetDraining()
	if cfg.DrainTimeout > 0 {
		time.Sleep(cfg.DrainTimeout)
	}

	// Phase 2: close done to unblock all worker receive loops and the metrics
	// server, then flush any pending OTLP exports before waiting for all
	// goroutines to exit.
	slog.Info("drain complete, closing ingress sockets")
	close(done)
	shutStart := time.Now()

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutCancel()
	rec.Shutdown(shutCtx)

	wg.Wait()

	slog.Info("all workers stopped; exiting cleanly", "shutdown_elapsed", time.Since(shutStart).Round(time.Millisecond))
}
