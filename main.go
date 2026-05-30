// Command shard-proxy accepts BSV transaction datagrams on a UDP
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
//	shard-proxy -iface eth0,eth1 -shard-bits 8 -scope site
//
// # Configuration
//
// All flags have environment variable equivalents; see [config.Load] for the
// full mapping. The most important parameters:
//
//   - -shard-bits (SHARD_BITS): controls how many bits of the txid prefix
//     are used as the multicast group key. Range 1–15.
//     8  →   256 groups (fits any managed switch)
//     12 →  4096 groups
//     15 → 32768 groups (maximum; top of 16-bit space reserved for control)
//
//   - -mc-group-id (MC_GROUP_ID): IANA group-id occupying bytes 12–13 of
//     the address. Default 0x000B (IANA Bitcoin allocation "FF0X::B").
//     Operators MAY override for testing/private deployments.
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

	"github.com/lightwebinc/shard-common/shard"
	"github.com/lightwebinc/shard-common/txidset"
	"github.com/lightwebinc/shard-proxy/config"
	"github.com/lightwebinc/shard-proxy/forwarder"
	"github.com/lightwebinc/shard-proxy/metrics"
	"github.com/lightwebinc/shard-proxy/worker"
)

// txidsetRecorder adapts *metrics.Recorder to the txidset.Recorder interface.
// The proxy only ever uses Claim (never Mark) so Mark-related callbacks are
// silent no-ops on the metric side; they remain on the interface for forward
// compatibility with shared Store usage patterns.
type txidsetRecorder struct{ rec *metrics.Recorder }

func (r txidsetRecorder) ClaimLocalHit(p string) { r.rec.TxidClaimLocalHit(p) }
func (r txidsetRecorder) ClaimWon(p string)      { r.rec.TxidClaimWon(p) }
func (r txidsetRecorder) ClaimLost(p string)     { r.rec.TxidClaimLost(p) }
func (r txidsetRecorder) ClaimError(p string)    { r.rec.TxidClaimError(p) }
func (r txidsetRecorder) MarkSet(string)         {}
func (r txidsetRecorder) MarkExisted(string)     {}
func (r txidsetRecorder) MarkError(string)       {}
func (r txidsetRecorder) MarkDropped(string)     {}

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
	engine := shard.New(cfg.MCPrefix, cfg.MCGroupID, cfg.ShardBits)

	slog.Info("shard-proxy starting",
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
	fwd := forwarder.New(engine, cfg.MCPrefix, cfg.MCGroupID, cfg.EgressPort, cfg.Debug, rec)
	if cfg.FragMTU > 0 {
		fwd.SetFragMTU(cfg.FragMTU)
		slog.Info("BRC-130 fragmentation enabled", "frag_mtu", cfg.FragMTU)
	}

	// Optional ingress TxID dedup. Two-tier (local LRU → Redis SETNX).
	// LocalCap=0 disables the feature entirely.
	var txStore *txidset.Store
	if cfg.TxidDedupLocalCap > 0 {
		txStore, err = txidset.New(txidset.Config{
			RedisAddr:     cfg.TxidDedupRedisAddr,
			TTL:           cfg.TxidDedupTTL,
			LocalCapacity: cfg.TxidDedupLocalCap,
			Recorder:      txidsetRecorder{rec: rec},
		})
		if err != nil {
			slog.Error("txid dedup init failed", "err", err)
			os.Exit(1)
		}
		defer func() { _ = txStore.Close() }()
		fwd.SetTxidDedup(txStore, cfg.TxidDedupPrefix)
		slog.Info("ingress TxID dedup enabled",
			"redis_addr", cfg.TxidDedupRedisAddr,
			"prefix", cfg.TxidDedupPrefix,
			"ttl", cfg.TxidDedupTTL,
			"local_cap", cfg.TxidDedupLocalCap,
		)
	}

	// done is closed to signal all workers to stop their receive loops.
	done := make(chan struct{})
	var wg sync.WaitGroup

	// Start the metrics HTTP server (blocks on done; shuts down gracefully).
	go rec.Serve(cfg.MetricsAddr, done)

	for i := range cfg.NumWorkers {
		w := worker.New(i, fwd, ifaces, rec)
		w.SetRecvBatch(cfg.RecvBatch)
		w.SetRecvBufBytes(cfg.RecvBufBytes)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.Run(cfg.ListenAddr, cfg.UDPListenPort, done); err != nil {
				slog.Error("worker exited with error", "worker", i, "err", err)
			}
		}()
	}

	// Start TCP ingress if configured. Mark it as a /readyz prerequisite
	// before launching the goroutine so /readyz returns 503 until the
	// listener has actually bound — otherwise senders can race the bind
	// (TCP_LISTEN_PORT > 0 ⇒ readyz must reflect TCP reachability, not
	// just worker count).
	if cfg.TCPListenPort > 0 {
		rec.RequireTCPIngress()
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
