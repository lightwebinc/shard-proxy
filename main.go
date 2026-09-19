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
//     are used as the multicast group key. BRC-129 zoning bounds shard group
//     indices to 0x0000–0x0FFF; the validator enforces 1–12.
//     8  →   256 groups (fits any managed switch)
//     12 →  4096 groups (BRC-129 maximum)
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
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/lightwebinc/shard-common/cache"
	"github.com/lightwebinc/shard-common/hostinfo"
	"github.com/lightwebinc/shard-common/logging"
	commanifest "github.com/lightwebinc/shard-common/manifest"
	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/shard-common/shard"
	"github.com/lightwebinc/shard-common/tracing"
	"github.com/lightwebinc/shard-common/txidset"
	"github.com/lightwebinc/shard-proxy/config"
	"github.com/lightwebinc/shard-proxy/forwarder"
	proxymanifest "github.com/lightwebinc/shard-proxy/manifest"
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

	// Initialise the unified structured logger. -debug is a deprecated alias
	// that forces debug level.
	logLevel := logging.ParseLevel(cfg.LogLevel)
	if cfg.Debug {
		logLevel = slog.LevelDebug
	}
	levelVar := logging.Init(logging.Options{
		Service:    metrics.ServiceName,
		InstanceID: cfg.InstanceID,
		Version:    metrics.Version,
		Level:      logLevel,
		Format:     logging.ParseFormat(cfg.LogFormat),
	})
	logging.InstallSIGHUPToggle(levelVar, logLevel)

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
	rec.SetLevelVar(levelVar)

	// One-shot host inventory: emit the descriptive payload as a log event and
	// mirror the slim numerics as the bsp_host_info gauge for dashboard joins.
	inv := hostinfo.Gather(metrics.ServiceName, metrics.Version)
	rec.SetHostInfo(inv)
	slog.Info("host.inventory", "inventory", inv)

	// Opt-in distributed tracing (no-op unless -trace-sampling > 0 with an OTLP
	// endpoint). Control-plane only; never wired into the forwarder hot path.
	_, traceShutdown, terr := tracing.Init(context.Background(), tracing.Options{
		Service:      metrics.ServiceName,
		InstanceID:   cfg.InstanceID,
		Version:      metrics.Version,
		OTLPEndpoint: cfg.OTLPEndpoint,
		Sampling:     cfg.TraceSampling,
	})
	if terr != nil {
		slog.Warn("tracing init failed; continuing without traces", "err", terr)
	}
	defer func() {
		tctx, tcancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer tcancel()
		_ = traceShutdown(tctx)
	}()

	// Construct the shard engine. It is immutable and safe for concurrent use.
	engine := shard.New(cfg.MCPrefix, cfg.MCGroupID, cfg.ShardBits)

	// BRC-148 BEEF object-plane engine (domain 0x1, band 0x1000+). BEEF is
	// an open ingress class, so the plane is always wired; only the optional
	// dedicated lane is flag-gated.
	beefEngine, err := shard.NewPlane(cfg.MCPrefix, cfg.MCGroupID, cfg.BEEFShardBits, shard.DomainBEEF)
	if err != nil {
		slog.Error("beef plane engine", "err", err)
		os.Exit(1)
	}

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
	fwd.SetEgressHopLimit(cfg.EgressHopLimit)
	fwd.SetEgressLoop(cfg.EgressLoop)
	fwd.SetRequireEF(cfg.RequireEF)
	fwd.SetVerifyPayloadHash(cfg.VerifyPayloadHash)
	fwd.SetAllowStampedIngress(cfg.AllowStampedIngress)
	fwd.SetVerifySubtreeRoot(cfg.VerifySubtreeRoot)
	fwd.SetBEEF(beefEngine, cfg.BEEFMaxObjectBytes)
	if cfg.RequireEF {
		slog.Info("EF-native ingress enabled: raw BRC-12/BRC-124 transaction submissions rejected")
	}
	if cfg.AllowStampedIngress {
		slog.Info("stamped ingress ALLOWED: framed BRC-124/BRC-128 input carrying a SeqNum is admitted — correct for a spine collect lane or relay hop, wrong for a public submission lane")
	}
	if cfg.VerifySubtreeRoot {
		slog.Info("subtree root verification enabled: a BRC-132 subtree whose nodes do not hash to its root is dropped before ingress dedup")
	}
	if cfg.VerifyPayloadHash {
		slog.Info("payload-hash verification enabled: framed BRC-124/BRC-128 input with a TxID that does not match its payload is dropped before ingress dedup")
	}
	if cfg.RequireBlockPoW {
		fwd.SetBlockPoW(true, cfg.MinPoWBits)
		slog.Info("block-announce proof-of-work gate enabled", "min_pow_bits", fmt.Sprintf("0x%08x", cfg.MinPoWBits))
	}
	if cfg.FragMTU > 0 {
		fwd.SetFragMTU(cfg.FragMTU)
		slog.Info("BRC-130 fragmentation enabled", "frag_mtu", cfg.FragMTU,
			"max_unfragmented_payload", cfg.FragMTU-140)
	} else {
		// Explicitly disabled. This is not "no fragmentation" — it is a hard
		// payload ceiling of MTU-140 bytes (1360 at 1500), above which frames
		// become oversize datagrams the fabric cannot carry. Every subtree and
		// block frame is above it. Loud on purpose: this state is silent on the
		// wire (senders see a healthy TCP connection and no error).
		slog.Warn("BRC-130 fragmentation DISABLED (-frag-mtu=0): payloads above ~1360B " +
			"will be emitted as oversize datagrams and dropped by the fabric; " +
			"subtree and block lanes cannot work")
	}
	if cfg.Coalesce {
		fwd.SetCoalesce(true, cfg.CoalesceMaxBytes, cfg.CoalesceMaxMembers, cfg.CoalesceCarryTxid)
		slog.Info("BRC-142 frame coalescing enabled",
			"max_bytes", cfg.CoalesceMaxBytes, "max_members", cfg.CoalesceMaxMembers, "carry_txid", cfg.CoalesceCarryTxid)
	}
	if cfg.BindSource != "" {
		ip := net.ParseIP(cfg.BindSource)
		// Config-time validation already ensured IPv6 when SSM; this is
		// belt-and-suspenders for ASM operators who supply a bindSource
		// without enabling SSM.
		if ip == nil || ip.To4() != nil {
			slog.Error("invalid bind-source (must be IPv6)", "value", cfg.BindSource)
			os.Exit(1)
		}
		fwd.SetBindSource(ip)
		slog.Info("multicast egress source bound", "bind_source", ip.String(), "source_mode", cfg.SourceMode)
	}
	fwd.SetStampSource(cfg.StampSource)
	if !cfg.StampSource {
		slog.Warn("authoritative source stamping DISABLED (-stamp-source=false): HashKey trusts the sender; use only behind a source-rewriting load balancer")
	}

	// Optional ingress TxID dedup. Two-tier: tier-1 local LRU (hot path) →
	// tier-2 modular cache backend SETNX (redis/aerospike/memory/none).
	// -ingress-dedup=false or TxidDedupLocalCap=0 disables the feature.
	var txStore *txidset.Store
	if cfg.TxidDedupEnabled && cfg.TxidDedupLocalCap > 0 {
		backend, berr := cache.Open(context.Background(), cache.Config{
			Backend:       cfg.TxidDedupBackend,
			RedisAddr:     cfg.TxidDedupRedisAddr,
			AeroHosts:     cfg.TxidDedupAeroHosts,
			AeroNamespace: cfg.TxidDedupAeroNamespace,
			AeroSet:       cfg.TxidDedupAeroSet,
		})
		if berr != nil {
			slog.Error("txid dedup backend init failed", "backend", cfg.TxidDedupBackend, "err", berr)
			os.Exit(1)
		}
		txStore, err = txidset.New(txidset.Config{
			Backend:       backend,
			TTL:           cfg.TxidDedupTTL,
			LocalCapacity: cfg.TxidDedupLocalCap,
			Recorder:      txidsetRecorder{rec: rec},
		})
		if err != nil {
			slog.Error("txid dedup init failed", "err", err)
			os.Exit(1)
		}
		defer func() {
			_ = txStore.Close()
			if backend != nil {
				_ = backend.Close()
			}
		}()
		fwd.SetTxidDedup(txStore, cfg.TxidDedupPrefix)
		slog.Info("ingress TxID dedup enabled",
			"backend", cfg.TxidDedupBackend,
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
	go rec.Serve(cfg.MetricsAddr, cfg.PprofEnabled, done)

	// User/consumer ingress is transaction-only. Privileged block/coinbase/
	// subtree-data frames are dropped here; the miner multicast port is
	// deprecated (2026-07-07). Blocks/subtrees enter only as BRC-144/BRC-143
	// push frames on the proxy's tunnel-bound push ports, reframed to the
	// fabric — never accepted as multicast frames from a submitter.
	userClass := forwarder.IngressTransaction
	for i := range cfg.NumWorkers {
		w := worker.New(i, fwd, ifaces, rec)
		w.SetRecvBatch(cfg.RecvBatch)
		w.SetRecvBufBytes(cfg.RecvBufBytes)
		w.SetIngressClass(userClass)
		w.SetRetryTee(cfg.RetryTee)
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
		tcpIngress.SetRetryTee(cfg.RetryTee)
		tcpIngress.SetIngressClass(userClass)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := tcpIngress.Run(cfg.ListenAddr, cfg.TCPListenPort, done); err != nil {
				slog.Error("TCP ingress exited with error", "err", err)
			}
		}()
	}

	// Push-frame ingest lanes (replaces the deprecated miner multicast port).
	// Each is a dedicated single-class TCP port carrying header-stripped push
	// objects, reframed to the fabric: subtree → BRC-132, block → BRC-131
	// carrying the BRC-144 body verbatim. Privileged — bind tunnel-side.
	if cfg.SubtreeListenPort > 0 {
		oi := worker.NewObjectIngress(fwd, ifaces, rec, objfmt.ClassSubtree)
		oi.SetRetryTee(cfg.RetryTee)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := oi.Run(cfg.ListenAddr, cfg.SubtreeListenPort, done); err != nil {
				slog.Error("subtree ingress exited with error", "err", err)
			}
		}()
		slog.Info("subtree push ingress enabled (BRC-143 → BRC-132)", "listen_port", cfg.SubtreeListenPort)
	}
	if cfg.BlockListenPort > 0 {
		oi := worker.NewObjectIngress(fwd, ifaces, rec, objfmt.ClassBlock)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := oi.Run(cfg.ListenAddr, cfg.BlockListenPort, done); err != nil {
				slog.Error("block ingress exited with error", "err", err)
			}
		}()
		slog.Info("block push ingress enabled (BRC-144 → BRC-131 body-verbatim)", "listen_port", cfg.BlockListenPort)
	}
	// Optional dedicated BRC-148 BEEF lane (flow separation / LB only —
	// records also ride the tx port as an open class).
	if cfg.BEEFListenPort > 0 {
		oi := worker.NewObjectIngress(fwd, ifaces, rec, objfmt.ClassBEEF)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := oi.Run(cfg.ListenAddr, cfg.BEEFListenPort, done); err != nil {
				slog.Error("beef ingress exited with error", "err", err)
			}
		}()
		slog.Info("beef lane enabled (BRC-148 records → FrameVer 0x09)", "listen_port", cfg.BEEFListenPort)
	}

	// ── BRC-139 manifest consumer (auto-shard-config) ────────────────────
	// Optional, off by default. When enabled, the proxy opens a beacon
	// socket and runs the manifest evaluator. A ShardBits/SourceMode
	// adoption triggers a restart by writing into restartSig, which the
	// signal-handler block below treats as an early SIGTERM.
	var restart proxymanifest.RestartRequest
	restartSig := make(chan struct{}, 1)
	if cfg.AutoConfigEnabled {
		// BRC-129 derives the control-plane group's prefix from the source
		// mode, so under SSM the manifest announcer publishes to FF3x. The
		// default compat value joins the legacy address too, which is what
		// lets the announcer move without stranding this proxy.
		beaconPrefixes, err := cfg.ManifestBeaconGroupPrefixes()
		if err != nil {
			slog.Error("manifest beacon group derivation failed",
				"scope", cfg.AutoConfigBeaconScope, "err", err)
			os.Exit(1)
		}
		beaconGroups := make([]*net.UDPAddr, 0, len(beaconPrefixes))
		for _, p := range beaconPrefixes {
			ip := shard.GroupAddr(p, cfg.MCGroupID, shard.GroupBeacon)
			beaconGroups = append(beaconGroups, &net.UDPAddr{IP: ip, Port: cfg.AutoConfigBeaconPort})
		}
		beaconGrp := beaconGroups[0]
		reg := commanifest.NewRegistry(0)

		// Resolve the first egress interface for the manifest socket;
		// any iface that can receive on the multicast group works.
		var mfIface *net.Interface
		if len(ifaces) > 0 {
			mfIface = ifaces[0]
		}
		ml := &proxymanifest.Listener{
			Group:    beaconGrp,
			Groups:   beaconGroups,
			Iface:    mfIface,
			Registry: reg,
			Rec:      rec,
			Debug:    cfg.Debug,
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := ml.Start(ctxForManifest(done)); err != nil {
				slog.Error("manifest listener error", "err", err)
			}
		}()

		ev := commanifest.NewEvaluator(commanifest.EvaluatorConfig{
			Quorum:     cfg.AutoConfigPilotQuorum,
			Hysteresis: cfg.AutoConfigHysteresis,
			Pin: commanifest.Pin{
				ShardBits:       uint8(cfg.ShardBits),
				HasShardBitsPin: true,
				// The BEEF plane's width is CLI-pinned too (manual wins),
				// so per-domain adoption is divergence-observability; drop
				// the pin to let quorum drive it.
				DomainShardBits: map[uint8]uint8{shard.DomainBEEF: uint8(cfg.BEEFShardBits)},
			},
		})
		hooks := proxymanifest.Hooks{
			OnShardBitsChange: func(prev, next uint8) {
				if cfg.AutoConfigLiveResharding {
					// Bridging mode: the OnSuccessorChange hook is the
					// authoritative driver for re-shard. Restart-on-
					// ShardBits is suppressed because the bridging path
					// keeps the proxy live; the actual swap to the new
					// active engine happens when the Successor cutover
					// triggers a process restart at TransitionEpoch
					// (achieved by re-requesting restart inside the
					// Successor handler when the window expires).
					slog.Info("auto-config noted ShardBits change (live-resharding mode; restart deferred to TransitionEpoch)",
						"prev", prev, "next", next)
					return
				}
				slog.Warn("auto-config adopted new ShardBits (restart mode)",
					"prev", prev, "next", next)
				restart.Request("shard_bits change")
				select {
				case restartSig <- struct{}{}:
				default:
				}
			},
			OnSourceModeChange: func(prevSSM, nextSSM bool) {
				if cfg.AutoConfigLiveResharding {
					slog.Info("auto-config noted SourceMode change (live-resharding mode; restart deferred to TransitionEpoch)",
						"prev_ssm", prevSSM, "next_ssm", nextSSM)
					return
				}
				slog.Warn("auto-config adopted new SourceMode (restart mode)",
					"prev_ssm", prevSSM, "next_ssm", nextSSM)
				restart.Request("source_mode change")
				select {
				case restartSig <- struct{}{}:
				default:
				}
			},
			OnDomainShardBitsChange: func(domain, prev, next uint8) {
				// v1: BEEF plane resharding is restart-on-adopt (no
				// bridging). With the CLI pin above this fires only if the
				// pin is reconfigured; without it, on a quorum shift.
				slog.Warn("auto-config adopted new object-plane ShardBits (restart mode)",
					"domain", domain, "prev", prev, "next", next)
				restart.Request("domain_shard_bits change")
				select {
				case restartSig <- struct{}{}:
				default:
				}
			},
		}
		if cfg.AutoConfigLiveResharding {
			hooks.OnSuccessorChange = func(before, after *commanifest.SuccessorView) {
				if after == nil {
					// Successor cleared (cutover or quorum loss).
					// Clearing here without a restart leaves the proxy
					// still emitting under the prior active engine; the
					// next ShardBitsChange (when the pilot rolls
					// GenerationID forward) is what triggers the actual
					// swap. Operators driving live re-shards typically
					// also roll GenerationID at TransitionEpoch.
					fwd.SetBridging(nil)
					slog.Info("live-resharding: bridging cleared")
					return
				}
				// Enter / refresh bridging: install a secondary engine
				// derived from the successor's parameters. The shard
				// engine uses the same MCPrefix unless successor declares
				// SSM, in which case we flip to the FF3x prefix (via
				// shard.Prefix). For brevity we use the SAME prefix as
				// the active engine; ASM↔SSM transitions in bridging
				// require a follow-up engine constructor that takes the
				// SourceMode/Scope tuple per the SSM plan.
				secondary := shard.New(cfg.MCPrefix, cfg.MCGroupID, uint(after.ShardBits))
				fwd.SetBridging(&forwarder.BridgingEngine{
					Secondary:       secondary,
					TransitionEpoch: int64(after.TransitionEpoch),
				})
				slog.Info("live-resharding: bridging engine installed",
					"successor_shard_bits", after.ShardBits,
					"transition_epoch", after.TransitionEpoch)
			}
		}
		applier := &proxymanifest.Applier{
			Registry:  reg,
			Evaluator: ev,
			Rec:       rec,
			Hooks:     hooks,
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			applier.Run(ctxForManifest(done))
		}()
		beaconIPs := make([]string, 0, len(beaconGroups))
		for _, g := range beaconGroups {
			beaconIPs = append(beaconIPs, g.IP.String())
		}
		slog.Info("manifest consumer enabled",
			"beacon", beaconIPs,
			"compat", cfg.ControlGroupCompat,
			"port", cfg.AutoConfigBeaconPort,
			"bootstrap", cfg.AutoConfigBootstrap,
			"quorum", cfg.AutoConfigPilotQuorum)
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

	var received os.Signal
	select {
	case received = <-sig:
	case <-restartSig:
		slog.Warn("auto-config restart triggered, beginning drain",
			"reason", restart.Reason())
		received = syscall.SIGTERM
	}

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

// ctxForManifest adapts the worker-style `done` channel (closed on
// shutdown) into a context.Context for the manifest subsystem. The
// returned context is cancelled when done is closed.
func ctxForManifest(done <-chan struct{}) context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-done
		cancel()
	}()
	return ctx
}
