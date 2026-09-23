// Package metrics initialises an OpenTelemetry MeterProvider backed by both
// a Prometheus exporter (for scraping) and an optional OTLP gRPC exporter
// (for push-based delivery to any OTel-compatible backend).
//
// # Instance identity
//
// The MeterProvider is constructed with an OTel Resource that carries
// service.name, service.instance.id, and service.version as resource
// attributes. These appear in OTLP payloads and as Prometheus target labels,
// allowing federation across horizontally-scaled instances without metric-label
// collisions.
//
// # Hot-path design
//
// All OTel instrument handles (Int64Counter, Int64Histogram, etc.) are
// allocated once at [New] time and stored on [Recorder]. Record methods
// use them directly — no map lookups on the critical path.
//
// Per-(interface, worker) and per-(interface, group) Prometheus counter
// handles are integer-indexed under [ifaceState] (slice by workerID / group),
// behind an atomic copy-on-write per-interface map. After warmup the hot path
// is a lock-free atomic load + short-string map lookup + slice index, with no
// per-packet sync.Map hashing or active-group mutex.
//
// # Health endpoints
//
// [Recorder.Serve] registers /metrics, /healthz, and /readyz on a single
// net/http.ServeMux. Readiness is tracked via an atomic counter incremented
// by [Recorder.WorkerReady] and decremented by [Recorder.WorkerDone],
// enabling load-balancer drain during graceful shutdown.
package metrics

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof" // mounts /debug/pprof/* on http.DefaultServeMux; gated by Serve(pprof=true)
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	prometheusexporter "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/exemplar"
	"go.opentelemetry.io/otel/sdk/resource"

	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/lightwebinc/shard-common/logging"
)

// ServiceName is the OTel service.name resource attribute value.
const ServiceName = "shard-proxy"

// Version is set at build time via -ldflags "-X metrics.Version=<ver>".
// Defaults to "dev" when not injected.
var Version = "dev"

// ifaceState holds the pre-bound hot-path counter handles for one egress
// interface, integer-indexed for O(1) lock-free reads after warmup. It
// replaces the per-packet interface-keyed sync.Map lookups (profiled at a
// large share of CPU on the zero-copy hot path): the hot path now costs one
// atomic load + a short-string map lookup + slice indexing.
type ifaceState struct {
	iface      string
	numWorkers int
	workers    []*hotPathCounters        // indexed by workerID; bound on first use
	flows      atomic.Pointer[flowTable] // COW, indexed by groupIdx
	mu         sync.Mutex                // guards worker binding + flow COW grow
}

// flowTable is an immutable (copy-on-write) per-group table indexed by group
// index. Stored behind ifaceState.flows so reads are lock-free. Each group's
// counters are sharded per worker so the hot-path increment is single-writer
// (uncontended); the flowCollector sums across workers at scrape time, keeping
// the exposed bsp_flow_*{iface,group} schema unchanged.
type flowTable struct {
	groups []*groupShard // indexed by groupIdx; nil = group never seen
}

// groupShard holds one shard counter cell per worker for a single group.
type groupShard struct {
	perWorker []flowCell // indexed by workerID; only that worker writes its cell
}

// flowCell is one (group, worker) packet/byte counter pair. atomic so the
// scrape goroutine can Load while the owning worker Adds; single-writer means
// no cross-core contention on the Add.
type flowCell struct {
	packets atomic.Uint64
	bytes   atomic.Uint64
}

// hotPathCounters bundles the pre-bound prometheus.Counter handles for a
// single (iface, worker) tuple. One lookup at startup of a flow gets all
// six in O(1).
type hotPathCounters struct {
	rxPackets promclient.Counter
	rxBytes   promclient.Counter
	txPackets promclient.Counter
	txBytes   promclient.Counter
	rxSize    promclient.Observer // histogram observer
}

// Recorder holds all pre-allocated OTel instrument handles and readiness
// state. Construct with [New]; pass the pointer to every worker.
type Recorder struct {
	provider    *sdkmetric.MeterProvider
	promReg     promclient.Gatherer
	promOtelReg *promclient.Registry
	runtimeReg  *promclient.Registry
	levelVar    *slog.LevelVar
	numWorkers  int
	startTime   time.Time
	readyCount  atomic.Int32

	// ── Hot-path metrics — direct prometheus client_golang ──
	// OTel SDK Add() was profiled at ~⅓ of total CPU on the 256 B Bitcoin
	// hot path. Direct Prometheus skips attribute hashing, exemplar
	// filtering, aggregation pipeline, allocator churn. shard-proxy never
	// used OTel beyond Prometheus scraping, so the SDK was pure overhead.

	// Per-packet ingress — labels: worker, iface (worker, network_interface_name).
	promRxPackets *promclient.CounterVec
	promRxBytes   *promclient.CounterVec
	promRxDrops   *promclient.CounterVec // also labelled by reason
	promRxSize    *promclient.HistogramVec

	// Per-packet egress — labels: worker, iface.
	promTxPackets     *promclient.CounterVec
	promTxBytes       *promclient.CounterVec
	promTxEgressErrs  *promclient.CounterVec
	promTxIngressErrs *promclient.CounterVec

	// Accepted-ingress volume by frame class and admission tier — labels:
	// class, tier. Tracked for operator accounting so downstream tooling has
	// data without re-instrumenting; miners emit block/coinbase/subtree, tx
	// senders emit tx. Handles are pre-resolved (small fixed label set) so the
	// admission hot path does one array index, no per-frame label lookup.
	promIngressBytes   *promclient.CounterVec
	promIngressPackets *promclient.CounterVec
	ingressBytesH      [ingressClassCount][2]promclient.Counter
	ingressPacketsH    [ingressClassCount][2]promclient.Counter

	// Per-flow / per-group — exposed as labels: iface, group. Backed by a
	// per-worker-sharded counter store summed at scrape by flowCollector, so the
	// hot-path increment is single-writer (no cross-core contention).
	flowPacketsDesc *promclient.Desc
	flowBytesDesc   *promclient.Desc

	// Per-interface integer-indexed hot-path cache. Reads are lock-free (one
	// atomic load + a short-string map lookup + slice index); first sight of an
	// (iface)/(iface,worker)/(iface,group) binds under a lock. Replaces the two
	// per-packet interface-keyed sync.Maps and the per-packet active-group mutex.
	ifaceStates atomic.Pointer[map[string]*ifaceState]
	ifaceMu     sync.Mutex // guards ifaceStates copy-on-write

	// Fragmentation counters (BRC-130)
	framesFragmented metric.Int64Counter
	fragmentsEmitted metric.Int64Counter

	// Coalescing counters (BRC-142) — cold path: fire once per flushed bundle.
	coalesceBundles      metric.Int64Counter
	coalesceMembers      metric.Int64Counter
	coalesceFlush        metric.Int64Counter
	coalesceMembersHisto metric.Int64Histogram

	// Loopback tee to the co-located retry cache / listener.
	teeFailed metric.Int64Counter

	// Control-plane forwarding (TCP ingress + BRC-127)
	ctrlFramesForwarded metric.Int64Counter
	tcpConnections      metric.Int64Counter
	tcpBytesReceived    metric.Int64Counter

	// Ingress TxID dedup — TxidClaim* are per-packet on the hot path
	// when dedup is on, so they are direct prometheus.Counter handles
	// keyed by prefix (one allocation per distinct prefix at startup).
	ingressDeduped        metric.Int64Counter // by worker, iface, frame_type (cold)
	privilegedRejected    metric.Int64Counter // by frame_type (cold; miner-tier gate)
	beefSubmissions       metric.Int64Counter // by result (cold; BRC-148 submission admission)
	beefTopics            metric.Int64Counter // topics named on admitted records, deliverable|label
	blockPoWRejected      metric.Int64Counter // block announces failing the PoW gate (cold)
	subtreeRootChecks     metric.Int64Counter // by result (cold; -verify-subtree-root)
	promTxidClaimLocalHit *promclient.CounterVec
	promTxidClaimWon      *promclient.CounterVec
	promTxidClaimLost     *promclient.CounterVec
	promTxidClaimError    *promclient.CounterVec

	// draining is set to true when a shutdown signal has been received and the
	// proxy is waiting for the load-balancer to stop routing new connections.
	// While true, /readyz returns 503 regardless of worker count.
	draining atomic.Bool

	// tcpIngressRequired is set true when TCP_LISTEN_PORT > 0; /readyz then
	// also gates on tcpIngressReady, which TCPIngress.Run flips after a
	// successful net.Listen. Prevents senders from racing the listener bind.
	tcpIngressRequired atomic.Bool
	tcpIngressReady    atomic.Bool

	// BRC-139 manifest consumer metrics. Updated by the proxy's
	// auto-config subsystem (shard-proxy/manifest). Names mirror the
	// listener's multicast_manifest_* family so one dashboard covers both
	// halves of the fleet.
	manifestReceived      atomic.Int64
	manifestPilotsKnown   atomic.Int64
	manifestQuorumMetBits atomic.Int32
	manifestReshardState  atomic.Int32
	manifestReshardWindow atomic.Int64
	manifestDivergence    metric.Int64Counter
	manifestAdoption      metric.Int64Counter
	manifestReshardEmit   metric.Int64Counter
	// Unix seconds of the most recent divergence per field. Read by an
	// observable gauge, so a stale timestamp is the signal — a field that
	// stops diverging keeps its last value rather than disappearing.
	lastDivergenceMu sync.Mutex
	lastDivergence   map[string]int64

	// Composed shutdown function (OTLP exporter + MeterProvider)
	shutdownFn func(context.Context) error
}

// New constructs and registers a Recorder.
//
//   - instanceID identifies this process in federated/horizontally-scaled
//     deployments (e.g. hostname or pod name). Falls back to os.Hostname().
//   - numWorkers is the total configured worker count, used by /readyz.
//   - otlpEndpoint is the gRPC endpoint for OTLP push (empty = disabled).
//   - otlpInterval is the OTLP push cadence.
func New(instanceID string, numWorkers int, otlpEndpoint string, otlpInterval time.Duration) (*Recorder, error) {
	if instanceID == "" {
		h, err := os.Hostname()
		if err != nil {
			h = "unknown"
		}
		instanceID = h
	}

	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			attribute.String("service.name", ServiceName),
			attribute.String("service.instance.id", instanceID),
			attribute.String("service.version", Version),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("metrics: build resource: %w", err)
	}

	// Dedicated Prometheus registry — avoids polluting prometheus.DefaultRegisterer.
	reg := promclient.NewRegistry()
	promExp, err := prometheusexporter.New(
		prometheusexporter.WithRegisterer(reg),
	)

	// Separate registry for Go runtime and process metrics.
	runtimeReg := promclient.NewRegistry()
	runtimeReg.MustRegister(collectors.NewGoCollector())
	runtimeReg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	if err != nil {
		return nil, fmt.Errorf("metrics: prometheus exporter: %w", err)
	}

	mpOpts := []sdkmetric.Option{
		sdkmetric.WithReader(promExp),
		sdkmetric.WithResource(res),
		// Exemplars require a trace context per measurement and add ~11%
		// of cumulative CPU on the hot path (aggregate.Builder.filter).
		// shard-proxy doesn't emit traces, so they are never useful.
		sdkmetric.WithExemplarFilter(exemplar.AlwaysOffFilter),
	}

	var shutdownFuncs []func(context.Context) error

	if otlpEndpoint != "" {
		otlpExp, oerr := otlpmetricgrpc.New(
			context.Background(),
			otlpmetricgrpc.WithEndpoint(otlpEndpoint),
			otlpmetricgrpc.WithInsecure(),
		)
		if oerr != nil {
			return nil, fmt.Errorf("metrics: OTLP exporter: %w", oerr)
		}
		mpOpts = append(mpOpts, sdkmetric.WithReader(
			sdkmetric.NewPeriodicReader(otlpExp,
				sdkmetric.WithInterval(otlpInterval),
			),
		))
		shutdownFuncs = append(shutdownFuncs, otlpExp.Shutdown)
		slog.Info("OTLP exporter enabled", "endpoint", otlpEndpoint, "interval", otlpInterval)
	}

	mp := sdkmetric.NewMeterProvider(mpOpts...)
	shutdownFuncs = append(shutdownFuncs, mp.Shutdown)

	r := &Recorder{
		provider:       mp,
		promReg:        promclient.Gatherers{reg, runtimeReg},
		promOtelReg:    reg,
		runtimeReg:     runtimeReg,
		numWorkers:     numWorkers,
		startTime:      time.Now(),
		lastDivergence: make(map[string]int64),
		shutdownFn: func(ctx context.Context) error {
			var last error
			for _, fn := range shutdownFuncs {
				if err := fn(ctx); err != nil {
					last = err
				}
			}
			return last
		},
	}

	meter := mp.Meter(ServiceName)

	// Hot-path metrics: direct prometheus client_golang. Same names and
	// label sets as the previous OTel int64Counter exposure so dashboards
	// keep working.
	r.promRxPackets = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsp_packets_received_total",
		Help: "Datagrams received",
	}, []string{"worker", "network_interface_name"})
	r.promRxBytes = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsp_bytes_received_total",
		Help: "Raw bytes received",
	}, []string{"worker", "network_interface_name"})
	r.promRxDrops = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsp_packets_dropped_total",
		Help: "Datagrams dropped",
	}, []string{"worker", "network_interface_name", "reason"})
	r.promRxSize = promclient.NewHistogramVec(promclient.HistogramOpts{
		Name:    "bsp_packet_size_bytes",
		Help:    "Datagram size distribution",
		Buckets: promclient.ExponentialBuckets(64, 2, 12), // 64 B .. 256 KiB
	}, []string{"worker", "network_interface_name"})
	r.promTxPackets = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsp_packets_forwarded_total",
		Help: "Datagrams successfully forwarded",
	}, []string{"worker", "network_interface_name"})
	r.promTxBytes = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsp_bytes_forwarded_total",
		Help: "Raw bytes forwarded",
	}, []string{"worker", "network_interface_name"})
	r.promTxEgressErrs = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsp_egress_errors_total",
		Help: "WriteTo errors on egress socket",
	}, []string{"worker", "network_interface_name"})
	r.promTxIngressErrs = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsp_ingress_errors_total",
		Help: "ReadFrom non-fatal errors on ingress socket",
	}, []string{"worker", "network_interface_name"})
	r.promIngressBytes = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsp_ingress_class_bytes_total",
		Help: "Accepted ingress bytes by frame class and admission tier (operator accounting)",
	}, []string{"class", "tier"})
	r.promIngressPackets = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsp_ingress_class_packets_total",
		Help: "Accepted ingress frames by frame class and admission tier (operator accounting)",
	}, []string{"class", "tier"})
	for c := IngressFrameClass(0); c < ingressClassCount; c++ {
		for ti, tier := range []string{"privileged", "transaction"} {
			r.ingressBytesH[c][ti] = r.promIngressBytes.WithLabelValues(c.String(), tier)
			r.ingressPacketsH[c][ti] = r.promIngressPackets.WithLabelValues(c.String(), tier)
		}
	}
	r.flowPacketsDesc = promclient.NewDesc("bsp_flow_packets_total",
		"Packets per shard group per interface (active groups only)",
		[]string{"network_interface_name", "group"}, nil)
	r.flowBytesDesc = promclient.NewDesc("bsp_flow_bytes_total",
		"Bytes per shard group per interface (active groups only)",
		[]string{"network_interface_name", "group"}, nil)
	for _, c := range []promclient.Collector{
		r.promRxPackets, r.promRxBytes, r.promRxDrops, r.promRxSize,
		r.promTxPackets, r.promTxBytes,
		r.promTxEgressErrs, r.promTxIngressErrs,
		r.promIngressBytes, r.promIngressPackets,
		&flowCollector{r: r},
	} {
		if err := reg.Register(c); err != nil {
			return nil, fmt.Errorf("metrics: register hot-path counter: %w", err)
		}
	}

	// Observable gauge: distinct active group count per interface.
	if _, err = meter.Int64ObservableGauge("bsp_active_groups",
		metric.WithDescription("Distinct shard groups seen since startup, per interface"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			if m := r.ifaceStates.Load(); m != nil {
				for iface, st := range *m {
					n := 0
					for _, gs := range st.flows.Load().groups {
						if gs != nil {
							n++
						}
					}
					o.Observe(int64(n),
						metric.WithAttributes(attribute.String("network.interface.name", iface)),
					)
				}
			}
			return nil
		}),
	); err != nil {
		return nil, err
	}

	// Observable gauge: running worker count.
	if _, err = meter.Int64ObservableGauge("bsp_workers_active",
		metric.WithDescription("Number of running worker goroutines"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(r.readyCount.Load()))
			return nil
		}),
	); err != nil {
		return nil, err
	}

	// Observable gauge: process uptime in seconds.
	if _, err = meter.Float64ObservableGauge("bsp_uptime_seconds",
		metric.WithDescription("Seconds since process start"),
		metric.WithUnit("s"),
		metric.WithFloat64Callback(func(_ context.Context, o metric.Float64Observer) error {
			o.Observe(time.Since(r.startTime).Seconds())
			return nil
		}),
	); err != nil {
		return nil, err
	}

	// BRC-139 manifest consumer family. Registered here (not just tracked in
	// atomics) so proxy-side divergence is observable — without it, half the
	// fleet reports nothing and a cross-peer disagreement looks like silence.
	if r.manifestDivergence, err = meter.Int64Counter("multicast_manifest_divergence_total",
		metric.WithDescription("Authoritative-peer disagreements observed by the auto-config evaluator (BRC-139 §Divergence telemetry)")); err != nil {
		return nil, err
	}
	if r.manifestAdoption, err = meter.Int64Counter("multicast_manifest_adoption_total",
		metric.WithDescription("Times the auto-config evaluator newly adopted a value (bootstrap, quorum-shift, pin-removed)")); err != nil {
		return nil, err
	}
	if r.manifestReshardEmit, err = meter.Int64Counter("multicast_manifest_resharding_emit_duplicates_total",
		metric.WithDescription("Frames dual-emitted to both the current and successor layouts during a bridging window")); err != nil {
		return nil, err
	}
	if _, err = meter.Int64ObservableGauge("multicast_manifest_received_total",
		metric.WithDescription("BRC-139 ShardManifest datagrams accepted and upserted into the manifest registry"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(r.manifestReceived.Load())
			return nil
		}),
	); err != nil {
		return nil, err
	}
	if _, err = meter.Int64ObservableGauge("multicast_manifest_pilots_known",
		metric.WithDescription("Distinct authoritative announcers currently within TTL"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(r.manifestPilotsKnown.Load())
			return nil
		}),
	); err != nil {
		return nil, err
	}
	if _, err = meter.Int64ObservableGauge("multicast_manifest_quorum_met_bits",
		metric.WithDescription("Bit0=shard_bits, bit1=source_mode, bit2=successor quorum-met flags"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(r.manifestQuorumMetBits.Load()))
			return nil
		}),
	); err != nil {
		return nil, err
	}
	if _, err = meter.Int64ObservableGauge("multicast_manifest_last_divergence_epoch",
		metric.WithDescription("Unix seconds of the most recent divergence observed per field; absent until a field first diverges"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			r.lastDivergenceMu.Lock()
			defer r.lastDivergenceMu.Unlock()
			for field, ts := range r.lastDivergence {
				o.Observe(ts, metric.WithAttributes(attribute.String("field", field)))
			}
			return nil
		}),
	); err != nil {
		return nil, err
	}
	if _, err = meter.Int64ObservableGauge("multicast_manifest_resharding_state",
		metric.WithDescription("0 steady, 1 bridging, 2 cutover-pending"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(r.manifestReshardState.Load()))
			return nil
		}),
	); err != nil {
		return nil, err
	}
	if _, err = meter.Int64ObservableGauge("multicast_manifest_resharding_window_seconds",
		metric.WithDescription("Seconds until TransitionEpoch (negative briefly during cutover)"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(r.manifestReshardWindow.Load())
			return nil
		}),
	); err != nil {
		return nil, err
	}

	if r.framesFragmented, err = meter.Int64Counter("bsp_frames_fragmented_total",
		metric.WithDescription("Frames that exceeded the fragmentation threshold and were split into BRC-130 fragments")); err != nil {
		return nil, err
	}
	if r.fragmentsEmitted, err = meter.Int64Counter("bsp_fragments_emitted_total",
		metric.WithDescription("Total BRC-130 fragment datagrams sent (K fragments per fragmented frame)")); err != nil {
		return nil, err
	}

	if r.coalesceBundles, err = meter.Int64Counter("bsp_coalesce_bundles_total",
		metric.WithDescription("BRC-142 coalesced bundle datagrams flushed to egress")); err != nil {
		return nil, err
	}
	if r.coalesceMembers, err = meter.Int64Counter("bsp_coalesce_members_total",
		metric.WithDescription("Total member transactions packed into BRC-142 bundles")); err != nil {
		return nil, err
	}
	if r.coalesceFlush, err = meter.Int64Counter("bsp_coalesce_flush_total",
		metric.WithDescription("BRC-142 coalesce flushes by reason (batch, relay, encode_error)")); err != nil {
		return nil, err
	}
	if r.coalesceMembersHisto, err = meter.Int64Histogram("bsp_coalesce_members_per_bundle",
		metric.WithDescription("Distribution of member transactions per BRC-142 bundle"),
		metric.WithExplicitBucketBoundaries(1, 2, 4, 8, 16, 32, 64, 128, 256)); err != nil {
		return nil, err
	}

	if r.teeFailed, err = meter.Int64Counter("bsp_tee_failed_total",
		metric.WithDescription("Datagrams the loopback tee could not deliver, by kind (retry cache / local listener mirror). "+
			"Non-zero means the co-located cache will MISS for those frames and listeners will escalate a tier.")); err != nil {
		return nil, err
	}
	if r.ctrlFramesForwarded, err = meter.Int64Counter("bsp_control_frames_forwarded_total",
		metric.WithDescription("BRC-127 control datagrams forwarded to multicast (e.g. SubtreeGroupAnnounce)")); err != nil {
		return nil, err
	}
	if r.tcpConnections, err = meter.Int64Counter("bsp_tcp_connections_total",
		metric.WithDescription("TCP connections accepted on the control-plane ingress port")); err != nil {
		return nil, err
	}
	if r.tcpBytesReceived, err = meter.Int64Counter("bsp_tcp_bytes_received_total",
		metric.WithDescription("Bytes read from TCP ingress connections"),
		metric.WithUnit("By")); err != nil {
		return nil, err
	}

	if r.ingressDeduped, err = meter.Int64Counter("bsp_ingress_deduped_total",
		metric.WithDescription("Frames suppressed by the ingress TxID dedup gate (sibling proxy or listener already claimed the TxID)")); err != nil {
		return nil, err
	}
	if r.privilegedRejected, err = meter.Int64Counter("bsp_privileged_frame_rejected_total",
		metric.WithDescription("Privileged control-plane frames (block announce, coinbase, subtree data) dropped on a transaction-only ingress socket (miner-tier gate)")); err != nil {
		return nil, err
	}
	if r.blockPoWRejected, err = meter.Int64Counter("bsp_block_pow_rejected_total",
		metric.WithDescription("BRC-131 block announces dropped by the proof-of-work gate (invalid header PoW or below the difficulty floor)")); err != nil {
		return nil, err
	}
	if r.subtreeRootChecks, err = meter.Int64Counter("bsp_subtree_root_checks_total",
		metric.WithDescription("BRC-132 subtree data frames checked by -verify-subtree-root, by result (ok, mismatch, malformed); mismatch and malformed are dropped")); err != nil {
		return nil, err
	}
	if r.beefSubmissions, err = meter.Int64Counter("bsp_beef_submissions_total",
		metric.WithDescription("BRC-148 BEEF submission records by admission result (ok, malformed, oversize, bad_marker, disabled)")); err != nil {
		return nil, err
	}
	if r.beefTopics, err = meter.Int64Counter("bsp_beef_topics_total",
		metric.WithDescription("Topic names on admitted BRC-149 submission records, by topic_role: deliverable (matched at the edge; 1 on the open path, up to the operator's cap on the authenticated path) or label (carried to the subscriber, never matched, never billed)")); err != nil {
		return nil, err
	}
	// TxidClaim* are per-packet when ingress dedup is on; direct prometheus
	// counters to skip the ~10% CPU spent in OTel int64Counter.Add.
	r.promTxidClaimLocalHit = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsp_txid_claim_local_hit_total",
		Help: "Tier-1 local-LRU short-circuits during TxID claim (no Redis call)",
	}, []string{"prefix"})
	r.promTxidClaimWon = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsp_txid_claim_won_total",
		Help: "Tier-2 Redis SETNX wins during TxID claim (frame proceeds to multicast)",
	}, []string{"prefix"})
	r.promTxidClaimLost = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsp_txid_claim_lost_total",
		Help: "Tier-2 Redis SETNX losses during TxID claim (sibling already claimed; frame dropped)",
	}, []string{"prefix"})
	r.promTxidClaimError = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsp_txid_claim_errors_total",
		Help: "Redis errors during TxID claim (fail-open: frame was forwarded)",
	}, []string{"prefix"})
	for _, c := range []promclient.Collector{
		r.promTxidClaimLocalHit, r.promTxidClaimWon,
		r.promTxidClaimLost, r.promTxidClaimError,
	} {
		if regErr := reg.Register(c); regErr != nil {
			return nil, fmt.Errorf("metrics: register txid-claim counter: %w", regErr)
		}
	}

	return r, nil
}

// ── Record methods (hot path) ────────────────────────────────────────────────

// PacketReceived records receipt of a raw datagram on the ingress socket.
// Gatherer exposes the recorder's Prometheus registry so an embedding binary
// (a downstream embedding binary) can merge the bsp_ family into its own /metrics endpoint
// instead of running Serve on a second port.
func (r *Recorder) Gatherer() promclient.Gatherer { return r.promReg }

func (r *Recorder) PacketReceived(iface string, workerID int, size int) {
	c := r.ifaceState(iface).worker(r, workerID)
	c.rxPackets.Inc()
	c.rxBytes.Add(float64(size))
	c.rxSize.Observe(float64(size))
}

// PacketDropped records a dropped datagram. reason is the free-form label
// written to bsp_packets_dropped_total{reason}; the live set is whatever the
// forwarder and worker packages pass (decode_error, write_error, ingress_not_ef,
// bare_tx_malformed, payload_hash_mismatch, bundle_short, …). Keep it a small
// bounded vocabulary — it is a metric label.
func (r *Recorder) PacketDropped(iface string, workerID int, reason string) {
	r.promRxDrops.WithLabelValues(strconv.Itoa(workerID), iface, reason).Inc()
}

// PacketForwarded records a successfully forwarded datagram.
func (r *Recorder) PacketForwarded(iface string, workerID int, groupIdx uint32, size int) {
	st := r.ifaceState(iface)
	c := st.worker(r, workerID)
	c.txPackets.Inc()
	c.txBytes.Add(float64(size))

	st.addFlow(workerID, groupIdx, uint64(size))
}

// FrameFragmented records one frame that exceeded the fragmentation threshold.
// k is the number of fragments it was split into. Not a per-packet hot path
// (only fires for > MTU frames) so still uses OTel.
func (r *Recorder) FrameFragmented(workerID int, k int) {
	opt := metric.WithAttributes(
		attribute.Int("worker", workerID),
		ifaceAttr(""),
	)
	ctx := context.Background()
	r.framesFragmented.Add(ctx, 1, opt)
	r.fragmentsEmitted.Add(ctx, int64(k), opt)
}

// CoalesceFlushed records one flushed BRC-142 bundle of n member transactions.
// reason is "batch" (per-batch origin flush), "relay" (verbatim spine re-emit
// via ProcessBundle), or "encode_error". Cold path (fires once per bundle, not
// per frame), so it uses OTel like FrameFragmented.
func (r *Recorder) CoalesceFlushed(iface string, workerID, n int, reason string) {
	ctx := context.Background()
	opt := metric.WithAttributes(
		attribute.Int("worker", workerID),
		ifaceAttr(iface),
	)
	r.coalesceFlush.Add(ctx, 1, metric.WithAttributes(
		attribute.Int("worker", workerID),
		ifaceAttr(iface),
		attribute.String("reason", reason),
	))
	if n > 0 {
		r.coalesceBundles.Add(ctx, 1, opt)
		r.coalesceMembers.Add(ctx, int64(n), opt)
		r.coalesceMembersHisto.Record(ctx, int64(n), opt)
	}
}

// TeeFailed records datagrams a loopback tee could not deliver. kind is "retry"
// (the co-located retry cache) or "mirror" (the co-located listener). The tee is
// best-effort by contract — a failure never touches real egress — so this counter
// is the ONLY evidence that the co-located cache is quietly incomplete.
func (r *Recorder) TeeFailed(kind string, n int64) {
	if n <= 0 {
		return
	}
	r.teeFailed.Add(context.Background(), n, metric.WithAttributes(
		attribute.String("kind", kind),
	))
}

// ControlFrameForwarded records a BRC-127 control datagram forwarded via ForwardControl.
// ctrlGroup names the destination control group (e.g. "subtree_group_announce").
func (r *Recorder) ControlFrameForwarded(ctrlGroup string) {
	r.ctrlFramesForwarded.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("ctrl_group", ctrlGroup),
	))
}

// IngressDeduped records a frame suppressed by the ingress TxID dedup gate.
// frameType is one of "brc124", "brc131", "brc132", "brc134" — matching the
// frame-version namespace used by other metrics. iface may be empty when the
// dropping worker has no resolved interface yet.
func (r *Recorder) IngressDeduped(iface string, workerID int, frameType string) {
	r.ingressDeduped.Add(context.Background(), 1, metric.WithAttributes(
		attribute.Int("worker", workerID),
		ifaceAttr(iface),
		attribute.String("frame_type", frameType),
	))
}

// IngressFrameClass identifies the BRC frame class of an accepted ingress frame,
// for the per-class admission volume counters. Values index a fixed handle array.
type IngressFrameClass uint8

const (
	IngressClassTx       IngressFrameClass = iota // BRC-12/124/128 transactions
	IngressClassBlock                             // BRC-131 block announce
	IngressClassCoinbase                          // BRC-133 coinbase
	IngressClassSubtree                           // BRC-132 subtree data
	IngressClassAnchor                            // BRC-134 chained anchor
	IngressClassBEEF                              // BRC-148 BEEF object
	ingressClassCount
)

func (c IngressFrameClass) String() string {
	switch c {
	case IngressClassBlock:
		return "block"
	case IngressClassCoinbase:
		return "coinbase"
	case IngressClassSubtree:
		return "subtree"
	case IngressClassAnchor:
		return "anchor"
	case IngressClassBEEF:
		return "beef"
	default:
		return "tx"
	}
}

// IngressMetered records one accepted ingress frame of size bytes under its
// frame class and admission tier (privileged miner socket vs transaction-only).
// Hot path: one array index + counter add, no label lookup. Rejected frames are
// not metered here (they are counted via PrivilegedFrameRejected/BlockPoWRejected).
func (r *Recorder) IngressMetered(class IngressFrameClass, privileged bool, size int) {
	if class >= ingressClassCount {
		return
	}
	ti := 1 // transaction
	if privileged {
		ti = 0
	}
	r.ingressBytesH[class][ti].Add(float64(size))
	r.ingressPacketsH[class][ti].Inc()
}

// PrivilegedFrameRejected records a privileged control-plane frame dropped
// because it arrived on a transaction-only ingress socket. frameType is one
// of "brc131", "brc133", "brc132" — block announce, coinbase, subtree data.
// Only miner-tier ingress sockets accept these frame classes.
func (r *Recorder) PrivilegedFrameRejected(frameType string) {
	r.privilegedRejected.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("frame_type", frameType),
	))
}

// BlockPoWRejected records a BRC-131 block announce dropped by the proof-of-work
// gate — its header failed PoW or claimed a difficulty below the configured floor.
func (r *Recorder) BlockPoWRejected() {
	r.blockPoWRejected.Add(context.Background(), 1)
}

// SubtreeRootCheck records one -verify-subtree-root outcome (result: ok,
// mismatch, malformed).
func (r *Recorder) SubtreeRootCheck(result string) {
	r.subtreeRootChecks.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("result", result),
	))
}

// BEEFSubmission records one BRC-148 submission-record admission outcome
// (result: ok, malformed, oversize, bad_marker, disabled). Topic count is
// never a rejection: a record names 1..15 and the plane delivers a prefix
// of them. Drives the ingress-abuse posture and the e2e admission assertions.
func (r *Recorder) BEEFSubmission(result string) {
	r.beefSubmissions.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("result", result),
	))
}

// BEEFTopics records the topic names on one admitted submission record:
// deliverable is how many of the leading names the frame's DeliverCount
// makes matchable, named is the record's TopicCount. The difference is
// labels: names the subscriber receives in the payload and nothing else.
//
// The dimension is topic_role, NOT role: the site-aggregator stamps every
// federated sample with the site label set (fabric, geo, location, node,
// region, role), so a metric carrying its own "role" makes the whole
// /federate payload invalid ("label name role is not unique") and takes the
// site's ENTIRE scrape down, not just this series.
func (r *Recorder) BEEFTopics(named, deliverable int) {
	if r == nil || r.beefTopics == nil {
		return
	}
	ctx := context.Background()
	r.beefTopics.Add(ctx, int64(deliverable), metric.WithAttributes(attribute.String("topic_role", "deliverable")))
	if labels := named - deliverable; labels > 0 {
		r.beefTopics.Add(ctx, int64(labels), metric.WithAttributes(attribute.String("topic_role", "label")))
	}
}

// TxidClaimLocalHit records a tier-1 local-LRU short-circuit during TxID
// dedup; the frame was suppressed without a Redis call.
func (r *Recorder) TxidClaimLocalHit(prefix string) {
	r.promTxidClaimLocalHit.WithLabelValues(prefix).Inc()
}

// TxidClaimWon records a tier-2 SETNX win during TxID dedup (frame proceeds).
func (r *Recorder) TxidClaimWon(prefix string) {
	r.promTxidClaimWon.WithLabelValues(prefix).Inc()
}

// TxidClaimLost records a tier-2 SETNX loss during TxID dedup (frame dropped).
func (r *Recorder) TxidClaimLost(prefix string) {
	r.promTxidClaimLost.WithLabelValues(prefix).Inc()
}

// TxidClaimError records a Redis error during TxID dedup (fail-open).
func (r *Recorder) TxidClaimError(prefix string) {
	r.promTxidClaimError.WithLabelValues(prefix).Inc()
}

// TCPConnectionAccepted records an accepted TCP ingress connection.
func (r *Recorder) TCPConnectionAccepted() {
	r.tcpConnections.Add(context.Background(), 1)
}

// TCPBytesReceived records bytes read from a TCP ingress connection.
func (r *Recorder) TCPBytesReceived(n int) {
	r.tcpBytesReceived.Add(context.Background(), int64(n))
}

// IngressError records a non-fatal ReadFrom error on the ingress socket.
func (r *Recorder) IngressError(iface string, workerID int) {
	r.promTxIngressErrs.WithLabelValues(strconv.Itoa(workerID), iface).Inc()
}

// EgressError records a WriteTo error on the egress socket.
func (r *Recorder) EgressError(iface string, workerID int) {
	r.promTxEgressErrs.WithLabelValues(strconv.Itoa(workerID), iface).Inc()
}

// WorkerReady signals that a worker has bound its sockets and entered its
// receive loop. Call once per worker after successful socket setup.
func (r *Recorder) WorkerReady() {
	r.readyCount.Add(1)
}

// WorkerDone signals that a worker has exited its receive loop.
// Defer at the top of Worker.Run before any early returns.
func (r *Recorder) WorkerDone() {
	r.readyCount.Add(-1)
}

// ManifestReceived increments the BRC-139 receive counter. nil-safe.
func (r *Recorder) ManifestReceived() {
	if r == nil {
		return
	}
	r.manifestReceived.Add(1)
}

// ManifestSetPilotsKnown updates the pilots-known gauge from the
// evaluator's most-recent snapshot. nil-safe.
func (r *Recorder) ManifestSetPilotsKnown(n int) {
	if r == nil {
		return
	}
	r.manifestPilotsKnown.Store(int64(n))
}

// ManifestSetQuorumMetBits encodes per-field quorum-met flags into a
// single bitmap gauge (bit0 shard_bits, bit1 source_mode, bit2 successor).
func (r *Recorder) ManifestSetQuorumMetBits(bits int32) {
	if r == nil {
		return
	}
	r.manifestQuorumMetBits.Store(bits)
}

// ManifestSetReshardState updates the re-sharding state gauge.
// 0=steady, 1=bridging, 2=cutover-pending.
func (r *Recorder) ManifestSetReshardState(state int32) {
	if r == nil {
		return
	}
	r.manifestReshardState.Store(state)
}

// ManifestSetReshardWindowSeconds updates the seconds-until-cutover
// gauge. May be negative briefly during cutover.
func (r *Recorder) ManifestSetReshardWindowSeconds(s int64) {
	if r == nil {
		return
	}
	r.manifestReshardWindow.Store(s)
}

// ManifestDivergence increments the divergence counter and stamps the
// per-field last-seen time. kind is currently always "peer-disagree" (the
// only case the evaluator reports). nil-safe.
func (r *Recorder) ManifestDivergence(field, kind string) {
	if r == nil {
		return
	}
	r.manifestDivergence.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("field", field),
		attribute.String("kind", kind),
	))
	r.lastDivergenceMu.Lock()
	r.lastDivergence[field] = time.Now().Unix()
	r.lastDivergenceMu.Unlock()
}

// ManifestAdoption increments the adoption counter when the evaluator newly
// adopts a value. reason: "bootstrap", "quorum-shift", "pin-removed".
// nil-safe.
func (r *Recorder) ManifestAdoption(field, reason string) {
	if r == nil {
		return
	}
	r.manifestAdoption.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("field", field),
		attribute.String("reason", reason),
	))
}

// ManifestReshardEmitDuplicate increments the live-reshard
// duplicate-emit counter (proxy-side equivalent of the listener-side
// metric).
func (r *Recorder) ManifestReshardEmitDuplicate() {
	if r == nil {
		return
	}
	r.manifestReshardEmit.Add(context.Background(), 1)
}

// SetDraining marks the recorder as draining. Once called, /readyz returns
// 503 regardless of how many workers are ready. Call this before sleeping the
// drain period so the load balancer stops routing new connections before the
// ingress socket closes.
func (r *Recorder) SetDraining() {
	r.draining.Store(true)
}

// RequireTCPIngress marks the TCP ingress listener as a /readyz prerequisite.
// Call before starting TCPIngress.Run so /readyz returns 503 until the
// listener has bound (signalled via TCPIngressReady).
func (r *Recorder) RequireTCPIngress() {
	r.tcpIngressRequired.Store(true)
}

// TCPIngressReady signals that the TCP ingress listener has bound and is
// accepting connections. Call once from TCPIngress.Run after net.Listen.
func (r *Recorder) TCPIngressReady() {
	r.tcpIngressReady.Store(true)
}

// Shutdown flushes all pending OTLP exports and releases SDK resources.
// Call once during graceful shutdown before wg.Wait().
func (r *Recorder) Shutdown(ctx context.Context) {
	if err := r.shutdownFn(ctx); err != nil {
		slog.Warn("metrics shutdown error", "err", err)
	}
}

// ── Attribute / option helpers ───────────────────────────────────────────────

// ifaceAttr returns the network.interface.name attribute for iface.
func ifaceAttr(iface string) attribute.KeyValue {
	return attribute.String("network.interface.name", iface)
}

// ifaceState returns the (lock-free after warmup) per-interface counter state,
// binding a new one on first sight via copy-on-write.
func (r *Recorder) ifaceState(iface string) *ifaceState {
	if m := r.ifaceStates.Load(); m != nil {
		if st, ok := (*m)[iface]; ok {
			return st
		}
	}
	r.ifaceMu.Lock()
	defer r.ifaceMu.Unlock()
	if m := r.ifaceStates.Load(); m != nil {
		if st, ok := (*m)[iface]; ok {
			return st
		}
	}
	nw := r.numWorkers
	if nw < 1 {
		nw = 1
	}
	st := &ifaceState{iface: iface, numWorkers: nw, workers: make([]*hotPathCounters, nw)}
	st.flows.Store(&flowTable{})
	nm := make(map[string]*ifaceState)
	if old := r.ifaceStates.Load(); old != nil {
		for k, v := range *old {
			nm[k] = v
		}
	}
	nm[iface] = st
	r.ifaceStates.Store(&nm)
	return st
}

// worker returns the pre-bound hot-path counter bundle for workerID, binding it
// on first use. Each worker only ever touches its own slot, so reads are
// lock-free; binding is serialized under st.mu.
func (st *ifaceState) worker(r *Recorder, workerID int) *hotPathCounters {
	if workerID >= 0 && workerID < len(st.workers) {
		if hp := st.workers[workerID]; hp != nil {
			return hp
		}
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if workerID >= 0 && workerID < len(st.workers) && st.workers[workerID] != nil {
		return st.workers[workerID]
	}
	w := strconv.Itoa(workerID)
	hp := &hotPathCounters{
		rxPackets: r.promRxPackets.WithLabelValues(w, st.iface),
		rxBytes:   r.promRxBytes.WithLabelValues(w, st.iface),
		txPackets: r.promTxPackets.WithLabelValues(w, st.iface),
		txBytes:   r.promTxBytes.WithLabelValues(w, st.iface),
		rxSize:    r.promRxSize.WithLabelValues(w, st.iface),
	}
	if workerID >= 0 && workerID < len(st.workers) {
		st.workers[workerID] = hp
	}
	return hp
}

// addFlow increments this worker's shard of the (group) counters. The cell is
// single-writer (only workerID touches it) so the Add is uncontended; the group
// shard is bound via copy-on-write on first sight. Reads are lock-free.
func (st *ifaceState) addFlow(workerID int, groupIdx uint32, size uint64) {
	ft := st.flows.Load()
	var gs *groupShard
	if int(groupIdx) < len(ft.groups) {
		gs = ft.groups[groupIdx]
	}
	if gs == nil {
		gs = st.bindFlow(groupIdx)
	}
	if workerID >= 0 && workerID < len(gs.perWorker) {
		gs.perWorker[workerID].packets.Add(1)
		gs.perWorker[workerID].bytes.Add(size)
	}
}

// bindFlow installs (copy-on-write) the per-worker shard for groupIdx.
func (st *ifaceState) bindFlow(groupIdx uint32) *groupShard {
	st.mu.Lock()
	defer st.mu.Unlock()
	ft := st.flows.Load()
	if int(groupIdx) < len(ft.groups) && ft.groups[groupIdx] != nil {
		return ft.groups[groupIdx]
	}
	n := len(ft.groups)
	if int(groupIdx)+1 > n {
		n = int(groupIdx) + 1
	}
	ng := make([]*groupShard, n)
	copy(ng, ft.groups) // copies *groupShard pointers, not the atomic cells
	gs := &groupShard{perWorker: make([]flowCell, st.numWorkers)}
	ng[groupIdx] = gs
	st.flows.Store(&flowTable{groups: ng})
	return gs
}

// flowCollector exposes the per-worker-sharded flow counters as the
// bsp_flow_{packets,bytes}_total{network_interface_name,group} series, summing
// each group's worker shards at scrape time. Reads are atomic Loads concurrent
// with hot-path Adds (safe; no torn reads).
type flowCollector struct{ r *Recorder }

func (fc *flowCollector) Describe(ch chan<- *promclient.Desc) {
	ch <- fc.r.flowPacketsDesc
	ch <- fc.r.flowBytesDesc
}

func (fc *flowCollector) Collect(ch chan<- promclient.Metric) {
	m := fc.r.ifaceStates.Load()
	if m == nil {
		return
	}
	for iface, st := range *m {
		for g, gs := range st.flows.Load().groups {
			if gs == nil {
				continue
			}
			var pkts, bytes uint64
			for i := range gs.perWorker {
				pkts += gs.perWorker[i].packets.Load()
				bytes += gs.perWorker[i].bytes.Load()
			}
			grp := fmt.Sprintf("%04x", g)
			ch <- promclient.MustNewConstMetric(fc.r.flowPacketsDesc, promclient.CounterValue, float64(pkts), iface, grp)
			ch <- promclient.MustNewConstMetric(fc.r.flowBytesDesc, promclient.CounterValue, float64(bytes), iface, grp)
		}
	}
}

// ── HTTP server ──────────────────────────────────────────────────────────────

// Serve starts the HTTP server on addr, registering /metrics, /healthz, and
// /readyz. It blocks until done is closed, then gracefully shuts down the
// HTTP server with a 5-second deadline.
//
// When pprof is true, net/http/pprof handlers are mounted at /debug/pprof/*
// for profiling sessions. Leave off in production.
func (r *Recorder) Serve(addr string, pprof bool, done <-chan struct{}) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(r.promReg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", r.handleHealthz)
	mux.HandleFunc("/readyz", r.handleReadyz)
	if r.levelVar != nil {
		mux.HandleFunc("/loglevel", logging.LevelHandler(r.levelVar))
	}
	if pprof {
		// Importing net/http/pprof registers the handlers on
		// http.DefaultServeMux as a side effect; re-export them on our mux.
		mux.Handle("/debug/pprof/", http.DefaultServeMux)
	}

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		slog.Info("metrics server listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server error", "err", err)
		}
	}()

	<-done

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		slog.Warn("metrics server shutdown error", "err", err)
	}
}

func (r *Recorder) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, `{"status":"ok","uptime_seconds":%.1f}`, time.Since(r.startTime).Seconds())
}

func (r *Recorder) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	ready := int(r.readyCount.Load())
	total := r.numWorkers
	tcpReq := r.tcpIngressRequired.Load()
	tcpRdy := r.tcpIngressReady.Load()
	w.Header().Set("Content-Type", "application/json")
	if r.draining.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprintf(w, `{"status":"draining","workers_ready":%d,"workers_total":%d,"tcp_ingress_required":%t,"tcp_ingress_ready":%t}`, ready, total, tcpReq, tcpRdy)
		return
	}
	if ready >= total && total > 0 && (!tcpReq || tcpRdy) {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, `{"status":"ready","workers_ready":%d,"workers_total":%d,"tcp_ingress_required":%t,"tcp_ingress_ready":%t}`, ready, total, tcpReq, tcpRdy)
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = fmt.Fprintf(w, `{"status":"starting","workers_ready":%d,"workers_total":%d,"tcp_ingress_required":%t,"tcp_ingress_ready":%t}`, ready, total, tcpReq, tcpRdy)
}
