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
// Per-(interface, group) metric.MeasurementOption values are cached in a
// sync.Map keyed by an ifaceGroupKey struct. The first packet to a new
// (iface, group) pair allocates and stores the option; subsequent packets
// retrieve it with a single sync.Map Load — zero allocation after first hit.
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
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	prometheusexporter "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"

	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ServiceName is the OTel service.name resource attribute value.
const ServiceName = "bitcoin-shard-proxy"

// Version is set at build time via -ldflags "-X metrics.Version=<ver>".
// Defaults to "dev" when not injected.
var Version = "dev"

// ifaceGroupKey is the composite cache key for per-(interface, group)
// OTel MeasurementOption values.
type ifaceGroupKey struct {
	iface string
	group uint32
}

// Recorder holds all pre-allocated OTel instrument handles and readiness
// state. Construct with [New]; pass the pointer to every worker.
type Recorder struct {
	provider    *sdkmetric.MeterProvider
	promReg     promclient.Gatherer
	promOtelReg *promclient.Registry
	runtimeReg  *promclient.Registry
	numWorkers  int
	startTime   time.Time
	readyCount  atomic.Int32

	// Per-packet ingress — labels: worker, iface
	rxPackets  metric.Int64Counter
	rxBytes    metric.Int64Counter
	rxDrops    metric.Int64Counter
	rxSizeHist metric.Int64Histogram

	// Per-packet egress — labels: worker, iface
	txPackets     metric.Int64Counter
	txBytes       metric.Int64Counter
	txEgressErrs  metric.Int64Counter
	txIngressErrs metric.Int64Counter

	// Per-flow / per-group — labels: iface, group (no worker dimension)
	flowPackets metric.Int64Counter
	flowBytes   metric.Int64Counter

	// Active group tracking — iface → set of group indices
	activeGroupsMu sync.Mutex
	activeGroups   map[string]map[uint32]struct{}

	// Fragmentation counters (BRC-130)
	framesFragmented metric.Int64Counter
	fragmentsEmitted metric.Int64Counter

	// Control-plane forwarding (TCP ingress + BRC-127)
	ctrlFramesForwarded metric.Int64Counter
	tcpConnections      metric.Int64Counter
	tcpBytesReceived    metric.Int64Counter

	// Per-(iface, group) MeasurementOption cache
	attrCache sync.Map

	// draining is set to true when a shutdown signal has been received and the
	// proxy is waiting for the load-balancer to stop routing new connections.
	// While true, /readyz returns 503 regardless of worker count.
	draining atomic.Bool

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
		provider:     mp,
		promReg:      promclient.Gatherers{reg, runtimeReg},
		promOtelReg:  reg,
		runtimeReg:   runtimeReg,
		numWorkers:   numWorkers,
		startTime:    time.Now(),
		activeGroups: make(map[string]map[uint32]struct{}),
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

	if r.rxPackets, err = meter.Int64Counter("bsp_packets_received_total",
		metric.WithDescription("Datagrams received")); err != nil {
		return nil, err
	}
	if r.rxBytes, err = meter.Int64Counter("bsp_bytes_received_total",
		metric.WithDescription("Raw bytes received")); err != nil {
		return nil, err
	}
	if r.rxDrops, err = meter.Int64Counter("bsp_packets_dropped_total",
		metric.WithDescription("Datagrams dropped")); err != nil {
		return nil, err
	}
	if r.rxSizeHist, err = meter.Int64Histogram("bsp_packet_size_bytes",
		metric.WithDescription("Datagram size distribution"),
		metric.WithUnit("By")); err != nil {
		return nil, err
	}
	if r.txPackets, err = meter.Int64Counter("bsp_packets_forwarded_total",
		metric.WithDescription("Datagrams successfully forwarded")); err != nil {
		return nil, err
	}
	if r.txBytes, err = meter.Int64Counter("bsp_bytes_forwarded_total",
		metric.WithDescription("Raw bytes forwarded")); err != nil {
		return nil, err
	}
	if r.txEgressErrs, err = meter.Int64Counter("bsp_egress_errors_total",
		metric.WithDescription("WriteTo errors on egress socket")); err != nil {
		return nil, err
	}
	if r.txIngressErrs, err = meter.Int64Counter("bsp_ingress_errors_total",
		metric.WithDescription("ReadFrom non-fatal errors on ingress socket")); err != nil {
		return nil, err
	}
	if r.flowPackets, err = meter.Int64Counter("bsp_flow_packets_total",
		metric.WithDescription("Packets per shard group per interface (active groups only)")); err != nil {
		return nil, err
	}
	if r.flowBytes, err = meter.Int64Counter("bsp_flow_bytes_total",
		metric.WithDescription("Bytes per shard group per interface (active groups only)")); err != nil {
		return nil, err
	}

	// Observable gauge: distinct active group count per interface.
	if _, err = meter.Int64ObservableGauge("bsp_active_groups",
		metric.WithDescription("Distinct shard groups seen since startup, per interface"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			r.activeGroupsMu.Lock()
			defer r.activeGroupsMu.Unlock()
			for iface, groups := range r.activeGroups {
				o.Observe(int64(len(groups)),
					metric.WithAttributes(attribute.String("network.interface.name", iface)),
				)
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

	if r.framesFragmented, err = meter.Int64Counter("bsp_frames_fragmented_total",
		metric.WithDescription("Frames that exceeded the fragmentation threshold and were split into BRC-130 fragments")); err != nil {
		return nil, err
	}
	if r.fragmentsEmitted, err = meter.Int64Counter("bsp_fragments_emitted_total",
		metric.WithDescription("Total BRC-130 fragment datagrams sent (K fragments per fragmented frame)")); err != nil {
		return nil, err
	}

	if r.ctrlFramesForwarded, err = meter.Int64Counter("bsp_control_frames_forwarded_total",
		metric.WithDescription("BRC-127 control datagrams forwarded to multicast (e.g. SubtreeAnnounce)")); err != nil {
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

	return r, nil
}

// ── Record methods (hot path) ────────────────────────────────────────────────

// PacketReceived records receipt of a raw datagram on the ingress socket.
func (r *Recorder) PacketReceived(iface string, workerID int, size int) {
	opt := workerIfaceOpt(iface, workerID)
	ctx := context.Background()
	r.rxPackets.Add(ctx, 1, opt)
	r.rxBytes.Add(ctx, int64(size), opt)
	r.rxSizeHist.Record(ctx, int64(size), opt)
}

// PacketDropped records a dropped datagram.
// reason must be one of: "decode_error", "write_error".
func (r *Recorder) PacketDropped(iface string, workerID int, reason string) {
	r.rxDrops.Add(context.Background(), 1,
		metric.WithAttributes(
			attribute.Int("worker", workerID),
			ifaceAttr(iface),
			attribute.String("reason", reason),
		),
	)
}

// PacketForwarded records a successfully forwarded datagram.
func (r *Recorder) PacketForwarded(iface string, workerID int, groupIdx uint32, size int) {
	ctx := context.Background()
	opt := workerIfaceOpt(iface, workerID)
	r.txPackets.Add(ctx, 1, opt)
	r.txBytes.Add(ctx, int64(size), opt)

	fopt := r.flowOpt(iface, groupIdx)
	r.flowPackets.Add(ctx, 1, fopt)
	r.flowBytes.Add(ctx, int64(size), fopt)

	r.trackGroup(iface, groupIdx)
}

// FrameFragmented records one frame that exceeded the fragmentation threshold.
// k is the number of fragments it was split into.
func (r *Recorder) FrameFragmented(workerID int, k int) {
	opt := workerIfaceOpt("", workerID)
	ctx := context.Background()
	r.framesFragmented.Add(ctx, 1, opt)
	r.fragmentsEmitted.Add(ctx, int64(k), opt)
}

// ControlFrameForwarded records a BRC-127 control datagram forwarded via ForwardControl.
// ctrlGroup names the destination control group (e.g. "subtree_announce").
func (r *Recorder) ControlFrameForwarded(ctrlGroup string) {
	r.ctrlFramesForwarded.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("ctrl_group", ctrlGroup),
	))
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
	r.txIngressErrs.Add(context.Background(), 1, workerIfaceOpt(iface, workerID))
}

// EgressError records a WriteTo error on the egress socket.
func (r *Recorder) EgressError(iface string, workerID int) {
	r.txEgressErrs.Add(context.Background(), 1, workerIfaceOpt(iface, workerID))
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

// SetDraining marks the recorder as draining. Once called, /readyz returns
// 503 regardless of how many workers are ready. Call this before sleeping the
// drain period so the load balancer stops routing new connections before the
// ingress socket closes.
func (r *Recorder) SetDraining() {
	r.draining.Store(true)
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

func workerIfaceOpt(iface string, workerID int) metric.MeasurementOption {
	return metric.WithAttributes(
		attribute.Int("worker", workerID),
		ifaceAttr(iface),
	)
}

// flowOpt returns a cached MeasurementOption for per-(iface, group) flow
// instruments. The first call for a given key allocates and stores the option;
// all subsequent calls perform a single sync.Map Load — zero allocation.
func (r *Recorder) flowOpt(iface string, groupIdx uint32) metric.MeasurementOption {
	key := ifaceGroupKey{iface: iface, group: groupIdx}
	if v, ok := r.attrCache.Load(key); ok {
		return v.(metric.MeasurementOption)
	}
	opt := metric.WithAttributes(
		ifaceAttr(iface),
		attribute.String("group", fmt.Sprintf("%04x", groupIdx)),
	)
	r.attrCache.Store(key, opt)
	return opt
}

// trackGroup records that groupIdx was observed on iface.
func (r *Recorder) trackGroup(iface string, groupIdx uint32) {
	r.activeGroupsMu.Lock()
	m, ok := r.activeGroups[iface]
	if !ok {
		m = make(map[uint32]struct{})
		r.activeGroups[iface] = m
	}
	m[groupIdx] = struct{}{}
	r.activeGroupsMu.Unlock()
}

// ── HTTP server ──────────────────────────────────────────────────────────────

// Serve starts the HTTP server on addr, registering /metrics, /healthz, and
// /readyz. It blocks until done is closed, then gracefully shuts down the
// HTTP server with a 5-second deadline.
func (r *Recorder) Serve(addr string, done <-chan struct{}) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(r.promReg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", r.handleHealthz)
	mux.HandleFunc("/readyz", r.handleReadyz)

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
	w.Header().Set("Content-Type", "application/json")
	if r.draining.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprintf(w, `{"status":"draining","workers_ready":%d,"workers_total":%d}`, ready, total)
		return
	}
	if ready >= total && total > 0 {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, `{"status":"ready","workers_ready":%d,"workers_total":%d}`, ready, total)
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = fmt.Fprintf(w, `{"status":"starting","workers_ready":%d,"workers_total":%d}`, ready, total)
}
