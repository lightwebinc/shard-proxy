package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// newTestRecorder creates a Recorder with no OTLP endpoint, suitable for
// unit tests that do not need network connectivity.
func newTestRecorder(t *testing.T, numWorkers int) *Recorder {
	t.Helper()
	rec, err := New("test-instance", numWorkers, "", 30*time.Second)
	if err != nil {
		t.Fatalf("metrics.New: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		rec.Shutdown(ctx)
	})
	return rec
}

func TestNewNoOTLP(t *testing.T) {
	rec := newTestRecorder(t, 2)
	if rec == nil {
		t.Fatal("expected non-nil Recorder")
	}
}

func TestNewWithOTLP(t *testing.T) {
	// Use a dummy endpoint; the gRPC dial is lazy so New should succeed.
	rec, err := New("test-instance", 1, "localhost:4317", 100*time.Millisecond)
	if err != nil {
		t.Fatalf("New with OTLP: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	rec.Shutdown(ctx)
}

func TestNewInstanceIDFallback(t *testing.T) {
	// Empty instanceID should fall back to hostname without error.
	rec, err := New("", 1, "", 30*time.Second)
	if err != nil {
		t.Fatalf("New with empty instanceID: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	rec.Shutdown(ctx)
}

// ── Record method smoke tests ─────────────────────────────────────────────────

func TestPacketReceived(t *testing.T) {
	rec := newTestRecorder(t, 1)
	rec.PacketReceived("eth0", 0, 512)
	rec.PacketReceived("eth0", 0, 1024)
}

func TestPacketDropped(t *testing.T) {
	rec := newTestRecorder(t, 1)
	rec.PacketDropped("eth0", 0, "decode_error")
	rec.PacketDropped("eth0", 0, "write_error")
	rec.PacketDropped("eth0", 0, "truncated")
}

func TestPacketForwarded(t *testing.T) {
	rec := newTestRecorder(t, 1)
	rec.PacketForwarded("eth0", 0, 42, 512)
	rec.PacketForwarded("eth0", 0, 42, 256) // same group — exercises cache hit
	rec.PacketForwarded("eth0", 0, 99, 128) // different group — new cache entry
}

func TestIngressError(t *testing.T) {
	rec := newTestRecorder(t, 1)
	rec.IngressError("eth0", 0)
}

func TestIngressMetered(t *testing.T) {
	rec := newTestRecorder(t, 1)
	rec.IngressMetered(IngressClassTx, false, 100)      // transaction socket
	rec.IngressMetered(IngressClassTx, false, 50)       // same class+tier
	rec.IngressMetered(IngressClassBlock, true, 250)    // privileged socket
	rec.IngressMetered(IngressClassCoinbase, true, 80)  // privileged socket
	rec.IngressMetered(ingressClassCount, true, 999999) // out of range → no-op, no panic

	const txT, privT = 1, 0
	if got := testutil.ToFloat64(rec.ingressBytesH[IngressClassTx][txT]); got != 150 {
		t.Errorf("tx/transaction bytes = %v, want 150", got)
	}
	if got := testutil.ToFloat64(rec.ingressPacketsH[IngressClassTx][txT]); got != 2 {
		t.Errorf("tx/transaction packets = %v, want 2", got)
	}
	if got := testutil.ToFloat64(rec.ingressBytesH[IngressClassBlock][privT]); got != 250 {
		t.Errorf("block/privileged bytes = %v, want 250", got)
	}
	if got := testutil.ToFloat64(rec.ingressPacketsH[IngressClassCoinbase][privT]); got != 1 {
		t.Errorf("coinbase/privileged packets = %v, want 1", got)
	}
	// The out-of-range call must not have leaked into any bucket.
	if got := testutil.ToFloat64(rec.ingressBytesH[IngressClassTx][privT]); got != 0 {
		t.Errorf("tx/privileged bytes = %v, want 0 (untouched)", got)
	}
}

func TestEgressError(t *testing.T) {
	rec := newTestRecorder(t, 1)
	rec.EgressError("eth0", 0)
}

// ── WorkerReady / WorkerDone ──────────────────────────────────────────────────

func TestWorkerReadyDone(t *testing.T) {
	rec := newTestRecorder(t, 2)
	if got := rec.readyCount.Load(); got != 0 {
		t.Fatalf("initial readyCount = %d, want 0", got)
	}
	rec.WorkerReady()
	rec.WorkerReady()
	if got := rec.readyCount.Load(); got != 2 {
		t.Errorf("readyCount after 2 Ready = %d, want 2", got)
	}
	rec.WorkerDone()
	if got := rec.readyCount.Load(); got != 1 {
		t.Errorf("readyCount after 1 Done = %d, want 1", got)
	}
}

// ── flowCounters cache ────────────────────────────────────────────────────────

func groupSum(st *ifaceState, groupIdx uint32) (uint64, uint64) {
	ft := st.flows.Load()
	if int(groupIdx) >= len(ft.groups) || ft.groups[groupIdx] == nil {
		return 0, 0
	}
	gs := ft.groups[groupIdx]
	var p, b uint64
	for i := range gs.perWorker {
		p += gs.perWorker[i].packets.Load()
		b += gs.perWorker[i].bytes.Load()
	}
	return p, b
}

// TestFlowShardingSumsAcrossWorkers verifies the per-worker shards are summed
// per group (no worker dimension leaks into the exposed counter).
func TestFlowShardingSumsAcrossWorkers(t *testing.T) {
	rec := newTestRecorder(t, 4)
	rec.PacketForwarded("eth0", 0, 7, 100)
	rec.PacketForwarded("eth0", 1, 7, 100) // same group, different worker
	rec.PacketForwarded("eth0", 3, 7, 50)
	st := rec.ifaceState("eth0")
	if p, b := groupSum(st, 7); p != 3 || b != 250 {
		t.Errorf("group 7 sum = (%d pkts, %d bytes), want (3, 250)", p, b)
	}
	if p, _ := groupSum(st, 9); p != 0 {
		t.Errorf("untouched group 9 = %d, want 0", p)
	}
}

// TestFlowCollectorSumsAtScrape verifies the custom collector exposes the
// per-worker shards summed into the unchanged bsp_flow_*{iface,group} schema.
func TestFlowCollectorSumsAtScrape(t *testing.T) {
	rec := newTestRecorder(t, 4)
	rec.PacketForwarded("eth0", 0, 5, 100)
	rec.PacketForwarded("eth0", 1, 5, 100) // same group, different worker
	rec.PacketForwarded("eth0", 3, 5, 50)

	mfs, err := rec.promReg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var pkts, bytes float64
	var found bool
	for _, mf := range mfs {
		name := mf.GetName()
		if name != "bsp_flow_packets_total" && name != "bsp_flow_bytes_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			grp := ""
			for _, l := range m.GetLabel() {
				if l.GetName() == "group" {
					grp = l.GetValue()
				}
			}
			if grp != "0005" {
				continue
			}
			found = true
			if name == "bsp_flow_packets_total" {
				pkts = m.GetCounter().GetValue()
			} else {
				bytes = m.GetCounter().GetValue()
			}
		}
	}
	if !found {
		t.Fatal("bsp_flow_*{group=0005} not exposed")
	}
	if pkts != 3 {
		t.Errorf("summed packets = %v, want 3", pkts)
	}
	if bytes != 250 {
		t.Errorf("summed bytes = %v, want 250", bytes)
	}
}

// ── active group tracking (now derived from the flow table) ───────────────────

func activeGroupCount(st *ifaceState) int {
	n := 0
	for _, gs := range st.flows.Load().groups {
		if gs != nil {
			n++
		}
	}
	return n
}

func TestActiveGroupTracking(t *testing.T) {
	rec := newTestRecorder(t, 1)
	st0 := rec.ifaceState("eth0")
	st0.addFlow(0, 1, 64)
	st0.addFlow(0, 2, 64)
	st0.addFlow(0, 1, 64) // duplicate group — should not bind a new shard
	st1 := rec.ifaceState("eth1")
	st1.addFlow(0, 1, 64)

	if got := activeGroupCount(st0); got != 2 {
		t.Errorf("eth0 active groups = %d, want 2", got)
	}
	if got := activeGroupCount(st1); got != 1 {
		t.Errorf("eth1 active groups = %d, want 1", got)
	}
}

// ── HTTP handlers ─────────────────────────────────────────────────────────────

func TestHandleHealthz(t *testing.T) {
	rec := newTestRecorder(t, 2)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	rec.handleHealthz(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("body not valid JSON: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("status field = %v, want ok", body["status"])
	}
	if _, ok := body["uptime_seconds"]; !ok {
		t.Error("missing uptime_seconds field")
	}
}

func TestHandleReadyzNotReady(t *testing.T) {
	rec := newTestRecorder(t, 2)
	// No workers have called WorkerReady — should be 503.
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	rec.handleReadyz(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("body not valid JSON: %v", err)
	}
	if body["status"] != "starting" {
		t.Errorf("status field = %v, want starting", body["status"])
	}
}

func TestHandleReadyzDraining(t *testing.T) {
	rec := newTestRecorder(t, 2)
	rec.WorkerReady()
	rec.WorkerReady()
	rec.SetDraining()

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	rec.handleReadyz(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d (draining)", w.Code, http.StatusServiceUnavailable)
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("body not valid JSON: %v", err)
	}
	if body["status"] != "draining" {
		t.Errorf("status field = %v, want draining", body["status"])
	}
}

func TestHandleReadyzReady(t *testing.T) {
	rec := newTestRecorder(t, 2)
	rec.WorkerReady()
	rec.WorkerReady()

	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	rec.handleReadyz(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("body not valid JSON: %v", err)
	}
	if body["status"] != "ready" {
		t.Errorf("status field = %v, want ready", body["status"])
	}
}

func TestHandleReadyzTCPIngressGate(t *testing.T) {
	rec := newTestRecorder(t, 1)
	rec.WorkerReady()
	rec.RequireTCPIngress()

	// Workers ready but TCP listener not yet bound → 503 starting.
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	w := httptest.NewRecorder()
	rec.handleReadyz(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("pre-bind status = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("pre-bind body not valid JSON: %v", err)
	}
	if body["status"] != "starting" {
		t.Errorf("pre-bind status field = %v, want starting", body["status"])
	}
	if body["tcp_ingress_required"] != true || body["tcp_ingress_ready"] != false {
		t.Errorf("pre-bind tcp flags = req:%v ready:%v, want req:true ready:false",
			body["tcp_ingress_required"], body["tcp_ingress_ready"])
	}

	// Once listener bound → 200 ready.
	rec.TCPIngressReady()
	w = httptest.NewRecorder()
	rec.handleReadyz(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("post-bind status = %d, want %d", w.Code, http.StatusOK)
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("post-bind body not valid JSON: %v", err)
	}
	if body["status"] != "ready" {
		t.Errorf("post-bind status field = %v, want ready", body["status"])
	}
}

// ── Serve (integration) ───────────────────────────────────────────────────────

func TestServeMetricsEndpoint(t *testing.T) {
	rec := newTestRecorder(t, 1)
	done := make(chan struct{})

	// Start the server in the background and give it a moment to bind.
	go rec.Serve("127.0.0.1:0", false, done)
	// We cannot easily get the ephemeral port from Serve as-is, so instead
	// we test the handler directly via httptest to avoid port-binding races.
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", rec.handleHealthz)
	mux.HandleFunc("/readyz", rec.handleReadyz)

	srv := httptest.NewServer(mux)
	defer srv.Close()
	close(done)

	for _, path := range []string{"/healthz", "/readyz"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Errorf("GET %s: %v", path, err)
			continue
		}
		_ = resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 600 {
			t.Errorf("GET %s: unexpected status %d", path, resp.StatusCode)
		}
	}
}

func TestShutdown(t *testing.T) {
	rec, err := New("test", 1, "", 30*time.Second)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Must not panic or block.
	rec.Shutdown(ctx)
}

func TestServeBadAddr(t *testing.T) {
	rec := newTestRecorder(t, 1)
	done := make(chan struct{})
	// Pass an invalid address so ListenAndServe errors immediately.
	// Serve must not panic and must unblock when done is closed.
	go rec.Serve("invalid-address-!!", false, done)
	// Give the goroutine time to attempt binding and log the error.
	time.Sleep(50 * time.Millisecond)
	close(done)
	// Give Serve time to return after done is closed.
	time.Sleep(50 * time.Millisecond)
}

func TestServeShutdownError(t *testing.T) {
	rec := newTestRecorder(t, 1)
	done := make(chan struct{})

	// Start Serve on a valid addr, then close done quickly so the HTTP server
	// shuts down. The srv.Shutdown path (including the warn branch) is exercised
	// when done is closed while the server is still running normally.
	go rec.Serve("127.0.0.1:0", false, done)
	time.Sleep(30 * time.Millisecond)
	close(done)
	time.Sleep(50 * time.Millisecond)
}

// BenchmarkPacketForwarded exercises the integer-indexed hot path (one atomic
// load + short-string map lookup + slice index + 4 counter ops), the metric
// replaced the per-packet interface-keyed sync.Map lookups + active-group mutex.
func BenchmarkPacketForwarded(b *testing.B) {
	rec, err := New("bench", 4, "", 30*time.Second)
	if err != nil {
		b.Fatalf("metrics.New: %v", err)
	}
	for i := 0; i < 4; i++ { // warm all worker + group binds
		for g := 0; g < 256; g++ {
			rec.PacketForwarded("eth0", i, uint32(g), 256)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec.PacketForwarded("eth0", i&3, uint32(i&0xff), 256)
	}
}

// Cached coalesce options must export the same series as building them per
// call: one flush series per (worker, reason), one bundle and members series
// per worker, with the counts recorded.
func TestCoalesceFlushedSeries(t *testing.T) {
	rec := newTestRecorder(t, 2)
	for i := 0; i < 3; i++ {
		rec.CoalesceFlushed("eth1", 0, 12, "relay")
	}
	rec.CoalesceFlushed("eth1", 1, 5, "batch")
	rec.CoalesceFlushed("eth1", 1, 0, "encode_error")

	mfs, err := rec.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	got := map[string]float64{}
	for _, mf := range mfs {
		switch mf.GetName() {
		case "bsp_coalesce_flush_total", "bsp_coalesce_bundles_total", "bsp_coalesce_members_total":
		default:
			continue
		}
		for _, m := range mf.GetMetric() {
			key := mf.GetName()
			for _, l := range m.GetLabel() {
				switch l.GetName() {
				case "worker", "reason", "network_interface_name":
					key += " " + l.GetName() + "=" + l.GetValue()
				}
			}
			got[key] = m.GetCounter().GetValue()
		}
	}
	want := map[string]float64{
		"bsp_coalesce_flush_total network_interface_name=eth1 reason=relay worker=0":        3,
		"bsp_coalesce_flush_total network_interface_name=eth1 reason=batch worker=1":        1,
		"bsp_coalesce_flush_total network_interface_name=eth1 reason=encode_error worker=1": 1,
		"bsp_coalesce_bundles_total network_interface_name=eth1 worker=0":                   3,
		"bsp_coalesce_bundles_total network_interface_name=eth1 worker=1":                   1,
		"bsp_coalesce_members_total network_interface_name=eth1 worker=0":                   36,
		"bsp_coalesce_members_total network_interface_name=eth1 worker=1":                   5,
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %v, want %v", k, got[k], v)
		}
	}
	if len(got) != len(want) {
		t.Errorf("got %d coalesce series, want %d: %v", len(got), len(want), got)
	}
}

func BenchmarkCoalesceFlushed(b *testing.B) {
	rec, err := New("bench", 1, "", 30*time.Second)
	if err != nil {
		b.Fatal(err)
	}
	defer rec.Shutdown(context.Background())
	b.ReportAllocs()
	for b.Loop() {
		rec.CoalesceFlushed("eth1", 0, 12, "relay")
	}
}
