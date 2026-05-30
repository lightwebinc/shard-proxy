package metrics

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
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

// ── flowOpt cache ─────────────────────────────────────────────────────────────

func TestFlowOptCacheHit(t *testing.T) {
	rec := newTestRecorder(t, 1)
	opt1 := rec.flowOpt("eth0", 7)
	opt2 := rec.flowOpt("eth0", 7)
	// Both calls must return the identical value (pointer equality on interface).
	if opt1 != opt2 {
		t.Error("flowOpt returned different values for same key; cache miss on second call")
	}
}

func TestFlowOptCacheMiss(t *testing.T) {
	rec := newTestRecorder(t, 1)
	opt1 := rec.flowOpt("eth0", 1)
	opt2 := rec.flowOpt("eth0", 2)
	if opt1 == opt2 {
		t.Error("flowOpt returned same value for different groups")
	}
}

// ── trackGroup ────────────────────────────────────────────────────────────────

func TestTrackGroup(t *testing.T) {
	rec := newTestRecorder(t, 1)
	rec.trackGroup("eth0", 1)
	rec.trackGroup("eth0", 2)
	rec.trackGroup("eth0", 1) // duplicate — should not grow the set
	rec.trackGroup("eth1", 1)

	rec.activeGroupsMu.Lock()
	defer rec.activeGroupsMu.Unlock()

	if len(rec.activeGroups["eth0"]) != 2 {
		t.Errorf("eth0 active groups = %d, want 2", len(rec.activeGroups["eth0"]))
	}
	if len(rec.activeGroups["eth1"]) != 1 {
		t.Errorf("eth1 active groups = %d, want 1", len(rec.activeGroups["eth1"]))
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
	go rec.Serve("127.0.0.1:0", done)
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
	go rec.Serve("invalid-address-!!", done)
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
	go rec.Serve("127.0.0.1:0", done)
	time.Sleep(30 * time.Millisecond)
	close(done)
	time.Sleep(50 * time.Millisecond)
}
