package forwarder

import (
	"net"
	"sync"
	"testing"

	"golang.org/x/net/ipv6"

	"github.com/lightwebinc/shard-common/shard"
)

// makeEgressNoFrag constructs an Egress backed by loopback conns with
// fragmentation disabled — pool is nil. Used by Egress unit tests that
// don't exercise the fragment path.
func makeEgressNoFrag(t *testing.T, batchHint int, n int) (*Egress, []*net.UDPConn) {
	t.Helper()
	fw := makeForwarder()
	conns := make([]*net.UDPConn, n)
	for i := range conns {
		c, _ := openLoopbackUDP(t)
		conns[i] = c
	}
	tgts := makeTargets(t, conns...)
	egr := NewEgress(fw, tgts, batchHint, nil)
	t.Cleanup(egr.Flush)
	return egr, conns
}

func TestNewEgress_BindsAllTargets(t *testing.T) {
	egr, _ := makeEgressNoFrag(t, 8, 3)
	if got := len(egr.Targets()); got != 3 {
		t.Fatalf("Targets() len = %d, want 3", got)
	}
	if len(egr.pcs) != 3 {
		t.Errorf("pcs len = %d, want 3", len(egr.pcs))
	}
	if len(egr.msgs) != 3 {
		t.Errorf("msgs len = %d, want 3", len(egr.msgs))
	}
	for i, pc := range egr.pcs {
		if pc == nil {
			t.Errorf("pcs[%d] = nil; OpenTargets should have populated it", i)
		}
	}
}

func TestNewEgress_BatchHintFloor(t *testing.T) {
	// batchHint < 1 must clamp to 1; we observe via Egress.meta capacity.
	fw := makeForwarder()
	egr := NewEgress(fw, nil, 0, nil)
	if cap(egr.meta) < 1 {
		t.Errorf("meta cap = %d after batchHint=0, want >= 1", cap(egr.meta))
	}
}

func TestNewEgress_NoFragMTU_PoolNil(t *testing.T) {
	egr, _ := makeEgressNoFrag(t, 4, 1)
	if egr.pool != nil {
		t.Error("pool should be nil when forwarder has fragMTU=0")
	}
	if got := egr.PoolGet(); got != nil {
		t.Errorf("PoolGet on disabled pool = %v, want nil", got)
	}
}

func TestNewEgress_WithFragMTU_PoolReady(t *testing.T) {
	fw := makeForwarder()
	fw.SetFragMTU(1500)
	conn, _ := openLoopbackUDP(t)
	egr := NewEgress(fw, makeTargets(t, conn), 4, nil)
	t.Cleanup(egr.Flush)

	if egr.pool == nil {
		t.Fatal("pool should be initialised when fragMTU > 0")
	}
	p := egr.PoolGet()
	if p == nil {
		t.Fatal("PoolGet returned nil with fragMTU > 0")
	}
	if got := len(*p); got != egr.poolSize {
		t.Errorf("pool buffer len = %d, want %d", got, egr.poolSize)
	}
	// Releasing via EnqueueDataPooled must not panic; we then Flush to
	// run the release path.
	dst := net.UDPAddr{IP: net.ParseIP("ff05::b:1"), Port: 9001}
	egr.EnqueueDataPooled((*p)[:32], dst, 1, 0, p)
	egr.Flush()
	if len(egr.pooledBufs) != 0 {
		t.Errorf("pooledBufs not cleared after Flush: %d entries", len(egr.pooledBufs))
	}
}

func TestEnqueueData_FansOutToAllTargets(t *testing.T) {
	egr, _ := makeEgressNoFrag(t, 4, 2)
	dst := net.UDPAddr{IP: net.ParseIP("ff05::b:1"), Port: 9001}
	raw := []byte("hello")
	egr.EnqueueData(raw, dst, 7, 0)

	for i, q := range egr.msgs {
		if len(q) != 1 {
			t.Errorf("target %d queue len = %d, want 1", i, len(q))
			continue
		}
		// Zone must match each target's iface name so the kernel sends
		// the datagram out the right interface.
		addr := q[0].Addr.(*net.UDPAddr)
		if addr.Zone != egr.targets[i].Iface.Name {
			t.Errorf("target %d zone = %q, want %q", i, addr.Zone, egr.targets[i].Iface.Name)
		}
		if got := q[0].Buffers[0]; len(got) != len(raw) {
			t.Errorf("target %d buf len = %d, want %d", i, len(got), len(raw))
		}
	}
	if len(egr.meta) != 1 {
		t.Errorf("meta len = %d, want 1 (shared across targets)", len(egr.meta))
	}
}

func TestEnqueueControl_LabelCapturedInMeta(t *testing.T) {
	egr, _ := makeEgressNoFrag(t, 4, 1)
	dst := net.UDPAddr{IP: shard.GroupAddr(0xFF05, shard.DefaultGroupID, shard.GroupBlockBroadcast), Port: 9001}
	egr.EnqueueControl([]byte("ctrl"), dst, "block_control", 0)

	if len(egr.meta) != 1 {
		t.Fatalf("meta len = %d, want 1", len(egr.meta))
	}
	if got := egr.meta[0].ctrlLabel; got != "block_control" {
		t.Errorf("ctrlLabel = %q, want %q", got, "block_control")
	}
	if egr.meta[0].size != len("ctrl") {
		t.Errorf("size = %d, want %d", egr.meta[0].size, len("ctrl"))
	}
}

func TestFlush_EmptyEgress_NoOp(t *testing.T) {
	egr, _ := makeEgressNoFrag(t, 4, 1)
	// Must not panic when nothing is queued.
	egr.Flush()
}

func TestFlush_ResetsQueuesAndMeta(t *testing.T) {
	egr, _ := makeEgressNoFrag(t, 4, 2)
	dst := net.UDPAddr{IP: net.ParseIP("ff05::b:1"), Port: 9001}
	egr.EnqueueData([]byte("a"), dst, 1, 0)
	egr.EnqueueData([]byte("b"), dst, 1, 0)

	egr.Flush()

	if got := len(egr.meta); got != 0 {
		t.Errorf("meta len after Flush = %d, want 0", got)
	}
	for i, q := range egr.msgs {
		if len(q) != 0 {
			t.Errorf("target %d queue len after Flush = %d, want 0", i, len(q))
		}
	}
}

// TestRecordWrite_NegativeSent guards the egress crash where WriteBatch
// returns -1 (sendmmsg failed before any send, e.g. "network is unreachable")
// and recordWrite indexed e.meta[-1]. sent must be clamped to [0,len(meta)].
func TestRecordWrite_NegativeSent(t *testing.T) {
	fw := makeForwarder()
	c, _ := openLoopbackUDP(t)
	egr := recEgress(t, fw, c)
	dst := net.UDPAddr{IP: net.ParseIP("ff05::b:1"), Port: 9001}
	egr.EnqueueData([]byte("a"), dst, 1, 0)
	egr.EnqueueData([]byte("b"), dst, 1, 0)
	// Must not panic: -1 (pre-send failure) and an oversized count are both
	// out-of-range relative to meta.
	egr.recordWrite(0, -1, net.ErrClosed)
	egr.recordWrite(0, len(egr.meta)+5, nil)
}

func TestFlush_MultipleCycles_QueueGrowthBounded(t *testing.T) {
	// Append/Flush/Append should reuse the underlying msgs[i] backing
	// arrays — capacity must not grow unboundedly across cycles.
	egr, _ := makeEgressNoFrag(t, 4, 1)
	dst := net.UDPAddr{IP: net.ParseIP("ff05::b:1"), Port: 9001}
	for i := 0; i < 100; i++ {
		egr.EnqueueData([]byte("x"), dst, 1, 0)
		egr.Flush()
	}
	if c := cap(egr.msgs[0]); c > 8 {
		// Capacity may grow modestly once (Go may double the initial
		// allocation), but it should plateau quickly. 8 is well above
		// the worst-case for batchHint=4 + one element.
		t.Errorf("msgs[0] cap grew to %d after 100 Flush cycles; suspected leak", c)
	}
}

// A sender goroutine writes every flushed datagram, in order, releases the
// pooled buffers, and StopSender leaves nothing unsent.
func TestEgressSenderKeepsOrder(t *testing.T) {
	w := &recordingWriter{}
	e := &Egress{
		targets:   []Target{{Iface: &net.Interface{Name: "lo"}}},
		pcs:       []batchWriter{w},
		msgs:      make([][]ipv6.Message, 1),
		addrCache: [][]*net.UDPAddr{make([]*net.UDPAddr, addrCacheInit)},
		reliable:  true,
		pool:      &sync.Pool{New: func() any { b := make([]byte, 16); return &b }},
	}
	e.StartSender(2)
	dst := net.UDPAddr{IP: net.ParseIP("ff02::1"), Port: 9}
	const batches, each = 50, 7
	for b := range batches {
		for n := range each {
			p := e.PoolGet()
			(*p)[0], (*p)[1] = byte(b), byte(n)
			e.EnqueueDataPooled((*p)[:2], dst, 0, 0, p)
		}
		e.Flush()
	}
	e.StopSender()
	got := w.taken()
	if len(got) != batches*each {
		t.Fatalf("%d datagrams written, want %d", len(got), batches*each)
	}
	for i, d := range got {
		if int(d[0]) != i/each || int(d[1]) != i%each {
			t.Fatalf("datagram %d is %v", i, d)
		}
	}
	// Flush after StopSender writes inline again.
	e.EnqueueData([]byte{1, 2}, dst, 0, 0)
	e.Flush()
	if n := len(w.taken()); n != batches*each+1 {
		t.Fatalf("%d datagrams after the inline flush", n)
	}
}

// recordingWriter is a batchWriter that keeps a copy of what it was handed.
type recordingWriter struct {
	mu  sync.Mutex
	out [][]byte
}

func (w *recordingWriter) WriteBatch(ms []ipv6.Message, _ int) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, m := range ms {
		w.out = append(w.out, append([]byte(nil), m.Buffers[0]...))
	}
	return len(ms), nil
}

func (w *recordingWriter) taken() [][]byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.out
}
