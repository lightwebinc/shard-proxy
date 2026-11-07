package forwarder

import (
	"net"
	"syscall"
	"testing"

	"golang.org/x/net/ipv6"

	"github.com/lightwebinc/shard-common/shard"
)

// fakeBatchWriter scripts WriteBatch results so the reliable lane's retry loop
// can be exercised against partial and errored sends: impossible against a
// healthy kernel socket, which is exactly why the naive "stream N frames,
// assert all egress" test would pass while masking a partial-send drop.
type fakeBatchWriter struct {
	// script[i] is consumed on call i: send min(n, len(batch)) messages and
	// return err. After the script runs out, everything succeeds.
	script []struct {
		n   int
		err error
	}
	call int
	sent int // total messages acknowledged as sent
}

func (f *fakeBatchWriter) WriteBatch(ms []ipv6.Message, _ int) (int, error) {
	step := struct {
		n   int
		err error
	}{n: len(ms)}
	if f.call < len(f.script) {
		step = f.script[f.call]
	}
	f.call++
	n := step.n
	if n > len(ms) {
		n = len(ms)
	}
	if n > 0 {
		f.sent += n
	}
	return n, step.err
}

// reliableEgress builds a one-target reliable Egress whose batch writer is the
// fake (swapped in after construction, the internal seam batchWriter exists
// for exactly this).
func reliableEgress(t *testing.T, fake *fakeBatchWriter) *Egress {
	t.Helper()
	engine := shard.New(0xFF05, shard.DefaultGroupID, 8)
	fw := New(engine, 0xFF05, shard.DefaultGroupID, 9001, false, nil)
	conn, _ := openLoopbackUDP(t)
	egr := NewEgress(fw, makeTargets(t, conn), 1, nil)
	egr.SetReliable(true)
	egr.pcs[0] = fake
	return egr
}

func enqueueN(egr *Egress, n int) {
	dst := net.UDPAddr{IP: net.ParseIP("ff05::1"), Port: 9001}
	for i := 0; i < n; i++ {
		egr.EnqueueData([]byte{byte(i), 0xAA, 0xBB}, dst, 1, 0)
	}
}

// A partial WriteBatch on the reliable lane must re-submit the remainder, not
// drop it: the batched-cadence TCP path queues up to a whole batch per flush,
// and the old per-frame path never lost more than one frame to a backpressure
// event. Script: 3-of-10 sent, then 2 with ENOBUFS, then the rest, all 10
// must reach the wire.
func TestReliableFlushRetriesPartialSend(t *testing.T) {
	fake := &fakeBatchWriter{script: []struct {
		n   int
		err error
	}{
		{n: 3},                       // partial, no error
		{n: 2, err: syscall.ENOBUFS}, // partial with transient error
		{n: 0, err: syscall.ENOBUFS}, // zero progress, transient: retry
	}}
	egr := reliableEgress(t, fake)
	enqueueN(egr, 10)
	egr.Flush()
	if fake.sent != 10 {
		t.Fatalf("reliable flush delivered %d of 10: partial-send remainder was dropped", fake.sent)
	}
	if len(egr.msgs[0]) != 0 || len(egr.meta) != 0 {
		t.Fatalf("queues not reset after flush: msgs=%d meta=%d", len(egr.msgs[0]), len(egr.meta))
	}
}

// A hard (non-transient) error must surrender the remainder: bounded loss with
// accounting, not spin. EPERM (a firewall drop) is the canonical case.
func TestReliableFlushSurrendersOnHardError(t *testing.T) {
	fake := &fakeBatchWriter{script: []struct {
		n   int
		err error
	}{
		{n: 4, err: syscall.EPERM},
	}}
	egr := reliableEgress(t, fake)
	enqueueN(egr, 10)
	egr.Flush()
	if fake.sent != 4 {
		t.Fatalf("sent %d, want exactly the pre-error 4 (no retry on hard error)", fake.sent)
	}
	if len(egr.msgs[0]) != 0 {
		t.Fatal("queue not reset after surrender")
	}
}

// Sustained zero-progress transient errors must exhaust the bounded retry
// budget and return, a wedged socket cannot hang the submission lane.
func TestReliableFlushBoundedRetryBudget(t *testing.T) {
	script := make([]struct {
		n   int
		err error
	}, maxReliableRetries+10)
	for i := range script {
		script[i] = struct {
			n   int
			err error
		}{n: 0, err: syscall.ENOBUFS}
	}
	fake := &fakeBatchWriter{script: script}
	egr := reliableEgress(t, fake)
	enqueueN(egr, 5)
	egr.Flush() // must return (bounded), not hang
	if fake.sent != 0 {
		t.Fatalf("sent %d, want 0", fake.sent)
	}
	if fake.call > maxReliableRetries+2 {
		t.Fatalf("WriteBatch called %d times; budget is %d", fake.call, maxReliableRetries)
	}
}

// The non-reliable (UDP) lane must keep drop-and-count semantics: one partial
// WriteBatch, no retry: lossy-lane throughput must not inherit the reliable
// lane's blocking behaviour.
func TestNonReliableFlushDoesNotRetry(t *testing.T) {
	fake := &fakeBatchWriter{script: []struct {
		n   int
		err error
	}{
		{n: 3, err: syscall.ENOBUFS},
	}}
	engine := shard.New(0xFF05, shard.DefaultGroupID, 8)
	fw := New(engine, 0xFF05, shard.DefaultGroupID, 9001, false, nil)
	conn, _ := openLoopbackUDP(t)
	egr := NewEgress(fw, makeTargets(t, conn), 8, nil)
	egr.pcs[0] = fake
	enqueueN(egr, 10)
	egr.Flush()
	if fake.call != 1 {
		t.Fatalf("non-reliable flush called WriteBatch %d times, want 1", fake.call)
	}
}

// FlushVia (the ingress→spine pipeline path) must honor the reliable lane the
// same way Flush does: a transient via-fn error retries the SAME message with
// the bounded budget instead of surrendering the whole batch remainder, a
// via transport self-heals by re-dialing on the next call, so pre-batching a
// disconnect lost ~1 frame and must not now lose up to a full batch.
func TestReliableFlushViaRetriesTransientError(t *testing.T) {
	engine := shard.New(0xFF05, shard.DefaultGroupID, 8)
	fw := New(engine, 0xFF05, shard.DefaultGroupID, 9001, false, nil)
	conn, _ := openLoopbackUDP(t)
	egr := NewEgress(fw, makeTargets(t, conn), 1, nil)
	egr.SetReliable(true)
	enqueueN(egr, 10)

	calls, delivered, failed := 0, 0, 2 // first two calls fail (transient disconnect)
	egr.FlushVia(func(_ int, _ []byte, _ *net.UDPAddr) error {
		calls++
		if failed > 0 {
			failed--
			return syscall.EPIPE // via-transport errors are opaque; all retryable
		}
		delivered++
		return nil
	})
	if delivered != 10 {
		t.Fatalf("reliable FlushVia delivered %d of 10: remainder surrendered on transient error", delivered)
	}

	// Non-reliable keeps the original first-error surrender.
	egr2 := NewEgress(fw, makeTargets(t, conn), 1, nil)
	enqueueN(egr2, 10)
	calls2 := 0
	egr2.FlushVia(func(_ int, _ []byte, _ *net.UDPAddr) error {
		calls2++
		return syscall.EPIPE
	})
	if calls2 != 1 {
		t.Fatalf("non-reliable FlushVia called fn %d times after error, want 1", calls2)
	}
}

// One shared TeeSocket serving two Egresses (the per-connection TCP form) must
// deliver every frame from both, and closing one Egress's tee must NOT close
// the shared socket under the other.
func TestSharedTeeSocketServesMultipleEgresses(t *testing.T) {
	sink, sinkAddr := openLoopbackUDP(t)
	shared, err := NewTeeSocket(net.JoinHostPort("::1", itoaPort(sinkAddr.Port)))
	if err != nil {
		t.Fatalf("NewTeeSocket: %v", err)
	}
	defer func() { _ = shared.Close() }()

	engine := shard.New(0xFF05, shard.DefaultGroupID, 8)
	fw := New(engine, 0xFF05, shard.DefaultGroupID, 9001, false, nil)
	c1, _ := openLoopbackUDP(t)
	c2, _ := openLoopbackUDP(t)
	egr1 := NewEgress(fw, makeTargets(t, c1), 4, nil)
	egr2 := NewEgress(fw, makeTargets(t, c2), 4, nil)
	egr1.EnableRetryTeeShared(shared, 4)
	egr2.EnableRetryTeeShared(shared, 4)

	dst := net.UDPAddr{IP: net.ParseIP("ff05::1"), Port: 9001}
	egr1.EnqueueData([]byte("from-egress-one-aaaaaaaaaaaaaaaa"), dst, 1, 0)
	egr1.Flush()

	// Closing egress 1's tee must leave the SHARED socket open for egress 2.
	if err := egr1.CloseRetryTee(); err != nil {
		t.Fatalf("CloseRetryTee: %v", err)
	}
	egr2.EnqueueData([]byte("from-egress-two-bbbbbbbbbbbbbbbb"), dst, 1, 0)
	egr2.Flush()

	got := drainSink(t, sink, 2)
	if len(got) != 2 {
		t.Fatalf("shared tee delivered %d datagrams, want 2 (socket closed early?)", len(got))
	}
	if egr2.RetryTeeFailed() != 0 {
		t.Errorf("egress 2 tee failures: %d, shared socket was closed under it", egr2.RetryTeeFailed())
	}
}

// The addrCache starts small and must grow transparently when a group index
// beyond the initial size appears (BRC-148 BEEF band indices start at 0x1000).
func TestAddrCacheGrowsBeyondInitialSize(t *testing.T) {
	egr, conns := makeEgressNoFrag(t, 4, 1)
	dst := net.UDPAddr{IP: net.ParseIP("ff05::1"), Port: 9001}
	egr.EnqueueData([]byte("low-group"), dst, 1, 0)
	egr.EnqueueData([]byte("beef-band"), dst, 0x1000, 0) // beyond addrCacheInit
	egr.EnqueueData([]byte("beef-band-2"), dst, 0x1001, 0)
	if len(egr.msgs[0]) != 3 {
		t.Fatalf("enqueued %d, want 3", len(egr.msgs[0]))
	}
	if egr.addrCache[0][0x1000] == nil || egr.addrCache[0][0x1001] == nil {
		t.Fatal("grown addrCache did not cache the high-group addresses")
	}
	_ = conns
	egr.Flush()
}

// DispatchBareTx must copy the submitted tx into a fresh frame: under the
// batched TCP egress cadence the enqueued frame is flushed later, so if it
// aliased the caller's (reused) buffer, a mutation before flush would corrupt
// the egressed frame (and its tee/mirror copies). This is the anti-aliasing
// guarantee that routing the bare lane through DispatchBareTx (never the
// magic-sniffing DispatchClass framed path) exists to hold.
func TestDispatchBareTxCopiesUnderDeferredFlush(t *testing.T) {
	engine := shard.New(0xFF05, shard.DefaultGroupID, 8)
	fw := New(engine, 0xFF05, shard.DefaultGroupID, 9001, false, nil)
	fw.SetRequireEF(false)
	var sunk [][]byte
	egr := NewEgress(fw, makeTargets(t, mustLoopback(t)), 4, nil)

	tx := bareEF1in1out()
	orig := append([]byte(nil), tx...)
	fw.DispatchBareTx(egr, tx, &net.UDPAddr{IP: net.ParseIP("::1")}, 0)
	// Reuse/overwrite the caller's buffer BEFORE flush, the objfmt reader
	// window would be reused exactly like this on the next record.
	for i := range tx {
		tx[i] = 0xFF
	}
	egr.FlushVia(func(_ int, raw []byte, _ *net.UDPAddr) error {
		sunk = append(sunk, append([]byte(nil), raw...))
		return nil
	})
	if len(sunk) != 1 {
		t.Fatalf("egressed %d frames, want 1", len(sunk))
	}
	// The reframed frame's payload (after the 92-byte BRC header) must equal the
	// ORIGINAL tx bytes, not the 0xFF-overwritten buffer.
	pay := sunk[0][92:]
	if len(pay) < len(orig) || string(pay[:len(orig)]) != string(orig) {
		t.Fatal("egressed frame aliased the caller buffer: bare tx was not copied")
	}
}

// bareEF1in1out is a minimal valid BRC-30 EF transaction.
func bareEF1in1out() []byte {
	b := []byte{0x01, 0, 0, 0, 0x00, 0x00, 0x00, 0x00, 0x00, 0xEF, 0x01}
	b = append(b, make([]byte, 32)...)
	b = append(b, 0xFF, 0xFF, 0xFF, 0xFF)
	b = append(b, 0x00)
	b = append(b, 0xFF, 0xFF, 0xFF, 0xFF)
	b = append(b, make([]byte, 8)...)
	b = append(b, 0x00)
	b = append(b, 0x01)
	b = append(b, make([]byte, 8)...)
	b = append(b, 0x00)
	b = append(b, 0, 0, 0, 0)
	return b
}

func mustLoopback(t *testing.T) *net.UDPConn {
	t.Helper()
	c, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.ParseIP("::1")})
	if err != nil {
		t.Fatalf("loopback: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}
