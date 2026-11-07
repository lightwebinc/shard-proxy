package forwarder

import (
	"net"
	"syscall"
	"testing"

	"golang.org/x/net/ipv6"

	"github.com/lightwebinc/shard-common/shard"
)

// oneShotWriter models golang.org/x/net/ipv6's NON-Linux WriteBatch exactly:
// it does SendMsg(&ms[0]) and returns (1, nil), never touching ms[1:]. That is
// the real behaviour on FreeBSD (and on every GOOS but Linux), a caller that
// treats one WriteBatch call as "the whole batch went out" silently drops every
// datagram after the first of each flush.
type oneShotWriter struct {
	calls int
	sent  []string // payload of every message actually written, in order
	err   error    // when set, returned instead of writing (with n=0)
}

func (w *oneShotWriter) WriteBatch(ms []ipv6.Message, _ int) (int, error) {
	w.calls++
	if w.err != nil {
		return 0, w.err
	}
	if len(ms) == 0 {
		return 0, nil
	}
	w.sent = append(w.sent, string(ms[0].Buffers[0]))
	return 1, nil
}

// stalledWriter reports neither progress nor an error, the pathological
// (0, nil) result. The drain must surrender, not spin forever.
type stalledWriter struct{ calls int }

func (w *stalledWriter) WriteBatch(_ []ipv6.Message, _ int) (int, error) {
	w.calls++
	return 0, nil
}

func portableEgress(t *testing.T, w batchWriter, batchHint int) *Egress {
	t.Helper()
	engine := shard.New(0xFF05, shard.DefaultGroupID, 8)
	fw := New(engine, 0xFF05, shard.DefaultGroupID, 9001, false, nil)
	conn, _ := openLoopbackUDP(t)
	egr := NewEgress(fw, makeTargets(t, conn), batchHint, nil)
	egr.pcs[0] = w
	return egr
}

// The LOSSY multicast egress must put every queued datagram on the wire even
// when the platform's WriteBatch accepts one message per call. Without the
// drain loop this delivers 1 of 8, the FreeBSD edge blocker: a whole receive
// batch collapses to its first frame, with no error, no metric and no log.
func TestLossyFlushDrainsWholeBatchOneMessageAtATime(t *testing.T) {
	w := &oneShotWriter{}
	egr := portableEgress(t, w, 8)

	dst := net.UDPAddr{IP: net.ParseIP("ff05::1"), Port: 9001}
	want := []string{"f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7"}
	for i, p := range want {
		egr.EnqueueData([]byte(p), dst, uint32(i), 0)
	}
	egr.Flush()

	if len(w.sent) != len(want) {
		t.Fatalf("lossy flush wrote %d of %d datagrams (%v), the batch remainder "+
			"was silently dropped", len(w.sent), len(want), w.sent)
	}
	for i := range want {
		if w.sent[i] != want[i] {
			t.Errorf("datagram %d = %q, want %q (order must be preserved)", i, w.sent[i], want[i])
		}
	}
	if len(egr.msgs[0]) != 0 || len(egr.meta) != 0 {
		t.Fatalf("queues not reset after flush: msgs=%d meta=%d", len(egr.msgs[0]), len(egr.meta))
	}
}

// The retry tee mirrors the same batch. A one-message-per-call WriteBatch must
// not leave copies 2..N uncached: the endpoint ADVERTISES itself as holding
// them, so a silent drop turns every NACK for those frames into a MISS plus a
// wasted tier hop.
func TestRetryTeeDrainsWholeBatchOneMessageAtATime(t *testing.T) {
	sink, sinkAddr := openLoopbackUDP(t)
	egr := portableEgress(t, &oneShotWriter{}, 8)
	if err := egr.EnableRetryTee(net.JoinHostPort("::1", itoaPort(sinkAddr.Port)), 8); err != nil {
		t.Fatalf("EnableRetryTee: %v", err)
	}
	t.Cleanup(func() { _ = egr.CloseRetryTee() })
	tw := &oneShotWriter{}
	egr.tee.w = tw // stand in for the platform socket; the fd stays open for close()

	dst := net.UDPAddr{IP: net.ParseIP("ff05::1"), Port: 9001}
	want := []string{"t0", "t1", "t2", "t3", "t4"}
	for i, p := range want {
		egr.EnqueueData([]byte(p), dst, uint32(i), 0)
	}
	egr.Flush()
	egr.tee.wait()

	if len(tw.sent) != len(want) {
		t.Fatalf("tee wrote %d of %d copies (%v), the cache would MISS for the rest",
			len(tw.sent), len(want), tw.sent)
	}
	if f := egr.RetryTeeFailed(); f != 0 {
		t.Errorf("tee reported %d failures on a fully drained batch", f)
	}
	_ = sink
}

// The local mirror (own-node delivery to a collapsed edge's listener) is the
// same retryTee type on a different port and must drain identically, without
// it a FreeBSD collapsed edge delivers one own-source frame per batch to its
// own listener.
func TestLocalMirrorDrainsWholeBatchOneMessageAtATime(t *testing.T) {
	_, sinkAddr := openLoopbackUDP(t)
	egr := portableEgress(t, &oneShotWriter{}, 8)
	if err := egr.EnableLocalMirror(net.JoinHostPort("::1", itoaPort(sinkAddr.Port)), 8); err != nil {
		t.Fatalf("EnableLocalMirror: %v", err)
	}
	t.Cleanup(func() { _ = egr.CloseLocalMirror() })
	mw := &oneShotWriter{}
	egr.mirror.w = mw

	dst := net.UDPAddr{IP: net.ParseIP("ff05::1"), Port: 9001}
	for i := 0; i < 6; i++ {
		egr.EnqueueData([]byte{byte('a' + i)}, dst, uint32(i), 0)
	}
	egr.Flush()
	egr.mirror.wait()

	if len(mw.sent) != 6 {
		t.Fatalf("mirror wrote %d of 6 copies: own-source frames never reach the "+
			"co-located listener", len(mw.sent))
	}
	if f := egr.LocalMirrorFailed(); f != 0 {
		t.Errorf("mirror reported %d failures on a fully drained batch", f)
	}
}

// FlushVia (the AF_XDP / pipeline transport path) also drains the tee, so the
// same portability requirement applies there.
func TestFlushViaTeeDrainsWholeBatch(t *testing.T) {
	_, sinkAddr := openLoopbackUDP(t)
	egr := portableEgress(t, &oneShotWriter{}, 8)
	if err := egr.EnableRetryTee(net.JoinHostPort("::1", itoaPort(sinkAddr.Port)), 8); err != nil {
		t.Fatalf("EnableRetryTee: %v", err)
	}
	t.Cleanup(func() { _ = egr.CloseRetryTee() })
	tw := &oneShotWriter{}
	egr.tee.w = tw

	dst := net.UDPAddr{IP: net.ParseIP("ff05::1"), Port: 9001}
	for i := 0; i < 4; i++ {
		egr.EnqueueData([]byte{byte('0' + i)}, dst, uint32(i), 0)
	}
	egr.FlushVia(func(int, []byte, *net.UDPAddr) error { return nil })
	egr.tee.wait()

	if len(tw.sent) != 4 {
		t.Fatalf("FlushVia tee wrote %d of 4 copies", len(tw.sent))
	}
}

// The reliable (TCP submission) lane must also drain a one-message-per-call
// writer, and must do it WITHOUT spending its retry budget, since an
// error-free partial write is not backpressure.
func TestReliableFlushDrainsOneMessageAtATime(t *testing.T) {
	w := &oneShotWriter{}
	egr := portableEgress(t, w, 8)
	egr.SetReliable(true)
	enqueueN(egr, 12)
	egr.Flush()

	if len(w.sent) != 12 {
		t.Fatalf("reliable flush wrote %d of 12 datagrams", len(w.sent))
	}
	if w.calls != 12 {
		t.Fatalf("WriteBatch called %d times for 12 datagrams, the drain should cost "+
			"exactly one call per accepted message, with no retry backoff", w.calls)
	}
}

// A (0, nil) WriteBatch must terminate the drain, not spin. Both lanes.
func TestFlushDoesNotSpinOnZeroProgress(t *testing.T) {
	w := &stalledWriter{}
	egr := portableEgress(t, w, 8)
	enqueueN(egr, 5)
	egr.Flush() // must return
	if w.calls != 1 {
		t.Fatalf("lossy flush called WriteBatch %d times on a (0, nil) result, want 1", w.calls)
	}

	w2 := &stalledWriter{}
	egr2 := portableEgress(t, w2, 8)
	egr2.SetReliable(true)
	enqueueN(egr2, 5)
	egr2.Flush() // bounded by maxReliableRetries, must return
	if w2.calls > maxReliableRetries+2 {
		t.Fatalf("reliable flush called WriteBatch %d times; budget is %d",
			w2.calls, maxReliableRetries)
	}
	if w2.calls < 2 {
		t.Fatalf("reliable flush gave up after %d call(s); a stall must be retried", w2.calls)
	}
}

// The lossy lane keeps drop-and-count on a real error: the drain stops at the
// first errno rather than inheriting the reliable lane's retry behaviour.
func TestLossyDrainStopsOnFirstError(t *testing.T) {
	w := &oneShotWriter{err: syscall.ENOBUFS}
	egr := portableEgress(t, w, 8)
	enqueueN(egr, 10)
	egr.Flush()
	if w.calls != 1 {
		t.Fatalf("lossy drain called WriteBatch %d times after ENOBUFS, want 1", w.calls)
	}
}

// writeBatchAll's own contract, exercised directly.
func TestWriteBatchAllContract(t *testing.T) {
	ms := make([]ipv6.Message, 5)
	for i := range ms {
		ms[i] = ipv6.Message{Buffers: [][]byte{{byte(i)}}}
	}

	w := &oneShotWriter{}
	if n, err := writeBatchAll(w, ms, 0); n != 5 || err != nil {
		t.Fatalf("one-at-a-time: n=%d err=%v, want 5, nil", n, err)
	}

	s := &stalledWriter{}
	n, err := writeBatchAll(s, ms, 0)
	if n != 0 || err == nil {
		t.Fatalf("stalled: n=%d err=%v, want 0 and a no-progress error", n, err)
	}

	// Empty batch: no call, no error.
	s2 := &stalledWriter{}
	if n, err := writeBatchAll(s2, nil, 0); n != 0 || err != nil || s2.calls != 0 {
		t.Fatalf("empty batch: n=%d err=%v calls=%d", n, err, s2.calls)
	}
}
