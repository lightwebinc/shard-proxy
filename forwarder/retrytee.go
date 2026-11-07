package forwarder

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"

	"golang.org/x/net/ipv6"

	"github.com/lightwebinc/shard-proxy/metrics"
)

// retryTee mirrors every egressed DATA datagram to a local retry-endpoint cache
// over loopback unicast.
//
// Why this exists: a node must never (S,G)-join its own source, that installs an
// iif==oif mroute and originated frames loop until hop-limit death, so a retry
// endpoint co-located with an origin (the collapsed edge) caches nothing of its
// own node's emissions. It still ADVERTISES itself as a usable cache (BRC-126
// ADVERT is built from static config, with no cache-content gating), so listeners
// discover it, ask it, get MISS, and burn a tier hop before escalating. The tee
// makes that existing advertisement truthful.
//
// It needs NO retry-endpoint change: that ingress socket is an AF_INET6 SOCK_DGRAM
// bound to the WILDCARD [::]:port, which accepts loopback unicast with no
// multicast join, and its cache key is (HashKey, SeqNum) read from the frame bytes
// at fixed offsets: independent of how the datagram was delivered. A locally
// originated unicast copy therefore caches identically to a multicast one.
//
// Cost is one extra syscall per BATCH, not per frame (on Linux): copies accumulate
// alongside the egress queue and go out via WriteBatch (sendmmsg) from Flush, the
// same cadence as the real egress. That is what keeps it viable next to AF_XDP:
// AF_XDP exists to avoid per-packet syscalls, and a per-frame sendto would negate
// it. Off Linux, x/net's WriteBatch writes one message per call, so the flush
// loops (see writeBatchAll), one syscall per frame there, unavoidably.
//
// The send runs on a goroutine of the tee's own. append copies each datagram
// into a buffer the tee owns, flush hands the batch to the sender, and the
// Egress goes on with its next batch. A 4 MiB BEEF record is 477 datagrams,
// and sending their copies over loopback cost the lane's dispatching
// goroutine as much as the multicast send itself (measured 2026-11-01: 6.7 ms
// against 7 ms a record, the lane's ceiling at 45 records a second); the copy
// costs it a tenth of that. The sender's queue is bounded (teeQueueDepth
// batches): a tee that falls behind drops whole batches and counts them,
// since the cache is an optimization and the forward path never waits on it.
type retryTee struct {
	// w is the WriteBatch seam: the socket itself in production, a fake in
	// tests that need to force partial/errored batch results. pc is that same
	// socket held concretely for the lifecycle (close) path only.
	w    batchWriter
	pc   *ipv6.PacketConn
	addr *net.UDPAddr
	log  *slog.Logger

	// owned marks a tee that dialed its own socket (newRetryTee). A tee built
	// over a shared TeeSocket (newSharedTee) does NOT own pc: close() leaves it
	// open for the other Egresses sharing it, the TeeSocket's owner closes it.
	owned bool

	// failed counts datagrams the tee could not deliver. A tee failure must never
	// affect real egress, the cache is an optimisation, the forward path is not:
	// so errors are counted and logged once, never propagated. It is ALSO exported
	// as bsp_tee_failed_total{kind}: a tee that silently drops leaves the cache
	// incomplete while the node keeps advertising it, and without a metric that is
	// invisible until a listener burns a tier hop on the MISS.
	rec    *metrics.Recorder
	kind   string
	failed atomic.Uint64
	logged atomic.Bool

	// msgs and bufs are the batch being built, on the Egress's goroutine:
	// the copies and the pooled buffers holding them.
	msgs []ipv6.Message
	bufs []*[]byte
	hint int
	// bufPool holds copy buffers, msgPool spare message slices.
	bufPool sync.Pool
	msgPool sync.Pool

	// The sender: started by the first flush, fed batches, stopped by close.
	mu      sync.Mutex
	started bool
	closed  bool
	batches chan teeBatch
	done    chan struct{}
}

const (
	// teeQueueDepth bounds the batches a tee holds for its sender; a full
	// queue drops the batch and counts its datagrams as failed.
	teeQueueDepth = 64
	// teeBufSize is the copy buffer a tee pools; a larger datagram gets a
	// buffer of its own.
	teeBufSize = 16 << 10
)

// errTeeBehind is the error counted when the sender's queue is full.
var errTeeBehind = errors.New("retry tee: sender behind, batch dropped")

// teeBatch is one flush's copies: the messages, the pooled buffers to release
// once written, and, for wait, a channel closed once the batch is processed.
type teeBatch struct {
	msgs []ipv6.Message
	bufs []*[]byte
	ack  chan struct{}
}

// TeeSocket is a shared tee/mirror socket: ONE loopback fd serving the tee
// buffers of many Egresses. The TCP lane builds one Egress per accepted
// connection; giving each its own tee socket cost two socket dials per accept
// and collapsed under high connection counts (measured: part of the 64+-conn
// stall). Appends stay per-Egress and lock-free; only the batched sendmmsg
// serializes on the shared fd, at flush cadence. The x/net PacketConn write
// path is goroutine-safe (per-fd write lock), so concurrent flushes are safe.
type TeeSocket struct {
	pc   *ipv6.PacketConn
	addr *net.UDPAddr
}

// NewTeeSocket dials the local cache-ingest (or mirror-ingest) address. addr is
// host:port; the host should be loopback ([::1]), a tee to a non-local address
// would put a full second copy of the stream on the wire.
func NewTeeSocket(addr string) (*TeeSocket, error) {
	ua, err := net.ResolveUDPAddr("udp6", addr)
	if err != nil {
		return nil, fmt.Errorf("retry tee: resolve %q: %w", addr, err)
	}
	if !ua.IP.IsLoopback() {
		// Not fatal, an operator may deliberately tee to a cache on an adjacent
		// host, but it doubles egress volume, so it must never happen silently.
		slog.Warn("retry tee target is not loopback; this duplicates the full egress "+
			"stream onto the network", "addr", addr)
	}
	c, err := net.ListenPacket("udp6", "[::]:0")
	if err != nil {
		return nil, fmt.Errorf("retry tee: listen: %w", err)
	}
	return &TeeSocket{pc: ipv6.NewPacketConn(c), addr: ua}, nil
}

// Close releases the shared socket. Call only after every Egress using it has
// flushed its final batch.
func (s *TeeSocket) Close() error { return s.pc.Close() }

// newRetryTee dials a private socket for this tee (the original per-Egress
// form, still used by the long-lived UDP worker egresses).
func newRetryTee(addr string, batchHint int, rec *metrics.Recorder, kind string) (*retryTee, error) {
	s, err := NewTeeSocket(addr)
	if err != nil {
		return nil, err
	}
	return newTee(s, true, batchHint, rec, kind), nil
}

// newSharedTee builds a tee buffer over an already-open shared TeeSocket.
func newSharedTee(s *TeeSocket, batchHint int, rec *metrics.Recorder, kind string) *retryTee {
	return newTee(s, false, batchHint, rec, kind)
}

func newTee(s *TeeSocket, owned bool, batchHint int, rec *metrics.Recorder, kind string) *retryTee {
	if batchHint < 1 {
		batchHint = 1
	}
	t := &retryTee{
		w:     s.pc,
		pc:    s.pc,
		addr:  s.addr,
		owned: owned,
		rec:   rec,
		kind:  kind,
		hint:  batchHint,
		log:   slog.Default().With("component", "retry-tee", "kind", kind),
	}
	t.bufPool.New = func() any {
		b := make([]byte, teeBufSize)
		return &b
	}
	t.msgPool.New = func() any {
		m := make([]ipv6.Message, 0, t.hint)
		return &m
	}
	t.msgs = *t.msgPool.Get().(*[]ipv6.Message)
	return t
}

// append queues one copy of raw, taken now: raw is the Egress's to reuse once
// its flush returns, and the tee's send runs after that.
func (t *retryTee) append(raw []byte) {
	var cp []byte
	if len(raw) <= teeBufSize {
		p := t.bufPool.Get().(*[]byte)
		cp = (*p)[:len(raw)]
		t.bufs = append(t.bufs, p)
	} else {
		cp = make([]byte, len(raw))
	}
	copy(cp, raw)
	t.msgs = append(t.msgs, ipv6.Message{Buffers: [][]byte{cp}, Addr: t.addr})
}

// flush hands the queued copies to the sender and starts a new batch. The
// first flush starts the sender; a flush after close, or with the sender's
// queue full, drops the batch and counts it.
func (t *retryTee) flush() {
	if len(t.msgs) == 0 {
		return
	}
	b := teeBatch{msgs: t.msgs, bufs: t.bufs}
	t.msgs = (*t.msgPool.Get().(*[]ipv6.Message))[:0]
	t.bufs = nil
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		t.dropped(b, errors.New("retry tee: closed"))
		return
	}
	if !t.started {
		t.started = true
		t.batches = make(chan teeBatch, teeQueueDepth)
		t.done = make(chan struct{})
		go t.run()
	}
	select {
	case t.batches <- b:
		t.mu.Unlock()
	default:
		t.mu.Unlock()
		t.dropped(b, errTeeBehind)
	}
}

// run is the sender: each batch is written through writeBatchAll rather than
// a bare WriteBatch (off Linux a single WriteBatch writes only msgs[0], so
// every copy after the first would be silently dropped and the co-located
// cache would answer MISS for frames it was told it held), then released.
func (t *retryTee) run() {
	defer close(t.done)
	for b := range t.batches {
		if len(b.msgs) > 0 {
			sent, err := writeBatchAll(t.w, b.msgs, 0)
			if err != nil || sent < len(b.msgs) {
				t.missed(len(b.msgs), sent, err)
			}
		}
		t.release(b)
	}
}

// dropped counts a batch that was never sent and releases it.
func (t *retryTee) dropped(b teeBatch, err error) {
	t.missed(len(b.msgs), 0, err)
	t.release(b)
}

// missed counts the datagrams of a batch of queued that were not sent, and
// logs the first time.
func (t *retryTee) missed(queued, sent int, err error) {
	missed := max(queued-sent, 0)
	t.failed.Add(uint64(missed))
	if t.rec != nil {
		t.rec.TeeFailed(t.kind, int64(missed))
	}
	if t.logged.CompareAndSwap(false, true) { // log once; the counter carries the rest
		t.log.Warn("retry tee write incomplete, the co-located cache will MISS "+
			"for these frames and listeners will escalate to another tier",
			"queued", queued, "sent", sent, "err", err)
	}
}

// release returns a batch's buffers and message slice to their pools and
// answers a wait.
func (t *retryTee) release(b teeBatch) {
	for _, p := range b.bufs {
		t.bufPool.Put(p)
	}
	clear(b.msgs)
	m := b.msgs[:0]
	t.msgPool.Put(&m)
	if b.ack != nil {
		close(b.ack)
	}
}

// wait returns once every batch flushed so far has been written (or dropped):
// the synchronous view, for tests and for close.
func (t *retryTee) wait() {
	t.mu.Lock()
	if !t.started || t.closed {
		t.mu.Unlock()
		return
	}
	b := teeBatch{ack: make(chan struct{})}
	t.batches <- b
	t.mu.Unlock()
	<-b.ack
}

// Failed reports datagrams the tee could not deliver.
func (t *retryTee) Failed() uint64 { return t.failed.Load() }

// close stops the sender once it has written what was flushed, then closes
// an owned socket. A flush after close drops its batch.
func (t *retryTee) close() error {
	t.mu.Lock()
	if !t.closed {
		t.closed = true
		if t.started {
			close(t.batches)
		}
	}
	started := t.started
	t.mu.Unlock()
	if started {
		<-t.done
	}
	if t.pc == nil || !t.owned {
		// Shared TeeSocket: the owner (the listener that dialed it) closes it.
		return nil
	}
	return t.pc.Close()
}
