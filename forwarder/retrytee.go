package forwarder

import (
	"fmt"
	"log/slog"
	"net"

	"golang.org/x/net/ipv6"

	"github.com/lightwebinc/shard-proxy/metrics"
)

// retryTee mirrors every egressed DATA datagram to a local retry-endpoint cache
// over loopback unicast.
//
// Why this exists: a node must never (S,G)-join its own source — that installs an
// iif==oif mroute and originated frames loop until hop-limit death — so a retry
// endpoint co-located with an origin (the collapsed edge) caches nothing of its
// own node's emissions. It still ADVERTISES itself as a usable cache (BRC-126
// ADVERT is built from static config, with no cache-content gating), so listeners
// discover it, ask it, get MISS, and burn a tier hop before escalating. The tee
// makes that existing advertisement truthful.
//
// It needs NO retry-endpoint change: that ingress socket is an AF_INET6 SOCK_DGRAM
// bound to the WILDCARD [::]:port, which accepts loopback unicast with no
// multicast join, and its cache key is (HashKey, SeqNum) read from the frame bytes
// at fixed offsets — independent of how the datagram was delivered. A locally
// originated unicast copy therefore caches identically to a multicast one.
//
// Cost is one extra syscall per BATCH, not per frame (on Linux): copies accumulate
// alongside the egress queue and go out via WriteBatch (sendmmsg) from Flush, the
// same cadence as the real egress. That is what keeps it viable next to AF_XDP —
// AF_XDP exists to avoid per-packet syscalls, and a per-frame sendto would negate
// it. Off Linux, x/net's WriteBatch writes one message per call, so the flush
// loops (see writeBatchAll) — one syscall per frame there, unavoidably.
type retryTee struct {
	// w is the WriteBatch seam: the socket itself in production, a fake in
	// tests that need to force partial/errored batch results. pc is that same
	// socket held concretely for the lifecycle (close) path only.
	w    batchWriter
	pc   *ipv6.PacketConn
	addr *net.UDPAddr
	msgs []ipv6.Message
	log  *slog.Logger

	// owned marks a tee that dialed its own socket (newRetryTee). A tee built
	// over a shared TeeSocket (newSharedTee) does NOT own pc: close() leaves it
	// open for the other Egresses sharing it — the TeeSocket's owner closes it.
	owned bool

	// failed counts datagrams the tee could not deliver. A tee failure must never
	// affect real egress — the cache is an optimisation, the forward path is not —
	// so errors are counted and logged once, never propagated. It is ALSO exported
	// as bsp_tee_failed_total{kind}: a tee that silently drops leaves the cache
	// incomplete while the node keeps advertising it, and without a metric that is
	// invisible until a listener burns a tier hop on the MISS.
	rec    *metrics.Recorder
	kind   string
	failed uint64
	logged bool
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
// host:port; the host should be loopback ([::1]) — a tee to a non-local address
// would put a full second copy of the stream on the wire.
func NewTeeSocket(addr string) (*TeeSocket, error) {
	ua, err := net.ResolveUDPAddr("udp6", addr)
	if err != nil {
		return nil, fmt.Errorf("retry tee: resolve %q: %w", addr, err)
	}
	if !ua.IP.IsLoopback() {
		// Not fatal — an operator may deliberately tee to a cache on an adjacent
		// host — but it doubles egress volume, so it must never happen silently.
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
	return &retryTee{
		w:     s.pc,
		pc:    s.pc,
		addr:  s.addr,
		owned: true,
		rec:   rec,
		kind:  kind,
		msgs:  make([]ipv6.Message, 0, batchHint),
		log:   slog.Default().With("component", "retry-tee", "kind", kind),
	}, nil
}

// newSharedTee builds a tee buffer over an already-open shared TeeSocket.
func newSharedTee(s *TeeSocket, batchHint int, rec *metrics.Recorder, kind string) *retryTee {
	return &retryTee{
		w:    s.pc,
		pc:   s.pc,
		addr: s.addr,
		rec:  rec,
		kind: kind,
		msgs: make([]ipv6.Message, 0, batchHint),
		log:  slog.Default().With("component", "retry-tee", "kind", kind),
	}
}

// append queues one copy. raw must stay valid until flush returns — the same
// contract the egress queue already has.
func (t *retryTee) append(raw []byte) {
	t.msgs = append(t.msgs, ipv6.Message{Buffers: [][]byte{raw}, Addr: t.addr})
}

// flush dispatches the queued copies and resets the queue. It drains through
// writeBatchAll rather than a bare WriteBatch: off Linux a single WriteBatch
// writes only msgs[0], so every copy after the first would be silently dropped
// and the co-located cache would answer MISS for frames it was told it held.
func (t *retryTee) flush() {
	if len(t.msgs) == 0 {
		return
	}
	sent, err := writeBatchAll(t.w, t.msgs, 0)
	if err != nil || sent < len(t.msgs) {
		missed := len(t.msgs) - sent
		if missed < 0 {
			missed = 0
		}
		t.failed += uint64(missed)
		if t.rec != nil {
			t.rec.TeeFailed(t.kind, int64(missed))
		}
		if !t.logged {
			t.logged = true // log once; the counter carries the rest
			t.log.Warn("retry tee write incomplete — the co-located cache will MISS "+
				"for these frames and listeners will escalate to another tier",
				"queued", len(t.msgs), "sent", sent, "err", err)
		}
	}
	clear(t.msgs)
	t.msgs = t.msgs[:0]
}

// Failed reports datagrams the tee could not deliver.
func (t *retryTee) Failed() uint64 { return t.failed }

func (t *retryTee) close() error {
	if t.pc == nil || !t.owned {
		// Shared TeeSocket: the owner (the listener that dialed it) closes it.
		return nil
	}
	return t.pc.Close()
}
