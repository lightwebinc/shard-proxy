// Package forwarder — egress.go implements the per-worker outbound message
// batcher. Each Process*/ForwardControl call enqueues outbound datagrams
// into an Egress; the worker calls [Egress.Flush] at the end of each receive
// batch, which drains each target's queue through ipv6.PacketConn.WriteBatch.
// On Linux that is one sendmmsg per target, amortising the egress syscall cost
// across the entire receive batch instead of paying it per packet per
// interface. On every other GOOS x/net's WriteBatch writes a single message per
// call, so the drain loops until the queue is empty (see writeBatchAll) — same
// bytes on the wire, one syscall per datagram.
//
// An Egress is owned by a single worker goroutine and is not safe for
// concurrent use.
package forwarder

import (
	"errors"
	"log/slog"
	"net"
	"sync"
	"syscall"
	"time"

	"golang.org/x/net/ipv6"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/logging"
	"github.com/lightwebinc/shard-proxy/metrics"
)

// Egress queues outbound multicast datagrams for one batch and emits them
// via WriteBatch (sendmmsg on Linux) on Flush. Append references the caller's
// payload bytes directly — the bytes must remain valid until the next Flush
// returns. The verbatim forwarding path feeds bytes from a receive batch
// whose buffers are reused only after Flush; fragment-path payloads come
// from a per-Egress sync.Pool and are released back at Flush time.
// maxGroupCache bounds the per-group destination-address caches. It covers
// the transaction plane (shardBits ≤ 12 ⇒ indices [0, 0x1000)) plus the
// BRC-148 BEEF object plane band (0x1000–0x1FFF). Out-of-range indices fall
// back to per-packet allocation (never hit in practice).
const maxGroupCache = 1 << 13

// addrCacheInit is the initial per-target addrCache length. Grown on demand in
// enqueue (next power of two covering the seen groupIdx, capped at
// maxGroupCache); 64 covers every real deployment's transaction-plane group
// space without growth while keeping the per-Egress (per-TCP-connection)
// allocation to half a KiB instead of maxGroupCache's 64 KiB.
const addrCacheInit = 64

// batchWriter is the sendmmsg seam: *ipv6.PacketConn in production, a fake in
// tests that need to force partial/errored WriteBatch results (the reliable
// lane's retry loop cannot be exercised against a healthy kernel socket).
type batchWriter interface {
	WriteBatch(ms []ipv6.Message, flags int) (int, error)
}

// errNoBatchProgress stands in for a WriteBatch that reported neither progress
// nor an error — nothing to classify, and nothing that a bare re-submit would
// change, so the drain loop surrenders instead of spinning.
var errNoBatchProgress = errors.New("egress: WriteBatch made no progress")

// writeBatchAll submits ms through w, re-submitting the unsent remainder until
// the batch is drained, w reports an error, or w stops making progress. It
// returns the total number of messages accepted and the error that stopped the
// drain (nil only when the whole batch went out).
//
// The loop is a CORRECTNESS requirement, not an optimisation.
// golang.org/x/net/ipv6's WriteBatch only issues sendmmsg on Linux; on every
// other GOOS it falls back to SendMsg(&ms[0]) and returns 1, so a single call
// silently writes just the first datagram of the batch and drops the rest on
// the floor. A caller that treats one WriteBatch as "the batch went out" loses
// everything after ms[0] on FreeBSD — the whole lossy multicast egress and its
// tees. Looping is the portable contract: on Linux the first call normally
// takes them all and the loop exits immediately (no extra syscall), on FreeBSD
// it degrades to one sendmsg per datagram, which is exactly what a correct
// per-frame egress would have cost there anyway. No runtime.GOOS gate is
// needed or wanted — a partial WriteBatch is legal on Linux too (short
// sendmmsg), and the same loop covers both.
//
// It deliberately does NOT retry: a returned error stops the drain and is
// handed back to the caller, so the lossy lane keeps its drop-and-count
// semantics and the reliable lane keeps sole ownership of backoff/budget.
func writeBatchAll(w batchWriter, ms []ipv6.Message, flags int) (int, error) {
	off := 0
	for off < len(ms) {
		n, err := w.WriteBatch(ms[off:], flags)
		if n > 0 {
			// Clamp: WriteBatch must never claim more than it was handed, and a
			// bogus over-count would run off the end of the slice.
			if n > len(ms)-off {
				n = len(ms) - off
			}
			off += n
		}
		if err != nil {
			// Includes the WriteBatch(-1, err) hard-failure form: n is ignored,
			// off stays where it was.
			return off, err
		}
		if n <= 0 {
			return off, errNoBatchProgress
		}
	}
	return off, nil
}

type Egress struct {
	targets []Target
	pcs     []batchWriter

	// reliable marks an Egress on the reliable (TCP) submission lane: Flush
	// re-submits the unsent remainder of a partial WriteBatch instead of
	// dropping it. The batched-cadence TCP path can queue up to a whole batch
	// per flush; without the retry, one ENOBUFS burst would drop the entire
	// batch where the old per-frame path lost at most one frame — a strict
	// reliability regression on the lane whose contract is reliability. The
	// lossy UDP lane keeps drop-and-count semantics (reliable=false).
	reliable bool

	// msgs[i] holds the queue of outbound datagrams for targets[i]. All
	// queues grow in lock-step: every Enqueue appends one entry to every
	// target's queue plus one entry to meta. len(msgs[i]) == len(meta) for
	// every i between Enqueue and Flush.
	msgs [][]ipv6.Message
	meta []msgMeta

	// pooledBufs collects fragment buffers from EnqueueDataPooled /
	// EnqueueControlPooled. The slice is drained at Flush, returning each
	// backing buffer to pool exactly once regardless of target count.
	pooledBufs []*[]byte

	// tee, when non-nil, mirrors DATA datagrams to a co-located retry cache over
	// loopback. See retrytee.go for why this exists and why it is batched.
	tee *retryTee

	// mirror, when non-nil, mirrors DATA datagrams to the co-located LISTENER over
	// loopback (a second, independent tee). On a collapsed edge the listener never
	// SSM-joins this node's OWN source (that would install an iif==oif mroute and
	// storm), so own-source frames are not multicast-received locally; the mirror is
	// the only path that delivers own-node frames to the co-located listener. It
	// targets a DEDICATED loopback port the listener binds exclusively — NOT the
	// retry cache's port, which the retry endpoint co-binds (unicast demux to a
	// shared wildcard port is kernel-chosen, not controllable).
	mirror *retryTee

	// addrCache[i][groupIdx] is the per-target destination *net.UDPAddr
	// (IP+Port+Zone) for a data frame in group groupIdx. The multicast
	// address is a pure function of (mcPrefix, groupID, groupIdx, port) and
	// the Zone is the fixed target iface, so a frame's destination never
	// changes for a given (target, group) — even across resharding/bridging
	// (which alter the group *index* a txid maps to, not the address of an
	// index). Lazily filled on first sight, eliminating a per-packet
	// per-target net.UDPAddr allocation on the egress hot path. Control
	// frames (distinct dst, no group) bypass the cache.
	addrCache [][]*net.UDPAddr

	pool     *sync.Pool
	poolSize int

	// coal buffers BRC-142 coalescing members during a batch (nil unless the
	// forwarder has coalescing enabled). Drained by Forwarder.FlushCoalesced
	// before Flush. Owned by this Egress's single worker goroutine.
	coal *coalBuffer

	log *slog.Logger
	rec *metrics.Recorder

	// throttle bounds category-8 OS/NIC egress-error logs to log-once-then-count
	// per interface, so a sustained ENOBUFS storm cannot itself become an outage.
	throttle *logging.Throttle
}

// msgMeta carries the per-message attributes needed to fire metrics at Flush
// time. Stored once per Enqueue and reused across all targets.
type msgMeta struct {
	groupIdx  uint32
	size      int
	workerID  int
	ctrlLabel string // non-empty → ControlFrameForwarded; empty → PacketForwarded

	// teeable marks a datagram the co-located retry cache can answer a NACK
	// for: one this proxy has SeqNum-stamped, so the (HashKey, SeqNum) key the
	// cache derives from the bytes is exactly the key a NACK will ask for.
	//
	// It is DELIBERATELY a separate field from ctrlLabel. ctrlLabel is a
	// METRICS label, and BRC-131 block / BRC-132 subtree / BRC-134 anchor
	// frames ride the control-group enqueue path purely because they carry an
	// arbitrary destination address — they are fully stamped, fragmented and
	// NACK-repairable DATA. Deriving teeability from ctrlLabel conflated the
	// two and silently withheld every one of those frames from the tee: a node
	// never (S,G)-joins its own source, so the tee is the ONLY path by which it
	// can cache its own emissions, and a submit edge was left unable to repair
	// any of its own subtree or block traffic (measured on testnet tn-edge-1:
	// 482 of 5787 own datagrams teed, cache_size 0, 0 hits / 1107 misses).
	// Only genuinely unstamped control frames (ForwardControl / BRC-127
	// SubtreeGroupAnnounce) are non-teeable — the cache would decode_error
	// those, which is the drop the original gate was reaching for.
	teeable bool
}

// NewEgress constructs an Egress bound to the given targets. batchHint sets
// the initial capacity reservation for the per-target message queue; growth
// beyond this is dynamic. The Egress draws fragment buffer memory from a
// sync.Pool sized to the forwarder's BRC-130 fragment datagram capacity;
// fragmentation must be enabled on fw for the pool to be initialised.
func NewEgress(fw *Forwarder, targets []Target, batchHint int, rec *metrics.Recorder) *Egress {
	if batchHint < 1 {
		batchHint = 1
	}
	e := &Egress{
		targets:   targets,
		pcs:       make([]batchWriter, len(targets)),
		msgs:      make([][]ipv6.Message, len(targets)),
		meta:      make([]msgMeta, 0, batchHint),
		addrCache: make([][]*net.UDPAddr, len(targets)),
		log:       slog.Default().With("component", "egress"),
		rec:       rec,
		throttle:  logging.NewThrottle(5 * time.Second),
	}
	for i, tgt := range targets {
		if tgt.PC != nil {
			e.pcs[i] = tgt.PC
		} else {
			e.pcs[i] = ipv6.NewPacketConn(tgt.Conn)
		}
		e.msgs[i] = make([]ipv6.Message, 0, batchHint)
		// Start the per-group destination cache SMALL and grow on demand (see
		// enqueue). The old fixed maxGroupCache slab was 64 KiB of zeroed
		// pointers per target per Egress — trivial for the handful of
		// long-lived UDP worker egresses it was built for, but the TCP lane
		// builds one Egress per accepted connection, and at high connection
		// counts the per-accept slab churn was a measured contributor to the
		// many-connection collapse. Real deployments use a handful of groups
		// (shardBits 2 ⇒ 4), so the small start covers them without growing.
		e.addrCache[i] = make([]*net.UDPAddr, addrCacheInit)
	}
	if fw != nil && fw.fragDataSize > 0 {
		e.poolSize = frame.HeaderSizeV3 + fw.fragDataSize
		e.pool = &sync.Pool{
			New: func() any {
				b := make([]byte, e.poolSize)
				return &b
			},
		}
	}
	// Coalescing needs a multi-frame batch to coalesce over, so it is enabled
	// only when batchHint > 1 (the UDP recvmmsg workers). The TCP ingress Egress
	// (batchHint = 1, flushes per frame and never calls FlushCoalesced) and the
	// legacy per-packet UDP path (-recv-batch 1) get coal == nil, so Process
	// never diverts to coalescing there and no member is ever stranded.
	if fw != nil && fw.coalesce && batchHint > 1 {
		e.coal = newCoalBuffer(fw.coalesceMaxMembers)
	}
	return e
}

// Targets returns the underlying target slice. Used by the worker for
// shutdown lifecycle; do not mutate.
func (e *Egress) Targets() []Target { return e.targets }

// EnableRetryTee mirrors egressed DATA datagrams to a co-located retry cache at
// addr (host:port, normally "[::1]:<ingress-port>"). See retrytee.go.
func (e *Egress) EnableRetryTee(addr string, batchHint int) error {
	t, err := newRetryTee(addr, batchHint, e.rec, "retry")
	if err != nil {
		return err
	}
	e.tee = t
	return nil
}

// EnableRetryTeeShared is EnableRetryTee over an already-open shared TeeSocket
// (one fd serving many per-connection egresses — see TeeSocket). CloseRetryTee
// on this Egress leaves the shared socket open; its owner closes it.
func (e *Egress) EnableRetryTeeShared(s *TeeSocket, batchHint int) {
	e.tee = newSharedTee(s, batchHint, e.rec, "retry")
}

// EnableLocalMirrorShared is EnableLocalMirror over a shared TeeSocket.
func (e *Egress) EnableLocalMirrorShared(s *TeeSocket, batchHint int) {
	e.mirror = newSharedTee(s, batchHint, e.rec, "mirror")
}

// CoalArmed reports whether this Egress has a BRC-142 coalescing buffer (i.e.
// -coalesce is on and it was built with batchHint>1). Observability/tests.
func (e *Egress) CoalArmed() bool { return e.coal != nil }

// SetReliable marks this Egress as belonging to the reliable (TCP) submission
// lane: Flush re-submits the unsent remainder of a partial WriteBatch instead
// of dropping it (bounded — see Flush). Call before the first Enqueue.
func (e *Egress) SetReliable(v bool) { e.reliable = v }

// RetryTeeFailed reports datagrams the tee could not deliver (0 when disabled).
func (e *Egress) RetryTeeFailed() uint64 {
	if e.tee == nil {
		return 0
	}
	return e.tee.Failed()
}

// CloseRetryTee releases the tee socket.
func (e *Egress) CloseRetryTee() error {
	if e.tee == nil {
		return nil
	}
	return e.tee.close()
}

// EnableLocalMirror mirrors egressed DATA datagrams to the co-located listener's
// dedicated loopback ingest at addr (host:port, normally "[::1]:<mirror-port>", a
// port the LISTENER binds exclusively — not the retry cache's). This is what
// delivers own-node frames to a collapsed edge's own listener, which cannot
// SSM-join its own source. Independent of EnableRetryTee (different target port).
func (e *Egress) EnableLocalMirror(addr string, batchHint int) error {
	t, err := newRetryTee(addr, batchHint, e.rec, "mirror")
	if err != nil {
		return err
	}
	e.mirror = t
	return nil
}

// LocalMirrorFailed reports datagrams the mirror could not deliver (0 when disabled).
func (e *Egress) LocalMirrorFailed() uint64 {
	if e.mirror == nil {
		return 0
	}
	return e.mirror.Failed()
}

// CloseLocalMirror releases the mirror socket.
func (e *Egress) CloseLocalMirror() error {
	if e.mirror == nil {
		return nil
	}
	return e.mirror.close()
}

// PoolGet returns a fragment-sized buffer from the pool, or nil if
// fragmentation is disabled on the forwarder. Caller passes the returned
// pointer to EnqueueDataPooled/EnqueueControlPooled so the buffer is
// recycled at Flush.
func (e *Egress) PoolGet() *[]byte {
	if e.pool == nil {
		return nil
	}
	return e.pool.Get().(*[]byte)
}

// EnqueueData queues raw for fan-out to every target with destination dst.
// raw must remain valid until the next Flush call. groupIdx and size are
// captured for the PacketForwarded metric at Flush time.
func (e *Egress) EnqueueData(raw []byte, dst net.UDPAddr, groupIdx uint32, workerID int) {
	e.enqueue(raw, dst, msgMeta{
		groupIdx: groupIdx,
		size:     len(raw),
		workerID: workerID,
		teeable:  true,
	}, nil)
}

// EnqueueDataPooled is EnqueueData where raw was obtained via PoolGet. The
// backing buffer (passed by pointer to the original slice) is returned to
// the pool after Flush completes.
func (e *Egress) EnqueueDataPooled(raw []byte, dst net.UDPAddr, groupIdx uint32, workerID int, pooled *[]byte) {
	e.enqueue(raw, dst, msgMeta{
		groupIdx: groupIdx,
		size:     len(raw),
		workerID: workerID,
		teeable:  true,
	}, pooled)
}

// EnqueueControl queues raw for fan-out to every target as a control-plane
// datagram. label is the metrics.ControlFrameForwarded label fired per
// target at Flush.
func (e *Egress) EnqueueControl(raw []byte, dst net.UDPAddr, label string, workerID int) {
	e.enqueue(raw, dst, msgMeta{
		size:      len(raw),
		workerID:  workerID,
		ctrlLabel: label,
	}, nil)
}

// EnqueueControlPooled is EnqueueControl with a pool-recycled backing buffer.
func (e *Egress) EnqueueControlPooled(raw []byte, dst net.UDPAddr, label string, workerID int, pooled *[]byte) {
	e.enqueue(raw, dst, msgMeta{
		size:      len(raw),
		workerID:  workerID,
		ctrlLabel: label,
	}, pooled)
}

// EnqueueCacheable is EnqueueControl for a control-GROUP datagram that the
// co-located retry cache can nonetheless answer NACKs for: BRC-131 block,
// BRC-132 subtree data and BRC-134 anchor frames (and their BRC-130 fragments)
// all reach multicast through this path because they address a fixed control
// group rather than a sharded data group, but every one of them is
// SeqNum-stamped before it gets here and is cached by class at the far end.
//
// They are metered as control frames (same ctrlLabel) and teed as data. Use
// EnqueueControl only for genuinely unstamped control traffic, which the cache
// would reject as a decode_error.
func (e *Egress) EnqueueCacheable(raw []byte, dst net.UDPAddr, label string, workerID int) {
	e.enqueue(raw, dst, msgMeta{
		size:      len(raw),
		workerID:  workerID,
		ctrlLabel: label,
		teeable:   true,
	}, nil)
}

// EnqueueCacheablePooled is EnqueueCacheable with a pool-recycled backing buffer.
func (e *Egress) EnqueueCacheablePooled(raw []byte, dst net.UDPAddr, label string, workerID int, pooled *[]byte) {
	e.enqueue(raw, dst, msgMeta{
		size:      len(raw),
		workerID:  workerID,
		ctrlLabel: label,
		teeable:   true,
	}, pooled)
}

func (e *Egress) enqueue(raw []byte, dst net.UDPAddr, m msgMeta, pooled *[]byte) {
	e.meta = append(e.meta, m)
	if pooled != nil {
		e.pooledBufs = append(e.pooledBufs, pooled)
	}
	// Data frames address a stable per-(target, group) multicast destination,
	// so cache it; control frames carry an arbitrary dst and always build fresh.
	isData := m.ctrlLabel == ""
	// Mirror to the co-located cache. Everything the cache can key by
	// (HashKey, SeqNum) — see msgMeta.teeable, which is what decides this and is
	// NOT the same question as "is this a control frame" for metrics.
	// This sits in enqueue deliberately — it is the one funnel every frame passes
	// through AFTER SeqNum stamping and AFTER fragmentation, so the cached bytes
	// are exactly what a NACK will ask for. Teeing any earlier would populate the
	// cache with frames that do not match the requests it must answer, which is
	// worse than an empty cache.
	if e.tee != nil && m.teeable {
		e.tee.append(raw)
	}
	// Mirror to the co-located listener (own-node delivery). Same funnel as the
	// retry tee but a distinct target port the listener binds exclusively, and
	// deliberately still DATA-only: this is a DELIVERY path, not a cache, so
	// widening it changes what a co-located consumer receives rather than what
	// can be repaired. It is disabled on every tier that runs egress-loop (the
	// own-source SSM join carries own-node delivery there instead), so it is not
	// on the path this teeable split was fixing. Revisit it with the same-edge
	// delivery work, not here.
	if e.mirror != nil && isData {
		e.mirror.append(raw)
	}
	cacheable := isData && m.groupIdx < maxGroupCache
	for i := range e.targets {
		var addr *net.UDPAddr
		if cacheable {
			if int(m.groupIdx) >= len(e.addrCache[i]) {
				// Grow to the next power of two covering groupIdx (≤ maxGroupCache).
				// Rare: fires once per Egress per band above the small initial size.
				n := len(e.addrCache[i]) * 2
				for n <= int(m.groupIdx) {
					n *= 2
				}
				if n > maxGroupCache {
					n = maxGroupCache
				}
				grown := make([]*net.UDPAddr, n)
				copy(grown, e.addrCache[i])
				e.addrCache[i] = grown
			}
			if addr = e.addrCache[i][m.groupIdx]; addr == nil {
				addr = &net.UDPAddr{IP: dst.IP, Port: dst.Port, Zone: e.targets[i].Iface.Name}
				e.addrCache[i][m.groupIdx] = addr
			}
		} else {
			addr = &net.UDPAddr{IP: dst.IP, Port: dst.Port, Zone: e.targets[i].Iface.Name}
		}
		e.msgs[i] = append(e.msgs[i], ipv6.Message{
			Buffers: [][]byte{raw},
			Addr:    addr,
		})
	}
}

// EgressWriteFunc delivers one queued datagram — the BRC frame payload raw to
// multicast destination dst — for target index target. Returning a non-nil
// error stops that target's drain and is recorded like a sendmmsg write error.
type EgressWriteFunc func(target int, raw []byte, dst *net.UDPAddr) error

// FlushVia drains the per-batch egress queue through fn instead of the kernel
// WriteBatch (sendmmsg) path, then resets the queue and releases pooled buffers
// exactly like Flush, recording forwarded/dropped metrics identically. It lets
// an alternative egress transport reuse the forwarder's decode/stamp/addressing
// pipeline without forking it.
func (e *Egress) FlushVia(fn EgressWriteFunc) {
	for i := range e.targets {
		msgs := e.msgs[i]
		sent := 0
		attempts := 0
		var werr error
		for sent < len(msgs) {
			m := &msgs[sent]
			dst, _ := m.Addr.(*net.UDPAddr)
			err := fn(i, m.Buffers[0], dst)
			if err == nil {
				sent++
				attempts = 0
				continue
			}
			// Reliable lane (TCP submission): retry the SAME message with the
			// bounded budget before surrendering the remainder. The batched
			// cadence queues up to a whole batch per flush, and via-transports
			// (e.g. the ingress→spine pipeline client) self-heal by re-dialing
			// on the next Send — a first-error break here would turn one
			// transient disconnect into a whole-batch-remainder loss where the
			// old per-frame flush lost at most one frame. Non-reliable lanes
			// keep the original first-error surrender.
			if !e.reliable || attempts >= maxReliableRetries {
				werr = err
				break
			}
			attempts++
			time.Sleep(reliableRetryBackoff)
		}
		if e.reliable && (werr != nil || sent < len(msgs)) {
			e.logWriteError(i, len(msgs), sent, werr)
		}
		e.recordWrite(i, sent, werr)
		clear(e.msgs[i])
		e.msgs[i] = e.msgs[i][:0]
	}
	// The alternative-transport path (AF_XDP TX) must drain the tee too, or its
	// queue grows without bound for the whole life of the process. The tee stays
	// an ordinary batched socket here: AF_XDP bypasses the kernel stack for the
	// real egress, but nothing stops this process also issuing one sendmmsg to
	// loopback per batch.
	if e.tee != nil {
		e.tee.flush()
	}
	if e.mirror != nil {
		e.mirror.flush()
	}
	for _, p := range e.pooledBufs {
		e.pool.Put(p)
	}
	e.pooledBufs = e.pooledBufs[:0]
	e.meta = e.meta[:0]
}

// reliableRetryBackoff/maxReliableRetries bound the reliable lane's
// zero-progress retry loop: ~50 × 200µs ⇒ at most ~10ms of in-line stall per
// flush before the remainder is surrendered as write_error drops (logged +
// counted; the retry tee holds the frames, so BRC-126 NACK repair is the
// backstop). Unbounded blocking is not an option — a wedged NIC queue must
// not hang the submission lane forever.
const (
	reliableRetryBackoff = 200 * time.Microsecond
	maxReliableRetries   = 50
)

// isTransientSendErr reports whether a sendmmsg error is worth retrying:
// kernel buffer/qdisc backpressure (ENOBUFS), a not-ready socket
// (EAGAIN/EWOULDBLOCK — normally absorbed by the runtime poller, but surfaced
// on some paths), or an interrupted syscall (EINTR).
func isTransientSendErr(err error) bool {
	return errors.Is(err, syscall.ENOBUFS) ||
		errors.Is(err, syscall.EAGAIN) ||
		errors.Is(err, syscall.EWOULDBLOCK) ||
		errors.Is(err, syscall.EINTR)
}

// Flush writes all queued messages to each target via WriteBatch (sendmmsg
// on Linux; per-packet fallback elsewhere). Per-target write errors are
// recorded as EgressError; messages beyond WriteBatch's sent-count fire
// PacketDropped with reason "write_error". Pool buffers are released exactly
// once each before return.
//
// A reliable Egress (SetReliable — the TCP submission lane) re-submits the
// unsent remainder of a partial WriteBatch instead of dropping it: the
// batched-cadence TCP path queues up to a whole batch per flush, and a single
// ENOBUFS burst would otherwise drop the lot where the old per-frame path
// lost at most one frame. Zero-progress retries are bounded (see
// maxReliableRetries) so a wedged socket cannot hang the lane.
func (e *Egress) Flush() {
	for i := range e.targets {
		if len(e.msgs[i]) == 0 {
			continue
		}
		if e.reliable {
			e.flushReliable(i)
			continue
		}
		// writeBatchAll, not a bare WriteBatch: a partial write must be
		// re-submitted, or the entire batch after the first datagram is lost on
		// any non-Linux GOOS (see writeBatchAll). It still surrenders on the
		// first error, which is what keeps the lossy lane drop-and-count.
		sent, err := writeBatchAll(e.pcs[i], e.msgs[i], 0)
		if err != nil {
			e.logWriteError(i, len(e.msgs[i]), sent, err)
		}
		e.recordWrite(i, sent, err)
		// Clear slice contents to drop references to pooled buffers /
		// net.UDPAddrs so they can be GC'd or reused safely.
		clear(e.msgs[i])
		e.msgs[i] = e.msgs[i][:0]
	}
	// Tee AFTER real egress: the forward path must never wait on a cache-fill,
	// and pooled buffers are still valid until the release below.
	if e.tee != nil {
		e.tee.flush()
	}
	if e.mirror != nil {
		e.mirror.flush()
	}
	for _, p := range e.pooledBufs {
		e.pool.Put(p)
	}
	e.pooledBufs = e.pooledBufs[:0]
	e.meta = e.meta[:0]
}

// flushReliable drains target i re-submitting the unsent remainder of every
// partial WriteBatch. Progress resets the retry budget; zero-progress
// transient errors back off briefly and retry up to maxReliableRetries, after
// which (or on any hard error) the remainder is surrendered to the normal
// write_error accounting.
func (e *Egress) flushReliable(i int) {
	msgs := e.msgs[i]
	off := 0
	attempts := 0
	var werr error
	for off < len(msgs) {
		// writeBatchAll already re-submits an error-free partial write (the
		// non-Linux one-message-per-call case), so a nil error here means the
		// remainder is fully drained. Everything below is the reliable lane's
		// extra layer: retrying an ERRORED or stalled batch.
		sent, err := writeBatchAll(e.pcs[i], msgs[off:], 0)
		if sent > 0 {
			off += sent
			attempts = 0
			if off >= len(msgs) {
				break
			}
		}
		// errNoBatchProgress is the helper's stand-in for a (0, nil) result:
		// no errno to classify, so treat it like a transient stall rather than
		// a hard failure.
		if err != nil && !errors.Is(err, errNoBatchProgress) && !isTransientSendErr(err) {
			werr = err // hard error: surrender the remainder
			break
		}
		// Zero progress on a transient (or unclassifiable) result: bounded backoff.
		if attempts >= maxReliableRetries {
			werr = err
			if werr == nil {
				werr = errNoBatchProgress
			}
			break
		}
		attempts++
		time.Sleep(reliableRetryBackoff)
	}
	if werr != nil || off < len(msgs) {
		e.logWriteError(i, len(msgs), off, werr)
	}
	e.recordWrite(i, off, werr)
	clear(e.msgs[i])
	e.msgs[i] = e.msgs[i][:0]
}

// logWriteError emits a category-8 (OS/NIC) log for a WriteBatch failure,
// classifying the kernel errno (ENOBUFS = socket buffer / qdisc backpressure)
// and reporting how many datagrams were dropped. It is throttled per interface
// to log-once-then-count so a sustained error storm cannot flood the log.
func (e *Egress) logWriteError(targetIdx, queued, sent int, err error) {
	iface := e.targets[targetIdx].Iface.Name
	emit, suppressed := e.throttle.Allow(iface)
	if !emit {
		return
	}
	dropped := queued
	if sent > 0 {
		dropped = queued - sent
	}
	attrs := []any{
		"iface", iface,
		"queued", queued,
		"dropped", dropped,
		"err", err,
		"suppressed", suppressed,
	}
	if errno, ok := errnoOf(err); ok {
		attrs = append(attrs, "errno", errno.Error(), "syscall", "sendmmsg")
		if errors.Is(err, syscall.ENOBUFS) {
			e.log.Warn("egress ENOBUFS: kernel send buffer / qdisc backpressure", attrs...)
			return
		}
	}
	e.log.Warn("egress write error", attrs...)
}

// errnoOf extracts a syscall.Errno from a (possibly wrapped) error.
func errnoOf(err error) (syscall.Errno, bool) {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno, true
	}
	return 0, false
}

// recordWrite fires the per-target metrics for one drain result. sent is the
// number of messages accepted; meta[0:sent] count as forwarded, meta[sent:] as
// write-error drops. WriteBatch can report -1 (not 0) when the underlying
// sendmmsg fails before any message is written, so sent is clamped to
// [0, len(meta)] to keep both loops in bounds.
func (e *Egress) recordWrite(targetIdx, sent int, err error) {
	if e.rec == nil {
		return
	}
	if sent < 0 {
		sent = 0
	} else if sent > len(e.meta) {
		sent = len(e.meta)
	}
	iface := e.targets[targetIdx].Iface.Name
	if err != nil {
		// Match the legacy behaviour: one EgressError per failing target.
		e.rec.EgressError(iface, 0)
	}
	for j := 0; j < sent && j < len(e.meta); j++ {
		m := e.meta[j]
		if m.ctrlLabel != "" {
			e.rec.ControlFrameForwarded(m.ctrlLabel)
		} else {
			e.rec.PacketForwarded(iface, m.workerID, m.groupIdx, m.size)
		}
	}
	for j := sent; j < len(e.meta); j++ {
		m := e.meta[j]
		e.rec.PacketDropped(iface, m.workerID, "write_error")
	}
}
