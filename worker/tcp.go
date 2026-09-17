// Package worker — tcp.go provides TCPIngress: a TCP listener that accepts
// reliable frame delivery connections and feeds them into the shared Forwarder.
//
// # Protocol
//
// A connection carries ONE of three grammars, detected from its first bytes —
// the TCP mirror of the UDP magic-detect unification (3-way per BRC-148):
//
//   - FRAMED: leads with the BSV network magic. A stream of BRC-12, BRC-124,
//     BRC-128, BRC-134, or BRC-149 frames (and BRC-127 control datagrams) with
//     no framing envelope. The proxy reads the minimum header first (44 bytes
//     for BRC-12, extended to 92 for the 92-byte-header versions), then the
//     declared payload, and forwards each assembled frame via
//     [forwarder.Forwarder.DispatchClass].
//   - BEEF: leads with the 0xBEEF record tag. A stream of BRC-149 submission
//     records (objfmt.ClassBEEF), each admitted via SubmitBEEF.
//   - BARE: anything else. A stream of bare transactions (BRC-30 EF
//     submissions; BRC-12 raw only when EF-native is off), one after another,
//     self-delimiting by transaction structure (objfmt.ClassTx). Each is
//     admitted through the shared bare path — TxSize-validated, EF-gated,
//     reframed to an unstamped BRC-124/128 frame, stamped from the observed
//     source — exactly as a bare UDP submission.
//
// The connection commits to its grammar for its lifetime.
//
// A [bufio.Reader] (64 KiB) absorbs kernel round-trips under burst load.
// Framed input is forwarded verbatim.
package worker

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/shard-common/shard"
	"github.com/lightwebinc/shard-proxy/forwarder"
	"github.com/lightwebinc/shard-proxy/metrics"
)

const tcpBufSize = 64 * 1024 // 64 KiB read buffer per TCP connection

// maxTCPBatch caps how many frames a connection may accumulate before a
// forced egress flush. Bounds per-frame added latency, the egress queue
// footprint, and the worst-case surrender size of the reliable lane's
// bounded retry (see Egress.Flush). 64 matches the UDP recvmmsg batch depth.
const maxTCPBatch = 64

// maxEgressPool caps the egress socket pool (see Run). Sized to the point of
// diminishing returns: the pool shards Go's per-fd write lock, but past a
// handful of fds the single NIC tx queue below is the residual serializer.
const maxEgressPool = 8

// tcpBatcher amortizes egress flushes across the frames a connection already
// has buffered. The old per-frame flush took the (shared) egress socket's
// write lock — and issued the tee + mirror sendmmsg — at the full frame rate,
// which measured as an idle-CPU ~7-9k pps cap: every connection convoyed on
// one fd's write lock, whose holder parks in the netpoller under NIC-queue
// backpressure. Batching cuts the lock-acquisition (and tee/mirror syscall)
// rate by up to maxTCPBatch. flushingReader preserves the lane's latency
// contract: the batch drains before ANY read that could block on the kernel,
// so a frame is never held back while the connection sits idle or mid-frame.
type tcpBatcher struct {
	ti      *TCPIngress
	egr     *forwarder.Egress
	pending int
}

func (b *tcpBatcher) add() {
	b.pending++
	if b.pending >= maxTCPBatch {
		b.flush()
	}
}

func (b *tcpBatcher) flush() {
	if b.pending > 0 {
		b.ti.flushEgr(b.egr)
		b.pending = 0
	}
}

// flushingReader drains the connection's pending egress before every
// underlying (kernel) read. bufio calls it only when its buffer cannot
// satisfy the current need — exactly the moments the goroutine may block —
// so the flush-before-blocking-read guarantee holds for every grammar
// (framed, bare, BEEF) through this single funnel: no frame ever sits
// enqueued-but-unsent across a header, payload, or objfmt read that has to
// wait on the peer, including the partial-next-frame case where the buffer
// is non-empty but short of a complete frame.
//
// When coalescing is on, linger > 0 turns the eager flush into a bounded
// accumulate window: rather than flush the instant the buffer empties, give
// same-flow frames up to linger to arrive so bundles pack denser (higher
// members/bundle) on a fast network. A frame is delayed at most linger; under
// sustained load the read returns immediately (no timeout) and the batch fills
// to maxTCPBatch instead. The in-progress frame (if the buffer emptied
// mid-frame) is not yet in the batch, so flushing on a linger timeout never
// strands it — the blocking retry completes it.
type flushingReader struct {
	conn   net.Conn
	b      *tcpBatcher
	linger time.Duration // >0 (coalesce only): accumulate window before flush
}

func (f *flushingReader) Read(p []byte) (int, error) {
	if f.linger > 0 && f.b.pending > 0 {
		_ = f.conn.SetReadDeadline(time.Now().Add(f.linger))
		n, err := f.conn.Read(p)
		_ = f.conn.SetReadDeadline(time.Time{}) // clear
		var ne net.Error
		if err != nil && errors.As(err, &ne) && ne.Timeout() {
			// Window elapsed with no new frame: flush the accumulated batch,
			// then block for the next frame (deadline cleared).
			f.b.flush()
			return f.conn.Read(p)
		}
		return n, err
	}
	f.b.flush()
	return f.conn.Read(p)
}

// TCPIngress listens for TCP connections carrying a stream of BRC-12, BRC-124, or BRC-128 frames
// and forwards each frame via the shared [forwarder.Forwarder].
type TCPIngress struct {
	fwd      *forwarder.Forwarder
	ifaces   []*net.Interface
	rec      *metrics.Recorder
	log      *slog.Logger
	class    forwarder.IngressClass
	via      forwarder.EgressWriteFunc
	viaFlush func() int
	// retryTee mirrors egressed DATA datagrams to a co-located retry cache.
	// Each ingress path builds its OWN Egress, so the tee must be enabled on
	// every one of them — a tee wired only into the UDP worker silently misses
	// everything submitted over this path.
	retryTee string
	// localMirror mirrors egressed DATA datagrams to the co-located LISTENER's
	// dedicated ingest (own-node delivery). Like retryTee, must be set on EVERY
	// ingress path's Egress.
	localMirror string
	// admit, when set, observes every submission this listener takes off the
	// wire, at the SAME point the UDP worker loop accounts for a received
	// datagram: after the read, before DispatchClass routes (or drops) it. A
	// downstream build uses it for per-submitter admitted-frame/byte counters,
	// which would otherwise miss the TCP lanes entirely — and TCP is the
	// supported submission transport, so an ingress-side attribution metric
	// wired only into the UDP loop reads zero on a real fabric. nil = no hook,
	// and the call sites cost one nil check.
	admit AdmitFunc
	// connAdmit is admit's per-connection form: it also receives the LOCAL
	// address the submitter dialed. A downstream build attributes admission by
	// destination address (one /128 per submitter), which the wire never
	// carries — only the accepted socket knows it. nil = no hook.
	connAdmit ConnAdmitFunc
	// coalesceLinger, when >0 AND coalescing is armed on a connection, is the
	// bounded window a connection accumulates same-flow frames before flushing,
	// trading up to this much added latency for denser bundles on a fast
	// network. Ignored when coalescing is off (the latency-first eager flush).
	coalesceLinger time.Duration
}

// SetRetryTee enables mirroring of egressed DATA datagrams to a co-located retry
// endpoint's cache-ingest address. See forwarder/retrytee.go.
func (ti *TCPIngress) SetRetryTee(addr string) { ti.retryTee = addr }

// SetCoalesceLinger sets the bounded accumulate window used when coalescing is
// on (see coalesceLinger). 0 keeps the latency-first eager flush.
func (ti *TCPIngress) SetCoalesceLinger(d time.Duration) { ti.coalesceLinger = d }

// SetLocalMirror enables mirroring of egressed DATA datagrams to the co-located
// listener's dedicated loopback ingest (own-node delivery).
func (ti *TCPIngress) SetLocalMirror(addr string) { ti.localMirror = addr }

// AdmitFunc observes one submission admitted at an ingress socket: frames is
// what it counts as on the fabric (1 for every grammar this package reads off a
// stream — a framed frame, a bare transaction, a control extension, a BEEF
// record) and bytes is what was read for it. It is called on the connection
// goroutine, so an implementation must be cheap and non-blocking; a counter
// increment is the intended use.
type AdmitFunc func(frames, bytes int)

// SetAdmitHook installs fn as this listener's admission observer. Call before
// [Run]. Attribution only — the hook cannot reject, and nothing downstream reads
// what it records.
func (ti *TCPIngress) SetAdmitHook(fn AdmitFunc) { ti.admit = fn }

// ConnAdmitFunc is [AdmitFunc] with the accepted socket's LOCAL address — the
// destination the submitter dialed. Same contract: cheap, non-blocking,
// attribution only.
type ConnAdmitFunc func(local net.Addr, frames, bytes int)

// SetConnAdmitHook installs fn as this listener's per-connection admission
// observer (see [ConnAdmitFunc]). Both hooks may be set; each admission
// reaches both. Call before [Run].
func (ti *TCPIngress) SetConnAdmitHook(fn ConnAdmitFunc) { ti.connAdmit = fn }

// admitFor binds this connection's observer: nil when no hook is set, so the
// per-submission cost stays one nil check.
func (ti *TCPIngress) admitFor(conn net.Conn) AdmitFunc {
	if ti.admit == nil && ti.connAdmit == nil {
		return nil
	}
	local := conn.LocalAddr()
	return func(frames, bytes int) {
		if ti.admit != nil {
			ti.admit(frames, bytes)
		}
		if ti.connAdmit != nil {
			ti.connAdmit(local, frames, bytes)
		}
	}
}

// NewTCPIngress constructs a TCPIngress. No sockets are opened until [Run] is
// called.
func NewTCPIngress(fwd *forwarder.Forwarder, ifaces []*net.Interface, rec *metrics.Recorder) *TCPIngress {
	return &TCPIngress{
		fwd:    fwd,
		ifaces: ifaces,
		rec:    rec,
		log:    slog.Default().With("component", "tcp-ingress"),
	}
}

// SetIngressClass sets the frame-class gate for this TCP listener. The zero
// value ([forwarder.IngressPrivileged]) accepts every frame class;
// [forwarder.IngressTransaction] rejects block/coinbase/subtree-data frames.
// Call before [Run].
func (ti *TCPIngress) SetIngressClass(c forwarder.IngressClass) {
	ti.class = c
}

// SetFlushVia replaces the default kernel-multicast egress flush with
// [forwarder.Egress.FlushVia] through fn, followed by flush (which reports the
// frames written). Used by an ingress-mode proxy to ship admitted lane frames
// to its spine over a reliable pipeline instead of emitting multicast locally.
// Call before [Run]. flush may be nil.
func (ti *TCPIngress) SetFlushVia(fn forwarder.EgressWriteFunc, flush func() int) {
	ti.via, ti.viaFlush = fn, flush
}

// flushEgr drains egr by the configured egress path: FlushVia + the sink's
// flush when SetFlushVia was called, the kernel multicast Flush otherwise.
func (ti *TCPIngress) flushEgr(egr *forwarder.Egress) {
	if egr == nil {
		return
	}
	// Drain the BRC-142 coalescing buffer into bundle datagrams BEFORE the
	// egress flush. No-op unless -coalesce is on AND this Egress has a coal
	// buffer (batchHint>1). This is what makes coalescing SAFE on the reliable
	// TCP lane: the old per-frame path never called FlushCoalesced (members
	// stranded — the documented loss hazard), but the batched cadence funnels
	// EVERY flush path (batcher, flushingReader-before-blocking-read, tail
	// defer) through here, so no member is ever left un-bundled across a read
	// or on close. Bundles cut the fabric fan-out + GRE-encap packet rate ~R×,
	// relieving the small single-queue VM's softirq ceiling.
	ti.fwd.FlushCoalesced(egr, -1)
	if ti.via != nil {
		egr.FlushVia(ti.via)
		if ti.viaFlush != nil {
			ti.viaFlush()
		}
		return
	}
	egr.Flush()
}

// Run starts the TCP accept loop on listenAddr:listenPort. The listener is
// dual-stack (IPv4 + IPv6) when listenAddr is empty/wildcard — public ingress
// must admit IPv4 submitters; the fabric behind it stays IPv6. It blocks until
// done is closed. Each accepted connection is handled in its own goroutine.
func (ti *TCPIngress) Run(listenAddr string, listenPort int, done <-chan struct{}) error {
	addr := fmt.Sprintf("%s:%d", listenAddr, listenPort)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("tcp-ingress: listen %s: %w", addr, err)
	}

	// Close the listener when done is signalled, unblocking Accept.
	go func() {
		<-done
		_ = ln.Close()
	}()

	if ti.rec != nil {
		ti.rec.TCPIngressReady()
	}
	ti.log.Info("TCP ingress ready", "addr", ln.Addr())

	// Egress socket pool: P independent target sets, connections sharded
	// across them by accept order. Every connection used to alias ONE shared
	// multicast egress socket; each per-frame WriteBatch then serialized on
	// that single fd's write lock, and a holder parked in the netpoller under
	// NIC-queue backpressure convoyed every other connection — measured as a
	// sender-independent ~7-9k pps cap with idle CPU that WORSENED as
	// connections were added. P sockets shard the lock domain. A connection
	// keeps its slot for life, so its own frames stay FIFO on one socket;
	// cross-connection interleaving is unchanged (it was already arbitrary
	// under the shared-fd lock).
	poolN := runtime.GOMAXPROCS(0)
	if poolN > maxEgressPool {
		poolN = maxEgressPool
	}
	if poolN < 1 {
		poolN = 1
	}
	pools := make([][]forwarder.Target, 0, poolN)
	for k := 0; k < poolN; k++ {
		t, err := ti.fwd.OpenTargets(ti.ifaces, k == 0) // probe the first set only
		if err != nil {
			for _, p := range pools {
				forwarder.CloseTargets(p, ti.log)
			}
			return fmt.Errorf("tcp-ingress: open targets: %w", err)
		}
		pools = append(pools, t)
	}
	defer func() {
		for _, p := range pools {
			forwarder.CloseTargets(p, ti.log)
		}
	}()

	// Shared tee/mirror sockets, one pair per pool slot. Each accepted
	// connection used to dial two private loopback sockets — a per-accept
	// cost that contributed to the measured many-connection collapse. Now a
	// connection takes only a per-Egress buffer over its slot's shared fd;
	// the tee/mirror FUNCTION (retry-cache fill, own-node delivery) is
	// unchanged, frame for frame. Closed after connWG.Wait() (defer LIFO), so
	// every connection's final flush lands on a live socket.
	// dialTeeSocks dials one shared socket per pool slot; on any failure it
	// closes the already-dialed slots and disables the feature (nil return) —
	// the tee is an optimisation and must never fail the forward path, but a
	// partial array would leak the dialed fds.
	dialTeeSocks := func(addr, what string) []*forwarder.TeeSocket {
		socks := make([]*forwarder.TeeSocket, poolN)
		for k := range socks {
			s, err := forwarder.NewTeeSocket(addr)
			if err != nil {
				ti.log.Error(what+" disabled", "addr", addr, "err", err)
				for _, d := range socks[:k] {
					_ = d.Close()
				}
				return nil
			}
			socks[k] = s
		}
		return socks
	}
	var teeSocks, mirSocks []*forwarder.TeeSocket
	if ti.retryTee != "" {
		teeSocks = dialTeeSocks(ti.retryTee, "retry tee")
	}
	if ti.localMirror != "" {
		mirSocks = dialTeeSocks(ti.localMirror, "local mirror")
	}
	defer func() {
		for _, s := range teeSocks {
			if s != nil {
				_ = s.Close()
			}
		}
		for _, s := range mirSocks {
			if s != nil {
				_ = s.Close()
			}
		}
	}()

	var connWG sync.WaitGroup
	defer connWG.Wait()

	var connSeq atomic.Uint64

	for {
		conn, err := ln.Accept()
		if err != nil {
			if isClosedErr(err) {
				return nil
			}
			ti.log.Warn("Accept error", "err", err)
			continue
		}
		if ti.rec != nil {
			ti.rec.TCPConnectionAccepted()
		}
		connWG.Add(1)
		go func() {
			defer connWG.Done()
			// Shutdown watcher with a per-connection exit: a bare <-done watcher
			// outlives its connection (parked until listener shutdown), leaking a
			// goroutine + the conn reference per historical connection under
			// short-lived-connection churn.
			stop := make(chan struct{})
			defer close(stop)
			go func() {
				select {
				case <-done:
					_ = conn.Close()
				case <-stop:
				}
			}()
			// Each connection owns its own Egress (single-goroutine contract);
			// the SOCKETS under it come from the pool slot. batchHint =
			// maxTCPBatch sizes the per-batch queues AND arms the BRC-142
			// coalescer (NewEgress gates coal on `fw.coalesce && batchHint>1`).
			// With -coalesce OFF (fw.coalesce=false) coal stays nil and this is
			// byte-for-byte the plain batched egress; with it ON, coal is
			// drained safely via flushEgr->FlushCoalesced on EVERY flush path —
			// the batched cadence is what removes the old TCP+coalescing
			// member-stranding hazard.
			// Same-source ordering note: proxy-stamped SeqNums are keyed by
			// (source IP, group, subtree) with no port, so two connections from
			// ONE submitter IP share a per-flow counter. Under batched cadence,
			// one connection can hold a batch while another races ahead on a
			// different slot, so same-flow SeqNums may reach the wire inverted by
			// up to maxTCPBatch frames (was a ~µs stamp-vs-flush race pre-batch).
			// This stays well under the listener's NACK hold-off (0-200ms): at
			// any real rate a 64-frame window drains in <1ms, and a stalled
			// connection surrenders after ~10ms (flushReliable), so the inversion
			// never outlasts the hold-off — no spurious repair. A submitter that
			// needs strict on-wire ordering uses one connection (per-connection
			// order is always preserved).
			slot := int(connSeq.Add(1)-1) % poolN
			egr := forwarder.NewEgress(ti.fwd, pools[slot], maxTCPBatch, ti.rec)
			egr.SetReliable(true)
			if teeSocks != nil {
				egr.EnableRetryTeeShared(teeSocks[slot], maxTCPBatch)
			}
			if mirSocks != nil {
				egr.EnableLocalMirrorShared(mirSocks[slot], maxTCPBatch)
			}
			defer ti.flushEgr(egr) // drain the tail batch on close/error paths
			ti.handleConn(conn, egr)
		}()
	}
}

// handleConn reads a stream of BRC-12, BRC-124, or BRC-128 frames from conn
// and forwards each via the per-connection Egress. The connection is closed
// on any read error or protocol violation. Each goroutine owns its own
// encode and assembly buffers; egr is flushed once per frame so latency
// for reliable-delivery senders is not held up by batching.
func (ti *TCPIngress) handleConn(conn net.Conn, egr *forwarder.Egress) {
	defer func() { _ = conn.Close() }()
	remote := conn.RemoteAddr()
	ti.log.Debug("TCP connection accepted", "remote", remote)
	admit := ti.admitFor(conn)

	// Egress flushes are batched by cadence: enqueue per frame, flush when
	// maxTCPBatch accumulates or — via flushingReader — before any read that
	// would block on the kernel. See tcpBatcher for the rationale.
	bat := &tcpBatcher{ti: ti, egr: egr}
	// Linger only when coalescing is actually armed on this connection's Egress
	// — the plain reliable lane keeps the latency-first eager flush.
	linger := time.Duration(0)
	if egr != nil && egr.CoalArmed() {
		linger = ti.coalesceLinger
	}
	br := bufio.NewReaderSize(&flushingReader{conn: conn, b: bat, linger: linger}, tcpBufSize)
	hdrBuf := make([]byte, frame.HeaderSize)

	// Grammar detection (once per connection), 3-way per BRC-148: a framed
	// stream leads with the BSV network magic; a BEEF submission-record
	// stream leads with the 0xBEEF record tag; anything else is a bare
	// transaction stream. A tx begins with its little-endian version
	// (0x01/0x02, byte[1] 0x00), which can never collide with magic byte
	// 0xE3 or tag byte 0xBE.
	if first, err := br.Peek(4); err != nil {
		return
	} else if first[0] == 0xBE && first[1] == 0xEF {
		ti.handleBEEFConn(br, remote, egr, bat, admit)
		return
	} else if first[0] != 0xE3 || first[1] != 0xE1 || first[2] != 0xF3 || first[3] != 0xE8 {
		ti.handleBareConn(br, remote, egr, bat, admit)
		return
	}

	for {
		// Step 1: read the minimum header (44 bytes). This covers both
		// BRC-12 (complete header) and the leading 44 bytes of a BRC-124/BRC-128 header.
		if _, err := io.ReadFull(br, hdrBuf[:frame.HeaderSizeLegacy]); err != nil {
			if err != io.EOF && !isClosedErr(err) {
				ti.log.Debug("TCP read header error", "remote", remote, "err", err)
			}
			return
		}

		// Validate magic before reading further.
		if hdrBuf[0] != 0xE3 || hdrBuf[1] != 0xE1 ||
			hdrBuf[2] != 0xF3 || hdrBuf[3] != 0xE8 {
			ti.log.Warn("TCP bad magic; closing connection", "remote", remote)
			return
		}

		var hdrSize, payLen int
		switch hdrBuf[6] {
		case frame.MsgTypeSubtreeGroupAnnounce:
			// BRC-127 SubtreeGroupAnnounce: 64-byte fixed datagram.
			// 44 bytes already read; read the remaining 20 bytes.
			// Explicitly heap-allocated per frame: the enqueued reference now
			// outlives this iteration (batched cadence), so a reused buffer
			// would alias a later control frame over an unflushed earlier one.
			ctrlBuf := make([]byte, frame.SubtreeGroupAnnounceSize)
			copy(ctrlBuf[:frame.HeaderSizeLegacy], hdrBuf[:frame.HeaderSizeLegacy])
			if _, err := io.ReadFull(br, ctrlBuf[frame.HeaderSizeLegacy:frame.SubtreeGroupAnnounceSize]); err != nil {
				ti.log.Debug("TCP read SubtreeGroupAnnounce extension error", "remote", remote, "err", err)
				return
			}
			if ti.rec != nil {
				ti.rec.TCPBytesReceived(frame.SubtreeGroupAnnounceSize)
			}
			if admit != nil {
				admit(1, frame.SubtreeGroupAnnounceSize)
			}
			ti.fwd.ForwardControl(egr, ctrlBuf, shard.GroupSubtreeGroupAnnounce, ti.fwd.EgressPort())
			bat.add()
			continue
		case frame.FrameVerV1:
			hdrSize = frame.HeaderSizeLegacy
			payLen = int(uint32(hdrBuf[40])<<24 | uint32(hdrBuf[41])<<16 |
				uint32(hdrBuf[42])<<8 | uint32(hdrBuf[43]))
		case frame.FrameVerV2, frame.FrameVerV4, frame.FrameVerV5, frame.FrameVerV6, frame.FrameVerV9:
			// Step 2: read the remaining 48 bytes to complete the 92-byte header
			// (BRC-124/BRC-128, BRC-131 block control, BRC-132 subtree data, or
			// BRC-148 BEEF object; includes PayLen at bytes 88–91).
			if _, err := io.ReadFull(br, hdrBuf[frame.HeaderSizeLegacy:frame.HeaderSize]); err != nil {
				ti.log.Debug("TCP read header extension error", "remote", remote, "err", err)
				return
			}
			hdrSize = frame.HeaderSize
			payLen = int(uint32(hdrBuf[88])<<24 | uint32(hdrBuf[89])<<16 |
				uint32(hdrBuf[90])<<8 | uint32(hdrBuf[91]))
		default:
			ti.log.Warn("TCP unsupported frame version; closing connection",
				"remote", remote, "ver", hdrBuf[6])
			return
		}

		// Step 3: bound the DECLARED length before allocating against it. A
		// 92-byte header can otherwise command a ~4 GiB allocation (payLen is
		// attacker-declared). The general ceiling matches the object-stream
		// reader (objfmt.DefaultMaxObject); FrameVer 0x09 additionally honours
		// the operator's -beef-max-object-bytes, per BRC-149's ingress MUST —
		// without this, pre-framing over TCP bypasses the submission bound.
		if payLen < 0 || payLen > objfmt.DefaultMaxObject {
			ti.log.Warn("TCP declared payload exceeds stream ceiling; closing",
				"remote", remote, "ver", hdrBuf[6], "paylen", payLen)
			return
		}
		if hdrBuf[6] == frame.FrameVerV9 {
			if maxObj := ti.fwd.BEEFMaxObject(); maxObj > 0 && payLen > maxObj {
				ti.log.Warn("TCP BEEF frame exceeds -beef-max-object-bytes; closing",
					"remote", remote, "paylen", payLen, "max", maxObj)
				return
			}
		}
		frameBuf := make([]byte, hdrSize+payLen)
		copy(frameBuf, hdrBuf[:hdrSize])
		if payLen > 0 {
			if _, err := io.ReadFull(br, frameBuf[hdrSize:]); err != nil {
				ti.log.Debug("TCP read payload error", "remote", remote, "err", err)
				return
			}
		}

		if ti.rec != nil {
			ti.rec.TCPBytesReceived(hdrSize + payLen)
		}
		if admit != nil {
			admit(1, hdrSize+payLen)
		}
		// The full frame is already read off the stream, so a class
		// rejection (block/coinbase/subtree on a transaction-only socket)
		// drops it without corrupting stream framing. DispatchClass is the
		// single routing authority shared with the UDP path.
		ti.fwd.DispatchClass(egr, frameBuf, remote, -1, ti.class)
		// Batched cadence: flushed by tcpBatcher at maxTCPBatch or before the
		// next kernel read blocks (flushingReader). frameBuf is fresh per
		// frame, so holding it enqueued across the batch is alias-safe.
		bat.add()
	}
}

// handleBareConn reads a stream of bare transactions (no frame header, no
// envelope; delimited by transaction structure via objfmt.ClassTx) and admits
// each through the shared [forwarder.Forwarder.DispatchClass] bare path:
// TxSize-validated, EF-gated by the forwarder's require-ef setting, reframed
// to an unstamped BRC-124/128 frame and stamped from the observed source —
// byte-for-byte the same admission a bare UDP submission gets. A malformed
// transaction desyncs the stream (there is no recovery boundary), so the
// connection closes.
// handleBEEFConn reads a stream of BRC-148 BEEF submission records (each an
// explicit length-carrying envelope: 0xBEEF tag ∥ recordVer ∥ topics ∥
// objectLen ∥ object) and admits each through the shared DispatchClass bare
// path, which expands the record into one FrameVer 0x09 frame per topic.
// BEEF is an open class, so the socket's IngressClass admits it regardless.
func (ti *TCPIngress) handleBEEFConn(br *bufio.Reader, remote net.Addr, egr *forwarder.Egress, bat *tcpBatcher, admit AdmitFunc) {
	rd := objfmt.NewReader(br, objfmt.ClassBEEF)
	for {
		rec, err := rd.Next()
		if err != nil {
			if err != io.EOF && !isClosedErr(err) {
				ti.log.Debug("beef record read error; closing connection", "remote", remote, "err", err)
			}
			return
		}
		if ti.rec != nil {
			ti.rec.TCPBytesReceived(len(rec))
		}
		if admit != nil {
			admit(1, len(rec))
		}
		// SubmitBEEF copies the object into fresh per-topic frame buffers, so
		// the Reader window may be reused on the next iteration — and the
		// enqueued per-topic buffers are batch-safe for the same reason.
		// Liveness: br is backed by the connection's flushingReader, so a
		// blocking rd.Next() drains the batch first.
		ti.fwd.DispatchClass(egr, rec, remote, -1, ti.class)
		bat.add()
	}
}

func (ti *TCPIngress) handleBareConn(br *bufio.Reader, remote net.Addr, egr *forwarder.Egress, bat *tcpBatcher, admit AdmitFunc) {
	rd := objfmt.NewReader(br, objfmt.ClassTx)
	for {
		obj, err := rd.Next()
		if err != nil {
			if err != io.EOF && !isClosedErr(err) {
				ti.log.Debug("bare tx read error; closing connection", "remote", remote, "err", err)
			}
			return
		}
		if ti.rec != nil {
			ti.rec.TCPBytesReceived(len(obj))
		}
		if admit != nil {
			admit(1, len(obj))
		}
		// DispatchBareTx (NOT DispatchClass): this connection committed to the
		// bare grammar, and the tx reader does not validate the version bytes,
		// so a crafted magic-prefixed "tx" would magic-route through
		// DispatchClass to the verbatim framed path and alias the reader's
		// reused window across the batch. The bare path always copies into a
		// fresh framed buffer, so the enqueued frame is batch-safe. Liveness:
		// br is backed by flushingReader, so a blocking rd.Next() drains first.
		ti.fwd.DispatchBareTx(egr, obj, remote, -1)
		bat.add()
	}
}
