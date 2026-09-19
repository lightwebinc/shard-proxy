// Package worker — objingress.go provides ObjectIngress: a TCP listener that
// accepts a single-class stream of BRC-143 subtree or BRC-144 block PUSH frames
// (header-stripped, self-delimiting by their own counts — see shard-common/
// objfmt) and reframes each into its multicast fabric frame for the forwarder.
//
// # Why a separate lane per class
//
// Push frames carry no magic and no FrameVer byte, so they cannot be routed on
// a shared socket the way framed input is: the class is known only from which
// port the object arrived on. Each class therefore rides its own dedicated,
// tunnel-bound port (subtree 8726, block 8727 by convention) — a miner submits
// as a tunnel consumer; the ports are never publicly exposed. This replaces the
// deprecated privileged multicast miner port.
//
// # Why TCP
//
// A subtree can reach ~128 MiB (millions of node hashes), far past any datagram,
// so objects are read from a byte stream with objfmt.Reader (which splits a
// single-class stream into whole objects with no outer framing). Each object is
// reframed with objfmt.MulticastBytes — subtree → BRC-132, block → the BRC-144
// body carried verbatim in a BRC-131 block-control frame — and dispatched
// through the shared forwarder exactly like any privileged frame (it stamps
// HashKey/SeqNum from the observed source and egresses to the control group).
package worker

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"

	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/shard-proxy/forwarder"
	"github.com/lightwebinc/shard-proxy/metrics"
)

// DefaultMaxObjectBytes bounds a single push object read off a connection. It
// covers a large subtree (Teranode's subtreevalidation_max_incoming_subtree_bytes
// default is 128 MiB) with headroom; a length field implying more is a protocol
// error that drops the connection.
const DefaultMaxObjectBytes = 256 << 20

// ObjectIngress accepts a single-class TCP stream of push objects (subtree or
// block) and reframes each into the fabric via the shared [forwarder.Forwarder].
type ObjectIngress struct {
	fwd       *forwarder.Forwarder
	ifaces    []*net.Interface
	rec       *metrics.Recorder
	log       *slog.Logger
	class     objfmt.Class
	maxObject int
	via       forwarder.EgressWriteFunc
	viaFlush  func() int
	// retryTee mirrors egressed DATA datagrams to a co-located retry cache.
	// Each ingress path builds its OWN Egress, so the tee must be enabled on
	// every one of them — a tee wired only into the UDP worker silently misses
	// everything submitted over this path.
	retryTee string
	// admit observes each object this lane takes off the wire, at the same
	// point the TCP lane and the UDP worker loop account for one. See
	// [TCPIngress.SetAdmitHook]; a push lane is an admission boundary too, so
	// leaving it unhooked would under-report a miner submitter's ingress.
	admit AdmitFunc
	// connAdmit is admit's per-connection form (see [TCPIngress.SetConnAdmitHook]).
	connAdmit ConnAdmitFunc
	// localMirror mirrors egressed DATA datagrams to the co-located LISTENER's
	// dedicated ingest (own-node delivery). Set on EVERY ingress path's Egress.
	localMirror string
}

// SetRetryTee enables mirroring of egressed DATA datagrams to a co-located retry
// endpoint's cache-ingest address. See forwarder/retrytee.go.
func (oi *ObjectIngress) SetRetryTee(addr string) { oi.retryTee = addr }

// SetLocalMirror enables mirroring of egressed DATA datagrams to the co-located
// listener's dedicated loopback ingest (own-node delivery).
func (oi *ObjectIngress) SetLocalMirror(addr string) { oi.localMirror = addr }

// SetAdmitHook installs fn as this lane's admission observer (see
// [TCPIngress.SetAdmitHook]). Call before [Run].
func (oi *ObjectIngress) SetAdmitHook(fn AdmitFunc) { oi.admit = fn }

// SetConnAdmitHook installs fn as this lane's per-connection admission
// observer (see [ConnAdmitFunc]). Call before [Run].
func (oi *ObjectIngress) SetConnAdmitHook(fn ConnAdmitFunc) { oi.connAdmit = fn }

// NewObjectIngress constructs an ObjectIngress for the given push class
// (objfmt.ClassSubtree or objfmt.ClassBlock). No sockets are opened until
// [ObjectIngress.Run] is called.
func NewObjectIngress(fwd *forwarder.Forwarder, ifaces []*net.Interface, rec *metrics.Recorder, class objfmt.Class) *ObjectIngress {
	return &ObjectIngress{
		fwd:       fwd,
		ifaces:    ifaces,
		rec:       rec,
		log:       slog.Default().With("component", "obj-ingress", "class", class.String()),
		class:     class,
		maxObject: DefaultMaxObjectBytes,
	}
}

// SetMaxObject overrides the single-object size bound. Call before [Run].
func (oi *ObjectIngress) SetMaxObject(n int) {
	if n > 0 {
		oi.maxObject = n
	}
}

// SetFlushVia replaces the default kernel-multicast egress flush with
// [forwarder.Egress.FlushVia] through fn, followed by flush (which reports the
// frames written). Used by an ingress-mode proxy to ship reframed push objects
// to its spine over a reliable pipeline instead of emitting multicast locally.
// Call before [Run]. flush may be nil.
func (oi *ObjectIngress) SetFlushVia(fn forwarder.EgressWriteFunc, flush func() int) {
	oi.via, oi.viaFlush = fn, flush
}

// flushEgr drains egr by the configured egress path: FlushVia + the sink's
// flush when SetFlushVia was called, the kernel multicast Flush otherwise.
func (oi *ObjectIngress) flushEgr(egr *forwarder.Egress) {
	if egr == nil {
		return
	}
	if oi.via != nil {
		egr.FlushVia(oi.via)
		if oi.viaFlush != nil {
			oi.viaFlush()
		}
		return
	}
	egr.Flush()
}

// Run starts the TCP accept loop on listenAddr:listenPort. The listener is
// dual-stack (IPv4 + IPv6) when listenAddr is empty/wildcard; the tunnel-side
// deployment fences the push lanes to miner-tier IPv6 sources at the firewall.
// It blocks until done is closed. Each accepted connection is handled in its
// own goroutine.
func (oi *ObjectIngress) Run(listenAddr string, listenPort int, done <-chan struct{}) error {
	addr := fmt.Sprintf("%s:%d", listenAddr, listenPort)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	go func() {
		<-done
		_ = ln.Close()
	}()
	oi.log.Info("object ingress ready", "addr", ln.Addr())

	targets, err := oi.fwd.OpenTargets(oi.ifaces, true)
	if err != nil {
		return err
	}
	defer forwarder.CloseTargets(targets, oi.log)

	var connWG sync.WaitGroup
	defer connWG.Wait()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if isClosedErr(err) {
				return nil
			}
			oi.log.Warn("Accept error", "err", err)
			continue
		}
		connWG.Add(1)
		go func() {
			defer connWG.Done()
			// Per-connection exit for the shutdown watcher (see tcp.go): a bare
			// <-done watcher leaks a goroutine + conn reference per historical
			// connection until listener shutdown.
			stop := make(chan struct{})
			defer close(stop)
			go func() {
				select {
				case <-done:
					_ = conn.Close()
				case <-stop:
				}
			}()
			// Per-connection Egress so the shared multicast sockets are not
			// mutated concurrently across goroutines; flush per object to keep
			// reliable-delivery latency low.
			egr := forwarder.NewEgress(oi.fwd, targets, 1, oi.rec)
			if oi.retryTee != "" {
				if err := egr.EnableRetryTee(oi.retryTee, 1); err != nil {
					oi.log.Error("retry tee disabled", "addr", oi.retryTee, "err", err)
				} else {
					defer func() { _ = egr.CloseRetryTee() }()
				}
			}
			if oi.localMirror != "" {
				if err := egr.EnableLocalMirror(oi.localMirror, 1); err != nil {
					oi.log.Error("local mirror disabled", "addr", oi.localMirror, "err", err)
				} else {
					defer func() { _ = egr.CloseLocalMirror() }()
				}
			}
			defer oi.flushEgr(egr)
			oi.handleConn(conn, egr)
		}()
	}
}

// handleConn reads whole push objects from conn and reframes each into the
// fabric. The connection is closed on EOF, read error, or a malformed object
// (which desyncs the single-class stream — there is no recovery point).
func (oi *ObjectIngress) handleConn(conn net.Conn, egr *forwarder.Egress) {
	defer func() { _ = conn.Close() }()
	remote := conn.RemoteAddr()
	local := conn.LocalAddr()

	rd := objfmt.NewReader(conn, oi.class)
	rd.SetMaxObject(oi.maxObject)
	// The BEEF lane's records are bounded by the submitter's object bound, not
	// the push-object ceiling sized for subtrees (DefaultMaxObjectBytes).
	if oi.class == objfmt.ClassBEEF {
		if bound := oi.fwd.BEEFRecordBound(remote); bound > 0 {
			rd.SetMaxObject(bound)
		}
	}

	for {
		obj, err := rd.Next()
		if err != nil {
			if oi.class == objfmt.ClassBEEF && errors.Is(err, objfmt.ErrObjectTooLarge) {
				oi.log.Warn("BEEF record exceeds the object bound; closing", "remote", remote, "err", err)
				if oi.rec != nil {
					oi.rec.BEEFSubmission("oversize")
				}
			} else if err != io.EOF && !isClosedErr(err) {
				oi.log.Debug("object read error; closing connection", "remote", remote, "err", err)
			}
			return
		}
		if oi.admit != nil {
			oi.admit(1, len(obj))
		}
		if oi.connAdmit != nil {
			oi.connAdmit(local, 1, len(obj))
		}
		if oi.class == objfmt.ClassBEEF {
			// The BEEF lane's object IS the submission record; the forwarder
			// expands it into one FrameVer 0x09 frame per topic. Dispatch
			// under IngressBEEF so a mis-sent non-BEEF grammar on this
			// single-class port is rejected rather than admitted as tx.
			oi.fwd.DispatchClass(egr, obj, remote, -1, forwarder.IngressBEEF)
			oi.flushEgr(egr)
			continue
		}
		raw, err := objfmt.MulticastBytes(oi.class, obj)
		if err != nil {
			// A malformed object desyncs the bare stream; drop the connection.
			oi.log.Warn("object reframe error; closing connection", "remote", remote, "err", err)
			return
		}
		// Privileged dispatch: raw carries the BRC-132/BRC-131 FrameVer, so the
		// forwarder routes it to ProcessSubtreeData / ProcessBlock, stamps from
		// the observed source, and egresses to the control group. raw is a fresh
		// buffer independent of the reader window, valid until Flush.
		oi.fwd.DispatchClass(egr, raw, remote, -1, forwarder.IngressPrivileged)
		oi.flushEgr(egr)
	}
}
