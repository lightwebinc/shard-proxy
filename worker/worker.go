// Package worker implements the per-CPU receive-and-retransmit loop for
// shard-proxy.
//
// # Design
//
// Each Worker owns a single ingress UDP socket bound via SO_REUSEPORT. The
// kernel distributes incoming datagrams across worker sockets with no
// userspace coordination on the receive path. Per-packet decode, sequence
// stamping, and egress queueing are delegated to [forwarder.Forwarder]; the
// worker holds a per-goroutine [forwarder.Egress] that the forwarder appends
// to for every processed frame and that the worker flushes once per receive
// batch.
//
// # SO_REUSEPORT
//
// SO_REUSEPORT (Linux 3.9+) allows multiple sockets to bind to the same
// address and port. The kernel hashes the 4-tuple of the incoming datagram
// to select which socket — and therefore which worker goroutine — receives
// each packet. This provides CPU-local receive processing with no shared
// data structures on the ingress path.
//
// # recvmmsg batching
//
// The receive loop uses [ipv6.PacketConn.ReadBatch], which maps to a single
// recvmmsg(2) syscall on Linux and falls back to a per-packet ReadFrom loop
// on other platforms. Each worker pre-allocates batchSize datagram buffers
// and reuses them across iterations; per packet the worker decodes, hands
// the raw slice to forwarder.Process*, and at the end of the batch calls
// Egress.Flush which dispatches one WriteBatch (sendmmsg on Linux) per
// configured egress interface. Buffer reuse is safe because Egress.Flush
// completes before the next ReadBatch overwrites the same buffers.
package worker

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"syscall"

	"golang.org/x/net/ipv6"
	"golang.org/x/sys/unix"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-proxy/forwarder"
	"github.com/lightwebinc/shard-proxy/metrics"
)

const (
	// RecvBufSize is the per-worker datagram read buffer in bytes.
	// 64 KiB covers jumbo-frame paths; individual BSV transactions sent over
	// UDP should stay well below the path MTU to avoid IP fragmentation.
	RecvBufSize = 65536

	// DefaultRecvBufBytes is the value requested for SO_RCVBUF when the
	// caller does not override it via [Worker.SetRecvBufBytes]. Larger
	// buffers absorb short-lived bursts without dropping datagrams; the
	// kernel clamps the request to net.core.rmem_max.
	DefaultRecvBufBytes = 4 * 1024 * 1024 // 4 MiB

	// DefaultRecvBatch is the fallback batch size used when New is called
	// without an explicit value (legacy callers). main passes the
	// configured value through.
	DefaultRecvBatch = 32
)

// Worker owns one SO_REUSEPORT ingress socket and delegates forwarding to a
// shared [forwarder.Forwarder]. Create with [New] and start with [Run].
type Worker struct {
	id           int
	fwd          *forwarder.Forwarder
	ifaces       []*net.Interface
	rec          *metrics.Recorder
	log          *slog.Logger
	recvBatch    int
	recvBufBytes int
}

// New constructs a Worker. No sockets are opened until [Run] is called.
//
//   - id is a small integer used in log fields to distinguish workers.
//   - fwd is the shared forwarder; handles decode, sequence, and egress.
//   - ifaces is the list of NICs passed to [forwarder.Forwarder.OpenTargets].
//   - rec is the shared metrics recorder; may be nil to disable metrics.
//
// The receive batch size defaults to [DefaultRecvBatch]; override via
// [Worker.SetRecvBatch] before calling [Run].
func New(id int, fwd *forwarder.Forwarder, ifaces []*net.Interface, rec *metrics.Recorder) *Worker {
	return &Worker{
		id:           id,
		fwd:          fwd,
		ifaces:       ifaces,
		rec:          rec,
		log:          slog.Default().With("worker", id),
		recvBatch:    DefaultRecvBatch,
		recvBufBytes: DefaultRecvBufBytes,
	}
}

// SetRecvBatch overrides the per-worker recvmmsg batch size. Values < 1 are
// clamped to 1 (per-packet receive, equivalent to the legacy pre-batched
// path). Call before [Run]; not safe to change while the worker is running.
func (w *Worker) SetRecvBatch(n int) {
	if n < 1 {
		n = 1
	}
	w.recvBatch = n
}

// SetRecvBufBytes overrides the requested SO_RCVBUF size. Values < 1 leave
// the default ([DefaultRecvBufBytes]) in place. Call before [Run].
func (w *Worker) SetRecvBufBytes(n int) {
	if n < 1 {
		return
	}
	w.recvBufBytes = n
}

// Run opens the ingress socket, opens egress targets via the forwarder, then
// enters the receive loop. It blocks until done is closed or an unrecoverable
// socket error occurs. Intended to be launched as a goroutine from main.
//
//   - listenAddr is the bind address string (e.g. "[::]"), without port.
//   - listenPort is the UDP port shared by all workers via SO_REUSEPORT.
//   - done is closed by the main goroutine to signal graceful shutdown.
func (w *Worker) Run(listenAddr string, listenPort int, done <-chan struct{}) error {
	// Open a raw IPv6 UDP socket so we can set SO_REUSEPORT before binding.
	// net.ListenPacket does not expose this option.
	fd, err := unix.Socket(unix.AF_INET6, unix.SOCK_DGRAM, unix.IPPROTO_UDP)
	if err != nil {
		return fmt.Errorf("worker %d: socket: %w", w.id, err)
	}

	// SO_REUSEPORT: allow all worker sockets to share the same port.
	// The kernel distributes incoming datagrams across them.
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEPORT, 1); err != nil {
		_ = unix.Close(fd)
		return fmt.Errorf("worker %d: SO_REUSEPORT: %w", w.id, err)
	}

	// Enlarge the receive buffer to absorb bursts of transaction datagrams.
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_RCVBUF, w.recvBufBytes); err != nil {
		w.log.Warn("could not set SO_RCVBUF", "err", err)
	}
	if actual, err := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_RCVBUF); err == nil {
		w.log.Debug("SO_RCVBUF", "requested_bytes", w.recvBufBytes, "actual_bytes", actual)
	}

	sa := &unix.SockaddrInet6{Port: listenPort}
	if err := unix.Bind(fd, sa); err != nil {
		_ = unix.Close(fd)
		return fmt.Errorf("worker %d: bind :%d: %w", w.id, listenPort, err)
	}

	// Wrap the raw fd in a net.PacketConn for idiomatic Read/Write calls.
	// os.NewFile duplicates the fd internally; close the original.
	file := os.NewFile(uintptr(fd), fmt.Sprintf("udp6-ingress-w%d", w.id))
	conn, err := net.FilePacketConn(file)
	_ = file.Close()
	if err != nil {
		return fmt.Errorf("worker %d: FilePacketConn: %w", w.id, err)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			w.log.Warn("close ingress conn", "err", err)
		}
	}()

	// Close the ingress socket when done is signalled, unblocking ReadFrom.
	go func() {
		<-done
		if err := conn.Close(); err != nil {
			w.log.Warn("close ingress conn on shutdown", "err", err)
		}
	}()

	// Open egress sockets via the forwarder. Worker 0 probes each interface.
	targets, err := w.fwd.OpenTargets(w.ifaces, w.id == 0)
	if err != nil {
		return fmt.Errorf("worker %d: open targets: %w", w.id, err)
	}
	defer forwarder.CloseTargets(targets, w.log)

	ifaceNames := make([]string, len(targets))
	for i, tgt := range targets {
		ifaceNames[i] = tgt.Iface.Name
	}
	w.log.Info("ready",
		"listen_port", listenPort,
		"egress_ifaces", ifaceNames,
		"recv_batch", w.recvBatch,
	)
	if w.rec != nil {
		w.rec.WorkerReady()
		defer w.rec.WorkerDone()
	}

	// Per-worker outbound queue. Flushed once per receive batch; a deferred
	// flush drains any in-flight datagrams on graceful shutdown so the
	// last batch's egress is not lost.
	egr := forwarder.NewEgress(w.fwd, targets, w.recvBatch, w.rec)
	defer egr.Flush()

	// Wrap the ingress PacketConn so we can use ReadBatch (recvmmsg on
	// Linux; per-packet fallback elsewhere).
	pc := ipv6.NewPacketConn(conn)

	// Pre-allocate batchSize receive slots. Each slot owns a single
	// RecvBufSize byte buffer that is reused across iterations; we always
	// flush the per-batch Egress before the next ReadBatch overwrites
	// these buffers, so it is safe for the forwarder to retain raw slices
	// across the dispatch loop within a single batch.
	msgs := make([]ipv6.Message, w.recvBatch)
	for i := range msgs {
		msgs[i] = ipv6.Message{Buffers: [][]byte{make([]byte, RecvBufSize)}}
	}
	ifaceName := ""
	if len(targets) > 0 {
		ifaceName = targets[0].Iface.Name
	}

	for {
		nMsgs, err := pc.ReadBatch(msgs, 0)
		if err != nil {
			if isClosedErr(err) {
				return nil
			}
			w.log.Warn("ReadBatch error", "err", err)
			if w.rec != nil && len(targets) > 0 {
				w.rec.IngressError(ifaceName, w.id)
			}
			continue
		}

		for i := 0; i < nMsgs; i++ {
			m := &msgs[i]
			n := m.N
			src := m.Addr
			buf := m.Buffers[0]

			if n == RecvBufSize {
				w.log.Warn("datagram fills recv buffer; may be truncated",
					"src", src, "len", n)
				if w.rec != nil && len(targets) > 0 {
					w.rec.PacketDropped(ifaceName, w.id, "truncated")
				}
				continue
			}

			if w.rec != nil && len(targets) > 0 {
				w.rec.PacketReceived(ifaceName, w.id, n)
			}
			switch {
			case n > 6 && buf[6] == frame.FrameVerV4:
				w.fwd.ProcessBlock(egr, buf[:n], src, w.id)
			case n > 6 && buf[6] == frame.FrameVerV5:
				w.fwd.ProcessSubtreeData(egr, buf[:n], src, w.id)
			case n > 6 && buf[6] == frame.FrameVerV6:
				w.fwd.ProcessAnchor(egr, buf[:n], src, w.id)
			default:
				w.fwd.Process(egr, buf[:n], src, w.id)
			}
		}

		egr.Flush()
	}
}

// isClosedErr returns true for errors that indicate the socket was closed
// deliberately (e.g. as part of graceful shutdown), as opposed to errors
// that should be logged as unexpected.
func isClosedErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, net.ErrClosed) {
		return true
	}
	return isErrno(err, syscall.EBADF) || isErrno(err, syscall.EINVAL)
}

// isErrno unwraps err and reports whether its innermost value is target.
func isErrno(err error, target syscall.Errno) bool {
	for err != nil {
		if e, ok := err.(syscall.Errno); ok {
			return e == target
		}
		err = unwrap(err)
	}
	return false
}

// unwrap returns the next error in the chain, or nil.
func unwrap(err error) error {
	type unwrapper interface{ Unwrap() error }
	if u, ok := err.(unwrapper); ok {
		return u.Unwrap()
	}
	return nil
}
