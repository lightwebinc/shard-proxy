package worker

import (
	"errors"
	"fmt"
	"io"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/lightwebinc/bitcoin-shard-common/frame"
	"github.com/lightwebinc/bitcoin-shard-common/shard"
	"github.com/lightwebinc/bitcoin-shard-proxy/forwarder"
)

// ── New ───────────────────────────────────────────────────────────────────────

func makeTestForwarder() *forwarder.Forwarder {
	engine := shard.New(0xFF05, shard.DefaultGroupID, 8)
	return forwarder.New(engine, 0xFF05, shard.DefaultGroupID, 9001, false, nil)
}

func TestNew(t *testing.T) {
	fwd := makeTestForwarder()
	ifaces := []*net.Interface{{Index: 1, Name: "lo"}}
	w := New(0, fwd, ifaces, nil)
	if w == nil {
		t.Fatal("New returned nil")
	}
	if w.id != 0 {
		t.Errorf("id = %d, want 0", w.id)
	}
	if w.fwd == nil {
		t.Error("fwd should not be nil")
	}
	if w.log == nil {
		t.Error("log should not be nil")
	}
}

func TestNewNilRec(t *testing.T) {
	fwd := makeTestForwarder()
	ifaces := []*net.Interface{{Index: 1, Name: "lo"}}
	w := New(1, fwd, ifaces, nil)
	if w.rec != nil {
		t.Error("rec should be nil when not provided")
	}
}

// ── isClosedErr ───────────────────────────────────────────────────────────────

func TestIsClosedErrNil(t *testing.T) {
	if isClosedErr(nil) {
		t.Error("nil should not be a closed error")
	}
}

func TestIsClosedErrNetErrClosed(t *testing.T) {
	if !isClosedErr(net.ErrClosed) {
		t.Error("net.ErrClosed should be recognised as a closed error")
	}
}

func TestIsClosedErrEBADF(t *testing.T) {
	if !isClosedErr(syscall.EBADF) {
		t.Error("EBADF should be recognised as a closed error")
	}
}

func TestIsClosedErrEINVAL(t *testing.T) {
	if !isClosedErr(syscall.EINVAL) {
		t.Error("EINVAL should be recognised as a closed error")
	}
}

func TestIsClosedErrWrapped(t *testing.T) {
	wrapped := &net.OpError{Op: "read", Err: net.ErrClosed}
	if !isClosedErr(wrapped) {
		t.Error("wrapped net.ErrClosed should be recognised as a closed error")
	}
}

func TestIsClosedErrUnrelated(t *testing.T) {
	if isClosedErr(syscall.ECONNREFUSED) {
		t.Error("ECONNREFUSED should not be a closed error")
	}
}

func TestIsClosedErrGeneric(t *testing.T) {
	if isClosedErr(errors.New("some other error")) {
		t.Error("generic error should not be a closed error")
	}
}

// ── isErrno ───────────────────────────────────────────────────────────────────

func TestIsErrnoDirectMatch(t *testing.T) {
	if !isErrno(syscall.EBADF, syscall.EBADF) {
		t.Error("direct EBADF should match")
	}
}

func TestIsErrnoNoMatch(t *testing.T) {
	if isErrno(syscall.EAGAIN, syscall.EBADF) {
		t.Error("EAGAIN should not match EBADF")
	}
}

func TestIsErrnoNil(t *testing.T) {
	if isErrno(nil, syscall.EBADF) {
		t.Error("nil error should not match")
	}
}

func TestIsErrnoWrapped(t *testing.T) {
	wrapped := fmt.Errorf("wrap: %w", syscall.EBADF)
	if !isErrno(wrapped, syscall.EBADF) {
		t.Error("wrapped EBADF should match")
	}
}

// ── unwrap ────────────────────────────────────────────────────────────────────

func TestUnwrapNil(t *testing.T) {
	if unwrap(nil) != nil {
		t.Error("unwrap(nil) should return nil")
	}
}

func TestUnwrapNoChain(t *testing.T) {
	if unwrap(errors.New("plain")) != nil {
		t.Error("unwrap of non-wrapping error should return nil")
	}
}

func TestUnwrapChain(t *testing.T) {
	inner := errors.New("inner")
	outer := fmt.Errorf("outer: %w", inner)
	if unwrap(outer) != inner {
		t.Error("unwrap should return the wrapped inner error")
	}
}

// ── NewTCPIngress ─────────────────────────────────────────────────────────────

func TestNewTCPIngress(t *testing.T) {
	fwd := makeTestForwarder()
	ifaces := []*net.Interface{{Index: 1, Name: "lo"}}
	ti := NewTCPIngress(fwd, ifaces, nil)
	if ti == nil {
		t.Fatal("NewTCPIngress returned nil")
	}
	if ti.fwd == nil {
		t.Error("fwd should not be nil")
	}
	if ti.log == nil {
		t.Error("log should not be nil")
	}
}

// ── handleConn ────────────────────────────────────────────────────────────────

// dialHandleConn opens a TCP loopback connection, runs handleConn in a
// goroutine, calls write() to populate the server side, then waits for
// handleConn to return (with a short timeout to prevent hangs).
func dialHandleConn(t *testing.T, write func(net.Conn)) {
	t.Helper()
	ln, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skipf("TCP loopback unavailable: %v", err)
	}
	defer func() { _ = ln.Close() }()

	clientConn, err := net.Dial("tcp6", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	serverConn, err := ln.Accept()
	if err != nil {
		t.Fatalf("accept: %v", err)
	}

	fwd := makeTestForwarder()
	ifaces := []*net.Interface{{Index: 1, Name: "lo"}}
	ti := NewTCPIngress(fwd, ifaces, nil)

	done := make(chan struct{})
	go func() {
		ti.handleConn(serverConn, nil)
		close(done)
	}()

	write(clientConn)
	_ = clientConn.Close()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("handleConn did not return within timeout")
	}
}

func buildTCPFrame(t *testing.T, txidByte byte, seq uint64, payload []byte) []byte {
	t.Helper()
	f := &frame.Frame{CurSeq: seq, Payload: payload}
	f.TxID[0] = txidByte
	buf := make([]byte, frame.HeaderSize+len(payload))
	n, err := frame.Encode(f, buf)
	if err != nil {
		t.Fatalf("frame.Encode: %v", err)
	}
	return buf[:n]
}

func TestHandleConnValidFrame(t *testing.T) {
	raw := buildTCPFrame(t, 0xAB, 1, []byte("hello"))
	dialHandleConn(t, func(conn net.Conn) {
		_, _ = conn.Write(raw)
	})
}

func TestHandleConnEmptyConn(t *testing.T) {
	dialHandleConn(t, func(_ net.Conn) {
		// write nothing — immediate EOF in handleConn
	})
}

func TestHandleConnTruncatedHeader(t *testing.T) {
	dialHandleConn(t, func(conn net.Conn) {
		_, _ = conn.Write([]byte{0xE3, 0xE1, 0xF3}) // only 3 bytes
	})
}

func TestHandleConnBadMagic(t *testing.T) {
	hdr := make([]byte, frame.HeaderSize)
	hdr[0] = 0x00 // bad magic — all zeros
	dialHandleConn(t, func(conn net.Conn) {
		_, _ = conn.Write(hdr)
	})
}

func buildV1TCPFrame(t *testing.T, txidByte byte, payload []byte) []byte {
	t.Helper()
	buf := make([]byte, frame.HeaderSizeLegacy+len(payload))
	// Magic
	buf[0], buf[1], buf[2], buf[3] = 0xE3, 0xE1, 0xF3, 0xE8
	// ProtoVer
	buf[4], buf[5] = 0x02, 0xBF
	// FrameVer BRC-12 (legacy)
	buf[6] = frame.FrameVerV1
	// TxID[0]
	buf[8] = txidByte
	// PayLen at @40
	buf[40] = byte(len(payload) >> 24)
	buf[41] = byte(len(payload) >> 16)
	buf[42] = byte(len(payload) >> 8)
	buf[43] = byte(len(payload))
	copy(buf[44:], payload)
	return buf
}

func TestHandleConnV1Frame(t *testing.T) {
	raw := buildV1TCPFrame(t, 0xAB, []byte("v1-payload"))
	dialHandleConn(t, func(conn net.Conn) {
		_, _ = conn.Write(raw)
	})
}

func TestHandleConnMultipleFrames(t *testing.T) {
	raw1 := buildTCPFrame(t, 0xAB, 1, nil)
	raw2 := buildTCPFrame(t, 0xCD, 2, []byte("world"))
	dialHandleConn(t, func(conn net.Conn) {
		_, _ = io.MultiWriter(conn).Write(append(raw1, raw2...))
	})
}

func TestHandleConnV2ThenV1(t *testing.T) {
	v2 := buildTCPFrame(t, 0xAA, 1, []byte("v2-payload"))
	v1 := buildV1TCPFrame(t, 0xBB, []byte("v1-payload"))
	dialHandleConn(t, func(conn net.Conn) {
		// Write mixed BRC-124+BRC-12 in one stream to exercise the version-switching
		// read path: the TCP reader must correctly advance past the 92-byte
		// BRC-124 header+payload and then parse the 44-byte BRC-12 header+payload.
		buf := append(v2, v1...)
		_, _ = conn.Write(buf)
	})
}

func TestHandleConnV2TruncatedExtension(t *testing.T) {
	// Write exactly 44 bytes of a BRC-124 header (the legacy prefix) and then
	// close the connection mid-extension. handleConn must return cleanly
	// (no panic, no hang).
	hdr := make([]byte, frame.HeaderSizeLegacy)
	hdr[0], hdr[1], hdr[2], hdr[3] = 0xE3, 0xE1, 0xF3, 0xE8
	hdr[4], hdr[5] = 0x02, 0xBF
	hdr[6] = frame.FrameVerV2
	dialHandleConn(t, func(conn net.Conn) {
		_, _ = conn.Write(hdr)
	})
}

func TestHandleConnV2LargePayload(t *testing.T) {
	payload := make([]byte, 64*1024) // 64 KiB
	for i := range payload {
		payload[i] = byte(i)
	}
	raw := buildTCPFrame(t, 0xCC, 3, payload)
	dialHandleConn(t, func(conn net.Conn) {
		_, _ = conn.Write(raw)
	})
}
