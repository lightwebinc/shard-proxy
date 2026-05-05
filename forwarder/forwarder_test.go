package forwarder

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"syscall"
	"testing"

	"github.com/lightwebinc/bitcoin-shard-common/frame"
	"github.com/lightwebinc/bitcoin-shard-common/shard"
)

// ── helpers ───────────────────────────────────────────────────────────────────

type fakeAddr struct{}

func (fakeAddr) Network() string { return "udp6" }
func (fakeAddr) String() string  { return "[::1]:12345" }

func openLoopbackUDP(t *testing.T) (*net.UDPConn, *net.UDPAddr) {
	t.Helper()
	addr, err := net.ResolveUDPAddr("udp6", "[::1]:0")
	if err != nil {
		t.Skipf("udp6 loopback not available: %v", err)
	}
	conn, err := net.ListenUDP("udp6", addr)
	if err != nil {
		t.Skipf("ListenUDP loopback: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn, conn.LocalAddr().(*net.UDPAddr)
}

func makeTargets(t *testing.T, conns ...*net.UDPConn) []Target {
	t.Helper()
	tgts := make([]Target, len(conns))
	for i, c := range conns {
		tgts[i] = Target{
			Iface: &net.Interface{Index: i + 1, Name: "lo"},
			Conn:  c,
		}
	}
	return tgts
}

func buildV2Frame(t *testing.T, txidByte0 byte, curSeq uint64, payload []byte) []byte {
	t.Helper()
	f := &frame.Frame{
		CurSeq:  curSeq,
		Payload: payload,
	}
	f.TxID[0] = txidByte0
	buf := make([]byte, frame.HeaderSize+len(payload))
	n, err := frame.Encode(f, buf)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return buf[:n]
}

func buildV1Frame(t *testing.T, txidByte0 byte, payload []byte) []byte {
	t.Helper()
	buf := make([]byte, frame.HeaderSizeLegacy+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], frame.MagicBSV)
	binary.BigEndian.PutUint16(buf[4:6], frame.ProtoVer)
	buf[6] = frame.FrameVerV1
	buf[8] = txidByte0
	binary.BigEndian.PutUint32(buf[40:44], uint32(len(payload)))
	copy(buf[44:], payload)
	return buf
}

func makeForwarder() *Forwarder {
	engine := shard.New(0xFF05, [11]byte{}, 8)
	return New(engine, 9001, false, nil)
}

// ── forward path ─────────────────────────────────────────────────────────────

func TestProcessV2FrameForwardedVerbatim(t *testing.T) {
	conn, _ := openLoopbackUDP(t)
	fw := makeForwarder()
	raw := buildV2Frame(t, 0xAB, 999, nil)
	// WriteTo to multicast dst will fail on loopback — that's fine, no panic.
	fw.Process(makeTargets(t, conn), raw, fakeAddr{}, 0)
}

// ── error paths ───────────────────────────────────────────────────────────────

func TestProcessInvalidFrame(t *testing.T) {
	fw := makeForwarder()
	conn, _ := openLoopbackUDP(t)
	// Truncated buffer — must not panic.
	fw.Process(makeTargets(t, conn), []byte{0x00, 0x01}, fakeAddr{}, 0)
}

func TestProcessBadMagic(t *testing.T) {
	fw := makeForwarder()
	conn, _ := openLoopbackUDP(t)
	fw.Process(makeTargets(t, conn), make([]byte, frame.HeaderSize), fakeAddr{}, 0)
}

func TestProcessV1FrameForwardedVerbatim(t *testing.T) {
	fw := makeForwarder()
	conn, _ := openLoopbackUDP(t)
	v1 := buildV1Frame(t, 0xAB, nil)
	// v1 frames are forwarded verbatim; must not panic.
	fw.Process(makeTargets(t, conn), v1, fakeAddr{}, 0)
}

func TestProcessMultipleTargets(t *testing.T) {
	conn1, _ := openLoopbackUDP(t)
	conn2, _ := openLoopbackUDP(t)
	fw := makeForwarder()
	raw := buildV2Frame(t, 0xAB, 1, nil)
	fw.Process(makeTargets(t, conn1, conn2), raw, fakeAddr{}, 0)
}

func TestProcessDebugMode(t *testing.T) {
	engine := shard.New(0xFF05, [11]byte{}, 8)
	fw := New(engine, 9001, true, nil)
	raw := buildV2Frame(t, 0xAB, 1, nil)
	conn, _ := openLoopbackUDP(t)
	fw.Process(makeTargets(t, conn), raw, fakeAddr{}, 0)
}

// ── OpenTargets / CloseTargets ────────────────────────────────────────────────

func realIface(t *testing.T) *net.Interface {
	t.Helper()
	ifaces, err := net.Interfaces()
	if err != nil || len(ifaces) == 0 {
		t.Skip("no network interfaces available")
	}
	return &ifaces[0]
}

func TestOpenAndCloseTargets(t *testing.T) {
	iface := realIface(t)
	fw := makeForwarder()
	targets, err := fw.OpenTargets([]*net.Interface{iface}, false)
	if err != nil {
		t.Skipf("OpenTargets(%s): %v", iface.Name, err)
	}
	if len(targets) != 1 {
		t.Errorf("len(targets) = %d, want 1", len(targets))
	}
	CloseTargets(targets, slog.Default())
}

func TestOpenTargetsEmpty(t *testing.T) {
	fw := makeForwarder()
	targets, err := fw.OpenTargets(nil, false)
	if err != nil {
		t.Errorf("OpenTargets(nil): unexpected error: %v", err)
	}
	if len(targets) != 0 {
		t.Errorf("expected 0 targets for nil ifaces, got %d", len(targets))
	}
}

func TestOpenTargetsProbe(t *testing.T) {
	iface := realIface(t)
	fw := makeForwarder()
	targets, err := fw.OpenTargets([]*net.Interface{iface}, true)
	if err != nil {
		t.Skipf("OpenTargets probe(%s): %v", iface.Name, err)
	}
	CloseTargets(targets, slog.Default())
}

// ── isErrno ───────────────────────────────────────────────────────────────────

func TestIsErrnoMatch(t *testing.T) {
	if !isErrno(syscall.EPERM, syscall.EPERM) {
		t.Error("isErrno should match EPERM directly")
	}
}

func TestIsErrnoNoMatch(t *testing.T) {
	if isErrno(syscall.EPERM, syscall.EBADF) {
		t.Error("isErrno should not match EBADF when err is EPERM")
	}
}

func TestIsErrnoNil(t *testing.T) {
	if isErrno(nil, syscall.EPERM) {
		t.Error("isErrno(nil) should return false")
	}
}

func TestIsErrnoWrapped(t *testing.T) {
	err := fmt.Errorf("wrapped: %w", syscall.EACCES)
	if !isErrno(err, syscall.EACCES) {
		t.Error("isErrno should match wrapped errno")
	}
}

// ── probeEgressSocket ─────────────────────────────────────────────────────────

func TestProbeEgressSocketLoopback(t *testing.T) {
	conn, _ := openLoopbackUDP(t)
	iface := &net.Interface{Index: 1, Name: "lo"}
	if err := probeEgressSocket(slog.Default(), conn, iface); err != nil {
		t.Errorf("probeEgressSocket on loopback: unexpected hard error: %v", err)
	}
}

func TestProbeEgressSocketClosedConn(t *testing.T) {
	conn, _ := openLoopbackUDP(t)
	iface := &net.Interface{Index: 1, Name: "lo"}
	_ = conn.Close()
	_ = probeEgressSocket(slog.Default(), conn, iface)
}
