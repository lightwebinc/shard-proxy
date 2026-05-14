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
	engine := shard.New(0xFF05, shard.DefaultGroupID, 8)
	return New(engine, 0xFF05, shard.DefaultGroupID, 9001, false, nil)
}

// ── PrevSeq/CurSeq stamping ───────────────────────────────────────────────────

func TestProcessV2_StampsSeqChain(t *testing.T) {
	fw := makeForwarder()
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}

	// First frame: PrevSeq should be 0 (no predecessor), CurSeq must be non-zero.
	raw1 := buildV2Frame(t, 0xAB, 0, []byte("p1"))
	fw.Process(nil, raw1, src, 0)

	prevAfter1 := binary.BigEndian.Uint64(raw1[40:48])
	curAfter1 := binary.BigEndian.Uint64(raw1[48:56])
	if prevAfter1 != 0 {
		t.Errorf("frame 1: PrevSeq = %d, want 0 (first in chain)", prevAfter1)
	}
	if curAfter1 == 0 {
		t.Errorf("frame 1: CurSeq = 0 after stamping, want non-zero")
	}

	// Second frame from same (sender, group): PrevSeq must equal first CurSeq.
	raw2 := buildV2Frame(t, 0xAB, 0, []byte("p2"))
	fw.Process(nil, raw2, src, 0)

	prevAfter2 := binary.BigEndian.Uint64(raw2[40:48])
	curAfter2 := binary.BigEndian.Uint64(raw2[48:56])
	if prevAfter2 != curAfter1 {
		t.Errorf("frame 2: PrevSeq = %d, want %d (frame 1 CurSeq)", prevAfter2, curAfter1)
	}
	if curAfter2 == 0 || curAfter2 == curAfter1 {
		t.Errorf("frame 2: CurSeq = %d, want distinct non-zero value", curAfter2)
	}
}

func TestProcessV2_DifferentSenders_IndependentChains(t *testing.T) {
	fw := makeForwarder()
	src1 := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 1}
	src2 := &net.UDPAddr{IP: net.ParseIP("::2"), Port: 2}

	raw1 := buildV2Frame(t, 0xAB, 0, nil)
	raw2 := buildV2Frame(t, 0xAB, 0, nil)
	fw.Process(nil, raw1, src1, 0)
	fw.Process(nil, raw2, src2, 0)

	// Both chains start fresh: both PrevSeq == 0.
	prev1 := binary.BigEndian.Uint64(raw1[40:48])
	prev2 := binary.BigEndian.Uint64(raw2[40:48])
	if prev1 != 0 || prev2 != 0 {
		t.Errorf("sender1 PrevSeq=%d sender2 PrevSeq=%d, both want 0 (fresh chains)", prev1, prev2)
	}
	// CurSeqs will differ because the IP inputs to the hash differ.
	cur1 := binary.BigEndian.Uint64(raw1[48:56])
	cur2 := binary.BigEndian.Uint64(raw2[48:56])
	if cur1 == cur2 {
		t.Errorf("CurSeq for distinct senders should differ, both got %d", cur1)
	}
}

func TestProcessV1_NotStamped(t *testing.T) {
	fw := makeForwarder()
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 1}

	v1 := buildV1Frame(t, 0xAB, []byte("v1-payload"))
	// Bytes 40-47 in a BRC-12 frame hold PayLen; they must NOT be overwritten.
	payLenBefore := binary.BigEndian.Uint32(v1[40:44])
	fw.Process(nil, v1, src, 0)
	payLenAfter := binary.BigEndian.Uint32(v1[40:44])
	if payLenAfter != payLenBefore {
		t.Errorf("BRC-12 frame: bytes 40-43 changed (%d → %d); BRC-12 frames must be forwarded verbatim",
			payLenBefore, payLenAfter)
	}
}

func TestProcessV2_NilSrc_NotStamped(t *testing.T) {
	fw := makeForwarder()
	raw := buildV2Frame(t, 0xAB, 0xDEAD, nil)
	// With src==nil the stamping branch is skipped; bytes must remain unchanged.
	fw.Process(nil, raw, nil, 0)
	curAfter := binary.BigEndian.Uint64(raw[48:56])
	if curAfter != 0xDEAD {
		t.Errorf("nil src: CurSeq = %d, want 0xDEAD (unchanged)", curAfter)
	}
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
	// BRC-12 frames are forwarded verbatim; must not panic.
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
	engine := shard.New(0xFF05, shard.DefaultGroupID, 8)
	fw := New(engine, 0xFF05, shard.DefaultGroupID, 9001, true, nil)
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
