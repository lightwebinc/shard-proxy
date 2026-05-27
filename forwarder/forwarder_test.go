package forwarder

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"syscall"
	"testing"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/shard"
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

func buildV2Frame(t *testing.T, txidByte0 byte, seqNum uint64, payload []byte) []byte {
	t.Helper()
	return buildV2FrameSub(t, txidByte0, seqNum, [32]byte{}, payload)
}

func buildV2FrameSub(t *testing.T, txidByte0 byte, seqNum uint64, sub [32]byte, payload []byte) []byte {
	t.Helper()
	f := &frame.Frame{
		SeqNum:    seqNum,
		SubtreeID: sub,
		Payload:   payload,
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

// ── HashKey/SeqNum stamping ───────────────────────────────────────────────────

func TestProcessV2_StampsHashKeyAndSeqNum(t *testing.T) {
	fw := makeForwarder()
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}

	// First frame: HashKey must be non-zero, SeqNum must be 1.
	raw1 := buildV2Frame(t, 0xAB, 0, []byte("p1"))
	fw.Process(nil, raw1, src, 0)

	hashKey1 := binary.BigEndian.Uint64(raw1[40:48])
	seqNum1 := binary.BigEndian.Uint64(raw1[48:56])
	if hashKey1 == 0 {
		t.Errorf("frame 1: HashKey = 0 after stamping, want non-zero")
	}
	if seqNum1 != 1 {
		t.Errorf("frame 1: SeqNum = %d, want 1", seqNum1)
	}

	// Second frame from same (sender, group): same HashKey, SeqNum increments to 2.
	raw2 := buildV2Frame(t, 0xAB, 0, []byte("p2"))
	fw.Process(nil, raw2, src, 0)

	hashKey2 := binary.BigEndian.Uint64(raw2[40:48])
	seqNum2 := binary.BigEndian.Uint64(raw2[48:56])
	if hashKey2 != hashKey1 {
		t.Errorf("frame 2: HashKey = %d, want %d (stable per flow)", hashKey2, hashKey1)
	}
	if seqNum2 != 2 {
		t.Errorf("frame 2: SeqNum = %d, want 2", seqNum2)
	}
}

func TestProcessV2_DifferentSenders_IndependentFlows(t *testing.T) {
	fw := makeForwarder()
	src1 := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 1}
	src2 := &net.UDPAddr{IP: net.ParseIP("::2"), Port: 2}

	raw1 := buildV2Frame(t, 0xAB, 0, nil)
	raw2 := buildV2Frame(t, 0xAB, 0, nil)
	fw.Process(nil, raw1, src1, 0)
	fw.Process(nil, raw2, src2, 0)

	// Both flows start at SeqNum=1.
	seq1 := binary.BigEndian.Uint64(raw1[48:56])
	seq2 := binary.BigEndian.Uint64(raw2[48:56])
	if seq1 != 1 || seq2 != 1 {
		t.Errorf("sender1 SeqNum=%d sender2 SeqNum=%d, both want 1 (fresh flows)", seq1, seq2)
	}
	// HashKeys differ because the sender IP inputs differ.
	hk1 := binary.BigEndian.Uint64(raw1[40:48])
	hk2 := binary.BigEndian.Uint64(raw2[40:48])
	if hk1 == hk2 {
		t.Errorf("HashKey for distinct senders should differ, both got %d", hk1)
	}
}

func TestProcessV2_DistinctSubtrees_IndependentFlows(t *testing.T) {
	fw := makeForwarder()
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 1}

	var subA, subB [32]byte
	subA[0] = 0xAA
	subB[0] = 0xBB

	// Interleave two subtree streams under the same (sender, group). Each
	// subtree must be an independent flow with its own HashKey and SeqNum.
	rawA1 := buildV2FrameSub(t, 0xAB, 0, subA, nil)
	rawB1 := buildV2FrameSub(t, 0xAB, 0, subB, nil)
	rawA2 := buildV2FrameSub(t, 0xAB, 0, subA, nil)
	fw.Process(nil, rawA1, src, 0)
	fw.Process(nil, rawB1, src, 0)
	fw.Process(nil, rawA2, src, 0)

	hkA1 := binary.BigEndian.Uint64(rawA1[40:48])
	hkB1 := binary.BigEndian.Uint64(rawB1[40:48])
	hkA2 := binary.BigEndian.Uint64(rawA2[40:48])
	seqA1 := binary.BigEndian.Uint64(rawA1[48:56])
	seqB1 := binary.BigEndian.Uint64(rawB1[48:56])
	seqA2 := binary.BigEndian.Uint64(rawA2[48:56])

	if hkA1 != hkA2 {
		t.Errorf("subtree A: HashKey changed between frames (%x != %x)", hkA1, hkA2)
	}
	if hkA1 == hkB1 {
		t.Errorf("subtree A and B share HashKey %x — flows are not isolated", hkA1)
	}
	if seqA1 != 1 {
		t.Errorf("subtree A frame 1: SeqNum = %d, want 1", seqA1)
	}
	if seqB1 != 1 {
		t.Errorf("subtree B frame 1: SeqNum = %d, want 1 (independent flow)", seqB1)
	}
	if seqA2 != 2 {
		t.Errorf("subtree A frame 2: SeqNum = %d, want 2", seqA2)
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
	seqAfter := binary.BigEndian.Uint64(raw[48:56])
	if seqAfter != 0xDEAD {
		t.Errorf("nil src: SeqNum = %d, want 0xDEAD (unchanged)", seqAfter)
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

// ── ProcessAnchor — BRC-134 ──────────────────────────────────────────────────

// buildAnchorFrame builds a minimal BRC-134 anchor transaction frame. It encodes
// using frame.Encode (FrameVerV2 layout) then patches the version byte to 0x06.
func buildAnchorFrame(t *testing.T, txidByte0 byte, seqNum uint64, payload []byte) []byte {
	t.Helper()
	f := &frame.Frame{
		SeqNum:  seqNum,
		Payload: payload,
	}
	f.TxID[0] = txidByte0
	buf := make([]byte, frame.HeaderSize+len(payload))
	n, err := frame.Encode(f, buf)
	if err != nil {
		t.Fatalf("frame.Encode: %v", err)
	}
	buf[6] = frame.FrameVerV6 // promote to anchor version
	return buf[:n]
}

func TestProcessAnchor_StampsHashKeyAndSeqNum(t *testing.T) {
	fw := makeForwarder()
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}

	// First frame: HashKey must be non-zero, SeqNum must be 1.
	raw1 := buildAnchorFrame(t, 0xAB, 0, []byte("anchor-tx-1"))
	fw.ProcessAnchor(nil, raw1, src, 0)

	hk1 := binary.BigEndian.Uint64(raw1[40:48])
	seq1 := binary.BigEndian.Uint64(raw1[48:56])
	if hk1 == 0 {
		t.Errorf("frame 1: HashKey = 0 after stamping, want non-zero")
	}
	if seq1 != 1 {
		t.Errorf("frame 1: SeqNum = %d, want 1", seq1)
	}

	// Second frame from same sender: same HashKey, SeqNum increments to 2.
	raw2 := buildAnchorFrame(t, 0xCD, 0, []byte("anchor-tx-2"))
	fw.ProcessAnchor(nil, raw2, src, 0)

	hk2 := binary.BigEndian.Uint64(raw2[40:48])
	seq2 := binary.BigEndian.Uint64(raw2[48:56])
	if hk2 != hk1 {
		t.Errorf("frame 2: HashKey = %d, want %d (stable per flow)", hk2, hk1)
	}
	if seq2 != 2 {
		t.Errorf("frame 2: SeqNum = %d, want 2", seq2)
	}
}

func TestProcessAnchor_PreStamped_NotOverwritten(t *testing.T) {
	fw := makeForwarder()
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 1}

	const preHashKey uint64 = 0xDEADBEEFCAFEBABE
	const preSeqNum uint64 = 99

	raw := buildAnchorFrame(t, 0x11, preSeqNum, nil)
	binary.BigEndian.PutUint64(raw[40:48], preHashKey) // pre-stamp HashKey too

	fw.ProcessAnchor(nil, raw, src, 0)

	hk := binary.BigEndian.Uint64(raw[40:48])
	seq := binary.BigEndian.Uint64(raw[48:56])
	if hk != preHashKey {
		t.Errorf("HashKey = %x, want %x (pre-stamped must not be overwritten)", hk, preHashKey)
	}
	if seq != preSeqNum {
		t.Errorf("SeqNum = %d, want %d (pre-stamped must not be overwritten)", seq, preSeqNum)
	}
}

func TestProcessAnchor_DifferentSenders_IndependentFlows(t *testing.T) {
	fw := makeForwarder()
	src1 := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 1}
	src2 := &net.UDPAddr{IP: net.ParseIP("::2"), Port: 2}

	raw1 := buildAnchorFrame(t, 0xAB, 0, nil)
	raw2 := buildAnchorFrame(t, 0xAB, 0, nil)
	fw.ProcessAnchor(nil, raw1, src1, 0)
	fw.ProcessAnchor(nil, raw2, src2, 0)

	// Both flows start at SeqNum=1.
	seq1 := binary.BigEndian.Uint64(raw1[48:56])
	seq2 := binary.BigEndian.Uint64(raw2[48:56])
	if seq1 != 1 || seq2 != 1 {
		t.Errorf("src1 SeqNum=%d src2 SeqNum=%d, both want 1 (fresh flows)", seq1, seq2)
	}
	// HashKeys differ because sender IP inputs differ.
	hk1 := binary.BigEndian.Uint64(raw1[40:48])
	hk2 := binary.BigEndian.Uint64(raw2[40:48])
	if hk1 == hk2 {
		t.Errorf("HashKey for distinct senders should differ, both got %x", hk1)
	}
}

func TestProcessAnchor_NilSrc_SkipsStamping(t *testing.T) {
	fw := makeForwarder()

	raw := buildAnchorFrame(t, 0x55, 0, nil) // SeqNum=0 means unstamped
	fw.ProcessAnchor(nil, raw, nil, 0)

	// With nil src the proxy does not stamp — HashKey and SeqNum remain 0.
	hk := binary.BigEndian.Uint64(raw[40:48])
	seq := binary.BigEndian.Uint64(raw[48:56])
	if hk != 0 {
		t.Errorf("nil src: HashKey = %x, want 0 (no stamping)", hk)
	}
	if seq != 0 {
		t.Errorf("nil src: SeqNum = %d, want 0 (no stamping)", seq)
	}
}

func TestProcessAnchor_DecodeError_Drops(t *testing.T) {
	fw := makeForwarder()
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 1}

	// Bad magic — should be silently dropped, not panic.
	bad := make([]byte, frame.HeaderSize)
	bad[6] = frame.FrameVerV6
	fw.ProcessAnchor(nil, bad, src, 0)
}

// ── anchor flow uses dedicated groupIdx, distinct from block control ──────────

func TestProcessAnchor_DistinctFlowFromBlock(t *testing.T) {
	fw := makeForwarder()
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 1}

	// Anchor frames use a virtual groupIdx (0xFFF9) for HashKey derivation
	// so they get their own independent flow identity and SeqNum counter,
	// distinct from BRC-131 block frames (which use CtrlGroupControl 0xFFFE).
	rawAnchor := buildAnchorFrame(t, 0xAA, 0, nil)
	rawBlock := buildBlockBufForwarder(t, 0xBB, nil)
	fw.ProcessAnchor(nil, rawAnchor, src, 0)
	fw.ProcessBlock(nil, rawBlock, src, 0)

	hkAnchor := binary.BigEndian.Uint64(rawAnchor[40:48])
	hkBlock := binary.BigEndian.Uint64(rawBlock[40:48])
	if hkAnchor == hkBlock {
		t.Errorf("anchor HashKey %x == block HashKey %x; expected distinct flows", hkAnchor, hkBlock)
	}

	// Both should start at SeqNum 1 (independent counters).
	seqAnchor := binary.BigEndian.Uint64(rawAnchor[48:56])
	seqBlock := binary.BigEndian.Uint64(rawBlock[48:56])
	if seqAnchor != 1 {
		t.Errorf("anchor SeqNum = %d, want 1", seqAnchor)
	}
	if seqBlock != 1 {
		t.Errorf("block SeqNum = %d, want 1", seqBlock)
	}
}

// buildBlockBufForwarder constructs a minimal valid BRC-131 BlockMsgAnnounce
// frame for use in forwarder tests.
func buildBlockBufForwarder(t *testing.T, contentIDByte byte, payload []byte) []byte {
	t.Helper()
	if payload == nil {
		payload = []byte("blk")
	}
	buf := make([]byte, frame.HeaderSize+len(payload))
	binary.BigEndian.PutUint32(buf[0:4], frame.MagicBSV)
	binary.BigEndian.PutUint16(buf[4:6], frame.ProtoVer)
	buf[6] = frame.FrameVerV4
	buf[7] = frame.BlockMsgAnnounce
	buf[8] = contentIDByte
	binary.BigEndian.PutUint32(buf[88:92], uint32(len(payload)))
	copy(buf[frame.HeaderSize:], payload)
	return buf
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
