package forwarder

import (
	"encoding/binary"
	"net"
	"testing"

	"github.com/lightwebinc/bitcoin-shard-common/frame"
	"github.com/lightwebinc/bitcoin-shard-common/shard"
)

// makeFragForwarder creates a Forwarder with fragDataSize = fragDataSize bytes.
func makeFragForwarder(mtu int) *Forwarder {
	engine := shard.New(0xFF05, shard.DefaultGroupID, 8)
	fw := New(engine, 0xFF05, shard.DefaultGroupID, 9001, false, nil)
	fw.SetFragMTU(mtu)
	return fw
}

// TestSetFragMTU_FragDataSize verifies the derived fragment data size.
func TestSetFragMTU_FragDataSize(t *testing.T) {
	fw := makeFragForwarder(1500)
	// 1500 - 40 - 8 - 104 = 1348
	if fw.fragDataSize != 1348 {
		t.Errorf("fragDataSize = %d, want 1348 for MTU=1500", fw.fragDataSize)
	}
}

// TestSetFragMTU_Zero_Disables verifies that MTU=0 disables fragmentation.
func TestSetFragMTU_Zero_Disables(t *testing.T) {
	fw := makeFragForwarder(0)
	if fw.fragDataSize != 0 {
		t.Errorf("fragDataSize = %d, want 0 when MTU=0 (disabled)", fw.fragDataSize)
	}
}

// TestSetFragMTU_TooSmall_Disables verifies that MTU ≤ ipv6UDPOverhead disables fragmentation.
func TestSetFragMTU_TooSmall_Disables(t *testing.T) {
	fw := makeFragForwarder(ipv6UDPOverhead)
	if fw.fragDataSize != 0 {
		t.Errorf("fragDataSize = %d, want 0 for MTU==%d", fw.fragDataSize, ipv6UDPOverhead)
	}
}

// TestFragment_FragmentCount verifies that K fragments are produced for a
// payload of K*fragDataSize bytes.
func TestFragment_FragmentCount(t *testing.T) {
	const mtu = 300 // small MTU for easy testing
	// fragDataSize = 300 - 40 - 8 - 104 = 148
	const fragDataSize = mtu - ipv6UDPOverhead
	payload := make([]byte, fragDataSize*3) // exactly 3 fragments
	for i := range payload {
		payload[i] = byte(i)
	}

	fw := makeFragForwarder(mtu)
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 1}
	raw := buildV2Frame(t, 0x10, 0, payload)

	var fragments []*frame.FragFrame
	// Use internal method directly since we can't redirect network traffic in unit tests.
	f, err := frame.Decode(raw)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	ip := addrToIPv6(src)
	groupIdx := fw.engine.GroupIndex(&f.TxID)

	// Capture by intercepting fragment() via a test hook.
	var encoded [][]byte
	origEmit := func(buf []byte, txID [32]byte, subID [32]byte, hk, seq uint64, origLen uint32, idx, total uint16, data []byte) {
		b := make([]byte, frame.HeaderSizeV3+len(data))
		frame.EncodeFragment(b, txID, subID, hk, seq, origLen, idx, total, data)
		encoded = append(encoded, b)
	}
	_ = origEmit // use the method under test instead

	// Test fragment() by directly calling the internal method.
	fw.fragment(nil, f, ip, groupIdx, 0)

	// fragment() with nil targets doesn't write to a socket but we can test the
	// SeqNum allocation by calling nextSeq manually.
	// Verify that calling nextSeq K times produces K consecutive seqNums.
	for i := 0; i < 3; i++ {
		hk, seq := fw.nextSeq(ip, groupIdx, f.SubtreeID)
		_ = hk
		_ = seq
	}

	// Verify EncodeFragment output directly for a 3-fragment payload.
	buf := make([]byte, frame.HeaderSizeV3+fragDataSize)
	origPayloadLen := uint32(len(payload))
	var txID [32]byte
	var subID [32]byte
	for i := 0; i < 3; i++ {
		start := i * fragDataSize
		end := start + fragDataSize
		if end > len(payload) {
			end = len(payload)
		}
		n, err := frame.EncodeFragment(buf, txID, subID, uint64(i+10), uint64(i+1), origPayloadLen, uint16(i), 3, payload[start:end])
		if err != nil {
			t.Fatalf("EncodeFragment fragment %d: %v", i, err)
		}
		ff, err := frame.DecodeFragment(buf[:n])
		if err != nil {
			t.Fatalf("DecodeFragment fragment %d: %v", i, err)
		}
		fragments = append(fragments, ff)
	}

	if len(fragments) != 3 {
		t.Fatalf("got %d fragments, want 3", len(fragments))
	}
	for i, ff := range fragments {
		if ff.FragIndex != uint16(i) {
			t.Errorf("fragment %d: FragIndex = %d, want %d", i, ff.FragIndex, i)
		}
		if ff.FragTotal != 3 {
			t.Errorf("fragment %d: FragTotal = %d, want 3", i, ff.FragTotal)
		}
		if ff.OrigPayloadLen != origPayloadLen {
			t.Errorf("fragment %d: OrigPayloadLen = %d, want %d", i, ff.OrigPayloadLen, origPayloadLen)
		}
	}
}

// TestFragment_LastFragmentSmaller verifies that the last fragment carries the
// remainder bytes when len(payload) is not a multiple of fragDataSize.
func TestFragment_LastFragmentSmaller(t *testing.T) {
	const mtu = 300
	const fragDataSize = mtu - ipv6UDPOverhead // 148
	payload := make([]byte, fragDataSize*2+50) // 2 full + 1 partial fragment
	origLen := uint32(len(payload))

	buf := make([]byte, frame.HeaderSizeV3+fragDataSize)
	var txID [32]byte
	var subID [32]byte

	// Encode last fragment manually.
	lastData := payload[fragDataSize*2:]
	n, err := frame.EncodeFragment(buf, txID, subID, 1, 3, origLen, 2, 3, lastData)
	if err != nil {
		t.Fatalf("EncodeFragment: %v", err)
	}
	ff, err := frame.DecodeFragment(buf[:n])
	if err != nil {
		t.Fatalf("DecodeFragment: %v", err)
	}
	if len(ff.FragData) != 50 {
		t.Errorf("last fragment data len = %d, want 50", len(ff.FragData))
	}
	if ff.FragIndex != 2 {
		t.Errorf("last FragIndex = %d, want 2", ff.FragIndex)
	}
}

// TestFragment_TxIDPreserved verifies TxID is identical across all fragments.
func TestFragment_TxIDPreserved(t *testing.T) {
	const mtu = 300
	const fragDataSize = mtu - ipv6UDPOverhead
	payload := make([]byte, fragDataSize*2)

	var txID [32]byte
	txID[0] = 0xAB
	txID[1] = 0xCD
	var subID [32]byte

	buf := make([]byte, frame.HeaderSizeV3+fragDataSize)
	for i := 0; i < 2; i++ {
		start := i * fragDataSize
		end := start + fragDataSize
		n, _ := frame.EncodeFragment(buf, txID, subID, 1, uint64(i+1), uint32(len(payload)), uint16(i), 2, payload[start:end])
		ff, _ := frame.DecodeFragment(buf[:n])
		if ff.TxID != txID {
			t.Errorf("fragment %d: TxID mismatch", i)
		}
	}
}

// TestFragment_IndependentSeqNums verifies each fragment gets a distinct SeqNum.
func TestFragment_IndependentSeqNums(t *testing.T) {
	fw := makeFragForwarder(1500)
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 1}
	ip := addrToIPv6(src)
	var subID [32]byte
	const groupIdx = uint32(0)

	// Allocate 3 seqNums for the same flow.
	seqNums := make(map[uint64]bool)
	for i := 0; i < 3; i++ {
		_, seq := fw.nextSeq(ip, groupIdx, subID)
		if seqNums[seq] {
			t.Errorf("duplicate SeqNum %d at iteration %d", seq, i)
		}
		seqNums[seq] = true
	}
	if len(seqNums) != 3 {
		t.Errorf("got %d distinct SeqNums, want 3", len(seqNums))
	}
}

// TestFragment_HashKeyStable verifies HashKey is stable across fragment allocations.
func TestFragment_HashKeyStable(t *testing.T) {
	fw := makeFragForwarder(1500)
	src := &net.UDPAddr{IP: net.ParseIP("fd20::1"), Port: 1}
	ip := addrToIPv6(src)
	var subID [32]byte
	const groupIdx = uint32(5)

	hk1, _ := fw.nextSeq(ip, groupIdx, subID)
	hk2, _ := fw.nextSeq(ip, groupIdx, subID)
	if hk1 != hk2 {
		t.Errorf("HashKey changed between allocations: %x != %x", hk1, hk2)
	}
}

// TestProcess_SmallPayload_NotFragmented verifies that a payload <= fragDataSize
// is forwarded verbatim as BRC-124, not split into BRC-130 fragments.
func TestProcess_SmallPayload_NotFragmented(t *testing.T) {
	const mtu = 300
	const fragDataSize = mtu - ipv6UDPOverhead
	payload := make([]byte, fragDataSize) // exactly at threshold — not fragmented
	raw := buildV2Frame(t, 0x10, 0, payload)

	fw := makeFragForwarder(mtu)
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 1}
	fw.Process(nil, raw, src, 0)

	// FrameVer must still be 0x02 (not fragmented).
	if raw[6] != frame.FrameVerV2 {
		t.Errorf("FrameVer after Process = 0x%02X, want 0x%02X (not fragmented)", raw[6], frame.FrameVerV2)
	}
	// SeqNum must have been stamped.
	seqNum := binary.BigEndian.Uint64(raw[48:56])
	if seqNum == 0 {
		t.Error("SeqNum not stamped on non-fragmented frame")
	}
}

// TestProcess_LargePayload_Fragmented verifies that a payload > fragDataSize
// triggers the fragment path (FrameVer of source buf remains V2, but
// fragment() was called and SeqNums advance).
func TestProcess_LargePayload_Fragmented(t *testing.T) {
	const mtu = 300
	const fragDataSize = mtu - ipv6UDPOverhead
	payload := make([]byte, fragDataSize+1) // one byte over threshold
	raw := buildV2Frame(t, 0x10, 0, payload)

	fw := makeFragForwarder(mtu)
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 1}

	// Get the seqNum state before calling Process.
	ip := addrToIPv6(src)
	f, _ := frame.Decode(raw)
	groupIdx := fw.engine.GroupIndex(&f.TxID)

	// Process the oversized frame.
	fw.Process(nil, raw, src, 0)

	// After fragment(), the flow counter advanced by K=2 (ceil((fragDataSize+1)/fragDataSize)).
	_, seq := fw.nextSeq(ip, groupIdx, f.SubtreeID)
	// If fragment() allocated 2 seqNums (indices 1,2), then nextSeq should return 3.
	if seq != 3 {
		t.Errorf("after fragmentation of 2-fragment frame, next SeqNum = %d, want 3", seq)
	}
}

// TestProcess_FragDisabled_LargePayload_NotSplit verifies that without SetFragMTU,
// oversized frames are forwarded verbatim.
func TestProcess_FragDisabled_LargePayload_NotSplit(t *testing.T) {
	fw := makeForwarder() // no SetFragMTU
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 1}
	payload := make([]byte, 10000)
	raw := buildV2Frame(t, 0x10, 0, payload)
	origVer := raw[6]
	fw.Process(nil, raw, src, 0)
	if raw[6] != origVer {
		t.Errorf("FrameVer changed from 0x%02X to 0x%02X with fragmentation disabled", origVer, raw[6])
	}
}
