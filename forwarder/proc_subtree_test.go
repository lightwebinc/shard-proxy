package forwarder

import (
	"encoding/binary"
	"net"
	"testing"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/shard"

	"github.com/lightwebinc/shard-proxy/metrics"
)

// buildSubtreeDataFrame constructs a BRC-132 (FrameVer V5) subtree data frame.
func buildSubtreeDataFrame(t *testing.T, subByte0 byte, payload []byte) []byte {
	t.Helper()
	sf := &frame.SubtreeDataFrame{
		MsgType: frame.SubtreeMsgHashesOnly,
		Payload: payload,
	}
	sf.SubtreeID[0] = subByte0
	buf := make([]byte, frame.HeaderSize+len(payload))
	n, err := frame.EncodeSubtreeData(sf, buf)
	if err != nil {
		t.Fatalf("EncodeSubtreeData: %v", err)
	}
	return buf[:n]
}

// recEgress builds an Egress wired to a real (private-registry) recorder so the
// recordWrite metrics path is exercised on Flush.
func recEgress(t *testing.T, fw *Forwarder, conns ...*net.UDPConn) *Egress {
	t.Helper()
	rec, err := metrics.New("test", 1, "", 0)
	if err != nil {
		t.Fatalf("metrics.New: %v", err)
	}
	egr := NewEgress(fw, makeTargets(t, conns...), 8, rec)
	t.Cleanup(egr.Flush)
	return egr
}

func TestSetBindSource(t *testing.T) {
	fw := makeForwarder()
	ip := net.ParseIP("2001:db8::1")
	fw.SetBindSource(ip)
	if !fw.bindSource.Equal(ip) {
		t.Errorf("bindSource = %v, want %v", fw.bindSource, ip)
	}
}

func TestProcessSubtreeData_StampsHashKeyAndSeqNum(t *testing.T) {
	fw := makeForwarder()
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}

	raw1 := buildSubtreeDataFrame(t, 0xA1, []byte("nodes-1"))
	fw.ProcessSubtreeData(nil, raw1, src, 0)
	hk1 := binary.BigEndian.Uint64(raw1[40:48])
	seq1 := binary.BigEndian.Uint64(raw1[48:56])
	if hk1 == 0 {
		t.Error("HashKey not stamped")
	}
	if seq1 != 1 {
		t.Errorf("SeqNum = %d, want 1", seq1)
	}

	// Same subtree → same flow, SeqNum increments.
	raw2 := buildSubtreeDataFrame(t, 0xA1, []byte("nodes-2"))
	fw.ProcessSubtreeData(nil, raw2, src, 0)
	if hk2 := binary.BigEndian.Uint64(raw2[40:48]); hk2 != hk1 {
		t.Errorf("HashKey changed across same flow: %x != %x", hk2, hk1)
	}
	if seq2 := binary.BigEndian.Uint64(raw2[48:56]); seq2 != 2 {
		t.Errorf("SeqNum = %d, want 2", seq2)
	}
}

func TestProcessSubtreeData_DistinctSubtrees_IndependentFlows(t *testing.T) {
	fw := makeForwarder()
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 1}

	rawA := buildSubtreeDataFrame(t, 0xAA, nil)
	rawB := buildSubtreeDataFrame(t, 0xBB, nil)
	fw.ProcessSubtreeData(nil, rawA, src, 0)
	fw.ProcessSubtreeData(nil, rawB, src, 0)

	if hkA, hkB := binary.BigEndian.Uint64(rawA[40:48]), binary.BigEndian.Uint64(rawB[40:48]); hkA == hkB {
		t.Errorf("distinct subtrees share HashKey %x", hkA)
	}
	// Each is a fresh flow at SeqNum 1.
	if seqA := binary.BigEndian.Uint64(rawA[48:56]); seqA != 1 {
		t.Errorf("subtree A SeqNum = %d, want 1", seqA)
	}
	if seqB := binary.BigEndian.Uint64(rawB[48:56]); seqB != 1 {
		t.Errorf("subtree B SeqNum = %d, want 1", seqB)
	}
}

func TestProcessSubtreeData_NilSrc_SkipsStamping(t *testing.T) {
	fw := makeForwarder()
	raw := buildSubtreeDataFrame(t, 0x55, nil)
	fw.ProcessSubtreeData(nil, raw, nil, 0)
	if hk := binary.BigEndian.Uint64(raw[40:48]); hk != 0 {
		t.Errorf("nil src stamped HashKey %x, want 0", hk)
	}
}

func TestProcessSubtreeData_DecodeError_Drops(t *testing.T) {
	fw := makeForwarder()
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 1}
	bad := make([]byte, frame.HeaderSize)
	bad[6] = frame.FrameVerV5               // right version, but bad magic/structure
	fw.ProcessSubtreeData(nil, bad, src, 0) // must not panic
}

func TestProcessSubtreeData_EgressEnqueue(t *testing.T) {
	fw := makeForwarder()
	conn, _ := openLoopbackUDP(t)
	egr := recEgress(t, fw, conn)
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 1}
	raw := buildSubtreeDataFrame(t, 0x77, []byte("payload"))
	fw.ProcessSubtreeData(egr, raw, src, 0)
	egr.Flush() // exercises recordWrite for the control-frame path
}

func TestFragmentSubtreeData_PooledFragments(t *testing.T) {
	const mtu = 300
	fragDataSize := mtu - ipv6UDPOverhead
	payload := make([]byte, fragDataSize*3+10) // 4 fragments
	for i := range payload {
		payload[i] = byte(i)
	}
	fw := makeFragForwarder(mtu)
	conn, _ := openLoopbackUDP(t)
	egr := recEgress(t, fw, conn)
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 1}

	raw := buildSubtreeDataFrame(t, 0x88, payload)
	ip := addrToIPv6(src)
	ctrlIdx := uint32(shard.GroupSubtreeAnnounce)
	var sub [32]byte
	sub[0] = 0x88

	fw.ProcessSubtreeData(egr, raw, src, 0) // routes into fragmentSubtreeData
	egr.Flush()

	// 4 fragments were emitted, so the flow counter advanced by 4.
	_, seq := fw.nextSeq(ip, ctrlIdx, sub)
	if seq != 5 {
		t.Errorf("after 4-fragment subtree split, next SeqNum = %d, want 5", seq)
	}
}

func TestFragmentBlock_PooledFragments(t *testing.T) {
	const mtu = 300
	fragDataSize := mtu - ipv6UDPOverhead
	payload := make([]byte, fragDataSize*2+5) // 3 fragments
	fw := makeFragForwarder(mtu)
	conn, _ := openLoopbackUDP(t)
	egr := recEgress(t, fw, conn)
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 1}

	raw := buildBlockBufForwarder(t, 0x99, payload)
	fw.ProcessBlock(egr, raw, src, 0) // routes into fragmentBlock
	egr.Flush()

	ip := addrToIPv6(src)
	var zeroSub [32]byte
	_, seq := fw.nextSeq(ip, uint32(shard.GroupBlockBroadcast), zeroSub)
	if seq != 4 {
		t.Errorf("after 3-fragment block split, next SeqNum = %d, want 4", seq)
	}
}
