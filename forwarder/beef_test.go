package forwarder

import (
	"bytes"
	"fmt"
	"net"
	"testing"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/shard-common/seqhash"
	"github.com/lightwebinc/shard-common/shard"
)

var beefTestObj = []byte{0x01, 0x00, 0xBE, 0xEF, 0xAA, 0xBB, 0xCC, 0xDD}

func makeBEEFForwarder(t *testing.T) (*Forwarder, *shard.PlaneEngine) {
	t.Helper()
	fw := makeForwarder()
	pe, err := shard.NewPlane(0xFF05, shard.DefaultGroupID, 4, shard.DomainBEEF)
	if err != nil {
		t.Fatalf("NewPlane: %v", err)
	}
	fw.SetBEEF(pe, 1<<20)
	return fw, pe
}

func buildBEEFRecordBytes(t *testing.T, topics []string, obj []byte) []byte {
	t.Helper()
	rec, err := objfmt.EncodeBEEFRecord(topics, obj)
	if err != nil {
		t.Fatalf("EncodeBEEFRecord: %v", err)
	}
	return rec
}

func buildBEEFFrameBytes(t *testing.T, topic string, obj []byte) []byte {
	t.Helper()
	raw, err := objfmt.BEEFMulticastBytes(objfmt.TopicID(topic), obj)
	if err != nil {
		t.Fatalf("BEEFMulticastBytes: %v", err)
	}
	return raw
}

// captureEnqueued drains egr, returning each queued datagram (copied) with
// its destination.
func captureEnqueued(egr *Egress) (frames [][]byte, dsts []*net.UDPAddr) {
	egr.FlushVia(func(_ int, b []byte, addr *net.UDPAddr) error {
		frames = append(frames, append([]byte(nil), b...))
		dsts = append(dsts, addr)
		return nil
	})
	return frames, dsts
}

// TestDispatchClass_BEEFOpenClass proves BEEF input (framed V9 and bare
// submission records) is admitted on every ingress class — BEEF is an open
// class, unlike the miner-gated subtree/block lanes.
func TestDispatchClass_BEEFOpenClass(t *testing.T) {
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}
	framed := func(t *testing.T) []byte { return buildBEEFFrameBytes(t, "tm_open", beefTestObj) }
	record := func(t *testing.T) []byte { return buildBEEFRecordBytes(t, []string{"tm_open"}, beefTestObj) }

	for _, class := range []IngressClass{IngressPrivileged, IngressTransaction, IngressBEEF} {
		for name, build := range map[string]func(*testing.T) []byte{"framed": framed, "record": record} {
			t.Run(fmt.Sprintf("%s/class_%d", name, class), func(t *testing.T) {
				fw, _ := makeBEEFForwarder(t)
				conn, _ := openLoopbackUDP(t)
				egr := makeEgress(t, fw, conn)
				fw.DispatchClass(egr, build(t), src, 0, class)
				if got := countEnqueued(egr); got != 1 {
					t.Errorf("class %d %s: enqueued %d, want 1", class, name, got)
				}
			})
		}
	}
}

// TestDispatchClass_BEEFLaneRejectsOthers proves the dedicated lane is
// single-class: non-BEEF grammars and frame classes drop.
func TestDispatchClass_BEEFLaneRejectsOthers(t *testing.T) {
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}
	cases := map[string][]byte{
		"brc124_tx":     buildV2Frame(t, 0xAB, 0, []byte("tx")),
		"brc134_anchor": buildAnchorFrame(t, 0xAB, 0, []byte("anchor")),
		"bare_tx_bytes": {0x01, 0x00, 0x00, 0x00, 0x00},
	}
	for name, raw := range cases {
		fw, _ := makeBEEFForwarder(t)
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		fw.DispatchClass(egr, raw, src, 0, IngressBEEF)
		if got := countEnqueued(egr); got != 0 {
			t.Errorf("%s on BEEF lane: enqueued %d, want 0", name, got)
		}
	}
}

// TestSubmitBEEF_SingleTopic proves one single-topic record emits one stamped
// frame addressed into the 0x1000 plane band, carrying the record verbatim as
// its payload and the record's ContentID.
func TestSubmitBEEF_SingleTopic(t *testing.T) {
	fw, pe := makeBEEFForwarder(t)
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}
	conn, _ := openLoopbackUDP(t)
	egr := makeEgress(t, fw, conn)

	topic := "tm_alpha"
	rec := buildBEEFRecordBytes(t, []string{topic}, beefTestObj)
	fw.SubmitBEEF(egr, rec, src, 0)

	frames, _ := captureEnqueued(egr)
	if len(frames) != 1 {
		t.Fatalf("enqueued %d frames, want 1 (single topic)", len(frames))
	}
	bf, err := frame.DecodeBEEF(frames[0])
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !bytes.Equal(bf.Payload, rec) {
		t.Errorf("payload is not the submission record verbatim")
	}
	if bf.ContentID != objfmt.ContentID(rec) {
		t.Errorf("ContentID mismatch (must be over the record payload)")
	}
	if bf.Deliverable() != 1 {
		t.Errorf("DeliverCount = %d, want 1", bf.DeliverCount)
	}
	if bf.TopicID != objfmt.TopicID(topic) {
		t.Errorf("TopicID mismatch")
	}
	if bf.SeqNum == 0 || bf.HashKey == 0 {
		t.Errorf("not stamped (HashKey %X SeqNum %d)", bf.HashKey, bf.SeqNum)
	}
	if idx := pe.GroupIndex(&bf.TopicID); idx < 0x1000 || idx > 0x1FFF {
		t.Errorf("group 0x%04X outside the BEEF band", idx)
	}
}

// TestSubmitBEEF_Rejects covers the admission bounds: bad grammar, oversize
// object, unknown leading marker.
func TestSubmitBEEF_Rejects(t *testing.T) {
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}

	good := buildBEEFRecordBytes(t, []string{"tm_x"}, beefTestObj)
	badVer := append([]byte(nil), good...)
	badVer[2] = 0x7F

	badMarker := buildBEEFRecordBytes(t, []string{"tm_x"}, []byte{0xDE, 0xAD, 0xBE, 0xAA, 0x01})

	cases := map[string]struct {
		rec      []byte
		maxBytes int
	}{
		"malformed":  {badVer, 1 << 20},
		"bad_marker": {badMarker, 1 << 20},
		"oversize":   {good, 4},
	}
	for name, c := range cases {
		fw, _ := makeBEEFForwarder(t)
		fw.beefMaxObject = c.maxBytes
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		fw.SubmitBEEF(egr, c.rec, src, 0)
		if got := countEnqueued(egr); got != 0 {
			t.Errorf("%s: enqueued %d, want 0", name, got)
		}
	}
}

// TestSubmitBEEF_MultiTopicOpenPath proves a record naming several topics is
// ADMITTED on the open path as ONE frame: the first topic is the shard key
// and the only deliverable one, every name rides in the payload, and no
// fan-out happens. A policy lifts the deliverable count for a source it
// knows, clamped to the record, and never past the wire ceiling.
func TestSubmitBEEF_MultiTopicOpenPath(t *testing.T) {
	names := []string{"tm_x", "tm_y", "tm_z", "tm_w", "tm_v"}
	rec := buildBEEFRecordBytes(t, names, beefTestObj)
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}

	fw, _ := makeBEEFForwarder(t)
	conn, _ := openLoopbackUDP(t)
	egr := makeEgress(t, fw, conn)
	fw.SubmitBEEF(egr, rec, src, 0)
	frames, _ := captureEnqueued(egr)
	if len(frames) != 1 {
		t.Fatalf("open path enqueued %d frames for a 5-topic record, want exactly 1", len(frames))
	}
	bf, err := frame.DecodeBEEF(frames[0])
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if bf.TopicID != objfmt.TopicID("tm_x") {
		t.Error("shard key is not the first topic")
	}
	if ids := objfmt.BEEFDeliverableTopicIDs(bf); len(ids) != 1 {
		t.Fatalf("open path deliverable = %d topics, want 1", len(ids))
	}
	if _, topics, _ := objfmt.SplitBEEFPayload(bf.Payload); len(topics) != 5 {
		t.Fatalf("payload carries %d names, want all 5", len(topics))
	}

	// An authenticated source, per the policy, delivers its cap of 3.
	fw.SetBEEFSubmitPolicy(&boundPolicy{auth: net.ParseIP("::1"), lift: 0, deliver: 3})
	egr2 := makeEgress(t, fw, conn)
	fw.SubmitBEEF(egr2, rec, src, 0)
	frames, _ = captureEnqueued(egr2)
	if len(frames) != 1 {
		t.Fatalf("authenticated path enqueued %d frames, want exactly 1 (no expansion)", len(frames))
	}
	bf, _ = frame.DecodeBEEF(frames[0])
	if ids := objfmt.BEEFDeliverableTopicIDs(bf); len(ids) != 3 || ids[2] != objfmt.TopicID("tm_z") {
		t.Fatalf("authenticated deliverable = %d topics, want the first 3", len(ids))
	}

	// A policy answer above the record's count clamps to the record.
	fw.SetBEEFSubmitPolicy(&boundPolicy{auth: net.ParseIP("::1"), deliver: 40})
	egr3 := makeEgress(t, fw, conn)
	fw.SubmitBEEF(egr3, buildBEEFRecordBytes(t, names[:2], beefTestObj), src, 0)
	frames, _ = captureEnqueued(egr3)
	bf, _ = frame.DecodeBEEF(frames[0])
	if bf.Deliverable() != 2 {
		t.Fatalf("DeliverCount = %d, want clamped to the record's 2", bf.DeliverCount)
	}
}

// TestProcessBEEF_PreFramedDeliverCountIsNotThePublishers proves a publisher
// pre-framing its own 0x09 cannot stamp fan-out: the ingress overwrites
// byte 7 from the policy, and a record whose header TopicID is not its first
// topic is dropped before the dedup claim.
func TestProcessBEEF_PreFramedDeliverCountIsNotThePublishers(t *testing.T) {
	names := []string{"tm_x", "tm_y", "tm_z"}
	rec := buildBEEFRecordBytes(t, names, beefTestObj)
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}

	fw, _ := makeBEEFForwarder(t)
	conn, _ := openLoopbackUDP(t)
	egr := makeEgress(t, fw, conn)
	raw, err := objfmt.BEEFMulticastRecord(rec, 3)
	if err != nil {
		t.Fatal(err)
	}
	fw.ProcessBEEF(egr, raw, src, 0)
	frames, _ := captureEnqueued(egr)
	if len(frames) != 1 {
		t.Fatalf("enqueued %d, want 1", len(frames))
	}
	if bf, _ := frame.DecodeBEEF(frames[0]); bf.Deliverable() != 1 {
		t.Fatalf("open ingress forwarded a publisher-stamped DeliverCount %d, want 1", bf.DeliverCount)
	}

	// Header TopicID disagreeing with topics[0] is a conformance drop.
	raw, _ = objfmt.BEEFMulticastRecord(rec, 1)
	wrong := objfmt.TopicID("tm_y")
	copy(raw[56:88], wrong[:])
	egr2 := makeEgress(t, fw, conn)
	fw.ProcessBEEF(egr2, raw, src, 0)
	if got := countEnqueued(egr2); got != 0 {
		t.Fatalf("mismatched header TopicID enqueued %d, want 0", got)
	}
}

// TestProcessBEEF_ZeroIngredientFlow is the D4 spec regression: BEEF flows
// are per (sender, group) with a ZERO 32-byte HashKey ingredient — two
// topics that co-reside in one group share one flow (equal HashKeys,
// consecutive SeqNums), and the HashKey matches
// XXH64(sender ∥ banded groupIdx ∥ zeros) exactly.
func TestProcessBEEF_ZeroIngredientFlow(t *testing.T) {
	fw, pe := makeBEEFForwarder(t)
	srcIP := net.ParseIP("fd00::1234")
	src := &net.UDPAddr{IP: srcIP, Port: 12345}
	conn, _ := openLoopbackUDP(t)
	egr := makeEgress(t, fw, conn)

	// Find two distinct topics whose TopicIDs collide into one group at the
	// test width (16 groups ⇒ a handful of tries).
	topicA := "tm_collide_a"
	tidA := objfmt.TopicID(topicA)
	groupA := pe.GroupIndex(&tidA)
	topicB := ""
	for i := 0; i < 1000; i++ {
		cand := fmt.Sprintf("tm_collide_b%d", i)
		tid := objfmt.TopicID(cand)
		if pe.GroupIndex(&tid) == groupA {
			topicB = cand
			break
		}
	}
	if topicB == "" {
		t.Fatal("no colliding topic found in 1000 tries")
	}

	fw.ProcessBEEF(egr, buildBEEFFrameBytes(t, topicA, beefTestObj), src, 0)
	fw.ProcessBEEF(egr, buildBEEFFrameBytes(t, topicB, append(beefTestObj, 0x01)), src, 0)

	frames, _ := captureEnqueued(egr)
	if len(frames) != 2 {
		t.Fatalf("enqueued %d, want 2", len(frames))
	}
	a, _ := frame.DecodeBEEF(frames[0])
	b, _ := frame.DecodeBEEF(frames[1])

	var ipArr [16]byte
	copy(ipArr[:], srcIP.To16())
	var zeroIngredient [32]byte
	want := seqhash.Hash(ipArr, groupA, zeroIngredient)

	if a.HashKey != want || b.HashKey != want {
		t.Fatalf("HashKeys %X/%X, want %X (zero ingredient, banded group)", a.HashKey, b.HashKey, want)
	}
	if a.SeqNum != 1 || b.SeqNum != 2 {
		t.Fatalf("SeqNums %d/%d, want 1/2 (one flow per (sender, group), topics interleave)", a.SeqNum, b.SeqNum)
	}
}

// pairDedup is an in-memory TxidDedup that remembers every claimed key.
type pairDedup struct{ seen map[[32]byte]bool }

func (f *pairDedup) Claim(_ string, txid [32]byte) (bool, error) {
	if f.seen[txid] {
		return false, nil
	}
	f.seen[txid] = true
	return true, nil
}

// TestProcessBEEF_PairDedup is the D5 regression: dedup claims key the
// (ContentID, TopicID) pair, so a multi-topic submission's siblings both
// pass while an exact re-submission is suppressed.
func TestProcessBEEF_PairDedup(t *testing.T) {
	fw, _ := makeBEEFForwarder(t)
	fw.SetTxidDedup(&pairDedup{seen: map[[32]byte]bool{}}, "bsp:beef:")
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}
	conn, _ := openLoopbackUDP(t)

	// The same object submitted to two DIFFERENT topics as separate
	// single-topic records are distinct (ContentID, TopicID) pairs — neither
	// suppresses the other.
	egrA := makeEgress(t, fw, conn)
	fw.SubmitBEEF(egrA, buildBEEFRecordBytes(t, []string{"tm_a"}, beefTestObj), src, 0)
	if got := countEnqueued(egrA); got != 1 {
		t.Fatalf("tm_a submission enqueued %d, want 1", got)
	}
	egrB := makeEgress(t, fw, conn)
	fw.SubmitBEEF(egrB, buildBEEFRecordBytes(t, []string{"tm_b"}, beefTestObj), src, 0)
	if got := countEnqueued(egrB); got != 1 {
		t.Fatalf("tm_b (same object, new topic) enqueued %d, want 1 (distinct pair)", got)
	}

	// Re-submitting the same (object, topic) pair is suppressed by the pair claim.
	egr2 := makeEgress(t, fw, conn)
	fw.SubmitBEEF(egr2, buildBEEFRecordBytes(t, []string{"tm_a"}, beefTestObj), src, 0)
	if got := countEnqueued(egr2); got != 0 {
		t.Fatalf("re-submission of tm_a enqueued %d, want 0 (pair claim held)", got)
	}

	// Same object to a NEW topic is a fresh pair — never suppressed.
	egr3 := makeEgress(t, fw, conn)
	fw.SubmitBEEF(egr3, buildBEEFRecordBytes(t, []string{"tm_c"}, beefTestObj), src, 0)
	if got := countEnqueued(egr3); got != 1 {
		t.Fatalf("new-topic submission enqueued %d, want 1", got)
	}
}

// TestProcessBEEF_Fragmentation proves an object exceeding the datagram
// capacity leaves as BRC-130 fragments carrying OrigFrameVer 0x09.
func TestProcessBEEF_Fragmentation(t *testing.T) {
	fw, _ := makeBEEFForwarder(t)
	fw.SetFragMTU(1280)
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}
	conn, _ := openLoopbackUDP(t)
	egr := makeEgress(t, fw, conn)

	big := make([]byte, 4096)
	copy(big, []byte{0x01, 0x00, 0xBE, 0xEF})
	fw.ProcessBEEF(egr, buildBEEFFrameBytes(t, "tm_big", big), src, 0)

	frames, _ := captureEnqueued(egr)
	if len(frames) < 2 {
		t.Fatalf("enqueued %d datagrams, want ≥2 fragments", len(frames))
	}
	for i, raw := range frames {
		if raw[6] != frame.FrameVerV3 {
			t.Fatalf("fragment %d: FrameVer 0x%02X, want 0x03", i, raw[6])
		}
		ff, err := frame.DecodeFragment(raw)
		if err != nil {
			t.Fatalf("fragment %d: %v", i, err)
		}
		if ff.OrigFrameVer != frame.FrameVerV9 {
			t.Fatalf("fragment %d: OrigFrameVer 0x%02X, want 0x09", i, ff.OrigFrameVer)
		}
	}
}

// TestFragmentBEEF_CarriesDeliverCount is the silent-under-delivery
// regression: byte 7 rides the fragment header's MsgType slot, so a record
// big enough to fragment reassembles with the same deliverable prefix as a
// small one. Without it a 3-topic record over the MTU reaches only its
// first topic's subscribers, and nothing counts a drop.
func TestFragmentBEEF_CarriesDeliverCount(t *testing.T) {
	fw, _ := makeBEEFForwarder(t)
	fw.SetFragMTU(1280)
	fw.SetBEEFSubmitPolicy(&boundPolicy{auth: net.ParseIP("::1"), deliver: 3})
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}
	conn, _ := openLoopbackUDP(t)
	egr := makeEgress(t, fw, conn)

	big := make([]byte, 4096)
	copy(big, []byte{0x01, 0x00, 0xBE, 0xEF})
	fw.SubmitBEEF(egr, buildBEEFRecordBytes(t, []string{"tm_a", "tm_b", "tm_c"}, big), src, 0)

	frames, _ := captureEnqueued(egr)
	if len(frames) < 2 {
		t.Fatalf("enqueued %d datagrams, want >=2 fragments", len(frames))
	}
	for i, raw := range frames {
		ff, err := frame.DecodeFragment(raw)
		if err != nil {
			t.Fatalf("fragment %d: %v", i, err)
		}
		if ff.MsgType != 3 {
			t.Fatalf("fragment %d: byte 7 = %d, want the DeliverCount 3", i, ff.MsgType)
		}
	}
}

// The object bound MUST hold on the pre-framed path too: without it, framing
// the object as FrameVer 0x09 bypasses -beef-max-object-bytes entirely
// (BRC-149 makes the bound an ingress MUST, not a per-grammar nicety).
func TestProcessBEEF_PreFramedOversizeDropped(t *testing.T) {
	fw, _ := makeBEEFForwarder(t)
	src := &net.UDPAddr{IP: net.ParseIP("fd00::99"), Port: 12345}
	conn, _ := openLoopbackUDP(t)
	egr := makeEgress(t, fw, conn)

	big := make([]byte, 4096)
	copy(big, beefTestObj) // valid marker, oversize body
	fw.SetBEEF(fw.beefEngine, 1024)

	fw.ProcessBEEF(egr, buildBEEFFrameBytes(t, "tm_oversize", big), src, 0)
	if frames, _ := captureEnqueued(egr); len(frames) != 0 {
		t.Fatalf("oversize pre-framed V9 forwarded (%d frames) — bound bypassed", len(frames))
	}

	// Same object under the bound passes (the bound, not the path, decides).
	fw.SetBEEF(fw.beefEngine, 8192)
	fw.ProcessBEEF(egr, buildBEEFFrameBytes(t, "tm_oversize2", big), src, 0)
	if frames, _ := captureEnqueued(egr); len(frames) != 1 {
		t.Fatalf("in-bound pre-framed V9 not forwarded")
	}
}
