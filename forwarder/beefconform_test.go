package forwarder

import (
	"net"
	"testing"

	"github.com/lightwebinc/shard-common/objfmt"
)

// buildBEEFFrameBytesTopicID builds a FrameVer 0x09 frame for an explicit
// TopicID, so a test can express identifiers no topic name hashes to (the
// zero value) rather than going through objfmt.TopicID.
func buildBEEFFrameBytesTopicID(t *testing.T, topicID [32]byte, obj []byte) []byte {
	t.Helper()
	raw, err := objfmt.BEEFMulticastBytes(topicID, obj)
	if err != nil {
		t.Fatalf("BEEFMulticastBytes: %v", err)
	}
	return raw
}

// TestProcessBEEF_ZeroTopicDropped: the submission-record grammar rejects a
// record naming no topic (count must be 1..15), so pre-framing must not be a
// way around it. A frame carrying the zero TopicID is undeliverable by
// construction, yet it would still cost a full fabric emission and, because
// an empty topic election matches every topic, land on every aggregator
// consumer and be billed to them.
func TestProcessBEEF_ZeroTopicDropped(t *testing.T) {
	fw, _ := makeBEEFForwarder(t)
	src := &net.UDPAddr{IP: net.ParseIP("fd00::99"), Port: 12345}
	conn, _ := openLoopbackUDP(t)
	egr := makeEgress(t, fw, conn)

	fw.ProcessBEEF(egr, buildBEEFFrameBytesTopicID(t, [32]byte{}, beefTestObj), src, 0)
	if frames, _ := captureEnqueued(egr); len(frames) != 0 {
		t.Fatalf("zero-TopicID frame forwarded (%d frames) - a frame addressed to no topic must not reach the plane", len(frames))
	}

	// A real topic on the same object passes: the TopicID decides, not the path.
	fw.ProcessBEEF(egr, buildBEEFFrameBytes(t, "tm_conform", beefTestObj), src, 0)
	if frames, _ := captureEnqueued(egr); len(frames) != 1 {
		t.Fatalf("named-topic frame not forwarded - the check rejected legitimate input")
	}
}

// TestProcessBEEF_BadMarkerDropped: BRC-149 makes the leading BEEF marker an
// ingress MUST on every acceptance path. SubmitBEEF enforces it for records;
// without the same check here, arbitrary non-BEEF bytes reach the object
// plane simply by being pre-framed.
func TestProcessBEEF_BadMarkerDropped(t *testing.T) {
	fw, _ := makeBEEFForwarder(t)
	src := &net.UDPAddr{IP: net.ParseIP("fd00::99"), Port: 12345}
	conn, _ := openLoopbackUDP(t)
	egr := makeEgress(t, fw, conn)

	notBEEF := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xAA, 0xBB}
	if objfmt.IsBEEFObject(notBEEF) {
		t.Fatal("test fixture is a valid BEEF object - pick different bytes")
	}
	fw.ProcessBEEF(egr, buildBEEFFrameBytes(t, "tm_marker", notBEEF), src, 0)
	if frames, _ := captureEnqueued(egr); len(frames) != 0 {
		t.Fatalf("non-BEEF payload forwarded (%d frames) - the marker MUST applies to pre-framed input too", len(frames))
	}

	// Each recognised marker still passes.
	for name, obj := range map[string][]byte{
		"brc62":  {0x01, 0x00, 0xBE, 0xEF, 0x11},
		"brc96":  {0x02, 0x00, 0xBE, 0xEF, 0x22},
		"atomic": {0x01, 0x01, 0x01, 0x01, 0x33},
	} {
		fw.ProcessBEEF(egr, buildBEEFFrameBytes(t, "tm_marker_"+name, obj), src, 0)
		if frames, _ := captureEnqueued(egr); len(frames) != 1 {
			t.Fatalf("%s-marked object not forwarded", name)
		}
	}
}

// TestProcessBEEF_RejectedFrameKeepsNoDedupClaim: both conformance checks run
// before the ingress dedup claim. If a drop burned the (ContentID, TopicID)
// key, a corrected re-submission of the same object would be suppressed as a
// duplicate for the whole TTL, which would turn a transient publisher error
// into a lasting one.
func TestProcessBEEF_RejectedFrameKeepsNoDedupClaim(t *testing.T) {
	fw, _ := makeBEEFForwarder(t)
	src := &net.UDPAddr{IP: net.ParseIP("fd00::99"), Port: 12345}
	conn, _ := openLoopbackUDP(t)
	egr := makeEgress(t, fw, conn)

	d := newFakeDedup(true)
	fw.SetTxidDedup(d, "test:")

	// Same object, same topic: first rejected for its marker, then corrected.
	const topic = "tm_reclaim"
	fw.ProcessBEEF(egr, buildBEEFFrameBytes(t, topic, []byte{0xFF, 0xFF, 0xFF, 0xFF}), src, 0)
	if frames, _ := captureEnqueued(egr); len(frames) != 0 {
		t.Fatal("bad-marker frame forwarded")
	}

	fw.ProcessBEEF(egr, buildBEEFFrameBytes(t, topic, beefTestObj), src, 0)
	if frames, _ := captureEnqueued(egr); len(frames) != 1 {
		t.Fatal("corrected re-submission suppressed - a rejected frame burned its dedup claim")
	}
}
