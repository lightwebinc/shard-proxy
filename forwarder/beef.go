// BRC-148 BEEF object plane ingress: submission-record admission and the
// FrameVer 0x09 process path.
//
// BEEF is an open ingress class (bounded, election-scoped amplification:
// one record is one frame at any topic count), so records and framed 0x09
// input are admitted on the public tx port as well as the optional dedicated
// lane. The forwarder carries one submission record as ONE frame whose
// payload is the record verbatim (every name reaches the subscriber) and
// whose DeliverCount says how many leading names are deliverable: 1 on the
// open path, the policy's cap on an authenticated path: claims ingress
// dedup per (ContentID, TopicID) pair, and stamps HashKey/SeqNum with the
// domain-tagged group index and a ZERO 32-byte ingredient, the spec
// excludes TopicID from the flow key so retransmission state stays bounded
// by groups × sources, independent of topic count.

package forwarder

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/shard-common/shard"
	"github.com/lightwebinc/shard-proxy/metrics"
)

// beefZeroIngredient is the zeroed 32-byte HashKey/flow-key ingredient for
// BEEF flows (BRC-148 §Frame carriage: TopicID is excluded, flows are per
// (sender, group)).
var beefZeroIngredient [32]byte

// SetBEEF wires the BEEF object plane: the plane-aware derivation engine and
// the accepted-object byte bound. Must be called before workers start; until
// then BEEF submissions and frames drop as "disabled".
// BEEFMaxObject returns the operator's per-object byte bound (0 = unbounded),
// for acceptance paths that must fail fast BEFORE buffering a declared length.
func (fw *Forwarder) BEEFMaxObject() int { return fw.beefMaxObject }

func (fw *Forwarder) SetBEEF(pe *shard.PlaneEngine, maxObjectBytes int) {
	fw.beefEngine = pe
	fw.beefMaxObject = maxObjectBytes
}

// beefClaimKey derives the ingress-dedup claim key for one emitted BEEF
// frame: SHA-256(ContentID ∥ TopicID). ContentID is over the payload, so the
// same object under a different label set is a new submission; keying the
// pair keeps a later re-submission to a new first topic from being
// suppressed as well.
func beefClaimKey(contentID, topicID [32]byte) [32]byte {
	h := sha256.New()
	h.Write(contentID[:])
	h.Write(topicID[:])
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// BEEFSubmitPolicy extends submission admission beyond the OSS stance. A
// record names 1..15 topics on every path and is never rejected for its
// count; what differs per path is how many of its leading names are
// DELIVERABLE (matched by delivery edges) rather than labels the subscriber
// merely receives. The OSS default (nil policy) delivers the first topic
// only, the open ingress has no identity to hang more on, and one
// deliverable topic per record is what keeps the open path free of
// amplification. A downstream build installs a policy that decides per
// SOURCE (e.g. consumer-tunnel STE ranges) up to its cap.
type BEEFSubmitPolicy interface {
	// DeliverCount returns how many of the leading n topics a record from src
	// may deliver. The forwarder clamps the answer to [1, n]; there is no
	// reject.
	DeliverCount(src net.IP, n int) int
	// MaxObjectBytes returns a per-source object bound overriding the operator's
	// open-ingress bound (0 = no override). Authenticated sources may carry a
	// larger allowance than the open path: identity makes abuse accountable
	// (roadmap D9). It may only RAISE the bound: the caller keeps the stricter
	// of the two, so a policy can never widen past the configured ceiling by
	// returning something absurd.
	MaxObjectBytes(src net.IP) int
}

// SetBEEFSubmitPolicy installs the deliverable-topic policy (nil = the OSS
// stance: one deliverable topic per record).
func (fw *Forwarder) SetBEEFSubmitPolicy(p BEEFSubmitPolicy) { fw.beefPolicy = p }

// beefObjectBoundFor resolves the object bound for one submitter: the policy's
// per-source allowance when it EXCEEDS the operator's open bound, else the open
// bound. Never returns less than configured, a policy cannot tighten below the
// operator's floor, only lift an authenticated source above it.
func (fw *Forwarder) beefObjectBoundFor(src net.Addr) int {
	base := fw.beefMaxObject
	if fw.beefPolicy == nil {
		return base
	}
	if lifted := fw.beefPolicy.MaxObjectBytes(srcIPOf(src)); lifted > base {
		return lifted
	}
	return base
}

// beefRecordMaxEnvelope is the largest BRC-149 submission-record envelope the
// grammar allows around its object.
const beefRecordMaxEnvelope = objfmt.BEEFRecordMaxEnvelope

// BEEFRecordBound returns how many bytes a stream reader may buffer for one
// submission record from src: that submitter's object bound plus the largest
// envelope. A record the reader cannot delimit within this many bytes carries
// an object SubmitBEEF would reject anyway, so the reader stops rather than
// buffering it. 0 = no bound configured (the caller keeps its own ceiling).
func (fw *Forwarder) BEEFRecordBound(src net.Addr) int {
	obj := fw.beefObjectBoundFor(src)
	if obj <= 0 {
		return 0
	}
	return obj + beefRecordMaxEnvelope
}

// srcIPOf extracts the bare IP from a UDP/TCP source address (nil if unknown).
func srcIPOf(src net.Addr) net.IP {
	switch a := src.(type) {
	case *net.UDPAddr:
		return a.IP
	case *net.TCPAddr:
		return a.IP
	}
	return nil
}

// rejectOnBEEFLane drops a non-BEEF datagram received on the dedicated BEEF
// lane (single-class port).
func (fw *Forwarder) rejectOnBEEFLane(egr *Egress, workerID int) {
	if fw.rec != nil {
		fw.rec.PacketDropped(egrIface(egr), workerID, "non_beef_on_beef_lane")
	}
}

// egrIface returns the metrics interface label for egr (mirrors the other
// drop paths).
func egrIface(egr *Egress) string {
	if egr != nil && len(egr.targets) > 0 {
		return egr.targets[0].Iface.Name
	}
	return ""
}

// SubmitBEEF admits one BRC-148 submission record: (topic list, BEEF
// object). It validates the record grammar, the object's leading marker, and
// the size bound, then emits ONE FrameVer 0x09 frame carrying the record
// verbatim via [Forwarder.ProcessBEEF], with DeliverCount from the policy
// (1 without one). rec must be the complete record (one record per UDP
// datagram; the TCP lane splits the stream via objfmt.Reader).
//
// It is [Forwarder.PrepareBEEF] then [Forwarder.DispatchBEEF]: a lane that
// reads ahead calls the two apart, the first off its dispatching goroutine.
func (fw *Forwarder) SubmitBEEF(egr *Egress, rec []byte, src net.Addr, workerID int) {
	fw.DispatchBEEF(egr, fw.PrepareBEEF(rec, src), src, workerID)
}

// PreparedBEEF is a submission record checked and framed for the plane by
// [Forwarder.PrepareBEEF], for [Forwarder.DispatchBEEF] to admit.
type PreparedBEEF struct {
	// frame is the FrameVer 0x09 frame, nil when the record was refused.
	frame []byte
	// result is the submission's result, "ok" when framed.
	result  string
	topics  int
	deliver int
}

// Len is the bytes the prepared frame holds: what a lane reading ahead
// counts against its bound.
func (p *PreparedBEEF) Len() int {
	if p == nil {
		return 0
	}
	return len(p.frame)
}

// PrepareBEEF is the part of SubmitBEEF that needs no order: the record's
// grammar, the object's bound and marker, the DeliverCount from the policy,
// and the frame with its ContentID (a SHA-256d of the whole object). A lane
// that reads ahead runs it on its reader goroutine, in parallel with the
// dispatch of the record before; the frame is a copy, so rec may be reused
// once it returns. Nothing here is a reject: the result is recorded by
// DispatchBEEF.
func (fw *Forwarder) PrepareBEEF(rec []byte, src net.Addr) *PreparedBEEF {
	p, r, deliver := fw.prepareBEEFRecord(rec, src)
	if r == nil {
		return p
	}
	buf, err := objfmt.BEEFMulticastRecord(rec, deliver)
	if err != nil {
		p.result = "malformed"
		return p
	}
	p.frame, p.topics, p.deliver = buf, len(r.Topics), deliver
	return p
}

// BEEFFrameHeadroom is the room a lane leaves ahead of a record it reads for
// [Forwarder.PrepareBEEFFramed]: the FrameVer 0x09 frame header.
const BEEFFrameHeadroom = frame.HeaderSize

// PrepareBEEFFramed is [Forwarder.PrepareBEEF] for a record that was read into
// buf[BEEFFrameHeadroom:], with the header's room left ahead of it: the frame is
// built in place, its header written into buf[:BEEFFrameHeadroom], instead of
// being copied into an allocation of its own. A lane that reads records into
// pooled buffers then frames each one without a fresh allocation the size of
// the object, the zeroing that comes with it, or a copy of the object. The
// prepared record is byte for byte what PrepareBEEF makes of the same record.
//
// The frame is buf itself, so the caller keeps buf untouched, and does not
// reuse it, until the record is done with: refused (the prepared record has no
// frame, or AdmitBEEF returns nil), or fragmented (FragmentBEEFInto has
// returned, whatever its result). buf shorter than the headroom is malformed.
func (fw *Forwarder) PrepareBEEFFramed(buf []byte, src net.Addr) *PreparedBEEF {
	if len(buf) < BEEFFrameHeadroom {
		return &PreparedBEEF{result: "malformed"}
	}
	rec := buf[BEEFFrameHeadroom:]
	p, r, deliver := fw.prepareBEEFRecord(rec, src)
	if r == nil {
		return p
	}
	// The header as frame.EncodeBEEF writes it for this record (unstamped:
	// HashKey and SeqNum zero, as objfmt.BEEFMulticastRecord leaves them),
	// with no payload to copy: the record is already where the payload goes.
	hdr := &frame.BEEFFrame{ContentID: objfmt.ContentID(rec), TopicID: objfmt.TopicID(r.Topics[0]), DeliverCount: uint8(deliver)}
	if _, err := frame.EncodeBEEF(hdr, buf[:BEEFFrameHeadroom]); err != nil {
		p.result = "malformed"
		return p
	}
	binary.BigEndian.PutUint32(buf[BEEFFrameHeadroom-4:BEEFFrameHeadroom], uint32(len(rec)))
	p.frame, p.topics, p.deliver = buf, len(r.Topics), deliver
	return p
}

// prepareBEEFRecord is what PrepareBEEF and PrepareBEEFFramed share: the
// record's grammar, the object's bound and marker, and the DeliverCount. r is
// nil when the record is refused, with p.result saying why.
func (fw *Forwarder) prepareBEEFRecord(rec []byte, src net.Addr) (p *PreparedBEEF, r *objfmt.BEEFRecord, deliver int) {
	p = &PreparedBEEF{result: "ok"}
	if fw.beefEngine == nil {
		p.result = "disabled"
		return p, nil, 0
	}
	r, n, err := objfmt.DecodeBEEFRecord(rec)
	if err != nil || n != len(rec) {
		p.result = "malformed"
		return p, nil, 0
	}
	if maxObj := fw.beefObjectBoundFor(src); maxObj > 0 && len(r.Object) > maxObj {
		p.result = "oversize"
		return p, nil, 0
	}
	if !objfmt.IsBEEFObject(r.Object) {
		p.result = "bad_marker"
		return p, nil, 0
	}
	if len(r.Topics) == 0 {
		// DecodeBEEFRecord admits no topicless record; this keeps the
		// framed path's r.Topics[0] safe whatever the grammar becomes.
		p.result = "malformed"
		return p, nil, 0
	}
	// One record, one frame, at any topic count: the payload is the record
	// verbatim so every name the publisher wrote reaches the subscriber, and
	// DeliverCount bounds how many of the leading names delivery edges match.
	// The open path (no policy) delivers the first topic only, which is what
	// keeps a free, anonymous record from becoming fan-out; a policy may lift
	// an identified source to its cap.
	return p, r, fw.beefDeliverCount(src, len(r.Topics))
}

// DispatchBEEF admits a prepared record: the submission is recorded, and a
// framed one is metered and processed in order. The frame's ContentID was
// computed by PrepareBEEF from the bytes it framed, so it is not computed a
// second time to be checked.
func (fw *Forwarder) DispatchBEEF(egr *Egress, p *PreparedBEEF, src net.Addr, workerID int) {
	if p == nil {
		return
	}
	if fw.rec != nil {
		defer fw.rec.BEEFSubmission(p.result)
	}
	if p.frame == nil {
		return
	}
	if fw.rec != nil {
		fw.rec.IngressMetered(metrics.IngressClassBEEF, false, len(p.frame))
		fw.rec.BEEFTopics(p.topics, p.deliver)
	}
	fw.processBEEF(egr, p.frame, src, workerID, false)
}

// beefDeliverCount resolves how many of a record's n leading topics are
// deliverable for src: the policy's answer clamped to [1, n], or 1 when no
// policy is installed or the source is unknown (relay, spine re-emit).
func (fw *Forwarder) beefDeliverCount(src net.Addr, n int) int {
	deliver := 1
	if fw.beefPolicy != nil {
		if ip := srcIPOf(src); ip != nil {
			deliver = fw.beefPolicy.DeliverCount(ip, n)
		}
	}
	if deliver < 1 {
		deliver = 1
	}
	if deliver > n {
		deliver = n
	}
	return deliver
}

// ProcessBEEF handles a framed BRC-148 BEEF object (FrameVer 0x09): decode,
// per-(ContentID, TopicID) ingress dedup, domain-tagged group derivation
// from the TopicID, HashKey/SeqNum stamping with the zero ingredient,
// BRC-130 fragmentation for objects exceeding the datagram capacity, and
// enqueue to the plane's multicast group.
//
// raw must remain valid until egr.Flush returns. egr may be nil for tests.
// A nil src (spine re-emit) skips stamping, mirroring [Forwarder.Process].
// The frame's ContentID is checked against its payload: a pre-framed
// submission presents its own.
func (fw *Forwarder) ProcessBEEF(egr *Egress, raw []byte, src net.Addr, workerID int) {
	fw.processBEEF(egr, raw, src, workerID, true)
}

// processBEEF is ProcessBEEF; checkContentID is false for a frame this
// forwarder built itself (DispatchBEEF).
func (fw *Forwarder) processBEEF(egr *Egress, raw []byte, src net.Addr, workerID int, checkContentID bool) {
	bf, groupIdx, ok := fw.checkBEEF(egr, raw, src, workerID, checkContentID)
	if !ok {
		return
	}

	if src != nil {
		ip := addrToIPv6(src)

		if fw.fragDataSize > 0 && len(bf.Payload) > fw.fragDataSize {
			fw.fragmentBEEF(egr, bf, ip, groupIdx, workerID)
			return
		}

		stampInPlace(raw, ip, groupIdx, beefZeroIngredient, fw)
	}

	if egr == nil {
		return
	}
	dst := fw.addrFor(groupIdx)
	egr.EnqueueData(raw, *dst, groupIdx, workerID)

	if fw.debug {
		fw.log.Debug("beef forwarded",
			"topic_prefix", fmt.Sprintf("%x", bf.TopicID[:4]),
			"group_idx", groupIdx,
			"src", src,
			"dst", dst,
		)
	}
}

// checkBEEF is the admission half of processBEEF: every check a FrameVer
// 0x09 frame must pass, the DeliverCount overwrite, the rate budget and the
// dedup claim, then the group the frame belongs to. ok is false for a drop,
// which is recorded; egr only names the interface the drop is counted
// against and may be nil.
func (fw *Forwarder) checkBEEF(egr *Egress, raw []byte, src net.Addr, workerID int, checkContentID bool) (bf *frame.BEEFFrame, groupIdx uint32, ok bool) {
	if fw.beefEngine == nil {
		if fw.rec != nil {
			fw.rec.PacketDropped(egrIface(egr), workerID, "beef_disabled")
		}
		return nil, 0, false
	}
	bf, err := frame.DecodeBEEF(raw)
	if err != nil {
		fw.log.Debug("beef frame decode error", "err", err, "len", len(raw))
		if fw.rec != nil {
			fw.rec.PacketDropped(egrIface(egr), workerID, "decode_error")
		}
		return nil, 0, false
	}

	// The operator's object bound applies to EVERY acceptance path, not only the
	// submission record: without this, pre-framing the object as FrameVer 0x09
	// bypasses -beef-max-object-bytes entirely (BRC-149 makes the bound an
	// ingress MUST). Whole frames are datagram/stream-bounded upstream, but the
	// declared length is what downstream reassembly would allocate against.
	// The payload is either the submission record verbatim or a bare object;
	// the bound is on the OBJECT (BRC-149's ingress MUST), so a record may
	// exceed it by its envelope and nothing more. It is the submitter's own
	// bound (beefObjectBoundFor): an authenticated record SubmitBEEF admitted
	// above the open bound must not be dropped here one step later. Only an
	// object over the open bound pays for the policy lookup.
	object, topics, err := objfmt.SplitBEEFPayload(bf.Payload)
	if err != nil {
		if fw.rec != nil {
			fw.rec.PacketDropped(egrIface(egr), workerID, "beef_bad_record")
		}
		return nil, 0, false
	}
	if fw.beefMaxObject > 0 && len(object) > fw.beefMaxObject && len(object) > fw.beefObjectBoundFor(src) {
		if fw.rec != nil {
			fw.rec.PacketDropped(egrIface(egr), workerID, "beef_oversize")
		}
		return nil, 0, false
	}

	// The submission-record grammar enforces a topic count of 1..15 and the
	// BEEF marker (objfmt.DecodeBEEFRecord and IsBEEFObject above), so a
	// publisher that pre-frames its own FrameVer 0x09 must meet the same two
	// conditions or it bypasses both. BRC-149 makes the marker an ingress
	// MUST on every acceptance path, and a frame addressed to no topic is
	// undeliverable by construction: it still costs a full fabric emission,
	// and because an empty topic election matches every topic it lands on
	// every aggregator consumer and is billed to them. A pre-framed RECORD
	// must also put its first topic in the header slot, or the shard key and
	// the names disagree and the edge matches a topic the publisher never
	// led with.
	//
	// These checks sit BEFORE the dedup claim on purpose. A drop taken after
	// the claim would burn the (ContentID, TopicID) key, so a corrected
	// re-submission of the same object would be suppressed as a duplicate
	// for the whole TTL.
	//
	// These are conformance checks, not an abuse control: a TopicID is an
	// unverifiable hash, so anything other than the zero value is accepted
	// here and only the delivery side can tell whether a consumer wanted it.
	if bf.TopicID == ([32]byte{}) {
		if fw.rec != nil {
			fw.rec.PacketDropped(egrIface(egr), workerID, "beef_no_topic")
		}
		return nil, 0, false
	}
	if topics != nil && bf.TopicID != objfmt.TopicID(topics[0]) {
		if fw.rec != nil {
			fw.rec.PacketDropped(egrIface(egr), workerID, "beef_topic_mismatch")
		}
		return nil, 0, false
	}
	if !objfmt.IsBEEFObject(object) {
		if fw.rec != nil {
			fw.rec.PacketDropped(egrIface(egr), workerID, "beef_bad_marker")
		}
		return nil, 0, false
	}

	// DeliverCount is the ingress's to set, never the publisher's: a
	// pre-framed record stamped with 15 deliverable topics from an anonymous
	// source is exactly the amplification the open path exists to refuse.
	// Overwrite it from the policy for any frame that entered here (src set);
	// relay and spine re-emission (src == nil) carry the door's value on.
	if src != nil {
		n := 1
		if topics != nil {
			n = len(topics)
		}
		raw[7] = uint8(fw.beefDeliverCount(src, n))
		bf.DeliverCount = raw[7]
	}

	// Rate budgets are charged before the dedup claim, so a flood of
	// duplicates costs the flooder its budget rather than being free. Relay
	// and spine re-emission (src == nil) is exempt: those frames were already
	// charged at the door where they entered.
	if d := fw.beefLimit.allow(src, len(raw), time.Now()); d != rateAllow {
		if fw.rec != nil {
			fw.rec.PacketDropped(egrIface(egr), workerID, d.reason())
		}
		return nil, 0, false
	}

	// A PRE-FRAMED submission carries its own ContentID, and the dedup key is
	// SHA-256(ContentID ‖ TopicID). Taking that field on trust lets anyone
	// CLAIM AN OBJECT THEY DO NOT HAVE: frame a topic with the ContentID of an
	// object about to be published, win the claim, and every later copy of the
	// real object is suppressed as a duplicate, fleet-wide, from one small
	// frame with no account. That is targeted censorship of exactly the thing
	// the plane exists to deliver. Recomputing it also restores BRC-148's
	// "identity is the bytes" for consumers that trust the field.
	//
	// The record path never had this hole: it computes the ContentID itself
	// (PrepareBEEF above), so a frame it built is not hashed a second time
	// here (checkContentID false). Only pre-framing can present one.
	//
	// Placed AFTER the rate budget and BEFORE the claim, deliberately. SHA-256d
	// costs ~2.8us at 256B and ~5.6ms at 1MiB (measured 2026-09-15), so hashing
	// before the budget would let an unauthenticated flood burn CPU at line
	// rate; after it, the cost is bounded by a budget the operator already
	// chose. Relay and spine re-emission (src == nil) is exempt for the same
	// reason the budget exempts it: the frame was verified at the door where it
	// entered, and re-hashing at every hop multiplies the cost fleet-wide.
	if checkContentID && src != nil && bf.ContentID != objfmt.ContentID(bf.Payload) {
		if fw.rec != nil {
			fw.rec.PacketDropped(egrIface(egr), workerID, "beef_bad_contentid")
		}
		return nil, 0, false
	}

	if !fw.claimBEEFIngress(beefClaimKey(bf.ContentID, bf.TopicID), "brc148", egrIface(egr), workerID) {
		return nil, 0, false
	}

	return bf, fw.beefEngine.GroupIndex(&bf.TopicID), true
}

// fragmentBEEF splits an oversized BEEF object into BRC-130 fragments
// (OrigFrameVer 0x09). ContentID rides the fragment TxID slot (it is the
// BRC-130 reassembly key and SHA-256d verification hash by construction),
// TopicID rides the SubtreeID slot, and DeliverCount rides byte 7 (the
// fragment header's MsgType slot, which exists to carry byte 7 through
// fragmentation), so all three identifiers appear in every fragment; flow
// stamping uses the zero ingredient like whole frames.
//
// Byte 7 matters here: without it a fragmented record reassembles with
// DeliverCount 0, which reads as one deliverable topic, so an object large
// enough to fragment would silently reach only its first topic's
// subscribers while a small one reached all of them.
func (fw *Forwarder) fragmentBEEF(egr *Egress, bf *frame.BEEFFrame, ip [16]byte, groupIdx uint32, workerID int) {
	k, ok := fw.beefFragTotal(bf, workerID)
	if !ok {
		return
	}
	if fw.rec != nil {
		fw.rec.FrameFragmented(workerID, k)
	}

	dst := fw.addrFor(groupIdx)

	for i := 0; i < k; i++ {
		hashKey, seqNum := fw.nextSeq(ip, groupIdx, beefZeroIngredient)
		if egr == nil {
			continue
		}

		bufPtr := egr.PoolGet()
		buf := *bufPtr
		n, err := fw.encodeBEEFFragment(buf, bf, i, k, hashKey, seqNum)
		if err != nil {
			fw.log.Error("EncodeFragment error", "err", err)
			egr.pool.Put(bufPtr)
			continue
		}
		egr.EnqueueDataPooled(buf[:n], *dst, groupIdx, workerID, bufPtr)
	}
}

// beefFragTotal is how many BRC-130 fragments bf's payload takes at the
// fragment data size. ok is false past the 16-bit FragTotal, a drop recorded
// as frag_overflow.
func (fw *Forwarder) beefFragTotal(bf *frame.BEEFFrame, workerID int) (k int, ok bool) {
	k = (len(bf.Payload) + fw.fragDataSize - 1) / fw.fragDataSize
	if k > 65535 {
		fw.log.Warn("beef fragment count exceeds 65535, dropping frame",
			"content_prefix", fmt.Sprintf("%x", bf.ContentID[:4]),
			"payload_len", len(bf.Payload),
		)
		if fw.rec != nil {
			fw.rec.PacketDropped("", workerID, "frag_overflow")
		}
		return 0, false
	}
	return k, true
}

// encodeBEEFFragment writes fragment i of k of bf into buf with the flow's
// hashKey and this fragment's seqNum, and returns the datagram's length.
func (fw *Forwarder) encodeBEEFFragment(buf []byte, bf *frame.BEEFFrame, i, k int, hashKey, seqNum uint64) (int, error) {
	payload := bf.Payload
	start := i * fw.fragDataSize
	end := min(start+fw.fragDataSize, len(payload))
	n, err := frame.EncodeFragment(
		buf,
		bf.ContentID,
		bf.TopicID,
		hashKey,
		seqNum,
		uint32(len(payload)),
		uint16(i),
		uint16(k),
		frame.FrameVerV9,
		payload[start:end],
	)
	if err != nil {
		return 0, err
	}
	// Carry DeliverCount through the fragment header's byte 7, the same
	// slot the block and subtree paths use for their MsgType.
	buf[7] = bf.DeliverCount
	return n, nil
}

// BEEFAdmission is a prepared record [Forwarder.AdmitBEEF] admitted for a
// caller to fragment itself with [Forwarder.FragmentBEEFInto]: the checks
// DispatchBEEF runs, the dedup claim, the group, and the flow's SeqNums,
// reserved in admission order. It is read-only once returned, so the
// fragmenting may run on any goroutine, in parallel with other records', and
// still stamp exactly what the in-line path would have.
type BEEFAdmission struct {
	raw      []byte
	bf       frame.BEEFFrame
	ip       [16]byte
	groupIdx uint32
	dst      *net.UDPAddr
	hashKey  uint64
	firstSeq uint64
	k        int // fragments; 0 = the whole frame, one datagram
	workerID int
}

// Datagrams is how many datagrams FragmentBEEFInto will produce.
func (a *BEEFAdmission) Datagrams() int {
	if a.k > 0 {
		return a.k
	}
	return 1
}

// Bytes is the sum of those datagrams' lengths, for a caller that stages
// them contiguously.
func (a *BEEFAdmission) Bytes() int {
	if a.k > 0 {
		return a.k*frame.HeaderSizeV3 + len(a.bf.Payload)
	}
	return len(a.raw)
}

// Dst is the multicast destination every datagram of the record goes to
// (address and port; the caller adds the interface zone). Shared, read-only.
func (a *BEEFAdmission) Dst() *net.UDPAddr { return a.dst }

// GroupIdx is the record's plane group, what the egress metrics key on.
func (a *BEEFAdmission) GroupIdx() uint32 { return a.groupIdx }

// Seqs is the flow's HashKey and the first of the Datagrams consecutive
// SeqNums the record was reserved (0, 0 when the frame is not stamped: a
// nil source, or a whole frame the submitter pre-stamped).
func (a *BEEFAdmission) Seqs() (hashKey, first uint64) { return a.hashKey, a.firstSeq }

// AdmitBEEF is DispatchBEEF up to the point the datagrams are built: the
// submission is recorded and metered, the frame checked, budgeted and
// claimed, and its SeqNums reserved on the (source, group) flow, in call
// order. nil means the record was refused or dropped, recorded exactly as
// DispatchBEEF records it. The caller then fragments the admission with
// [Forwarder.FragmentBEEFInto], on any goroutine, and sends the datagrams in
// admission order. egr only names the interface drops are counted against
// and may be nil; nothing is enqueued on it.
//
// Reserving on admission is what keeps a flow's SeqNums in emission order
// when records are fragmented in parallel: the counters are shared with
// every other path that stamps the flow (the UDP lane, DispatchBEEF), so
// the calls to this method must be made in the order the records will
// leave.
func (fw *Forwarder) AdmitBEEF(egr *Egress, p *PreparedBEEF, src net.Addr, workerID int) *BEEFAdmission {
	if p == nil {
		return nil
	}
	if fw.rec != nil {
		defer fw.rec.BEEFSubmission(p.result)
	}
	if p.frame == nil {
		return nil
	}
	if fw.rec != nil {
		fw.rec.IngressMetered(metrics.IngressClassBEEF, false, len(p.frame))
		fw.rec.BEEFTopics(p.topics, p.deliver)
	}
	bf, groupIdx, ok := fw.checkBEEF(egr, p.frame, src, workerID, false)
	if !ok {
		return nil
	}
	a := &BEEFAdmission{raw: p.frame, bf: *bf, groupIdx: groupIdx, dst: fw.addrFor(groupIdx), workerID: workerID}
	if src == nil {
		return a
	}
	a.ip = addrToIPv6(src)
	if fw.fragDataSize > 0 && len(bf.Payload) > fw.fragDataSize {
		k, ok := fw.beefFragTotal(bf, workerID)
		if !ok {
			return nil
		}
		a.k = k
		a.hashKey, a.firstSeq = fw.reserveSeq(a.ip, groupIdx, beefZeroIngredient, k)
		return a
	}
	stampInPlace(p.frame, a.ip, groupIdx, beefZeroIngredient, fw)
	a.hashKey = binary.BigEndian.Uint64(p.frame[40:48])
	a.firstSeq = binary.BigEndian.Uint64(p.frame[48:56])
	return a
}

// ReserveBEEFSeqs reserves n consecutive SeqNums on the BEEF flow of src and
// groupIdx (the zero ingredient, as every BEEF frame is stamped) and returns
// the flow's HashKey and the first of them; the next reservation or stamp on
// the flow follows at first+n. AdmitBEEF reserves through it; it is exposed
// for a caller that stamps BEEF datagrams of its own on the same counters.
func (fw *Forwarder) ReserveBEEFSeqs(src net.Addr, groupIdx uint32, n int) (hashKey, first uint64) {
	return fw.reserveSeq(addrToIPv6(src), groupIdx, beefZeroIngredient, n)
}

// errBEEFAlloc is FragmentBEEFInto's error for an alloc that did not return
// the length asked for.
var errBEEFAlloc = errors.New("forwarder: FragmentBEEFInto: alloc returned the wrong length")

// FragmentBEEFInto builds the admitted record's datagrams, each in memory
// from alloc(n) (a slice of exactly n bytes), and returns them in emission
// order: the BRC-130 fragments DispatchBEEF would have enqueued, byte for
// byte, with the reserved SeqNums, or the one whole frame. alloc is called
// once per datagram in that order, so an allocator that carves one
// contiguous buffer lays them out back to back, all but the last of equal
// length. It records the fragmentation metric the in-line path records and
// may run on any goroutine. A nil error means every datagram was built; on
// an error the record must be dropped whole, since its SeqNums are spent.
func (fw *Forwarder) FragmentBEEFInto(a *BEEFAdmission, alloc func(n int) []byte) ([][]byte, error) {
	if a == nil {
		return nil, errors.New("forwarder: FragmentBEEFInto: nil admission")
	}
	if a.k == 0 {
		b := alloc(len(a.raw))
		if len(b) != len(a.raw) {
			return nil, errBEEFAlloc
		}
		copy(b, a.raw)
		return [][]byte{b}, nil
	}
	if fw.rec != nil {
		fw.rec.FrameFragmented(a.workerID, a.k)
	}
	out := make([][]byte, 0, a.k)
	for i := range a.k {
		size := frame.HeaderSizeV3 + min(fw.fragDataSize, len(a.bf.Payload)-i*fw.fragDataSize)
		b := alloc(size)
		if len(b) != size {
			return nil, errBEEFAlloc
		}
		n, err := fw.encodeBEEFFragment(b, &a.bf, i, a.k, a.hashKey, a.firstSeq+uint64(i))
		if err != nil {
			return nil, err
		}
		out = append(out, b[:n])
	}
	return out, nil
}
