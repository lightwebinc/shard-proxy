// BRC-148 BEEF object plane ingress: submission-record expansion and the
// FrameVer 0x09 process path.
//
// BEEF is an open ingress class (bounded, election-scoped amplification —
// each frame delivers to a single topical group), so records and framed 0x09
// input are admitted on the public tx port as well as the optional dedicated
// lane. The forwarder expands one submission record into one frame per
// submitted topic, claims ingress dedup per (ContentID, TopicID) pair, and
// stamps HashKey/SeqNum with the domain-tagged group index and a ZERO
// 32-byte ingredient — the spec excludes TopicID from the flow key so
// retransmission state stays bounded by groups × sources, independent of
// topic count.

package forwarder

import (
	"crypto/sha256"
	"fmt"
	"net"
	"time"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/shard-common/shard"
	"github.com/lightwebinc/shard-proxy/metrics"
)

// beefZeroIngredient is the zeroed 32-byte HashKey/flow-key ingredient for
// BEEF flows (BRC-148 §Frame carriage: TopicID is excluded — flows are per
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
// frame: SHA-256(ContentID ∥ TopicID). Keying the pair — never the bare
// ContentID — keeps a multi-topic submission's sibling emissions and a later
// re-submission of the same object to a new topic from being suppressed.
func beefClaimKey(contentID, topicID [32]byte) [32]byte {
	h := sha256.New()
	h.Write(contentID[:])
	h.Write(topicID[:])
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

// BEEFSubmitPolicy extends submission admission beyond the OSS stance. The
// OSS default (nil policy) admits TopicCount == 1 only — multi-topic fan-out
// (one object → N topic frames, up-to-15× amplification) is an authenticated
// capability per BRC-149 §Fan-out admission, and the open ingress has no
// identity to hang it on. A downstream build installs a policy that decides
// per SOURCE (e.g. consumer-tunnel STE ranges) and observes each admitted
// fan-out for accounting (first-N-free overage is the caller's concern; the
// forwarder reports what happened, it does not price it).
type BEEFSubmitPolicy interface {
	// AdmitTopics reports whether src may submit a record naming n topics
	// (n ≥ 2; single-topic records are always admitted).
	AdmitTopics(src net.IP, n int) bool
	// OnFanout observes one admitted multi-topic record: the submitter, the
	// number of topic frames emitted, and the object's byte length.
	OnFanout(src net.IP, topics, objectBytes int)
	// MaxObjectBytes returns a per-source object bound overriding the operator's
	// open-ingress bound (0 = no override). Authenticated sources may carry a
	// larger allowance than the open path — identity makes abuse accountable
	// (roadmap D9). It may only RAISE the bound: the caller keeps the stricter
	// of the two, so a policy can never widen past the configured ceiling by
	// returning something absurd.
	MaxObjectBytes(src net.IP) int
}

// SetBEEFSubmitPolicy installs the multi-topic admission policy (nil = the
// OSS single-topic stance).
func (fw *Forwarder) SetBEEFSubmitPolicy(p BEEFSubmitPolicy) { fw.beefPolicy = p }

// beefObjectBoundFor resolves the object bound for one submitter: the policy's
// per-source allowance when it EXCEEDS the operator's open bound, else the open
// bound. Never returns less than configured — a policy cannot tighten below the
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
// the size bound, then emits one FrameVer 0x09 frame per submitted topic via
// [Forwarder.ProcessBEEF]. rec must be the complete record (one record per
// UDP datagram; the TCP lane splits the stream via objfmt.Reader).
func (fw *Forwarder) SubmitBEEF(egr *Egress, rec []byte, src net.Addr, workerID int) {
	result := "ok"
	defer func() {
		if fw.rec != nil {
			fw.rec.BEEFSubmission(result)
		}
	}()

	if fw.beefEngine == nil {
		result = "disabled"
		return
	}
	r, n, err := objfmt.DecodeBEEFRecord(rec)
	if err != nil || n != len(rec) {
		result = "malformed"
		return
	}
	if maxObj := fw.beefObjectBoundFor(src); maxObj > 0 && len(r.Object) > maxObj {
		result = "oversize"
		return
	}
	if !objfmt.IsBEEFObject(r.Object) {
		result = "bad_marker"
		return
	}
	// OSS default stance: single-topic only. Multi-topic fan-out (one object →
	// N topic frames = up-to-15× amplification) is an authenticated capability
	// reserved for authenticated ingress policies per BRC-149 §Fan-out admission; the
	// OSS open ingress admits TopicCount == 1 and rejects any record naming more
	// than one topic. The wire grammar still carries 1..15 (the codec is shared);
	// this is an admission gate, not a format change.
	if len(r.Topics) > 1 {
		if fw.beefPolicy == nil || !fw.beefPolicy.AdmitTopics(srcIPOf(src), len(r.Topics)) {
			result = "multi_topic"
			return
		}
	}

	// Compute the object identity once; emit the single topic's frame (the
	// grammar permits more, but OSS admission caps at one — see above).
	contentID := objfmt.ContentID(r.Object)
	for _, topic := range r.Topics {
		bf := &frame.BEEFFrame{
			ContentID: contentID,
			TopicID:   objfmt.TopicID(topic),
			Payload:   r.Object,
		}
		buf := make([]byte, frame.HeaderSize+len(r.Object))
		wn, err := frame.EncodeBEEF(bf, buf)
		if err != nil {
			result = "malformed"
			return
		}
		if fw.rec != nil {
			fw.rec.IngressMetered(metrics.IngressClassBEEF, false, wn)
		}
		fw.ProcessBEEF(egr, buf[:wn], src, workerID)
	}
	if len(r.Topics) > 1 && fw.beefPolicy != nil {
		fw.beefPolicy.OnFanout(srcIPOf(src), len(r.Topics), len(r.Object))
	}
}

// ProcessBEEF handles a framed BRC-148 BEEF object (FrameVer 0x09): decode,
// per-(ContentID, TopicID) ingress dedup, domain-tagged group derivation
// from the TopicID, HashKey/SeqNum stamping with the zero ingredient,
// BRC-130 fragmentation for objects exceeding the datagram capacity, and
// enqueue to the plane's multicast group.
//
// raw must remain valid until egr.Flush returns. egr may be nil for tests.
// A nil src (spine re-emit) skips stamping, mirroring [Forwarder.Process].
func (fw *Forwarder) ProcessBEEF(egr *Egress, raw []byte, src net.Addr, workerID int) {
	if fw.beefEngine == nil {
		if fw.rec != nil {
			fw.rec.PacketDropped(egrIface(egr), workerID, "beef_disabled")
		}
		return
	}
	bf, err := frame.DecodeBEEF(raw)
	if err != nil {
		fw.log.Debug("beef frame decode error", "err", err, "len", len(raw))
		if fw.rec != nil {
			fw.rec.PacketDropped(egrIface(egr), workerID, "decode_error")
		}
		return
	}

	// The operator's object bound applies to EVERY acceptance path, not only the
	// submission record: without this, pre-framing the object as FrameVer 0x09
	// bypasses -beef-max-object-bytes entirely (BRC-149 makes the bound an
	// ingress MUST). Whole frames are datagram/stream-bounded upstream, but the
	// declared length is what downstream reassembly would allocate against.
	if fw.beefMaxObject > 0 && len(bf.Payload) > fw.beefMaxObject {
		if fw.rec != nil {
			fw.rec.PacketDropped(egrIface(egr), workerID, "beef_oversize")
		}
		return
	}

	// The submission-record grammar enforces a topic count of 1..15 and the
	// BEEF marker (objfmt.DecodeBEEFRecord and IsBEEFObject above), so a
	// publisher that pre-frames its own FrameVer 0x09 must meet the same two
	// conditions or it bypasses both. BRC-149 makes the marker an ingress
	// MUST on every acceptance path, and a frame addressed to no topic is
	// undeliverable by construction: it still costs a full fabric emission,
	// and because an empty topic election matches every topic it lands on
	// every aggregator consumer and is billed to them.
	//
	// Both checks sit BEFORE the dedup claim on purpose. A drop taken after
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
		return
	}
	if !objfmt.IsBEEFObject(bf.Payload) {
		if fw.rec != nil {
			fw.rec.PacketDropped(egrIface(egr), workerID, "beef_bad_marker")
		}
		return
	}

	// Rate budgets are charged before the dedup claim, so a flood of
	// duplicates costs the flooder its budget rather than being free. Relay
	// and spine re-emission (src == nil) is exempt: those frames were already
	// charged at the door where they entered.
	if d := fw.beefLimit.allow(src, len(raw), time.Now()); d != rateAllow {
		if fw.rec != nil {
			fw.rec.PacketDropped(egrIface(egr), workerID, d.reason())
		}
		return
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
	// (SubmitBEEF above). Only pre-framing can present one.
	//
	// Placed AFTER the rate budget and BEFORE the claim, deliberately. SHA-256d
	// costs ~2.8us at 256B and ~5.6ms at 1MiB (measured 2026-09-15), so hashing
	// before the budget would let an unauthenticated flood burn CPU at line
	// rate; after it, the cost is bounded by a budget the operator already
	// chose. Relay and spine re-emission (src == nil) is exempt for the same
	// reason the budget exempts it: the frame was verified at the door where it
	// entered, and re-hashing at every hop multiplies the cost fleet-wide.
	if src != nil && bf.ContentID != objfmt.ContentID(bf.Payload) {
		if fw.rec != nil {
			fw.rec.PacketDropped(egrIface(egr), workerID, "beef_bad_contentid")
		}
		return
	}

	if !fw.claimBEEFIngress(beefClaimKey(bf.ContentID, bf.TopicID), "brc148", egrIface(egr), workerID) {
		return
	}

	groupIdx := fw.beefEngine.GroupIndex(&bf.TopicID)

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

// fragmentBEEF splits an oversized BEEF object into BRC-130 fragments
// (OrigFrameVer 0x09). ContentID rides the fragment TxID slot (it is the
// BRC-130 reassembly key and SHA-256d verification hash by construction) and
// TopicID rides the SubtreeID slot, so both identifiers appear in every
// fragment; flow stamping uses the zero ingredient like whole frames.
func (fw *Forwarder) fragmentBEEF(egr *Egress, bf *frame.BEEFFrame, ip [16]byte, groupIdx uint32, workerID int) {
	payload := bf.Payload
	origLen := uint32(len(payload))
	dataSize := fw.fragDataSize

	k := (len(payload) + dataSize - 1) / dataSize
	if k > 65535 {
		fw.log.Warn("beef fragment count exceeds 65535, dropping frame",
			"content_prefix", fmt.Sprintf("%x", bf.ContentID[:4]),
			"payload_len", len(payload),
		)
		if fw.rec != nil {
			fw.rec.PacketDropped("", workerID, "frag_overflow")
		}
		return
	}
	if fw.rec != nil {
		fw.rec.FrameFragmented(workerID, k)
	}

	fragTotal := uint16(k)
	dst := fw.addrFor(groupIdx)

	for i := 0; i < k; i++ {
		start := i * dataSize
		end := start + dataSize
		if end > len(payload) {
			end = len(payload)
		}
		fragData := payload[start:end]

		hashKey, seqNum := fw.nextSeq(ip, groupIdx, beefZeroIngredient)
		if egr == nil {
			continue
		}

		bufPtr := egr.PoolGet()
		buf := *bufPtr
		n, err := frame.EncodeFragment(
			buf,
			bf.ContentID,
			bf.TopicID,
			hashKey,
			seqNum,
			origLen,
			uint16(i),
			fragTotal,
			frame.FrameVerV9,
			fragData,
		)
		if err != nil {
			fw.log.Error("EncodeFragment error", "err", err)
			egr.pool.Put(bufPtr)
			continue
		}
		egr.EnqueueDataPooled(buf[:n], *dst, groupIdx, workerID, bufPtr)
	}
}
