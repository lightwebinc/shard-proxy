package forwarder

import (
	"encoding/binary"

	"github.com/lightwebinc/shard-common/bundle"
	"github.com/lightwebinc/shard-common/frame"
)

// DefaultCoalesceMaxBytes is the path-MTU budget used when coalescing is
// enabled without an explicit one. 1500 = the public-internet Ethernet MTU
// (the realistic baseline; jumbo is a controlled-underlay upside). It bounds
// the EMITTED DATAGRAM, so the IPv6 + UDP headers are subtracted from it
// before the bundle body is packed (see [Forwarder.SetCoalesce]).
const DefaultCoalesceMaxBytes = 1500

// coalBuffer accumulates eligible BRC-124/128 transactions during one receive
// batch, bucketed by flow (sender, group, subtree), for packing into BRC-142
// bundles at batch end. It is owned by a single Egress (hence one worker
// goroutine) and is not safe for concurrent use.
//
// Within-batch model: members reference payload slices that live in the worker's
// reused receive buffers and are valid only until the next ReadBatch, so the
// buffer MUST be flushed (members encoded into owned bundle datagrams) before
// the batch's Egress.Flush returns. The buffer holds no cross-batch state — the
// per-flow monotonic SeqNum lives in the Forwarder's striped counter map and is
// drawn at flush via nextSeq, so a flow's bundles and any individual frames
// share one contiguous sequence space.
type coalBuffer struct {
	buckets    []coalBucket
	maxMembers int
}

type coalBucket struct {
	ip      [16]byte
	group   uint32
	subtree [32]byte
	members []bundle.Member
}

func newCoalBuffer(maxMembers int) *coalBuffer {
	if maxMembers <= 0 || maxMembers > bundle.MaxMembers {
		maxMembers = bundle.MaxMembers
	}
	return &coalBuffer{maxMembers: maxMembers}
}

// add appends a member to its flow bucket. payload aliases the caller's receive
// buffer; it must stay valid until the next flush (true within a batch).
func (c *coalBuffer) add(ip [16]byte, group uint32, subtree, txid [32]byte, payload []byte) {
	for i := range c.buckets {
		b := &c.buckets[i]
		if b.group == group && b.ip == ip && b.subtree == subtree {
			b.members = append(b.members, bundle.Member{TxID: txid, Tx: payload})
			return
		}
	}
	c.buckets = append(c.buckets, coalBucket{
		ip:      ip,
		group:   group,
		subtree: subtree,
		members: []bundle.Member{{TxID: txid, Tx: payload}},
	})
}

// reset clears the buffer for the next batch. Truncating to zero length lets the
// next batch overwrite the bucket headers (and their member-slice references to
// now-stale receive buffers) on append.
func (c *coalBuffer) reset() { c.buckets = c.buckets[:0] }

// ProcessBundle re-emits an already-coalesced BRC-142 bundle datagram verbatim
// to its shard group. A bundle is a complete, self-describing multicast frame —
// its HashKey/SeqNum were stamped by the coalescing origin (a collapsed or
// ingress proxy) — so a relay forwards it UNCHANGED: not re-coalesced, not
// re-stamped, not split. This is the regional spine's path (it collects
// ingress-coalesced bundles over the unicast pipeline and re-emits each as one
// multicast datagram), so the upstream packing turns into one egress datagram /
// one AF_XDP TX descriptor here.
//
// The group is read from the bundle header (GroupIdx at offset 56) rather than
// derived from a TxID: a bundle carries many transactions and is bound to the
// single group it was built for. raw must remain valid until egr.Flush returns.
func (fw *Forwarder) ProcessBundle(egr *Egress, raw []byte, workerID int) {
	iface := func() string {
		if egr != nil && len(egr.targets) > 0 {
			return egr.targets[0].Iface.Name
		}
		return ""
	}
	if len(raw) < bundle.HeaderSize {
		if fw.rec != nil {
			fw.rec.PacketDropped(iface(), workerID, "bundle_short")
		}
		return
	}
	// Cheap structural validation of an untrusted relayed datagram before a
	// verbatim re-emit: the BSV magic and a self-consistent declared payload
	// length (raw[62:66]). Members are opaque pass-through bytes (never indexed
	// here), so this is the whole integrity check — it stops a corrupt header
	// (e.g. a garbage GroupIdx) from silently mis-routing the bundle to the wrong
	// group, and gives upstream corruption an observable counter.
	if binary.BigEndian.Uint32(raw[0:4]) != frame.MagicBSV ||
		bundle.HeaderSize+int(binary.BigEndian.Uint32(raw[62:66])) > len(raw) {
		if fw.rec != nil {
			fw.rec.PacketDropped(iface(), workerID, "bundle_malformed")
		}
		return
	}
	if egr == nil {
		return
	}
	group := uint32(binary.BigEndian.Uint16(raw[56:58]))
	dst := fw.addrFor(group)
	egr.EnqueueData(raw, *dst, group, workerID)
	if fw.rec != nil {
		fw.rec.CoalesceFlushed(iface(), workerID, int(binary.BigEndian.Uint16(raw[60:62])), "relay")
	}
}

// FlushCoalesced packs the per-flow buffered members on egr into BRC-142 bundle
// datagrams and enqueues them, then resets the buffer. Called by the worker at
// batch end, immediately before egr.Flush, so the encoded bundles (owned memory)
// are sent before the receive buffers the members alias are overwritten.
//
// Each bundle's HashKey/SeqNum is drawn from the same per-flow counter
// (nextSeq) that stamps individual frames, so a flow's bundles interleave
// contiguously with any individual frames it also emits — a listener gap-tracks
// the (group, subtree, HashKey) flow uniformly regardless of frame version.
func (fw *Forwarder) FlushCoalesced(egr *Egress, workerID int) {
	if egr == nil || egr.coal == nil || len(egr.coal.buckets) == 0 {
		return
	}
	// Budget the bundle BODY: the caller's number is a path MTU, and the
	// datagram it bounds also carries the IPv6 and UDP headers.
	maxBytes := fw.coalesceBudget
	if maxBytes <= 0 {
		maxBytes = DefaultCoalesceMaxBytes - ipv6UDPHeaderSize
	}
	overhead := bundle.MemberOverhead(fw.coalesceCarryTxid)
	shardBits := uint8(fw.engine.ShardBits())
	iface := ""
	if len(egr.targets) > 0 {
		iface = egr.targets[0].Iface.Name
	}

	for bi := range egr.coal.buckets {
		bk := &egr.coal.buckets[bi]
		dst := fw.addrFor(bk.group)
		for i := 0; i < len(bk.members); {
			b := &bundle.Bundle{
				GroupIdx:  uint16(bk.group),
				ShardBits: shardBits,
				SubtreeID: bk.subtree,
			}
			if fw.coalesceCarryTxid {
				b.Flags |= bundle.FlagTxIDsPresent
			}
			size := bundle.HeaderSize
			for i < len(bk.members) && len(b.Members) < egr.coal.maxMembers {
				add := overhead + len(bk.members[i].Tx)
				if len(b.Members) > 0 && size+add > maxBytes {
					break
				}
				b.Members = append(b.Members, bk.members[i])
				size += add
				i++
			}
			// Stamp the bundle flow's HashKey/SeqNum from the shared per-flow
			// counter (same one individual frames use).
			b.HashKey, b.SeqNum = fw.nextSeq(bk.ip, bk.group, bk.subtree)

			raw, err := b.Encode()
			if err != nil {
				// Encode only fails on impossible inputs here (count ≤ maxMembers
				// ≤ uint16, members ≤ datagram ≤ uint16); count it and skip.
				fw.log.Error("bundle encode error", "err", err, "members", len(b.Members))
				if fw.rec != nil {
					fw.rec.CoalesceFlushed(iface, workerID, 0, "encode_error")
				}
				continue
			}
			egr.EnqueueData(raw, *dst, bk.group, workerID)
			if fw.rec != nil {
				fw.rec.CoalesceFlushed(iface, workerID, len(b.Members), "batch")
			}
		}
	}
	egr.coal.reset()
}
