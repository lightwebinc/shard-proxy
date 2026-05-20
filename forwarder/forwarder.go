// Package forwarder implements the decode → forward pipeline for
// bitcoin-shard-proxy.
//
// # Hot path
//
// [Forwarder.Process] decodes the ingress frame (BRC-12, BRC-124, or BRC-128),
// derives the multicast group from the TxID, then for BRC-124/BRC-128 frames
// conditionally stamps HashKey and SeqNum in-place at raw[40:48] and raw[48:56]:
//
//   - If SeqNum (raw[48:56]) is already non-zero the sender pre-stamped the
//     frame; the proxy forwards it verbatim without modification.
//   - If SeqNum is zero the proxy stamps: HashKey = XXH64(senderIPv6 ∥ groupIdx ∥ subtreeID)
//     (stable per flow); SeqNum = per-(sender, group, subtree) monotonic counter
//     starting at 1. Each subtree therefore owns an independent sequence so
//     loss in one subtree cannot create false gaps in another.
//
// # BRC-130 fragmentation
//
// When [Forwarder.SetFragMTU] is called with a positive MTU, BRC-124/BRC-128
// frames whose payload exceeds fragDataSize (= MTU − 40 − 8 − 104) are split
// into K BRC-130 fragment datagrams instead of being forwarded verbatim.
// Each fragment receives its own HashKey and SeqNum so it is independently
// cacheable and retransmittable by the retry endpoint.
// Frames at or below the threshold are forwarded verbatim.
//
// BRC-12 frames are always forwarded verbatim.
//
// # Egress socket lifecycle
//
// [Forwarder.OpenTargets] opens one UDP socket per interface with
// IPV6_MULTICAST_IF applied. Pass the returned slice to every [Forwarder.Process]
// call and release with [CloseTargets] during graceful shutdown.
package forwarder

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/lightwebinc/bitcoin-shard-common/frame"
	"github.com/lightwebinc/bitcoin-shard-common/seqhash"
	"github.com/lightwebinc/bitcoin-shard-common/shard"
	"github.com/lightwebinc/bitcoin-shard-proxy/metrics"
)

// chainKey identifies a unique (sender IPv6, multicast group, subtree) chain.
type chainKey struct {
	ip  [16]byte
	grp uint32
	sub [32]byte
}

// flowState holds the monotonic per-flow SeqNum counter.
type flowState struct {
	counter uint64
}

// Target pairs a network interface with its pre-opened multicast egress socket.
type Target struct {
	Iface *net.Interface
	Conn  *net.UDPConn
}

// ipv6UDPOverhead is the fixed per-datagram overhead subtracted from the
// path MTU to derive the fragment data capacity: 40 bytes IPv6 header +
// 8 bytes UDP header + 104 bytes BRC-130 frame header.
const ipv6UDPOverhead = 40 + 8 + 104

// Forwarder decodes ingress frames (BRC-12 or BRC-124/BRC-128), derives the multicast
// destination from the TxID, stamps HashKey/SeqNum for BRC-124/BRC-128 frames, and
// optionally splits large payloads into BRC-130 fragment datagrams.
type Forwarder struct {
	engine       *shard.Engine
	mcPrefix     uint16
	mcGroupID    uint16
	egressPort   int
	debug        bool
	rec          *metrics.Recorder
	log          *slog.Logger
	fragDataSize int // >0 = fragmentation enabled; fragment capacity per datagram

	mu     sync.Mutex
	chains map[chainKey]*flowState
}

// New creates a Forwarder. No sockets are opened here; call [OpenTargets] in
// each worker's Run loop.
//
//   - engine: immutable shard derivation engine.
//   - mcPrefix: upper 16-bit scope prefix for control-plane group address derivation.
//   - mcGroupID: IANA group-id occupying bytes 12–13 (default [shard.DefaultGroupID]).
//   - egressPort: UDP destination port written into outgoing multicast datagrams.
//   - debug: enable per-packet debug logging.
//   - rec: metrics recorder; may be nil.
func New(engine *shard.Engine, mcPrefix uint16, mcGroupID uint16, egressPort int, debug bool, rec *metrics.Recorder) *Forwarder {
	return &Forwarder{
		engine:     engine,
		mcPrefix:   mcPrefix,
		mcGroupID:  mcGroupID,
		egressPort: egressPort,
		debug:      debug,
		rec:        rec,
		log:        slog.Default().With("component", "forwarder"),
		chains:     make(map[chainKey]*flowState),
	}
}

// OpenTargets opens one multicast egress UDP socket per interface. On worker 0
// (probeWorker == true) each socket is probed with a zero-byte send to verify
// multicast egress is functional.
//
// On error, all partially opened sockets are closed before returning.
func (fw *Forwarder) OpenTargets(ifaces []*net.Interface, probeWorker bool) ([]Target, error) {
	loopback := 0
	if fw.debug {
		loopback = 1
	}
	targets := make([]Target, 0, len(ifaces))
	for _, iface := range ifaces {
		conn, err := openEgressSocket(iface, loopback)
		if err != nil {
			closeTargets(targets, fw.log)
			return nil, fmt.Errorf("forwarder: open egress socket (%s): %w", iface.Name, err)
		}
		if probeWorker {
			if err := probeEgressSocket(fw.log, conn, iface); err != nil {
				_ = conn.Close()
				closeTargets(targets, fw.log)
				return nil, fmt.Errorf("forwarder: egress probe (%s): %w", iface.Name, err)
			}
		}
		targets = append(targets, Target{Iface: iface, Conn: conn})
	}
	return targets, nil
}

// CloseTargets closes all egress sockets opened by [OpenTargets].
func CloseTargets(targets []Target, log *slog.Logger) {
	closeTargets(targets, log)
}

func closeTargets(targets []Target, log *slog.Logger) {
	for _, t := range targets {
		if err := t.Conn.Close(); err != nil {
			log.Warn("close egress conn", "iface", t.Iface.Name, "err", err)
		}
	}
}

// SetFragMTU enables BRC-130 fragmentation for the given path MTU.
// Frames with payload larger than (mtu - 40 - 8 - 104) bytes are split into
// multiple BRC-130 datagrams. Pass mtu <= 0 to disable fragmentation.
func (fw *Forwarder) SetFragMTU(mtu int) {
	if mtu > ipv6UDPOverhead {
		fw.fragDataSize = mtu - ipv6UDPOverhead
	} else {
		fw.fragDataSize = 0
	}
}

// Process is the hot path: decode raw for routing, conditionally stamp HashKey/SeqNum, then forward.
//
// For BRC-124/BRC-128 frames: if raw[48:56] (SeqNum) is non-zero the sender has
// pre-stamped the frame and it is forwarded verbatim. If SeqNum is zero the
// proxy stamps raw[40:48] (HashKey) and raw[48:56] (SeqNum) in-place: HashKey is
// stable per (sender, group, subtree) flow; SeqNum is a per-flow monotonic counter
// starting at 1. BRC-12 frames are always forwarded verbatim. workerID is used only for metrics labels.
func (fw *Forwarder) Process(targets []Target, raw []byte, src net.Addr, workerID int) {
	f, err := frame.Decode(raw)
	if err != nil {
		fw.log.Debug("frame decode error", "err", err, "len", len(raw))
		if fw.rec != nil && len(targets) > 0 {
			fw.rec.PacketDropped(targets[0].Iface.Name, workerID, "decode_error")
		}
		return
	}

	groupIdx := fw.engine.GroupIndex(&f.TxID)

	if f.Version == frame.FrameVerV2 && src != nil {
		ip := addrToIPv6(src)

		// BRC-130 fragmentation path: payload exceeds per-datagram capacity.
		if fw.fragDataSize > 0 && len(f.Payload) > fw.fragDataSize {
			fw.fragment(targets, f, ip, groupIdx, workerID)
			return
		}

		// Standard BRC-124/BRC-128 path: stamp in-place if not pre-stamped.
		if binary.BigEndian.Uint64(raw[48:56]) == 0 {
			hashKey, seqNum := fw.nextSeq(ip, groupIdx, f.SubtreeID)
			binary.BigEndian.PutUint64(raw[40:48], hashKey)
			binary.BigEndian.PutUint64(raw[48:56], seqNum)
		}
	}

	dst := fw.engine.Addr(groupIdx, fw.egressPort)

	for _, tgt := range targets {
		dst.Zone = tgt.Iface.Name
		if _, err := tgt.Conn.WriteTo(raw, dst); err != nil {
			fw.log.Warn("WriteTo error", "iface", tgt.Iface.Name, "dst", dst, "err", err)
			if fw.rec != nil {
				fw.rec.PacketDropped(tgt.Iface.Name, workerID, "write_error")
				fw.rec.EgressError(tgt.Iface.Name, workerID)
			}
			continue
		}
		if fw.rec != nil {
			fw.rec.PacketForwarded(tgt.Iface.Name, workerID, groupIdx, len(raw))
		}
	}

	if fw.debug {
		fw.log.Debug("forwarded",
			"txid_prefix", fmt.Sprintf("%08X", groupIdx),
			"group_idx", groupIdx,
			"src", src,
			"dst", dst,
		)
	}
}

// fragment splits f.Payload into BRC-130 fragment datagrams and forwards each
// to all targets. Each fragment receives an independent HashKey+SeqNum pair
// allocated from the same flow as a regular frame would use.
func (fw *Forwarder) fragment(targets []Target, f *frame.Frame, ip [16]byte, groupIdx uint32, workerID int) {
	payload := f.Payload
	origLen := uint32(len(payload))
	dataSize := fw.fragDataSize

	// Compute total fragment count.
	k := (len(payload) + dataSize - 1) / dataSize
	if k > 65535 {
		// Pathologically large frame; drop and log.
		fw.log.Warn("fragment count exceeds 65535, dropping frame",
			"txid_prefix", fmt.Sprintf("%x", f.TxID[:4]),
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
	dst := fw.engine.Addr(groupIdx, fw.egressPort)
	buf := make([]byte, frame.HeaderSizeV3+dataSize)

	for i := 0; i < k; i++ {
		start := i * dataSize
		end := start + dataSize
		if end > len(payload) {
			end = len(payload)
		}
		fragData := payload[start:end]

		hashKey, seqNum := fw.nextSeq(ip, groupIdx, f.SubtreeID)

		n, err := frame.EncodeFragment(
			buf,
			f.TxID,
			f.SubtreeID,
			hashKey,
			seqNum,
			origLen,
			uint16(i),
			fragTotal,
			0, // OrigFrameVer: 0 = default to FrameVerV2
			fragData,
		)
		if err != nil {
			fw.log.Error("EncodeFragment error", "err", err)
			continue
		}

		for _, tgt := range targets {
			dst.Zone = tgt.Iface.Name
			if _, werr := tgt.Conn.WriteTo(buf[:n], dst); werr != nil {
				fw.log.Warn("WriteTo fragment error", "iface", tgt.Iface.Name, "dst", dst, "err", werr)
				if fw.rec != nil {
					fw.rec.PacketDropped(tgt.Iface.Name, workerID, "write_error")
					fw.rec.EgressError(tgt.Iface.Name, workerID)
				}
				continue
			}
			if fw.rec != nil {
				fw.rec.PacketForwarded(tgt.Iface.Name, workerID, groupIdx, n)
			}
		}
	}

	if fw.debug {
		fw.log.Debug("fragmented",
			"txid_prefix", fmt.Sprintf("%x", f.TxID[:4]),
			"group_idx", groupIdx,
			"fragments", k,
			"payload_len", origLen,
		)
	}
}

// ProcessBlock handles BRC-131 block control frames (FrameVer 0x04).
// It validates the frame, stamps HashKey/SeqNum if needed, optionally
// fragments large payloads via BRC-130, and forwards to the control
// multicast group (CtrlGroupControl) instead of a shard group.
func (fw *Forwarder) ProcessBlock(targets []Target, raw []byte, src net.Addr, workerID int) {
	bf, err := frame.DecodeBlock(raw)
	if err != nil {
		fw.log.Debug("block frame decode error", "err", err, "len", len(raw))
		if fw.rec != nil && len(targets) > 0 {
			fw.rec.PacketDropped(targets[0].Iface.Name, workerID, "decode_error")
		}
		return
	}

	if src != nil {
		ip := addrToIPv6(src)
		ctrlIdx := uint32(shard.CtrlGroupControl)
		var zeroSub [32]byte

		// BRC-130 fragmentation path for large block payloads.
		if fw.fragDataSize > 0 && len(bf.Payload) > fw.fragDataSize {
			fw.fragmentBlock(targets, raw, bf, ip, ctrlIdx, workerID)
			return
		}

		// Stamp HashKey/SeqNum in-place if not pre-stamped.
		if binary.BigEndian.Uint64(raw[48:56]) == 0 {
			hashKey, seqNum := fw.nextSeq(ip, ctrlIdx, zeroSub)
			binary.BigEndian.PutUint64(raw[40:48], hashKey)
			binary.BigEndian.PutUint64(raw[48:56], seqNum)
		}
	}

	dst := shard.ControlGroupAddr(fw.mcPrefix, fw.mcGroupID, shard.CtrlGroupControl)
	addr := &net.UDPAddr{IP: dst, Port: fw.egressPort}

	for _, tgt := range targets {
		addr.Zone = tgt.Iface.Name
		if _, err := tgt.Conn.WriteTo(raw, addr); err != nil {
			fw.log.Warn("WriteTo block error", "iface", tgt.Iface.Name, "dst", addr, "err", err)
			if fw.rec != nil {
				fw.rec.PacketDropped(tgt.Iface.Name, workerID, "write_error")
				fw.rec.EgressError(tgt.Iface.Name, workerID)
			}
			continue
		}
		if fw.rec != nil {
			fw.rec.ControlFrameForwarded("block_control")
		}
	}

	if fw.debug {
		fw.log.Debug("block forwarded",
			"msg_type", bf.MsgType,
			"content_id", fmt.Sprintf("%x", bf.ContentID[:8]),
			"dst", addr,
		)
	}
}

// ProcessAnchor handles BRC-134 chained anchor transaction frames (FrameVer 0x06).
// Anchor transactions are the root of a chain of dependent transactions and
// must reach every subscriber regardless of shard assignment. They are
// validated, HashKey/SeqNum-stamped, and forwarded to CtrlGroupControl
// (FF0E::B:FFFE) — the same control-plane group as BRC-131 block frames.
func (fw *Forwarder) ProcessAnchor(targets []Target, raw []byte, src net.Addr, workerID int) {
	f, err := frame.DecodeAnchor(raw)
	if err != nil {
		fw.log.Debug("anchor frame decode error", "err", err, "len", len(raw))
		if fw.rec != nil && len(targets) > 0 {
			fw.rec.PacketDropped(targets[0].Iface.Name, workerID, "decode_error")
		}
		return
	}

	if src != nil {
		ip := addrToIPv6(src)
		// Anchor frames use a dedicated virtual group index (0xFFF9) for
		// HashKey derivation so they get their own independent SeqNum counter
		// and flow identity, distinct from BRC-131 block frames which share
		// the same CtrlGroupControl multicast address.
		const anchorGroupIdx = uint32(0xFFF9)
		var zeroSub [32]byte

		// Stamp HashKey/SeqNum in-place if not pre-stamped.
		if binary.BigEndian.Uint64(raw[48:56]) == 0 {
			hashKey, seqNum := fw.nextSeq(ip, anchorGroupIdx, zeroSub)
			binary.BigEndian.PutUint64(raw[40:48], hashKey)
			binary.BigEndian.PutUint64(raw[48:56], seqNum)
		}
	}

	dst := shard.ControlGroupAddr(fw.mcPrefix, fw.mcGroupID, shard.CtrlGroupControl)
	addr := &net.UDPAddr{IP: dst, Port: fw.egressPort}

	for _, tgt := range targets {
		addr.Zone = tgt.Iface.Name
		if _, err := tgt.Conn.WriteTo(raw, addr); err != nil {
			fw.log.Warn("WriteTo anchor error", "iface", tgt.Iface.Name, "dst", addr, "err", err)
			if fw.rec != nil {
				fw.rec.PacketDropped(tgt.Iface.Name, workerID, "write_error")
				fw.rec.EgressError(tgt.Iface.Name, workerID)
			}
			continue
		}
		if fw.rec != nil {
			fw.rec.ControlFrameForwarded("anchor")
		}
	}

	if fw.debug {
		fw.log.Debug("anchor forwarded",
			"txid", fmt.Sprintf("%x", f.TxID[:8]),
			"dst", addr,
		)
	}
}

// fragmentBlock splits a large BRC-131 block payload into BRC-130 fragments
// and forwards each to the control group. Each fragment receives OrigFrameVer=0x04
// so that reassembly can reconstruct the correct frame version.
func (fw *Forwarder) fragmentBlock(targets []Target, raw []byte, bf *frame.BlockFrame, ip [16]byte, ctrlIdx uint32, workerID int) {
	payload := bf.Payload
	origLen := uint32(len(payload))
	dataSize := fw.fragDataSize

	k := (len(payload) + dataSize - 1) / dataSize
	if k > 65535 {
		fw.log.Warn("block fragment count exceeds 65535, dropping frame",
			"content_id", fmt.Sprintf("%x", bf.ContentID[:8]),
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

	// Build the ContentID (goes into TxID slot of BRC-130 header).
	var contentID [32]byte
	copy(contentID[:], bf.ContentID[:])
	var zeroSub [32]byte

	fragTotal := uint16(k)
	dst := shard.ControlGroupAddr(fw.mcPrefix, fw.mcGroupID, shard.CtrlGroupControl)
	addr := &net.UDPAddr{IP: dst, Port: fw.egressPort}
	buf := make([]byte, frame.HeaderSizeV3+dataSize)

	for i := 0; i < k; i++ {
		start := i * dataSize
		end := start + dataSize
		if end > len(payload) {
			end = len(payload)
		}
		fragData := payload[start:end]

		hashKey, seqNum := fw.nextSeq(ip, ctrlIdx, zeroSub)

		n, err := frame.EncodeFragment(
			buf,
			contentID,
			zeroSub,
			hashKey,
			seqNum,
			origLen,
			uint16(i),
			fragTotal,
			frame.FrameVerV4, // OrigFrameVer: V4 block control
			fragData,
		)
		if err != nil {
			fw.log.Error("EncodeFragment block error", "err", err)
			continue
		}

		// Write BlockMsgType into the Reserved byte (offset 7) so the
		// reassembler can reconstruct the full V4 header.
		buf[7] = raw[7]

		for _, tgt := range targets {
			addr.Zone = tgt.Iface.Name
			if _, werr := tgt.Conn.WriteTo(buf[:n], addr); werr != nil {
				fw.log.Warn("WriteTo block fragment error", "iface", tgt.Iface.Name, "dst", addr, "err", werr)
				if fw.rec != nil {
					fw.rec.PacketDropped(tgt.Iface.Name, workerID, "write_error")
					fw.rec.EgressError(tgt.Iface.Name, workerID)
				}
				continue
			}
			if fw.rec != nil {
				fw.rec.ControlFrameForwarded("block_control")
			}
		}
	}

	if fw.debug {
		fw.log.Debug("block fragmented",
			"content_id", fmt.Sprintf("%x", bf.ContentID[:8]),
			"fragments", k,
			"payload_len", origLen,
		)
	}
}

// ProcessSubtreeData handles BRC-132 subtree data frames (FrameVer 0x05).
// It validates the frame, stamps HashKey/SeqNum per (sender, 0xFFFB, subtreeID)
// flow, optionally fragments large payloads via BRC-130, and forwards to the
// CtrlGroupSubtreeAnnounce multicast group.
func (fw *Forwarder) ProcessSubtreeData(targets []Target, raw []byte, src net.Addr, workerID int) {
	sf, err := frame.DecodeSubtreeData(raw)
	if err != nil {
		fw.log.Debug("subtree data frame decode error", "err", err, "len", len(raw))
		if fw.rec != nil && len(targets) > 0 {
			fw.rec.PacketDropped(targets[0].Iface.Name, workerID, "decode_error")
		}
		return
	}

	if src != nil {
		ip := addrToIPv6(src)
		ctrlIdx := uint32(shard.CtrlGroupSubtreeAnnounce)

		// BRC-130 fragmentation path for large subtree data payloads.
		if fw.fragDataSize > 0 && len(sf.Payload) > fw.fragDataSize {
			fw.fragmentSubtreeData(targets, raw, sf, ip, ctrlIdx, workerID)
			return
		}

		// Stamp HashKey/SeqNum in-place if not pre-stamped.
		// SubtreeID is read from bytes 8–39 (the ContentID slot).
		if binary.BigEndian.Uint64(raw[48:56]) == 0 {
			hashKey, seqNum := fw.nextSeq(ip, ctrlIdx, sf.SubtreeID)
			binary.BigEndian.PutUint64(raw[40:48], hashKey)
			binary.BigEndian.PutUint64(raw[48:56], seqNum)
		}
	}

	dst := shard.ControlGroupAddr(fw.mcPrefix, fw.mcGroupID, shard.CtrlGroupSubtreeAnnounce)
	addr := &net.UDPAddr{IP: dst, Port: fw.egressPort}

	for _, tgt := range targets {
		addr.Zone = tgt.Iface.Name
		if _, err := tgt.Conn.WriteTo(raw, addr); err != nil {
			fw.log.Warn("WriteTo subtree data error", "iface", tgt.Iface.Name, "dst", addr, "err", err)
			if fw.rec != nil {
				fw.rec.PacketDropped(tgt.Iface.Name, workerID, "write_error")
				fw.rec.EgressError(tgt.Iface.Name, workerID)
			}
			continue
		}
		if fw.rec != nil {
			fw.rec.ControlFrameForwarded("subtree_data")
		}
	}

	if fw.debug {
		fw.log.Debug("subtree data forwarded",
			"msg_type", sf.MsgType,
			"subtree_id", fmt.Sprintf("%x", sf.SubtreeID[:8]),
			"dst", addr,
		)
	}
}

// fragmentSubtreeData splits a large BRC-132 subtree data payload into BRC-130
// fragments and forwards each to the CtrlGroupSubtreeAnnounce group.
// Each fragment receives OrigFrameVer=0x05 so that reassembly routes the
// completed payload to processSubtreeDataFrame on the listener.
// MsgType is preserved in byte 7 of each fragment datagram.
func (fw *Forwarder) fragmentSubtreeData(targets []Target, raw []byte, sf *frame.SubtreeDataFrame, ip [16]byte, ctrlIdx uint32, workerID int) {
	payload := sf.Payload
	origLen := uint32(len(payload))
	dataSize := fw.fragDataSize

	k := (len(payload) + dataSize - 1) / dataSize
	if k > 65535 {
		fw.log.Warn("subtree data fragment count exceeds 65535, dropping frame",
			"subtree_id", fmt.Sprintf("%x", sf.SubtreeID[:8]),
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

	// SubtreeID goes into both the TxID slot and the SubtreeID slot of
	// the BRC-130 fragment header so that reassembly and gap-tracking
	// both read the correct identifier.
	var zeroSub [32]byte

	fragTotal := uint16(k)
	dst := shard.ControlGroupAddr(fw.mcPrefix, fw.mcGroupID, shard.CtrlGroupSubtreeAnnounce)
	addr := &net.UDPAddr{IP: dst, Port: fw.egressPort}
	buf := make([]byte, frame.HeaderSizeV3+dataSize)

	for i := 0; i < k; i++ {
		start := i * dataSize
		end := start + dataSize
		if end > len(payload) {
			end = len(payload)
		}
		fragData := payload[start:end]

		hashKey, seqNum := fw.nextSeq(ip, ctrlIdx, sf.SubtreeID)

		n, err := frame.EncodeFragment(
			buf,
			sf.SubtreeID, // TxID slot: SubtreeID (reassembly key)
			zeroSub,      // SubtreeID slot: zeros (LayoutPad32 convention)
			hashKey,
			seqNum,
			origLen,
			uint16(i),
			fragTotal,
			frame.FrameVerV5, // OrigFrameVer: V5 subtree data
			fragData,
		)
		if err != nil {
			fw.log.Error("EncodeFragment subtree data error", "err", err)
			continue
		}

		// Write MsgType into byte 7 so the reassembler can reconstruct
		// the full V5 header (same pattern as fragmentBlock / BRC-131).
		buf[7] = raw[7]

		for _, tgt := range targets {
			addr.Zone = tgt.Iface.Name
			if _, werr := tgt.Conn.WriteTo(buf[:n], addr); werr != nil {
				fw.log.Warn("WriteTo subtree data fragment error", "iface", tgt.Iface.Name, "dst", addr, "err", werr)
				if fw.rec != nil {
					fw.rec.PacketDropped(tgt.Iface.Name, workerID, "write_error")
					fw.rec.EgressError(tgt.Iface.Name, workerID)
				}
				continue
			}
			if fw.rec != nil {
				fw.rec.ControlFrameForwarded("subtree_data")
			}
		}
	}

	if fw.debug {
		fw.log.Debug("subtree data fragmented",
			"subtree_id", fmt.Sprintf("%x", sf.SubtreeID[:8]),
			"fragments", k,
			"payload_len", origLen,
		)
	}
}

// EgressPort returns the configured UDP destination port for multicast egress.
func (fw *Forwarder) EgressPort() int { return fw.egressPort }

// ForwardControl sends a raw BRC-127 control datagram (e.g. SubtreeAnnounce)
// to the given control-plane multicast group index on all egress targets.
// The destination address is derived using [shard.ControlGroupAddr] with the
// engine's configured scope prefix and IANA group-id.
// Unlike [Process], no sequence stamping or frame decoding is performed.
func (fw *Forwarder) ForwardControl(targets []Target, raw []byte, ctrlGroupIdx uint16, port int) {
	dst := shard.ControlGroupAddr(fw.mcPrefix, fw.mcGroupID, ctrlGroupIdx)
	addr := &net.UDPAddr{IP: dst, Port: port}
	for _, tgt := range targets {
		addr.Zone = tgt.Iface.Name
		if _, err := tgt.Conn.WriteTo(raw, addr); err != nil {
			fw.log.Warn("ForwardControl WriteTo error",
				"iface", tgt.Iface.Name, "dst", addr, "err", err)
		}
	}
	if fw.rec != nil {
		fw.rec.ControlFrameForwarded(ctrlGroupName(ctrlGroupIdx))
	}
	if fw.debug {
		fw.log.Debug("control forwarded",
			"ctrl_group", fmt.Sprintf("0x%04X", ctrlGroupIdx),
			"dst", addr,
		)
	}
}

// ctrlGroupName returns a human-readable label for a control group index.
func ctrlGroupName(idx uint16) string {
	switch idx {
	case shard.CtrlGroupSubtreeAnnounce:
		return "subtree_announce"
	case shard.CtrlGroupBeacon:
		return "beacon"
	case shard.CtrlGroupControl:
		return "control"
	default:
		return fmt.Sprintf("0x%04x", idx)
	}
}

// nextSeq returns (hashKey, seqNum) for the given (sender IP, group, subtree) flow.
// hashKey is stable (same for every frame in the flow); seqNum is monotonically
// incremented per frame.
func (fw *Forwarder) nextSeq(ip [16]byte, groupIdx uint32, subtreeID [32]byte) (hashKey, seqNum uint64) {
	key := chainKey{ip: ip, grp: groupIdx, sub: subtreeID}
	fw.mu.Lock()
	st, ok := fw.chains[key]
	if !ok {
		st = &flowState{}
		fw.chains[key] = st
	}
	st.counter++
	hashKey = seqhash.Hash(ip, groupIdx, subtreeID)
	seqNum = st.counter
	fw.mu.Unlock()
	return hashKey, seqNum
}

// openEgressSocket opens a UDP6 socket with IPV6_MULTICAST_IF set to iface
// and IPV6_MULTICAST_LOOP set to loopback (1 for debug, 0 otherwise).
func openEgressSocket(iface *net.Interface, loopback int) (*net.UDPConn, error) {
	conn, err := net.ListenPacket("udp6", "[::]:0")
	if err != nil {
		return nil, err
	}
	udpConn := conn.(*net.UDPConn)

	rawConn, err := udpConn.SyscallConn()
	if err != nil {
		_ = udpConn.Close()
		return nil, err
	}
	var setsockoptErr error
	if ctrlErr := rawConn.Control(func(fd uintptr) {
		if err := unix.SetsockoptInt(int(fd), unix.IPPROTO_IPV6, unix.IPV6_MULTICAST_IF, iface.Index); err != nil {
			setsockoptErr = fmt.Errorf("IPV6_MULTICAST_IF: %w", err)
			return
		}
		if err := unix.SetsockoptInt(int(fd), unix.IPPROTO_IPV6, unix.IPV6_MULTICAST_LOOP, loopback); err != nil {
			slog.Default().Warn("could not configure multicast loopback", "iface", iface.Name, "err", err)
		}
	}); ctrlErr != nil {
		_ = udpConn.Close()
		return nil, ctrlErr
	}
	if setsockoptErr != nil {
		_ = udpConn.Close()
		return nil, setsockoptErr
	}
	return udpConn, nil
}

// probeEgressSocket sends a zero-byte datagram to ff02::1 (link-local
// all-nodes) on the given interface to verify the egress path at startup.
// Hard errors (EPERM, EADDRNOTAVAIL) are returned; other errors are warnings.
func probeEgressSocket(log *slog.Logger, conn *net.UDPConn, iface *net.Interface) error {
	dst := &net.UDPAddr{IP: net.ParseIP("ff02::1"), Port: 9}
	_, err := conn.WriteTo([]byte{}, dst)
	if err == nil {
		return nil
	}
	if isErrno(err, syscall.EPERM) || isErrno(err, syscall.EADDRNOTAVAIL) {
		return fmt.Errorf("interface not usable for multicast egress: %w", err)
	}
	log.Warn("egress probe warning", "iface", iface.Name, "err", err)
	return nil
}

// addrToIPv6 extracts the IP address from a net.Addr and returns it as a
// 16-byte IPv6 address via net.IP.To16(). IPv4 addresses become IPv4-mapped
// IPv6 (::ffff:a.b.c.d). Returns all-zeros if addr is nil or unrecognised.
func addrToIPv6(addr net.Addr) [16]byte {
	var ip net.IP
	switch a := addr.(type) {
	case *net.UDPAddr:
		ip = a.IP
	case *net.TCPAddr:
		ip = a.IP
	}
	var result [16]byte
	if ip16 := ip.To16(); ip16 != nil {
		copy(result[:], ip16)
	}
	return result
}

func isErrno(err error, target syscall.Errno) bool {
	for err != nil {
		if e, ok := err.(syscall.Errno); ok {
			return e == target
		}
		type unwrapper interface{ Unwrap() error }
		if u, ok := err.(unwrapper); ok {
			err = u.Unwrap()
		} else {
			break
		}
	}
	return false
}
