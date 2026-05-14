// Package forwarder implements the decode → forward pipeline for
// bitcoin-shard-proxy.
//
// # Hot path
//
// [Forwarder.Process] decodes the ingress frame (BRC-12, BRC-124, or BRC-128), derives the
// multicast group from the TxID, then for BRC-124/BRC-128 frames conditionally stamps
// PrevSeq and CurSeq in-place at raw[40:48] and raw[48:56]:
//
//   - If CurSeq (raw[48:56]) is already non-zero the sender pre-stamped the
//     frame; the proxy forwards it verbatim without modification.
//   - If CurSeq is zero the proxy stamps using the seqhash package:
//     CurSeq = XXH64(senderIPv6 ∥ groupIdx ∥ monotonic_counter); PrevSeq =
//     the previous CurSeq for this (sender, group) pair.
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

// senderGroupKey identifies a unique (sender IPv6, multicast group) chain.
type senderGroupKey struct {
	ip  [16]byte
	grp uint32
}

// senderGroupState holds the monotonic counter and last CurSeq for one chain.
type senderGroupState struct {
	counter uint64
	curSeq  uint64
}

// Target pairs a network interface with its pre-opened multicast egress socket.
type Target struct {
	Iface *net.Interface
	Conn  *net.UDPConn
}

// Forwarder decodes ingress frames (BRC-12 or BRC-124/BRC-128), derives the multicast
// destination from the TxID, stamps PrevSeq/CurSeq for BRC-124/BRC-128 frames, and
type Forwarder struct {
	engine     *shard.Engine
	mcPrefix   uint16
	mcGroupID  uint16
	egressPort int
	debug      bool
	rec        *metrics.Recorder
	log        *slog.Logger

	mu     sync.Mutex
	chains map[senderGroupKey]*senderGroupState
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
		chains:     make(map[senderGroupKey]*senderGroupState),
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

// Process is the hot path: decode raw for routing, conditionally stamp PrevSeq/CurSeq, then forward.
//
// For BRC-124/BRC-128 frames: if raw[48:56] (CurSeq) is non-zero the sender has
// pre-stamped the frame and it is forwarded verbatim. If CurSeq is zero the
// proxy stamps raw[40:48] (PrevSeq) and raw[48:56] (CurSeq) in-place using
// seqhash and a per-(sender, group) monotonic counter.
// BRC-12 frames are always forwarded verbatim. workerID is used only for metrics labels.
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
		if binary.BigEndian.Uint64(raw[48:56]) == 0 {
			ip := addrToIPv6(src)
			prevSeq, curSeq := fw.nextSeq(ip, groupIdx)
			binary.BigEndian.PutUint64(raw[40:48], prevSeq)
			binary.BigEndian.PutUint64(raw[48:56], curSeq)
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

// nextSeq returns (prevSeq, curSeq) for the given (sender IP, group) pair,
// advancing the per-chain monotonic counter atomically.
func (fw *Forwarder) nextSeq(ip [16]byte, groupIdx uint32) (prevSeq, curSeq uint64) {
	key := senderGroupKey{ip: ip, grp: groupIdx}
	fw.mu.Lock()
	st, ok := fw.chains[key]
	if !ok {
		st = &senderGroupState{}
		fw.chains[key] = st
	}
	st.counter++
	prevSeq = st.curSeq
	st.curSeq = seqhash.Hash(ip, groupIdx, st.counter)
	curSeq = st.curSeq
	fw.mu.Unlock()
	return prevSeq, curSeq
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
