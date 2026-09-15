// Package forwarder implements the decode → forward pipeline for
// shard-proxy.
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
// Per-flow counters live in a striped map (chainStripes) so concurrent
// workers contend on independent shards rather than a single mutex. Once a
// flow entry exists, counter increment is lock-free via atomic.Uint64.
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
// # Egress lifecycle
//
// [Forwarder.OpenTargets] opens one UDP socket per interface with
// IPV6_MULTICAST_IF applied and wraps it in an [ipv6.PacketConn] cached on
// the returned [Target] for batched WriteBatch (sendmmsg on Linux). Each
// worker constructs an [Egress] over the targets and passes it to every
// Process* call; the worker calls [Egress.Flush] at the end of each receive
// batch and once more during graceful shutdown to drain in-flight messages.
// Sockets are released with [CloseTargets].
package forwarder

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math/big"
	"net"
	"sync"
	"sync/atomic"
	"syscall"

	"golang.org/x/net/ipv6"
	"golang.org/x/sys/unix"

	"github.com/lightwebinc/shard-common/bundle"
	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/shard-common/pow"
	"github.com/lightwebinc/shard-common/seqhash"
	"github.com/lightwebinc/shard-common/shard"
	"github.com/lightwebinc/shard-proxy/metrics"
)

// chainKey identifies a unique (sender IPv6, multicast group, subtree) chain.
type chainKey struct {
	ip  [16]byte
	grp uint32
	sub [32]byte
}

// flowState holds the monotonic per-flow SeqNum counter. The counter is
// incremented lock-free; the enclosing stripe lock is held only for the
// map lookup/insert that resolves *flowState the first time a flow appears.
type flowState struct {
	counter atomic.Uint64
}

// chainStripes is the shard count of the striped per-flow counter map.
// Power-of-two so stripe selection is a single bit-mask. 64 stripes give
// each worker plenty of headroom even on high-core hosts.
const chainStripes = 64

// chainStripe is one shard of the per-flow counter map. Each stripe owns
// an independent mutex; concurrent workers contend on independent shards.
type chainStripe struct {
	mu sync.Mutex
	m  map[chainKey]*flowState
}

// Target pairs a network interface with its pre-opened multicast egress
// socket and the cached ipv6.PacketConn wrapper used for batched WriteBatch
// (sendmmsg) calls during Egress.Flush.
type Target struct {
	Iface *net.Interface
	Conn  *net.UDPConn
	PC    *ipv6.PacketConn
}

// ipv6UDPOverhead is the fixed per-datagram overhead subtracted from the
// path MTU to derive the fragment data capacity: 40 bytes IPv6 header +
// 8 bytes UDP header + 104 bytes BRC-130 frame header.
const ipv6UDPOverhead = 40 + 8 + 104

// TxidDedup is the minimal interface a TxID claim store must satisfy. It is
// satisfied by *txidset.Store from shard-common; the forwarder depends
// only on this interface so that tests can inject lightweight fakes.
//
// Claim must report (true, nil) when the caller wins the claim (proceed),
// (false, nil) when another claimant already holds the TxID (suppress), or
// (true, err) on Redis error (fail-open). Errors are surfaced through Recorder
// callbacks supplied to the Store rather than re-reported by the forwarder.
type TxidDedup interface {
	Claim(prefix string, txid [32]byte) (bool, error)
}

// Forwarder decodes ingress frames (BRC-12 or BRC-124/BRC-128), derives the multicast
// destination from the TxID, stamps HashKey/SeqNum for BRC-124/BRC-128 frames, and
// optionally splits large payloads into BRC-130 fragment datagrams.
// BridgingEngine carries the secondary shard engine used during a
// BRC-139 live-resharding bridging window. When set on a Forwarder via
// SetBridging, the per-frame emit path computes both the active and the
// bridging shard indices and emits to BOTH destinations. The listener's
// per-TxID egress dedup absorbs the duplicate frame on the receive side.
//
// SetBridging is safe to call concurrently with the hot path. A nil
// pointer means "no bridging in flight" and the hot path is a single
// emit per frame (the normal, steady-state behaviour).
type BridgingEngine struct {
	// Secondary is the successor generation's shard engine. Distinct
	// ShardBits and (optionally) addressing prefix so the dual-emit
	// destination is the SUCCESSOR layout.
	Secondary *shard.Engine

	// TransitionEpoch (Unix seconds) is the time at which the consumer
	// MUST stop dual-emitting; the applier replaces Secondary with the
	// new active engine and clears the BridgingEngine pointer.
	TransitionEpoch int64
}

type Forwarder struct {
	engine       *shard.Engine
	mcPrefix     uint16
	mcGroupID    uint16
	egressPort   int
	debug        bool
	rec          *metrics.Recorder
	log          *slog.Logger
	fragDataSize int    // >0 = fragmentation enabled; fragment capacity per datagram
	bindSource   net.IP // optional; when non-nil egress sockets are syscall.Bind'd to this IPv6 so SSM receivers can pre-declare it
	stampSource  bool   // when true, always (re)stamp HashKey from the observed source IP, even if the sender pre-stamped it (authoritative source identity for own-traffic exclusion); false behind a source-rewriting LB
	hopLimit     int    // IPV6_MULTICAST_HOPS for egress; 0 = leave kernel default (1)
	egressLoop   bool   // IPV6_MULTICAST_LOOP for egress; required on collapsed/mesh router nodes so locally-originated multicast enters the kernel MFC and is forwarded to tunnel/consumer OIFs

	// coalesce enables BRC-142 within-batch frame coalescing: eligible small
	// transactions are buffered per (sender, group, subtree) during a receive
	// batch and packed into bundle datagrams at batch end to cut egress
	// packets-per-second. Opt-in, off by default. See coalesce.go.
	coalesce           bool
	coalesceMaxBytes   int  // bundle datagram cap in bytes; resolved >0 by SetCoalesce
	coalesceMaxMembers int  // members per bundle; ≤0 ⇒ uint16 max (MTU-bound in practice)
	coalesceCarryTxid  bool // carry per-member TxID on the wire vs recompute on receipt

	// requireBlockPoW gates BRC-131 block-announce frames on a cheap stateless
	// proof-of-work check (the permissionless complement to admission control):
	// the in-frame 80-byte header must hash under the target its nBits claims,
	// and that target must be ≤ powFloor. nil powFloor = self-consistency only.
	// Off by default; coinbase/subtree carry no in-frame PoW and are unaffected.
	requireBlockPoW bool
	powFloor        *big.Int

	// requireEF makes the ingress EF-native: a transaction submission (an
	// unstamped BRC-124 frame or a bare tx) must be BRC-30 Extended Format;
	// legacy BRC-12 (V1) frames and raw BRC-124 submissions are rejected. Off by
	// default (accepts raw + EF). Relayed (already-stamped) frames are exempt —
	// they were validated at their ingress — so the relay hot path is untouched.
	// See docs/architecture.md § Transaction ingress (framed, bare, EF-native).
	requireEF bool

	// allowStampedIngress admits framed BRC-124/BRC-128 input that already
	// carries a SeqNum — a frame some other proxy stamped and emitted. Off by
	// default: an ingress proxy accepts submissions, and a submitter has no
	// business presenting a stamped frame. Turn it on only where the lane
	// really does carry fabric traffic (a spine collect lane, a relay hop).
	allowStampedIngress bool

	// verifyPayloadHash gates framed BRC-124/BRC-128 (V2) input on the
	// canonical TxID of its payload. Off by default. Unlike requireEF this
	// deliberately does NOT exempt already-stamped frames: SeqNum is
	// attacker-chosen, so exempting on it is the bypass, not an optimisation.
	// See SetVerifyPayloadHash.
	verifyPayloadHash bool

	// groupAddrs caches the multicast destination address by group index. The
	// address is a pure function of (mcPrefix, mcGroupID, idx, egressPort) —
	// invariant across resharding/bridging, which change the index a txid maps
	// to, not the address of an index — so the cache never needs invalidation.
	// Shared across worker goroutines; atomic per slot for lock-free lazy fill.
	// Eliminates a per-packet net.UDPAddr allocation on the forward hot path.
	groupAddrs []atomic.Pointer[net.UDPAddr]

	// bridging is the optional secondary engine used during a BRC-139
	// live-resharding bridging window. nil ⇒ single-emit (steady
	// state); non-nil ⇒ dual-emit. Atomic so the applier can swap
	// without locking the hot path.
	bridging atomic.Pointer[BridgingEngine]

	// Ingress TxID dedup (nil = disabled). When non-nil, every BRC-124/128,
	// BRC-131 block, BRC-132 subtree data, and BRC-134 anchor frame claims
	// its TxID/ContentID in the configured namespace before stamping; if the
	// claim is lost (another proxy or listener already published this TxID)
	// the frame is dropped instead of multicast.
	txDedup       TxidDedup
	txDedupPrefix string

	// beefDedup is the OPTIONAL separate dedup store for the BRC-148 object
	// plane. Nil means BEEF shares the transaction store, which is the
	// historical behaviour and is preserved so an upgrade changes nothing.
	// See SetBEEFDedup for why a shared store is a hazard once the object
	// plane carries open-class volume.
	beefDedup       TxidDedup
	beefDedupPrefix string

	// BEEF object plane (BRC-148). beefEngine derives domain-tagged group
	// indices (0x1000 + shardIndex(TopicID)) at the plane's own shard-bit
	// width; beefMaxObject bounds an accepted submission's object bytes
	// (the spec's ingress MUST-bound). Nil/zero until SetBEEF is called —
	// submissions and V9 frames are then dropped as "disabled".
	beefEngine    *shard.PlaneEngine
	beefMaxObject int
	beefPolicy    BEEFSubmitPolicy
	// beefLimit bounds open-class BEEF ingress per source and per plane at
	// this door. Nil or unconfigured permits everything; see ratelimit.go.
	beefLimit *beefLimiter

	// chains is a striped per-flow counter map. Stripe index is derived
	// from a hash of the sender IP, so concurrent workers handling distinct
	// senders rarely contend on the same stripe lock. Once a flowState
	// entry exists the counter is incremented lock-free via atomic.Uint64;
	// the stripe lock guards only the map lookup/insert path.
	chains [chainStripes]chainStripe
}

// SetBridging publishes (or clears) the BRC-139 live-resharding
// bridging engine. Safe to call from any goroutine; the per-frame
// emit path reads the pointer atomically.
//
// Pass nil to exit bridging mode. Bridging-mode operators MUST
// orchestrate the swap-and-clear at TransitionEpoch (the applier
// rebuilds the primary engine with the successor's parameters and
// then calls SetBridging(nil)).
func (fw *Forwarder) SetBridging(b *BridgingEngine) {
	fw.bridging.Store(b)
}

// Bridging returns the currently-active bridging engine, or nil when
// the proxy is in steady state. Intended for tests and telemetry.
func (fw *Forwarder) Bridging() *BridgingEngine {
	return fw.bridging.Load()
}

// SetBindSource configures the source IPv6 the kernel will bind for
// every multicast egress socket. Required when source-mode=ssm so SSM
// receivers can pre-declare this proxy in their (S,G) join calls. Each
// proxy replica MUST use a distinct bindSource — anycast / shared
// source IPs break PIM-SSM RPF.
//
// ip must be a valid IPv6 address (To4() == nil). Pass nil to clear.
// Must be called before [OpenTargets].
func (fw *Forwarder) SetBindSource(ip net.IP) {
	fw.bindSource = ip
}

// SetStampSource controls whether the proxy authoritatively (re)stamps the BRC
// HashKey from the observed packet source IP. When true (default), a sender-
// supplied HashKey is overwritten with seqhash.Hash(observedSrc, groupIdx,
// subtreeID) — so the per-flow identity is the real ingress source, which is
// what makes own-traffic exclusion per-consumer at a direct edge. Set false
// only when a load balancer / upstream proxy rewrites the source (every
// consumer would otherwise collapse to the LB address); the sender's HashKey is
// then preserved.
func (fw *Forwarder) SetStampSource(on bool) {
	fw.stampSource = on
}

// SetEgressHopLimit configures IPV6_MULTICAST_HOPS for every egress socket.
// The default multicast hop limit is 1 (single L2 segment); routed or tunneled
// mesh fabrics (ip6gre + mc-router) must raise it so per-hop decrement does not
// drop frames before they reach remote subscribers. 0 leaves the kernel
// default. Must be called before [OpenTargets].
func (fw *Forwarder) SetEgressHopLimit(n int) {
	fw.hopLimit = n
}

// SetEgressLoop enables IPV6_MULTICAST_LOOP on every egress socket. Off by
// default. On a COLLAPSED or MESH node (the proxy shares a host with a multicast
// router, mc_forwarding=1, emitting onto a dummy/tunnel iface), the kernel only
// submits locally-originated multicast to the MFC when loopback is on; without
// it the frames leave the emit iface but are never forwarded to the fabric/
// consumer OIFs. Independent of -debug (which also adds per-packet logging).
// Must be called before [OpenTargets].
func (fw *Forwarder) SetEgressLoop(on bool) {
	fw.egressLoop = on
}

// SetBlockPoW enables the cheap stateless proof-of-work gate for BRC-131
// block-announce frames. When require is true, a block announce is forwarded
// only if its in-frame 80-byte header hashes under the target its nBits
// claims and that target is at least as hard as floorBits (the difficulty
// floor, in Bitcoin compact form). floorBits 0 disables the floor (header
// self-consistency only — weak, since a forger can claim trivial difficulty;
// set a real floor in production). This validates the artifact, not the
// emitter, so it stays permissionless. Coinbase (BRC-133) and subtree data
// (BRC-132) carry no in-frame PoW and are unaffected. Must be called before
// any worker starts.
func (fw *Forwarder) SetBlockPoW(require bool, floorBits uint32) {
	fw.requireBlockPoW = require
	if require && floorBits != 0 {
		fw.powFloor = pow.CompactToTarget(floorBits)
	} else {
		fw.powFloor = nil
	}
}

// SetRequireEF enables EF-native ingress: transaction submissions must be BRC-30
// Extended Format; raw BRC-12 (V1) and raw BRC-124 (V2 without the marker)
// submissions are rejected. Off by default. Relayed frames (already stamped) are
// unaffected. Must be called before any worker starts.
func (fw *Forwarder) SetRequireEF(require bool) { fw.requireEF = require }

// SetAllowStampedIngress admits framed BRC-124/BRC-128 (V2) input whose SeqNum
// is already set. Off by default. Must be called before any worker starts.
//
// A stamped frame is one another proxy has already admitted, sharded, and
// emitted. On a submission lane that is not a thing a submitter can legitimately
// send, and SeqNum is just bytes in the frame — so accepting stamped input by
// default let anyone opt out of the gates that key off it. Enable it only on a
// lane that genuinely carries fabric traffic: a spine's collect socket, or a
// relay hop forwarding another proxy's output.
//
// This does not exempt anything from the other gates. A stamped frame admitted
// here is still subject to -require-ef and -verify-payload-hash.
func (fw *Forwarder) SetAllowStampedIngress(allow bool) { fw.allowStampedIngress = allow }

// SetVerifyPayloadHash enables canonical-TxID verification of framed
// BRC-124/BRC-128 (V2) input: the frame's TxID must equal the transaction id
// of its payload — SHA256d over the standard serialization, EF extras
// excluded ([objfmt.TxID]), which is the id producers stamp and consumers
// derive. Mismatches are dropped before the ingress dedup claim and before
// group derivation. Off by default. Must be called before any worker starts.
//
// This is an ingress-side gate, not a duplicate of the listener's flag of the
// same name. Two things key off the wire TxID here and neither is recoverable
// downstream:
//
//   - Group derivation is the top shard bits of TxID[0:4], so an unverified
//     TxID lets a submitter choose its own shard.
//   - The ingress dedup claim is keyed on the TxID. With -ingress-dedup on
//     (the default), a frame carrying an honest transaction's TxID over a
//     forged payload wins the claim and the honest transaction is then
//     suppressed at ingress — it never reaches a listener, so the listener's
//     own verification cannot undo it.
//
// Cost is one SHA256d over the payload per framed transaction, which on a CPU
// without SHA-NI can exceed the whole forward path; measure on the target
// hardware before enabling on a hot lane. Bare submissions are unaffected:
// the proxy derives their TxID itself, so there is nothing to verify and no
// cost is added. BRC-12 (V1) frames are forwarded verbatim regardless — the
// legacy wire format carries no payload-bound identifier.
func (fw *Forwarder) SetVerifyPayloadHash(v bool) { fw.verifyPayloadHash = v }

// SetCoalesce enables BRC-142 within-batch frame coalescing. When enabled,
// eligible BRC-124/128 transactions are buffered per (sender, group, subtree)
// during each receive batch and packed into bundle datagrams at batch end,
// cutting egress packets-per-second for shard-dense traffic at zero added
// latency (the window is one receive batch). maxBytes caps the bundle datagram
// (≤0 ⇒ [DefaultCoalesceMaxBytes]); maxMembers caps members per bundle (≤0 ⇒
// uint16 max); carryTxid includes the per-member TxID on the wire. Off by
// default; must be called before any worker constructs its Egress.
func (fw *Forwarder) SetCoalesce(enabled bool, maxBytes, maxMembers int, carryTxid bool) {
	fw.coalesce = enabled
	if maxBytes <= 0 {
		maxBytes = DefaultCoalesceMaxBytes
	}
	fw.coalesceMaxBytes = maxBytes
	fw.coalesceMaxMembers = maxMembers
	fw.coalesceCarryTxid = carryTxid
}

// SetTxidDedup attaches a TxID claim store used to suppress ingress
// duplicates before multicast. prefix is the Redis key prefix associated
// with the proxy ingress namespace (typically "bsp:tx:"). Pass nil to
// disable. Must be called before any worker goroutine starts processing.
func (fw *Forwarder) SetTxidDedup(d TxidDedup, prefix string) {
	fw.txDedup = d
	fw.txDedupPrefix = prefix
}

// SetBEEFDedup gives the BRC-148 object plane its OWN dedup store and
// namespace. Pass nil (the default) to leave BEEF sharing the transaction
// store, which is what every build did before this existed.
//
// Why a separate store matters, and why capacity is the point rather than the
// prefix: a dedup store is a bounded set, and the transaction plane and the
// object plane have completely different volume characteristics. Transactions
// are settlement traffic whose rate is a property of the chain; the object
// plane is an OPEN class that anyone may publish to with no account. Sharing
// one bounded set means a high-rate BEEF flood evicts transaction entries,
// and an evicted entry is a LOST claim: the next copy of that transaction
// wins the claim again and is re-stamped and re-emitted onto the fabric.
// So an anonymous object-plane flood turns into duplicate TRANSACTION
// delivery fleet-wide, on the paying path, without ever touching the
// transaction port. Separate stores make the two planes' capacity
// independent, so the blast radius of a flood stays inside the plane it
// arrived on.
//
// The distinct prefix is hygiene rather than protection: BEEF claims key
// SHA-256(ContentID ‖ TopicID) while transactions key the TxID, so a
// collision was already negligible. Capacity is the hazard.
//
// Must be called before any worker goroutine starts processing.
func (fw *Forwarder) SetBEEFDedup(d TxidDedup, prefix string) {
	fw.beefDedup = d
	fw.beefDedupPrefix = prefix
}

// claimIngress consults the configured TxID dedup store. Returns true when
// the caller should proceed (claim won or dedup disabled or fail-open) and
// false when the frame must be suppressed. The frameType label is used for
// the suppression metric.
func (fw *Forwarder) claimIngress(txid [32]byte, frameType, iface string, workerID int) bool {
	return fw.claimIngressIn(fw.txDedup, fw.txDedupPrefix, txid, frameType, iface, workerID)
}

// claimBEEFIngress claims on the object plane's own store when one is
// configured, falling back to the transaction store so a build that never
// calls SetBEEFDedup behaves exactly as it did before.
func (fw *Forwarder) claimBEEFIngress(key [32]byte, frameType, iface string, workerID int) bool {
	if fw.beefDedup != nil {
		return fw.claimIngressIn(fw.beefDedup, fw.beefDedupPrefix, key, frameType, iface, workerID)
	}
	return fw.claimIngressIn(fw.txDedup, fw.txDedupPrefix, key, frameType, iface, workerID)
}

func (fw *Forwarder) claimIngressIn(store TxidDedup, prefix string, txid [32]byte, frameType, iface string, workerID int) bool {
	if store == nil {
		return true
	}
	claimed, _ := store.Claim(prefix, txid)
	if claimed {
		return true
	}
	if fw.rec != nil {
		fw.rec.IngressDeduped(iface, workerID, frameType)
	}
	if fw.debug {
		fw.log.Debug("ingress dedup suppressed", "frame_type", frameType, "txid_prefix", fmt.Sprintf("%x", txid[:8]))
	}
	return false
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
	fw := &Forwarder{
		engine:     engine,
		mcPrefix:   mcPrefix,
		mcGroupID:  mcGroupID,
		egressPort: egressPort,
		debug:      debug,
		rec:        rec,
		log:        slog.Default().With("component", "forwarder"),
		groupAddrs: make([]atomic.Pointer[net.UDPAddr], maxGroupCache),
	}
	for i := range fw.chains {
		fw.chains[i].m = make(map[chainKey]*flowState)
	}
	return fw
}

// addrFor returns the cached multicast destination *net.UDPAddr for group idx,
// computing it via the shard engine and caching it on first use. The address of
// an index is engine-independent, so this is also correct for bridging's
// secondary-engine indices. The result is shared; callers must treat it as
// read-only.
func (fw *Forwarder) addrFor(idx uint32) *net.UDPAddr {
	if idx >= uint32(len(fw.groupAddrs)) {
		return fw.engine.Addr(idx, fw.egressPort)
	}
	if a := fw.groupAddrs[idx].Load(); a != nil {
		return a
	}
	a := fw.engine.Addr(idx, fw.egressPort)
	fw.groupAddrs[idx].Store(a)
	return a
}

// OpenTargets opens one multicast egress UDP socket per interface and wraps
// each in an ipv6.PacketConn for batched WriteBatch (sendmmsg on Linux) calls.
// On worker 0 (probeWorker == true) each socket is probed with a zero-byte
// send to verify multicast egress is functional.
//
// On error, all partially opened sockets are closed before returning.
func (fw *Forwarder) OpenTargets(ifaces []*net.Interface, probeWorker bool) ([]Target, error) {
	loopback := 0
	if fw.debug || fw.egressLoop {
		loopback = 1
	}
	targets := make([]Target, 0, len(ifaces))
	for _, iface := range ifaces {
		conn, err := openEgressSocket(iface, loopback, fw.bindSource, fw.hopLimit)
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
		targets = append(targets, Target{
			Iface: iface,
			Conn:  conn,
			PC:    ipv6.NewPacketConn(conn),
		})
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

// IngressClass classifies the frame set an ingress socket is permitted to
// accept. It is a per-socket property (not a forwarder-wide one) so a single
// proxy can bind a transaction-only socket for consumers alongside a
// privileged socket exposed only to miner-tier peers over their tunnels.
type IngressClass uint8

const (
	// IngressPrivileged accepts every frame class, including the
	// miner-only control-plane frames: BRC-131 block announce, BRC-133
	// coinbase, and BRC-132 subtree data. Bind this only on sockets
	// reachable solely by miner-tier peers.
	IngressPrivileged IngressClass = iota

	// IngressTransaction accepts the open class set: transaction-bearing
	// frames (BRC-12/124/128 and BRC-134 anchor), bare tx submissions, and
	// BRC-148 BEEF objects (framed FrameVerV9 or a bare submission record).
	// BRC-131 block / BRC-133 coinbase (FrameVerV4) and BRC-132 subtree
	// data (FrameVerV5) are dropped and counted — broadcast-amplified
	// classes enter only through a privileged socket. This is the class for
	// the public user/consumer ingress port.
	IngressTransaction

	// IngressBEEF accepts only BRC-148 BEEF traffic (framed FrameVerV9 and
	// bare submission records). It is the class for the optional dedicated
	// BEEF lane (flow separation / load balancing — never admission: the
	// open port accepts BEEF regardless).
	IngressBEEF
)

// IngressOpen is the preferred name for the open class set accepted on the
// public ingress port (tx + anchor + BEEF). Alias of [IngressTransaction],
// kept for API compatibility.
const IngressOpen = IngressTransaction

// Dispatch routes one ingress datagram to the correct Process* entry point by
// its BRC frame-version byte (raw[6]). It accepts every frame class
// (equivalent to IngressPrivileged) and is retained for callers that do not
// enforce a miner-tier gate. New callers should use [Forwarder.DispatchClass].
func (fw *Forwarder) Dispatch(egr *Egress, raw []byte, src net.Addr, workerID int) {
	fw.DispatchClass(egr, raw, src, workerID, IngressPrivileged)
}

// DispatchClass routes one ingress datagram to the correct Process* entry
// point by its BRC frame-version byte (raw[6]), gated by the socket's
// [IngressClass]. It is the single source of truth for frame-version routing,
// shared by the worker receive loop and any alternative ingress that drives
// the forwarder directly, so version handling cannot drift between callers.
//
// On an IngressTransaction socket, BRC-131 block/BRC-133 coinbase (FrameVerV4)
// and BRC-132 subtree data (FrameVerV5) are dropped (counted via
// PrivilegedFrameRejected) — those frames may only enter through a privileged
// (miner-tier) socket. raw is the UDP payload (the BRC frame); src is its
// source; egr is the caller's per-worker egress queue. raw must remain valid
// until egr.Flush returns.
func (fw *Forwarder) DispatchClass(egr *Egress, raw []byte, src net.Addr, workerID int, class IngressClass) {
	n := len(raw)
	// Bare (header-stripped) transaction detection. A multicast frame begins with
	// the BSV network magic; anything else on this port is a bare transaction
	// submitted without a frame. The 4-byte magic read is in the same cache line
	// as the raw[6] version byte the switch below already reads, so the framed
	// relay hot path is byte-identical; the bare branch is cold (submissions only).
	if n < 4 || binary.BigEndian.Uint32(raw[0:4]) != frame.MagicBSV {
		// The bare grammar is 3-way (BRC-148): a BEEF submission record
		// leads with the 0xBEEF tag (a bare tx's version byte[1] is 0x00; a
		// framed datagram starts with the 0xE3 magic); anything else is a
		// bare transaction. BEEF is an open class, so records are admitted
		// on every ingress class.
		if n >= 2 && binary.BigEndian.Uint16(raw[0:2]) == objfmt.BEEFRecordTag {
			fw.SubmitBEEF(egr, raw, src, workerID)
			return
		}
		if class == IngressBEEF {
			fw.rejectOnBEEFLane(egr, workerID)
			return
		}
		fw.dispatchBareTx(egr, raw, src, workerID)
		return
	}
	privileged := class == IngressPrivileged
	switch {
	case n > 6 && raw[6] == frame.FrameVerV4:
		if !privileged {
			fw.rejectPrivileged(raw, workerID)
			return
		}
		fc := metrics.IngressClassBlock
		if n > 7 && raw[7] == frame.BlockMsgCoinbase {
			fc = metrics.IngressClassCoinbase
		}
		if fw.rec != nil {
			fw.rec.IngressMetered(fc, privileged, n)
		}
		fw.ProcessBlock(egr, raw, src, workerID)
	case n > 6 && raw[6] == frame.FrameVerV5:
		if !privileged {
			fw.rejectPrivileged(raw, workerID)
			return
		}
		if fw.rec != nil {
			fw.rec.IngressMetered(metrics.IngressClassSubtree, privileged, n)
		}
		fw.ProcessSubtreeData(egr, raw, src, workerID)
	case n > 6 && raw[6] == frame.FrameVerV6:
		if class == IngressBEEF {
			fw.rejectOnBEEFLane(egr, workerID)
			return
		}
		if fw.rec != nil {
			fw.rec.IngressMetered(metrics.IngressClassAnchor, privileged, n)
		}
		fw.ProcessAnchor(egr, raw, src, workerID)
	case n > 6 && raw[6] == frame.FrameVerV9:
		// BRC-148 BEEF object frame — open class (relay or pre-framed
		// submission), admitted on every ingress class.
		if fw.rec != nil {
			fw.rec.IngressMetered(metrics.IngressClassBEEF, privileged, n)
		}
		fw.ProcessBEEF(egr, raw, src, workerID)
	case n > 6 && raw[6] == frame.FrameVerV8:
		// BRC-142 bundle: an already-coalesced datagram of many transactions.
		// Re-emit it verbatim to its group (a relay/spine forwarding bundles a
		// collapsed/ingress proxy coalesced upstream). Bundles are tx-class, so
		// no miner-tier gate; the origin already authorised the members.
		if class == IngressBEEF {
			fw.rejectOnBEEFLane(egr, workerID)
			return
		}
		if fw.rec != nil {
			fw.rec.IngressMetered(metrics.IngressClassTx, privileged, n)
		}
		fw.ProcessBundle(egr, raw, workerID)
	default:
		if class == IngressBEEF {
			fw.rejectOnBEEFLane(egr, workerID)
			return
		}
		if fw.rec != nil {
			fw.rec.IngressMetered(metrics.IngressClassTx, privileged, n)
		}
		fw.Process(egr, raw, src, workerID)
	}
}

// dispatchBareTx admits a header-stripped transaction submitted without a
// multicast frame (the push-ingest submission path). It is EF-only: a BRC-30
// Extended Format transaction, one per datagram. BRC-12 raw is rejected — the
// fabric is EF-native (Teranode requires Extended Format; extending a raw tx
// needs a UTXO lookup, a wallet/submitter concern, not a fabric one). A bare tx
// is inherently transaction-class, so it composes with the miner-tier gate for
// free — no privileged (block/subtree/coinbase) frame can be expressed here.
// DispatchBareTx admits a transaction on a connection that has ALREADY
// committed to the bare grammar, WITHOUT the leading-magic sniff DispatchClass
// does. A bare stream reader (objfmt.ClassTx) validates structure but not the
// version bytes, so a crafted tx whose first 4 bytes equal the BSV magic would,
// through DispatchClass, misroute to the framed relay path and be enqueued
// VERBATIM — aliasing the reader's reused window, which under batched egress is
// overwritten before flush. The bare lane must never magic-route; this copies
// into a fresh frame (via dispatchBareTx) so the enqueued bytes are stable.
func (fw *Forwarder) DispatchBareTx(egr *Egress, tx []byte, src net.Addr, workerID int) {
	fw.dispatchBareTx(egr, tx, src, workerID)
}

func (fw *Forwarder) dispatchBareTx(egr *Egress, tx []byte, src net.Addr, workerID int) {
	iface := ""
	if egr != nil && len(egr.targets) > 0 {
		iface = egr.targets[0].Iface.Name
	}
	sz, err := objfmt.TxSize(tx)
	if err != nil || sz != len(tx) {
		if fw.rec != nil {
			fw.rec.PacketDropped(iface, workerID, "bare_tx_malformed")
		}
		return
	}
	if fw.requireEF && !objfmt.IsEF(tx) {
		// EF-native ingress: BRC-12 raw is rejected. The submitter must send
		// Extended Format (BRC-30); a raw tx cannot be delivered to a Teranode
		// consumer and extension is not a fabric-side operation.
		if fw.rec != nil {
			fw.rec.PacketDropped(iface, workerID, "bare_tx_not_ef")
		}
		return
	}
	framed, err := objfmt.MulticastBytes(objfmt.ClassTx, tx)
	if err != nil {
		if fw.rec != nil {
			fw.rec.PacketDropped(iface, workerID, "bare_tx_reframe")
		}
		return
	}
	if fw.rec != nil {
		fw.rec.IngressMetered(metrics.IngressClassTx, false, len(tx))
	}
	// Self-framed: MulticastBytes derived the TxID from these exact payload
	// bytes, so the payload-hash gate would compare our own hash to itself.
	fw.process(egr, framed, src, workerID, false)
}

// rejectPrivileged drops a privileged control-plane frame received on a
// transaction-only ingress socket and records it under the wire frame-type
// label (brc131 announce / brc133 coinbase / brc132 subtree data).
func (fw *Forwarder) rejectPrivileged(raw []byte, workerID int) {
	frameType := "brc132" // FrameVerV5 subtree data
	if len(raw) > 6 && raw[6] == frame.FrameVerV4 {
		frameType = "brc131" // BlockMsgAnnounce
		if len(raw) > 7 && raw[7] == frame.BlockMsgCoinbase {
			frameType = "brc133"
		}
	}
	if fw.rec != nil {
		fw.rec.PrivilegedFrameRejected(frameType)
	}
	if fw.debug {
		fw.log.Debug("privileged frame rejected on transaction-only ingress",
			"frame_type", frameType, "worker", workerID)
	}
}

// Process is the hot path: decode raw for routing, conditionally stamp
// HashKey/SeqNum, then enqueue into egr for batched egress via Egress.Flush.
//
// For BRC-124/BRC-128 frames: if raw[48:56] (SeqNum) is non-zero the sender has
// pre-stamped the frame and it is forwarded verbatim. If SeqNum is zero the
// proxy stamps raw[40:48] (HashKey) and raw[48:56] (SeqNum) in-place: HashKey is
// stable per (sender, group, subtree) flow; SeqNum is a per-flow monotonic counter
// starting at 1. BRC-12 frames are always forwarded verbatim. workerID is used
// only for metrics labels.
//
// raw must remain valid until egr.Flush returns. egr may be nil; in that
// case the frame is decoded and stamped but not enqueued — used by tests
// that exercise only the stamping logic.
func (fw *Forwarder) Process(egr *Egress, raw []byte, src net.Addr, workerID int) {
	fw.process(egr, raw, src, workerID, fw.verifyPayloadHash)
}

// process is Process with the payload-hash gate made explicit, so the one
// caller that framed the transaction itself can skip it. verify is only ever
// fw.verifyPayloadHash (external input) or false (self-framed input).
func (fw *Forwarder) process(egr *Egress, raw []byte, src net.Addr, workerID int, verify bool) {
	f, err := frame.Decode(raw)
	if err != nil {
		fw.log.Debug("frame decode error", "err", err, "len", len(raw))
		if fw.rec != nil && egr != nil && len(egr.targets) > 0 {
			fw.rec.PacketDropped(egr.targets[0].Iface.Name, workerID, "decode_error")
		}
		return
	}

	// Stamped-ingress gate. A frame arriving with a SeqNum was admitted,
	// sharded and emitted by some other proxy; on a submission lane that is
	// not something a submitter can legitimately present. Off by default, so
	// the ordinary posture is "this proxy accepts submissions, not relay".
	// One compare, and V1 decodes with a zero SeqNum so legacy frames are
	// unaffected. See SetAllowStampedIngress.
	if !fw.allowStampedIngress && f.Version == frame.FrameVerV2 && f.SeqNum != 0 {
		if fw.rec != nil {
			fw.rec.PacketDropped(egrIface(egr), workerID, "stamped_ingress")
		}
		if fw.debug {
			fw.log.Debug("stamped frame on a submission lane",
				"txid_prefix", fmt.Sprintf("%x", f.TxID[:8]),
				"seq_num", f.SeqNum,
			)
		}
		return
	}

	// EF-native ingress (opt-in). A transaction must be Extended Format: a
	// legacy BRC-12 (V1) frame, or a BRC-124 frame without the marker, is
	// rejected. This deliberately does NOT exempt stamped frames: SeqNum is
	// chosen by whoever sent the frame, so an exemption keyed on it is a
	// one-byte opt-out of the EF posture rather than a relay optimisation. A
	// relay lane that legitimately carries stamped frames declares itself with
	// -allow-stamped-ingress, and its traffic is Extended Format anyway on an
	// EF-native fabric. When requireEF is off the check short-circuits to a
	// single predicted branch.
	if fw.requireEF && (f.Version == frame.FrameVerV1 || !objfmt.IsEF(f.Payload)) {
		if fw.rec != nil {
			fw.rec.PacketDropped(egrIface(egr), workerID, "ingress_not_ef")
		}
		return
	}

	// Payload-hash verification (opt-in). This MUST stay ahead of the dedup
	// claim below: a forged frame that reaches the claim first burns the
	// honest transaction's dedup slot, and suppressing the honest frame at
	// ingress is exactly the outcome the gate exists to prevent. Only V2
	// carries a payload-bound TxID; V1 is forwarded verbatim.
	if verify && f.Version == frame.FrameVerV2 {
		// The canonical id binds only the transaction TxSize walks, so a
		// payload of "one valid transaction followed by junk" would otherwise
		// verify against the honest id and carry the tail onto the fabric.
		// Require the payload to be exactly one transaction — the same bound
		// dispatchBareTx already applies to a bare submission.
		if sz, szErr := objfmt.TxSize(f.Payload); szErr != nil || sz != len(f.Payload) {
			if fw.rec != nil {
				fw.rec.PacketDropped(egrIface(egr), workerID, "payload_not_one_tx")
			}
			if fw.debug {
				fw.log.Debug("payload is not exactly one transaction",
					"txid_prefix", fmt.Sprintf("%x", f.TxID[:8]),
					"payload_len", len(f.Payload),
					"tx_size", sz,
					"err", szErr,
				)
			}
			return
		}
		computed, idErr := objfmt.TxID(f.Payload)
		if idErr != nil || computed != f.TxID {
			if fw.rec != nil {
				fw.rec.PacketDropped(egrIface(egr), workerID, "payload_hash_mismatch")
			}
			if fw.debug {
				fw.log.Debug("payload hash mismatch",
					"txid_prefix", fmt.Sprintf("%x", f.TxID[:8]),
					"computed_prefix", fmt.Sprintf("%x", computed[:8]),
					"payload_len", len(f.Payload),
					"ef", objfmt.IsEF(f.Payload),
					"err", idErr,
				)
			}
			return
		}
	}

	// Ingress TxID dedup gate. BRC-124/BRC-128 (V2) frames claim by TxID;
	// legacy BRC-12 (V1) frames pass through unconditionally because the
	// V1 wire format does not carry a stable per-flow identifier and
	// listeners cannot dedup them downstream either.
	if f.Version == frame.FrameVerV2 {
		ifaceName := ""
		if egr != nil && len(egr.targets) > 0 {
			ifaceName = egr.targets[0].Iface.Name
		}
		if !fw.claimIngress(f.TxID, "brc124", ifaceName, workerID) {
			return
		}
	}

	groupIdx := fw.engine.GroupIndex(&f.TxID)

	if f.Version == frame.FrameVerV2 && src != nil {
		ip := addrToIPv6(src)

		// BRC-130 fragmentation path: payload exceeds per-datagram capacity.
		if fw.fragDataSize > 0 && len(f.Payload) > fw.fragDataSize {
			fw.fragment(egr, f, ip, groupIdx, workerID)
			return
		}

		// BRC-142 within-batch coalescing: buffer eligible small txs into the
		// per-worker bundle buffer (flushed at batch end). Skipped during a
		// live-reshard bridging window (a bundle is not dual-emitted) and when a
		// single member would not fit the bundle MTU. The bundle — not the
		// member — is HashKey/SeqNum-stamped at flush, so raw is left untouched.
		if fw.coalesce && egr != nil && egr.coal != nil && fw.bridging.Load() == nil &&
			bundle.HeaderSize+bundle.MemberOverhead(fw.coalesceCarryTxid)+len(f.Payload) <= fw.coalesceMaxBytes {
			egr.coal.add(ip, groupIdx, f.SubtreeID, f.TxID, f.Payload)
			return
		}

		// Standard BRC-124/BRC-128 path: stamp HashKey and SeqNum
		// independently. The gap-injection load generator pre-stamps
		// SeqNum but leaves HashKey=0, so we must stamp HashKey
		// whenever it is zero regardless of SeqNum.
		stampInPlace(raw, ip, groupIdx, f.SubtreeID, fw)
	}

	if egr == nil {
		return
	}

	dst := fw.addrFor(groupIdx)
	egr.EnqueueData(raw, *dst, groupIdx, workerID)

	// BRC-139 live-resharding bridging: when a secondary engine is
	// published, dual-emit to the successor layout as well. The
	// listener's per-TxID egress dedup collapses the duplicate frame
	// on the receive side. One extra atomic.Pointer.Load + (if
	// non-nil) one extra EnqueueData per frame; the second branch
	// short-circuits the common case.
	if bs := fw.bridging.Load(); bs != nil {
		bridgeIdx := bs.Secondary.GroupIndex(&f.TxID)
		// Only emit the secondary when the successor's group differs
		// from the active group (so frames whose two indices collide
		// are not literally doubled to the same address).
		if bridgeIdx != groupIdx {
			bridgeDst := fw.addrFor(bridgeIdx)
			egr.EnqueueData(raw, *bridgeDst, bridgeIdx, workerID)
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

// fragment splits f.Payload into BRC-130 fragment datagrams and enqueues each
// for fan-out to every target. Each fragment receives an independent
// HashKey+SeqNum pair allocated from the same flow as a regular frame would
// use. Fragment buffers come from the Egress sync.Pool and are released back
// at Flush time.
func (fw *Forwarder) fragment(egr *Egress, f *frame.Frame, ip [16]byte, groupIdx uint32, workerID int) {
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
	dst := fw.addrFor(groupIdx)

	for i := 0; i < k; i++ {
		start := i * dataSize
		end := start + dataSize
		if end > len(payload) {
			end = len(payload)
		}
		fragData := payload[start:end]

		hashKey, seqNum := fw.nextSeq(ip, groupIdx, f.SubtreeID)
		if egr == nil {
			// Tests exercise fragment() with no egress to verify seq
			// allocation only — skip encode + queue.
			continue
		}

		bufPtr := egr.PoolGet()
		buf := *bufPtr
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
			egr.pool.Put(bufPtr)
			continue
		}
		egr.EnqueueDataPooled(buf[:n], *dst, groupIdx, workerID, bufPtr)
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
// fragments large payloads via BRC-130, and enqueues into egr for the
// GroupBlockBroadcast multicast group instead of a shard group.
//
// raw must remain valid until egr.Flush returns. egr may be nil for tests.
func (fw *Forwarder) ProcessBlock(egr *Egress, raw []byte, src net.Addr, workerID int) {
	bf, err := frame.DecodeBlock(raw)
	if err != nil {
		fw.log.Debug("block frame decode error", "err", err, "len", len(raw))
		if fw.rec != nil && egr != nil && len(egr.targets) > 0 {
			fw.rec.PacketDropped(egr.targets[0].Iface.Name, workerID, "decode_error")
		}
		return
	}

	// Permissionless proof-of-work gate: a block announcement must carry valid
	// work. Validates the artifact (the in-frame header), not the emitter, so
	// no key/allowlist is consulted — anyone may announce, but a forged
	// announcement costs work proportional to the difficulty floor. Applies
	// only to BlockMsgAnnounce (coinbase carries no header).
	if fw.requireBlockPoW && bf.MsgType == frame.BlockMsgAnnounce {
		if len(bf.Payload) < pow.HeaderSize || !pow.CheckHeader(bf.Payload[:pow.HeaderSize], fw.powFloor) {
			if fw.rec != nil {
				fw.rec.BlockPoWRejected()
			}
			if fw.debug {
				fw.log.Debug("block announce rejected: invalid proof of work",
					"content_id", fmt.Sprintf("%x", bf.ContentID[:8]))
			}
			return
		}
	}

	// Ingress TxID dedup gate. BRC-131 block frames carry a ContentID
	// (block hash) in the TxID slot; we treat it as the TxID for dedup
	// purposes so the same block is never multicasted twice by sibling proxies.
	{
		ifaceName := ""
		if egr != nil && len(egr.targets) > 0 {
			ifaceName = egr.targets[0].Iface.Name
		}
		if !fw.claimIngress(bf.ContentID, "brc131", ifaceName, workerID) {
			return
		}
	}

	if src != nil {
		ip := addrToIPv6(src)
		// BRC-131 block announces and BRC-133 coinbase txs share the
		// GroupBlockBroadcast multicast destination but use distinct virtual
		// HashKey ingredients so each carries its own independent SeqNum
		// counter on the proxy.
		ctrlIdx := uint32(shard.GroupBlockBroadcast)
		if bf.MsgType == frame.BlockMsgCoinbase {
			ctrlIdx = uint32(shard.GroupCoinbaseFlow)
		}
		var zeroSub [32]byte

		// BRC-130 fragmentation path for large block payloads.
		if fw.fragDataSize > 0 && len(bf.Payload) > fw.fragDataSize {
			fw.fragmentBlock(egr, raw, bf, ip, ctrlIdx, workerID)
			return
		}

		// Stamp HashKey/SeqNum in-place; HashKey is stamped even when SeqNum
		// is pre-set (e.g. gap injection in subtx-gen) so the chain rate
		// limiter and per-flow cache keys are well-defined.
		stampInPlace(raw, ip, ctrlIdx, zeroSub, fw)
	}

	if egr == nil {
		return
	}

	dst := shard.GroupAddr(fw.mcPrefix, fw.mcGroupID, shard.GroupBlockBroadcast)
	addr := net.UDPAddr{IP: dst, Port: fw.egressPort}
	egr.EnqueueControl(raw, addr, "block_control", workerID)

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
// validated, HashKey/SeqNum-stamped, and enqueued for GroupBlockBroadcast
// (FF0E::B:FFFE) — the same multicast group as BRC-131 block frames.
//
// raw must remain valid until egr.Flush returns. egr may be nil for tests.
func (fw *Forwarder) ProcessAnchor(egr *Egress, raw []byte, src net.Addr, workerID int) {
	f, err := frame.DecodeAnchor(raw)
	if err != nil {
		fw.log.Debug("anchor frame decode error", "err", err, "len", len(raw))
		if fw.rec != nil && egr != nil && len(egr.targets) > 0 {
			fw.rec.PacketDropped(egr.targets[0].Iface.Name, workerID, "decode_error")
		}
		return
	}

	// Ingress TxID dedup gate for BRC-134 anchor frames.
	{
		ifaceName := ""
		if egr != nil && len(egr.targets) > 0 {
			ifaceName = egr.targets[0].Iface.Name
		}
		if !fw.claimIngress(f.TxID, "brc134", ifaceName, workerID) {
			return
		}
	}

	if src != nil {
		ip := addrToIPv6(src)
		// Anchor frames use a dedicated virtual group index for HashKey
		// derivation so they get their own independent SeqNum counter
		// and flow identity, distinct from BRC-131/BRC-133 frames which
		// share the same GroupBlockBroadcast multicast address.
		var zeroSub [32]byte

		// Stamp HashKey/SeqNum in-place; HashKey is stamped even when SeqNum
		// is pre-set so chain RL and cache keys are deterministic.
		stampInPlace(raw, ip, uint32(shard.GroupAnchorFlow), zeroSub, fw)
	}

	if egr == nil {
		return
	}

	dst := shard.GroupAddr(fw.mcPrefix, fw.mcGroupID, shard.GroupBlockBroadcast)
	addr := net.UDPAddr{IP: dst, Port: fw.egressPort}
	egr.EnqueueControl(raw, addr, "anchor", workerID)

	if fw.debug {
		fw.log.Debug("anchor forwarded",
			"txid", fmt.Sprintf("%x", f.TxID[:8]),
			"dst", addr,
		)
	}
}

// fragmentBlock splits a large BRC-131 block payload into BRC-130 fragments
// and enqueues each for the GroupBlockBroadcast control group. Each
// fragment receives OrigFrameVer=0x04 so that reassembly can reconstruct
// the correct frame version. Fragment buffers come from the Egress pool.
func (fw *Forwarder) fragmentBlock(egr *Egress, raw []byte, bf *frame.BlockFrame, ip [16]byte, ctrlIdx uint32, workerID int) {
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
	dst := shard.GroupAddr(fw.mcPrefix, fw.mcGroupID, shard.GroupBlockBroadcast)
	addr := net.UDPAddr{IP: dst, Port: fw.egressPort}

	for i := 0; i < k; i++ {
		start := i * dataSize
		end := start + dataSize
		if end > len(payload) {
			end = len(payload)
		}
		fragData := payload[start:end]

		hashKey, seqNum := fw.nextSeq(ip, ctrlIdx, zeroSub)
		if egr == nil {
			continue
		}

		bufPtr := egr.PoolGet()
		buf := *bufPtr
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
			egr.pool.Put(bufPtr)
			continue
		}

		// Write BlockMsgType into the Reserved byte (offset 7) so the
		// reassembler can reconstruct the full V4 header.
		buf[7] = raw[7]

		egr.EnqueueControlPooled(buf[:n], addr, "block_control", workerID, bufPtr)
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
// flow, optionally fragments large payloads via BRC-130, and enqueues for
// the GroupSubtreeDataAnnounce multicast group.
//
// raw must remain valid until egr.Flush returns. egr may be nil for tests.
func (fw *Forwarder) ProcessSubtreeData(egr *Egress, raw []byte, src net.Addr, workerID int) {
	sf, err := frame.DecodeSubtreeData(raw)
	if err != nil {
		fw.log.Debug("subtree data frame decode error", "err", err, "len", len(raw))
		if fw.rec != nil && egr != nil && len(egr.targets) > 0 {
			fw.rec.PacketDropped(egr.targets[0].Iface.Name, workerID, "decode_error")
		}
		return
	}

	// Ingress TxID dedup gate for BRC-132 subtree data frames. The SubtreeID
	// is the stable per-payload identifier (parallel role to TxID for V2/V6
	// or ContentID for V4).
	{
		ifaceName := ""
		if egr != nil && len(egr.targets) > 0 {
			ifaceName = egr.targets[0].Iface.Name
		}
		if !fw.claimIngress(sf.SubtreeID, "brc132", ifaceName, workerID) {
			return
		}
	}

	if src != nil {
		ip := addrToIPv6(src)
		ctrlIdx := uint32(shard.GroupSubtreeDataAnnounce)

		// BRC-130 fragmentation path for large subtree data payloads.
		if fw.fragDataSize > 0 && len(sf.Payload) > fw.fragDataSize {
			fw.fragmentSubtreeData(egr, raw, sf, ip, ctrlIdx, workerID)
			return
		}

		// Stamp HashKey/SeqNum in-place; HashKey is stamped even when SeqNum
		// is pre-set so chain RL and cache keys are deterministic.
		stampInPlace(raw, ip, ctrlIdx, sf.SubtreeID, fw)
	}

	if egr == nil {
		return
	}

	dst := shard.GroupAddr(fw.mcPrefix, fw.mcGroupID, shard.GroupSubtreeDataAnnounce)
	addr := net.UDPAddr{IP: dst, Port: fw.egressPort}
	egr.EnqueueControl(raw, addr, "subtree_data", workerID)

	if fw.debug {
		fw.log.Debug("subtree data forwarded",
			"msg_type", sf.MsgType,
			"subtree_id", fmt.Sprintf("%x", sf.SubtreeID[:8]),
			"dst", addr,
		)
	}
}

// fragmentSubtreeData splits a large BRC-132 subtree data payload into BRC-130
// fragments and enqueues each for the GroupSubtreeDataAnnounce control group.
// Each fragment receives OrigFrameVer=0x05 so that reassembly routes the
// completed payload to processSubtreeDataFrame on the listener.
// MsgType is preserved in byte 7 of each fragment datagram.
// Fragment buffers come from the Egress pool.
func (fw *Forwarder) fragmentSubtreeData(egr *Egress, raw []byte, sf *frame.SubtreeDataFrame, ip [16]byte, ctrlIdx uint32, workerID int) {
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
	dst := shard.GroupAddr(fw.mcPrefix, fw.mcGroupID, shard.GroupSubtreeDataAnnounce)
	addr := net.UDPAddr{IP: dst, Port: fw.egressPort}

	for i := 0; i < k; i++ {
		start := i * dataSize
		end := start + dataSize
		if end > len(payload) {
			end = len(payload)
		}
		fragData := payload[start:end]

		hashKey, seqNum := fw.nextSeq(ip, ctrlIdx, sf.SubtreeID)
		if egr == nil {
			continue
		}

		bufPtr := egr.PoolGet()
		buf := *bufPtr
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
			egr.pool.Put(bufPtr)
			continue
		}

		// Write MsgType into byte 7 so the reassembler can reconstruct
		// the full V5 header (same pattern as fragmentBlock / BRC-131).
		buf[7] = raw[7]

		egr.EnqueueControlPooled(buf[:n], addr, "subtree_data", workerID, bufPtr)
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

// ForwardControl enqueues a raw BRC-127 control datagram (e.g.
// SubtreeGroupAnnounce) for the given network-service multicast group index. The
// destination address is derived using [shard.GroupAddr] with the engine's
// configured scope prefix and IANA group-id. Unlike [Process], no sequence
// stamping or frame decoding is performed. raw must remain valid until
// egr.Flush returns.
func (fw *Forwarder) ForwardControl(egr *Egress, raw []byte, idx shard.GroupIdx, port int) {
	if egr == nil {
		return
	}
	dst := shard.GroupAddr(fw.mcPrefix, fw.mcGroupID, idx)
	addr := net.UDPAddr{IP: dst, Port: port}
	egr.EnqueueControl(raw, addr, idx.String(), 0)
	if fw.debug {
		fw.log.Debug("control forwarded",
			"group", idx.String(),
			"dst", addr,
		)
	}
}

// stampInPlace stamps the HashKey (raw[40:48]) and SeqNum (raw[48:56]) header
// fields in-place if they are zero. HashKey is always re-derivable from
// (senderIP, groupIdx, subtreeID), so we stamp it whenever it is zero,
// independent of whether SeqNum was pre-stamped by the sender. SeqNum is only
// stamped (and the flow counter advanced) when it is zero — pre-stamped
// SeqNums are preserved verbatim (used by gap injection in subtx-gen).
func stampInPlace(raw []byte, ip [16]byte, groupIdx uint32, subtreeID [32]byte, fw *Forwarder) {
	preSeq := binary.BigEndian.Uint64(raw[48:56])
	preHash := binary.BigEndian.Uint64(raw[40:48])
	if preSeq == 0 {
		hashKey, seqNum := fw.nextSeq(ip, groupIdx, subtreeID)
		binary.BigEndian.PutUint64(raw[40:48], hashKey)
		binary.BigEndian.PutUint64(raw[48:56], seqNum)
		return
	}
	if preHash == 0 || fw.stampSource {
		// SeqNum was pre-stamped by the sender (e.g. gap-injection mode), so the
		// nextSeq counter is not advanced here. Stamp the HashKey from the
		// observed source whenever it was left zero OR the proxy is configured to
		// authoritatively own the source identity (stampSource) — overwriting any
		// sender-supplied HashKey so own-traffic exclusion keys on the real
		// ingress source. (When behind a source-rewriting LB, stampSource=false
		// preserves the sender's HashKey.)
		binary.BigEndian.PutUint64(raw[40:48], seqhash.Hash(ip, groupIdx, subtreeID))
	}
}

// nextSeq returns (hashKey, seqNum) for the given (sender IP, group, subtree) flow.
// hashKey is stable (same for every frame in the flow); seqNum is monotonically
// incremented per frame.
//
// The flow's *flowState is resolved via a striped map keyed by a hash of the
// sender IP, so concurrent workers handling distinct senders almost never
// contend on the same stripe mutex. Once the entry exists the seqNum
// increment is lock-free via atomic.AddUint64.
func (fw *Forwarder) nextSeq(ip [16]byte, groupIdx uint32, subtreeID [32]byte) (hashKey, seqNum uint64) {
	key := chainKey{ip: ip, grp: groupIdx, sub: subtreeID}
	stripe := &fw.chains[stripeIndex(ip)]
	stripe.mu.Lock()
	st, ok := stripe.m[key]
	if !ok {
		st = &flowState{}
		stripe.m[key] = st
	}
	stripe.mu.Unlock()
	hashKey = seqhash.Hash(ip, groupIdx, subtreeID)
	seqNum = st.counter.Add(1)
	return hashKey, seqNum
}

// stripeIndex maps a sender IP to one of chainStripes buckets via FNV-1a
// over the 16 IP bytes. The mask works because chainStripes is a power of
// two.
func stripeIndex(ip [16]byte) uint8 {
	h := fnv.New32a()
	_, _ = h.Write(ip[:])
	return uint8(h.Sum32() & (chainStripes - 1))
}

// openEgressSocket opens a UDP6 socket with IPV6_MULTICAST_IF set to iface
// and IPV6_MULTICAST_LOOP set to loopback (1 for debug, 0 otherwise).
//
// When bindSource is non-nil the socket is bound to that IPv6 (instead of
// the wildcard "::"), so the kernel emits multicast egress with that
// specific source IPv6. Required when source-mode=ssm so SSM receivers
// can pre-declare this proxy in their (S,G) join calls.
func openEgressSocket(iface *net.Interface, loopback int, bindSource net.IP, hopLimit int) (*net.UDPConn, error) {
	listenAddr := "[::]:0"
	if bindSource != nil {
		listenAddr = "[" + bindSource.String() + "]:0"
	}
	conn, err := net.ListenPacket("udp6", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", listenAddr, err)
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
		if hopLimit > 0 {
			if err := unix.SetsockoptInt(int(fd), unix.IPPROTO_IPV6, unix.IPV6_MULTICAST_HOPS, hopLimit); err != nil {
				setsockoptErr = fmt.Errorf("IPV6_MULTICAST_HOPS: %w", err)
				return
			}
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
