// Package config loads and validates runtime configuration for
// shard-proxy. Parameters are accepted from CLI flags first;
// environment variables serve as fallbacks; hard-coded defaults apply when
// neither is present.
//
// The authoritative flag / environment-variable reference (every flag, its
// env var, default, and description) is docs/configuration.md; it is not
// duplicated here.
package config

import (
	"flag"
	"fmt"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/lightwebinc/shard-common/shard"
)

// Scopes maps a human-readable scope name to the two-byte big-endian IPv6
// multicast prefix. See RFC 4291 §2.7.
//
//	"link"   → FF02::/16  link-local   (does not cross routers)
//	"site"   → FF05::/16  site-local   (recommended default for closed fabrics)
//	"org"    → FF08::/16  org-local
//	"global" → FF0E::/16  global       (routable across BGP domains)
var Scopes = map[string]uint16{
	"link":   0xFF02,
	"site":   0xFF05,
	"org":    0xFF08,
	"global": 0xFF0E,
}

// Config holds all runtime parameters for the proxy. Fields are read-only
// after [Load] returns; treat the value as immutable.
type Config struct {
	// Network
	ListenAddr    string // e.g. "[::]"
	UDPListenPort int    // UDP port to receive BSV BRC-124/BRC-128 transaction frames
	TCPListenPort int    // TCP ingress port; 0 = disabled

	// Push-frame ingest (replaces the deprecated miner multicast port).
	// Blocks and subtrees enter as header-stripped BRC-144 / BRC-143 push
	// frames on dedicated single-class TCP ports, reframed to the fabric
	// internally (subtree → BRC-132, block → BRC-131 carrying the BRC-144 body
	// verbatim). These ports are privileged and MUST be bound tunnel-side /
	// firewalled to miner-tier peers — only the transaction port (8725) is
	// public. Standard numbers: 8726 subtree, 8727 block. 0 = disabled.
	SubtreeListenPort int
	BlockListenPort   int

	// BRC-148 BEEF object plane. BEEF is an OPEN class: submission records
	// (0xBEEF-tagged envelopes) and framed FrameVer 0x09 input are accepted
	// on the public transaction port regardless of these knobs.
	// BEEFListenPort optionally binds a dedicated single-class lane
	// (standard 8728) for flow separation / load balancing — never
	// admission. BEEFShardBits is the plane's shard-bit width (band
	// 0x1000 + 2^bits groups); BEEFMaxObjectBytes bounds one accepted
	// object (the spec's ingress MUST-bound).
	BEEFListenPort     int
	BEEFShardBits      uint
	BEEFMaxObjectBytes int

	EgressIfaces   []string // NIC names for multicast egress, e.g. ["eth0", "eth1"]
	EgressPort     int      // Destination UDP port written into outgoing multicast datagrams
	EgressHopLimit int      // IPV6_MULTICAST_HOPS for egress; 1 = single L2 segment, raise for routed/tunneled mesh fabrics
	EgressLoop     bool     // IPV6_MULTICAST_LOOP for egress; required on collapsed/mesh router nodes so locally-originated multicast is MFC-forwarded to tunnel/consumer OIFs

	// Proof-of-work gate for BRC-131 block announces. Permissionless: validates
	// the in-frame header carries real work rather than authorizing the emitter.
	// RequireBlockPoW is ON by default. MinPoWBits is the difficulty floor in
	// Bitcoin compact (nBits) form; 0 = header self-consistency only (weak).
	RequireBlockPoW bool
	MinPoWBits      uint32

	// RequireEF makes the ingress EF-native: transaction submissions must be
	// BRC-30 Extended Format; raw BRC-12/BRC-124 submissions are rejected. Off by
	// default (accepts raw + EF). Relayed (already-stamped) frames are unaffected.
	RequireEF bool

	// AllowStampedIngress admits framed BRC-124/BRC-128 input that already
	// carries a SeqNum (a frame another proxy stamped and emitted). Off by
	// default — an ingress proxy accepts submissions, not relay. Enable on a
	// spine collect lane or a relay hop. See
	// forwarder.Forwarder.SetAllowStampedIngress.
	AllowStampedIngress bool

	// VerifyPayloadHash gates framed BRC-124/BRC-128 input on the canonical
	// TxID of its payload, dropping mismatches before the ingress dedup claim
	// and before group derivation. Off by default; costs one SHA256d per
	// framed transaction. Bare submissions are unaffected (the proxy derives
	// their TxID itself). See forwarder.Forwarder.SetVerifyPayloadHash.
	VerifyPayloadHash bool

	// Sharding
	ShardBits   uint   // Number of txid prefix bits used as the group key (1–12)
	NumGroups   uint32 // Derived: 1 << ShardBits — total distinct multicast groups
	MCScope     string // "site" | "global" — see shard.Scope. Legacy values "link"/"org" are still accepted in ASM mode.
	MCPrefix    uint16 // Derived from (SourceMode, MCScope) — upper 16 bits of the IPv6 group address
	MCGroupID   uint16 // IANA group-id occupying bytes 12–13 (default 0x000B)
	SourceMode  string // "asm" (default) | "ssm" — selects ASM (FF0x) vs SSM (FF3x) addressing per RFC 4607
	BindSource  string // Optional IPv6 literal; when SSM is enabled each proxy replica MUST bind a distinct source IPv6 so receivers can pre-declare it in (S,G) joins
	StampSource bool   // When true (default), the proxy AUTHORITATIVELY stamps the BRC HashKey from the OBSERVED packet source IP — overriding any value the sender supplied — so the per-flow identity is the real ingress source. This is what makes own-traffic exclusion per-consumer in a collapsed/direct-ingress edge (the proxy sees each consumer's distinct source). Set false ONLY when the proxy sits behind a load balancer / proxy that rewrites the source (every consumer would then appear as the LB's address) — there the upstream-supplied HashKey is trusted instead.

	// Runtime
	NumWorkers   int           // Worker goroutine count; defaults to runtime.NumCPU()
	Debug        bool          // Enables per-packet debug logging and multicast loopback
	DrainTimeout time.Duration // Pre-drain delay before closing ingress sockets; 0 = disabled

	// Fragmentation (BRC-130)
	// FragMTU is the path MTU used to derive the fragment data size per
	// datagram (fragDataSize = FragMTU - 40 - 8 - 104). Frames whose payload
	// exceeds fragDataSize are split into BRC-130 fragment datagrams.
	//
	// ON by default at the Ethernet baseline. Disabling it does NOT make large
	// frames "unfragmented" — it makes them UNDELIVERABLE: an unfragmented
	// BRC-124 frame occupies 40 (IPv6) + 8 (UDP) + 92 (header) + payload, so a
	// payload over MTU-140 becomes a single oversize datagram that can only
	// cross the fabric via IPv6 IP-layer fragmentation, which is exactly what
	// BRC-130 exists to avoid. At MTU 1500 that cliff is 1360 payload bytes,
	// below every subtree and block frame, so those lanes go 100% dark.
	//
	// Set it to the smallest MTU on the egress path, NOT the local NIC's: on a
	// tunnelled fabric that is the ip6gre inner MTU (underlay - 100), not 1500.
	// 0 disables fragmentation (frames forwarded verbatim regardless of size).
	FragMTU int

	// Coalescing (BRC-142 bundle frame)
	// Opt-in: pack many small same-(group, subtree) transactions arriving in one
	// receive batch into a single bundle datagram to cut egress pps. Off by default.
	Coalesce           bool
	CoalesceMaxBytes   int  // path MTU budget for the emitted bundle datagram, IPv6+UDP headers included (0 ⇒ 1500, the Ethernet MTU baseline)
	CoalesceMaxMembers int  // max members per bundle (0 ⇒ MTU-bound)
	CoalesceCarryTxid  bool // include per-member TxID on the wire (dedup/accounting) vs recompute

	// RecvBatch is the number of datagrams a worker requests per recvmmsg
	// syscall (and matching default queue capacity for sendmmsg-style
	// egress flushes). Larger values amortise the syscall cost across
	// more packets at the cost of slightly higher per-packet latency at
	// low ingress rates. Minimum 1 (per-packet, equivalent to the legacy
	// path). Default 32.
	RecvBatch int
	// RetryTee is the co-located retry-endpoint cache-ingest address ("" = off).
	// A node that originates frames cannot (S,G)-join its own source, so its
	// co-located cache holds nothing of its own emissions without this.
	RetryTee string

	// RecvBufBytes is the requested SO_RCVBUF size per worker socket. The
	// kernel clamps to net.core.rmem_max. 0 = use the worker package default.
	RecvBufBytes int

	// PprofEnabled mounts net/http/pprof at /debug/pprof/* on the metrics
	// server. Off by default; turn on only for profiling sessions.
	PprofEnabled bool

	// Observability
	MetricsAddr   string        // HTTP bind address for /metrics, /healthz, /readyz
	InstanceID    string        // OTel service.instance.id for federation; defaults to hostname
	OTLPEndpoint  string        // gRPC OTLP endpoint (empty = disabled)
	OTLPInterval  time.Duration // OTLP push interval
	LogFormat     string        // "text" (default) | "json"
	LogLevel      string        // debug|info|warn|error
	TraceSampling float64       // 0..1 head sampling ratio; 0 disables tracing

	// TxID ingress dedup (proxy-side)
	//
	// The proxy may consult a two-tier (local LRU → Redis SETNX) TxID claim
	// store before stamping and multicasting a frame. The first proxy to
	// claim a TxID in Redis multicasts it; siblings drop. Listeners may
	// optionally mark the same Redis namespace on receive to inform the
	// proxy when a TxID arrived via a path the proxy itself did not see.
	//
	// Tier-2 backend is modular (shard-common/cache): redis (Redis/Valkey/
	// Dragonfly), aerospike, memory, or none. TxidDedupBackend empty infers
	// "redis" when TxidDedupRedisAddr is set, else "none" (tier-1 LRU only).
	// TxidDedupEnabled=false → dedup feature disabled entirely (overrides
	// TxidDedupLocalCap). Off-path is one nil check per packet.
	TxidDedupEnabled       bool
	TxidDedupBackend       string
	TxidDedupRedisAddr     string
	TxidDedupAeroHosts     []string
	TxidDedupAeroNamespace string
	TxidDedupAeroSet       string
	TxidDedupPrefix        string
	TxidDedupTTL           time.Duration
	TxidDedupLocalCap      int

	// Auto-shard-config (BRC-139 manifest consumer). All fields are opt-in.
	// When AutoConfigEnabled is false, the proxy does not join the beacon
	// group at all and the other fields are ignored. When true, the proxy
	// opens a beacon socket (posture-aware) and runs the manifest evaluator.
	AutoConfigEnabled     bool
	AutoConfigBootstrap   string        // "optional" (default) | "required"
	AutoConfigPilotQuorum int           // default 2
	AutoConfigHysteresis  time.Duration // 0 ⇒ 2 × AnnounceInterval
	AutoConfigBeaconScope string        // "" ⇒ inherit MCScope
	// ControlGroupCompat selects which prefix the control-plane group
	// (BRC-129 index 0xFFFD) is derived from during the FF0x→FF3x
	// transition: asm-only | both | derived. See controlgroup.go. The
	// proxy is a receiver on that group, so the default is "both".
	ControlGroupCompat       string
	AutoConfigBeaconPort     int           // default 9001 (BRC-139 manifest port)
	AutoConfigLiveResharding bool          // opt-in bridging mode (default: restart-on-adopt)
	AutoConfigBridgingWindow time.Duration // 0 ⇒ honour pilot TransitionEpoch
}

// Load parses flags and environment variables, validates all values, and
// returns a populated [Config]. It calls [flag.Parse] internally; callers
// must not call flag.Parse separately.
//
// Returns an error if any value is out of range or the specified network
// interface does not exist on this host.
func Load() (*Config, error) {
	c := &Config{}

	flag.StringVar(&c.ListenAddr, "listen", envStr("LISTEN_ADDR", "[::]"),
		"ingress bind address (without port)")
	flag.IntVar(&c.UDPListenPort, "udp-listen-port", envInt("UDP_LISTEN_PORT", 8725),
		"UDP listen port for incoming BSV BRC-124/BRC-128 transaction frames")
	flag.IntVar(&c.TCPListenPort, "tcp-listen-port", envInt("TCP_LISTEN_PORT", 0),
		"TCP ingress port for reliable frame delivery (0 = disabled)")
	flag.IntVar(&c.SubtreeListenPort, "subtree-listen-port", envInt("SUBTREE_LISTEN_PORT", 0),
		"TCP port accepting BRC-143 subtree push frames (privileged; bind tunnel-side; standard 8726; 0 = disabled)")
	flag.IntVar(&c.BlockListenPort, "block-listen-port", envInt("BLOCK_LISTEN_PORT", 0),
		"TCP port accepting BRC-144 block push frames (privileged; bind tunnel-side; standard 8727; 0 = disabled)")
	flag.IntVar(&c.BEEFListenPort, "beef-listen-port", envInt("BEEF_LISTEN_PORT", 0),
		"optional dedicated TCP lane for BRC-148 BEEF submission records (flow separation only — BEEF also rides the tx port; standard 8728; 0 = disabled)")
	beefBits := flag.Uint("beef-shard-bits", uint(envInt("BEEF_SHARD_BITS", 0)),
		"BRC-148 BEEF plane shard-bit width (band 0x1000 + 2^bits groups; 0-12, 0 = single group)")
	flag.IntVar(&c.BEEFMaxObjectBytes, "beef-max-object-bytes", envInt("BEEF_MAX_OBJECT_BYTES", 1<<20),
		"maximum accepted BEEF object size in bytes (BRC-149 ingress bound)")
	flag.BoolVar(&c.RequireBlockPoW, "require-block-pow", envBool("REQUIRE_BLOCK_POW", true),
		"gate BRC-131 block announces on a cheap stateless proof-of-work check of the in-frame header (permissionless: validates work, not identity)")
	flag.BoolVar(&c.RequireEF, "require-ef", envBool("REQUIRE_EF", false),
		"EF-native ingress: reject raw BRC-12/BRC-124 transaction submissions; only Extended Format (BRC-30) is admitted (relayed frames unaffected)")
	flag.BoolVar(&c.AllowStampedIngress, "allow-stamped-ingress", envBool("ALLOW_STAMPED_INGRESS", false),
		"admit framed BRC-124/BRC-128 input that already carries a SeqNum (another proxy's output). Off by default: an ingress proxy accepts submissions, not relay. Enable on a spine collect lane or relay hop")
	flag.BoolVar(&c.VerifyPayloadHash, "verify-payload-hash", envBool("VERIFY_PAYLOAD_HASH", false),
		"verify the canonical TxID of framed BRC-124/BRC-128 input against its payload and drop mismatches before ingress dedup (bare submissions unaffected; costs one SHA256d per framed tx)")
	minPoWBits := flag.String("min-pow-bits", envStr("MIN_POW_BITS", "0"),
		"difficulty floor for -require-block-pow in Bitcoin compact nBits form (e.g. 0x1d00ffff); 0 = header self-consistency only")
	ifaceFlag := flag.String("iface", envStr("MULTICAST_IF", "eth0"),
		"comma-separated NIC names for multicast egress (e.g. eth0,eth1)")
	flag.IntVar(&c.EgressPort, "egress-port", envInt("EGRESS_PORT", 9001),
		"destination UDP port written into outgoing multicast datagrams")
	flag.IntVar(&c.EgressHopLimit, "egress-hoplimit", envInt("EGRESS_HOPLIMIT", 1),
		"IPv6 multicast hop limit for egress (IPV6_MULTICAST_HOPS); raise above 1 for routed/tunneled mesh fabrics")
	flag.BoolVar(&c.EgressLoop, "egress-loop", envBool("EGRESS_MULTICAST_LOOP", false),
		"enable IPV6_MULTICAST_LOOP on egress; required on collapsed/mesh router nodes so locally-originated multicast is forwarded by the kernel MFC to tunnel/consumer OIFs")
	flag.IntVar(&c.NumWorkers, "workers", envInt("NUM_WORKERS", runtime.NumCPU()),
		"number of worker goroutines (0 = runtime.NumCPU)")
	flag.StringVar(&c.MCScope, "scope", envStr("MC_SCOPE", "site"),
		"multicast scope: link | site | org | global (site|global also accepted in SSM mode)")
	flag.StringVar(&c.SourceMode, "source-mode", envStr("SOURCE_MODE", "asm"),
		"multicast addressing model: asm | ssm (SSM uses FF3x::/32 per RFC 4607; requires PIM-SSM in fabric)")
	flag.StringVar(&c.BindSource, "bind-source", envStr("BIND_SOURCE", ""),
		"optional IPv6 literal to bind for multicast egress (required and MUST be unique per replica when source-mode=ssm)")
	flag.BoolVar(&c.StampSource, "stamp-source", envBool("STAMP_SOURCE", true),
		"authoritatively stamp the BRC HashKey from the observed packet source IP (default true; makes own-traffic exclusion per-consumer at a direct edge). Set false only behind a source-rewriting load balancer.")
	groupIDFlag := flag.String("mc-group-id", envStr("MC_GROUP_ID", "0x000B"),
		"IANA group-id (bytes 12–13 of the IPv6 multicast address); default 0x000B (IANA Bitcoin)")
	flag.BoolVar(&c.Debug, "debug", envBool("DEBUG", false),
		"enable per-packet debug logging and multicast loopback (single-host testing); deprecated alias for -log-level=debug")
	flag.StringVar(&c.LogFormat, "log-format", envStr("LOG_FORMAT", "text"),
		"log output format: text (default, stderr) | json (stdout, for fleet aggregation)")
	flag.StringVar(&c.LogLevel, "log-level", envStr("LOG_LEVEL", "info"),
		"log level: debug|info|warn|error (overridden to debug when -debug is set)")
	flag.Float64Var(&c.TraceSampling, "trace-sampling", envFloat("TRACE_SAMPLING", 0),
		"distributed-trace head sampling ratio 0..1 (0 = tracing off; exports via -otlp-endpoint)")
	flag.DurationVar(&c.DrainTimeout, "drain-timeout", envDuration("DRAIN_TIMEOUT", 0),
		"pre-drain delay before closing ingress sockets; /readyz returns 503 during this window (0 = disabled)")
	flag.IntVar(&c.FragMTU, "frag-mtu", envInt("FRAG_MTU", 1500),
		"path MTU for BRC-130 fragmentation; default 1500 (Ethernet baseline). Set to the SMALLEST MTU on the egress path — on a tunnelled fabric that is the ip6gre inner MTU (underlay-100), not the local NIC. 0 = disabled, which strands every payload above MTU-140 (1360 at 1500) as an undeliverable oversize datagram")
	flag.BoolVar(&c.Coalesce, "coalesce", envBool("COALESCE", false),
		"opt-in BRC-142 frame coalescing (pack many small same-(group,subtree) tx per datagram to cut pps)")
	flag.IntVar(&c.CoalesceMaxBytes, "coalesce-max-bytes", envInt("COALESCE_MAX_BYTES", 1500),
		"path MTU budget for a coalesced BRC-142 bundle, in bytes ON THE WIRE (typical: 1500 Ethernet, 9000 jumbo). The 48-byte IPv6+UDP header is subtracted before packing, like -frag-mtu, so a bundle never exceeds this on the path")
	flag.IntVar(&c.CoalesceMaxMembers, "coalesce-max-members", envInt("COALESCE_MAX_MEMBERS", 0),
		"max member transactions per bundle (0 = MTU-bound)")
	flag.BoolVar(&c.CoalesceCarryTxid, "coalesce-carry-txid", envBool("COALESCE_CARRY_TXID", false),
		"carry per-member TxID in the bundle (dedup/accounting) instead of recomputing on receipt")
	flag.IntVar(&c.RecvBatch, "recv-batch", envInt("BSP_RECV_BATCH", 32),
		"datagrams per recvmmsg syscall (1 = per-packet legacy path; 32 default)")
	flag.StringVar(&c.RetryTee, "retry-tee", envStr("BSP_RETRY_TEE", ""),
		"BRC-126: mirror each egressed DATA datagram to a co-located retry endpoint's cache-ingest address (e.g. \"[::1]:9001\"). Needed on a node that ORIGINATES frames and hosts its own retry cache (the collapsed edge): it must never (S,G)-join its own source, so its cache otherwise holds nothing of its own emissions and answers MISS for them. Copies are batched into ONE sendmmsg per egress batch, so the cost is per-batch, not per-frame. Empty = disabled")
	flag.IntVar(&c.RecvBufBytes, "recv-buf-bytes", envInt("BSP_RECV_BUF_BYTES", 0),
		"per-worker SO_RCVBUF in bytes (0 = worker default; capped by net.core.rmem_max)")
	flag.BoolVar(&c.PprofEnabled, "pprof", envBool("BSP_PPROF", false),
		"expose net/http/pprof at /debug/pprof/* on the metrics server (profiling only)")

	flag.BoolVar(&c.TxidDedupEnabled, "ingress-dedup", envBool("INGRESS_DEDUP", true),
		"enable ingress TxID dedup (false = bypass entirely; measured ~17% CPU at high pps with no-Redis local-only LRU)")
	flag.StringVar(&c.TxidDedupBackend, "txid-dedup-backend", envStr("TXID_DEDUP_BACKEND", ""),
		"tier-2 dedup backend: redis|aerospike|memory|none (empty = infer redis when -txid-dedup-redis-addr set, else none)")
	flag.StringVar(&c.TxidDedupRedisAddr, "txid-dedup-redis-addr", envStr("TXID_DEDUP_REDIS_ADDR", ""),
		"Redis-protocol address (Redis/Valkey/Dragonfly) for ingress TxID dedup (empty = local-only tier-1 LRU)")
	aeroHosts := flag.String("txid-dedup-aerospike-hosts", envStr("TXID_DEDUP_AEROSPIKE_HOSTS", ""),
		"Aerospike seed nodes host:port (comma-separated); required when -txid-dedup-backend=aerospike")
	flag.StringVar(&c.TxidDedupAeroNamespace, "txid-dedup-aerospike-namespace", envStr("TXID_DEDUP_AEROSPIKE_NAMESPACE", "cache"),
		"Aerospike namespace for ingress TxID dedup")
	flag.StringVar(&c.TxidDedupAeroSet, "txid-dedup-aerospike-set", envStr("TXID_DEDUP_AEROSPIKE_SET", "bsp"),
		"Aerospike set for ingress TxID dedup")
	flag.StringVar(&c.TxidDedupPrefix, "txid-dedup-prefix", envStr("TXID_DEDUP_PREFIX", "bsp:tx:"),
		"Redis key prefix for ingress TxID dedup entries (must match listener's -ingress-set-prefix at the same site)")
	flag.DurationVar(&c.TxidDedupTTL, "txid-dedup-ttl", envDuration("TXID_DEDUP_TTL", 10*time.Minute),
		"TTL for ingress TxID dedup Redis entries; 1m–30m typical")
	flag.IntVar(&c.TxidDedupLocalCap, "txid-dedup-local-cap", envInt("TXID_DEDUP_LOCAL_CAP", 1<<20),
		"tier-1 local TxID set capacity (0 = disable proxy ingress dedup entirely)")

	flag.StringVar(&c.MetricsAddr, "metrics-addr", envStr("METRICS_ADDR", ":9100"),
		"HTTP bind address for /metrics, /healthz, /readyz")
	flag.StringVar(&c.InstanceID, "instance", envStr("INSTANCE_ID", ""),
		"OTel service.instance.id for federation (default: hostname)")
	flag.StringVar(&c.OTLPEndpoint, "otlp-endpoint", envStr("OTLP_ENDPOINT", ""),
		"OTLP gRPC endpoint for metric push (empty = disabled)")

	otlpInterval := flag.Duration("otlp-interval", envDuration("OTLP_INTERVAL", 30*time.Second),
		"OTLP push interval")

	bits := flag.Uint("shard-bits", uint(envInt("SHARD_BITS", 2)),
		"txid prefix bit width used as the shard key (1–12)")

	flag.BoolVar(&c.AutoConfigEnabled, "manifest-consumer-enabled", envBool("MANIFEST_CONSUMER_ENABLED", false),
		"opt-in BRC-139 manifest consumer for auto-shard-config (off by default)")
	flag.StringVar(&c.AutoConfigBootstrap, "manifest-bootstrap", envStr("MANIFEST_BOOTSTRAP", "optional"),
		"manifest bootstrap behavior: 'optional' (default) | 'required' (refuse data-plane bind until quorum)")
	flag.IntVar(&c.AutoConfigPilotQuorum, "pilot-quorum", envInt("PILOT_QUORUM", 2),
		"min distinct authoritative announcers required for adoption; 1 allowed but logs a warning")
	flag.DurationVar(&c.AutoConfigHysteresis, "pilot-hysteresis", envDuration("PILOT_HYSTERESIS", 0),
		"hysteresis window before adoption; 0 ⇒ 2 × AnnounceInterval of the candidate manifest")
	flag.StringVar(&c.ControlGroupCompat, "control-group-compat", envStr("CONTROL_GROUP_COMPAT", ControlGroupBoth),
		"control-plane group prefix during the FF0x->FF3x transition: "+
			"asm-only (pre-fix wire) | both (join legacy and derived; default, receiver-safe) | "+
			"derived (BRC-126/BRC-129 conformant). Temporary; see docs/configuration.md")
	flag.StringVar(&c.AutoConfigBeaconScope, "manifest-beacon-scope", envStr("MANIFEST_BEACON_SCOPE", ""),
		"multicast scope for the beacon-group join; empty ⇒ inherit -scope")
	flag.IntVar(&c.AutoConfigBeaconPort, "manifest-beacon-port", envInt("MANIFEST_BEACON_PORT", 9001),
		"UDP port on which the proxy joins the beacon group to receive BRC-139 manifests")
	flag.BoolVar(&c.AutoConfigLiveResharding, "live-resharding", envBool("LIVE_RESHARDING", false),
		"opt-in BRC-139 live-resharding bridging mode (default: restart on ShardBits adoption)")
	flag.DurationVar(&c.AutoConfigBridgingWindow, "bridging-window", envDuration("BRIDGING_WINDOW", 0),
		"local floor on bridging duration; 0 ⇒ honour pilot TransitionEpoch verbatim")

	flag.Parse()

	// Validate shard bit width. BRC-129 zones the 16-bit shard space: shard
	// group indices are 0x0000–0x0FFF, so bits is bounded at 12.
	if *bits < 1 || *bits > 12 {
		return nil, fmt.Errorf("shard-bits must be in [1, 12], got %d", *bits)
	}
	c.ShardBits = *bits
	c.NumGroups = 1 << c.ShardBits
	c.OTLPInterval = *otlpInterval

	// BRC-148 BEEF plane width. v1 caps at 12 (SlotSpan 1, band
	// 0x1000–0x1FFF); wide planes are a spec-supported follow-up.
	if *beefBits > 12 {
		return nil, fmt.Errorf("beef-shard-bits must be in [0, 12], got %d", *beefBits)
	}
	c.BEEFShardBits = *beefBits
	if c.BEEFMaxObjectBytes < 1 {
		return nil, fmt.Errorf("beef-max-object-bytes must be positive, got %d", c.BEEFMaxObjectBytes)
	}

	// The TCP-family ports (reliable ingress + the two push-ingest lanes) must
	// be mutually distinct — they all bind tcp6. UDP ingress is a separate
	// transport namespace and may share a number.
	tcpPorts := map[int]string{}
	for _, p := range []struct {
		v    int
		name string
	}{
		{c.TCPListenPort, "tcp-listen-port"},
		{c.SubtreeListenPort, "subtree-listen-port"},
		{c.BlockListenPort, "block-listen-port"},
		{c.BEEFListenPort, "beef-listen-port"},
	} {
		if p.v == 0 {
			continue
		}
		if prev, ok := tcpPorts[p.v]; ok {
			return nil, fmt.Errorf("%s (%d) collides with %s", p.name, p.v, prev)
		}
		tcpPorts[p.v] = p.name
	}

	// Parse the PoW difficulty floor (Bitcoin compact nBits): hex (0x… / bare) or decimal.
	bitsStr := strings.TrimSpace(*minPoWBits)
	base := 10
	if low := strings.ToLower(bitsStr); strings.HasPrefix(low, "0x") {
		bitsStr = bitsStr[2:]
		base = 16
	}
	v, perr := strconv.ParseUint(bitsStr, base, 32)
	if perr != nil {
		return nil, fmt.Errorf("invalid -min-pow-bits %q: %w", *minPoWBits, perr)
	}
	c.MinPoWBits = uint32(v)

	// Resolve multicast scope + source-mode → upper-16-bit prefix.
	//
	// SSM uses the shared shard.Prefix() helper, which enforces RFC 8815
	// (no inter-domain ASM) and yields FF35 / FF3E for SSM. ASM at the
	// legacy "link"/"org" scopes is preserved via the Scopes map.
	switch strings.ToLower(c.SourceMode) {
	case "asm":
		c.SourceMode = "asm"
		prefix, ok := Scopes[c.MCScope]
		if !ok {
			return nil, fmt.Errorf("unknown scope %q; valid values: link, site, org, global", c.MCScope)
		}
		c.MCPrefix = prefix
	case "ssm":
		c.SourceMode = "ssm"
		scope, err := shard.ParseScope(c.MCScope)
		if err != nil {
			return nil, fmt.Errorf("source-mode=ssm requires -scope site|global: %w", err)
		}
		prefix, err := shard.Prefix(shard.SourceModeSSM, scope)
		if err != nil {
			return nil, err
		}
		c.MCPrefix = prefix
		if c.BindSource == "" {
			return nil, fmt.Errorf("source-mode=ssm requires -bind-source (distinct IPv6 per replica)")
		}
		ip := net.ParseIP(c.BindSource)
		if ip == nil || ip.To4() != nil {
			return nil, fmt.Errorf("invalid -bind-source %q: must be an IPv6 literal", c.BindSource)
		}
	default:
		return nil, fmt.Errorf("invalid source-mode %q (asm|ssm)", c.SourceMode)
	}

	// Parse IANA group-id (default 0x000B = IANA Bitcoin allocation).
	gid, err := parseGroupID(*groupIDFlag)
	if err != nil {
		return nil, fmt.Errorf("invalid -mc-group-id %q: %w", *groupIDFlag, err)
	}
	c.MCGroupID = gid

	// Default workers to NumCPU if the flag or env was set to zero.
	if c.NumWorkers <= 0 {
		c.NumWorkers = runtime.NumCPU()
	}

	// Clamp RecvBatch to a sane floor; 1 keeps the legacy per-packet
	// semantics intact for sanity comparisons.
	if c.RecvBatch < 1 {
		c.RecvBatch = 1
	}

	// Parse and validate egress interfaces.
	for _, name := range strings.Split(*ifaceFlag, ",") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if _, err := net.InterfaceByName(name); err != nil {
			return nil, fmt.Errorf("multicast interface %q not found: %w", name, err)
		}
		c.EgressIfaces = append(c.EgressIfaces, name)
	}
	if len(c.EgressIfaces) == 0 {
		return nil, fmt.Errorf("at least one egress interface must be specified via -iface")
	}

	// Parse Aerospike seed nodes for the dedup backend.
	for _, h := range strings.Split(*aeroHosts, ",") {
		if h = strings.TrimSpace(h); h != "" {
			c.TxidDedupAeroHosts = append(c.TxidDedupAeroHosts, h)
		}
	}

	// Infer the tier-2 backend when not set explicitly: redis if an address
	// was provided (back-compat), else none (tier-1 LRU only).
	if c.TxidDedupBackend == "" {
		if c.TxidDedupRedisAddr != "" {
			c.TxidDedupBackend = "redis"
		} else {
			c.TxidDedupBackend = "none"
		}
	}

	// Validate TxID dedup parameters when the feature is enabled.
	if c.TxidDedupLocalCap > 0 {
		if c.TxidDedupTTL <= 0 {
			return nil, fmt.Errorf("txid-dedup-ttl must be > 0 when dedup is enabled (got %s)", c.TxidDedupTTL)
		}
		if c.TxidDedupPrefix == "" {
			return nil, fmt.Errorf("txid-dedup-prefix must not be empty when dedup is enabled")
		}
		switch c.TxidDedupBackend {
		case "redis", "aerospike", "memory", "none":
		default:
			return nil, fmt.Errorf("txid-dedup-backend %q unknown; valid: redis, aerospike, memory, none", c.TxidDedupBackend)
		}
		if c.TxidDedupBackend == "redis" && c.TxidDedupRedisAddr == "" {
			return nil, fmt.Errorf("txid-dedup-redis-addr required when txid-dedup-backend=redis")
		}
		if c.TxidDedupBackend == "aerospike" && len(c.TxidDedupAeroHosts) == 0 {
			return nil, fmt.Errorf("txid-dedup-aerospike-hosts required when txid-dedup-backend=aerospike")
		}
	}

	// Auto-shard-config validation.
	switch c.AutoConfigBootstrap {
	case "optional", "required":
	default:
		return nil, fmt.Errorf("manifest-bootstrap %q unknown; valid: optional, required", c.AutoConfigBootstrap)
	}
	if c.AutoConfigPilotQuorum < 1 {
		return nil, fmt.Errorf("pilot-quorum must be >= 1, got %d", c.AutoConfigPilotQuorum)
	}
	if c.AutoConfigBeaconScope == "" {
		c.AutoConfigBeaconScope = c.MCScope
	}
	if _, ok := Scopes[c.AutoConfigBeaconScope]; !ok {
		return nil, fmt.Errorf("manifest-beacon-scope %q unknown; valid values: link, site, org, global", c.AutoConfigBeaconScope)
	}

	return c, nil
}

// envStr returns the value of environment variable key, or def if unset or empty.
func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// envInt returns the integer value of environment variable key, or def if
// the variable is unset, empty, or not parseable as a base-10 integer.
func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// envFloat returns the float value of environment variable key, or def if the
// variable is unset, empty, or not parseable as a float.
func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

// envBool returns the boolean value of environment variable key, or def if
// the variable is unset, empty, or not parseable as a bool.
func envBool(key string, def bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

// envDuration returns the time.Duration value of environment variable key,
// or def if the variable is unset, empty, or not parseable.
func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

// parseGroupID accepts either a hex literal (0x000B, 000B) or a decimal
// integer in the range [0, 0xFFFF].
func parseGroupID(s string) (uint16, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty value")
	}
	base := 10
	low := strings.ToLower(s)
	if strings.HasPrefix(low, "0x") {
		s = s[2:]
		base = 16
	} else if _, err := strconv.ParseUint(s, 10, 16); err != nil {
		// fall back to hex if not a valid decimal
		base = 16
	}
	n, err := strconv.ParseUint(s, base, 16)
	if err != nil {
		return 0, err
	}
	return uint16(n), nil
}
