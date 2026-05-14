// Package config loads and validates runtime configuration for
// bitcoin-shard-proxy. Parameters are accepted from CLI flags first;
// environment variables serve as fallbacks; hard-coded defaults apply when
// neither is present.
//
// # Environment variable mapping
//
//	Flag             Env var          Default       Description
//	-listen               LISTEN_ADDR           [::]      Ingress bind address
//	-udp-listen-port      UDP_LISTEN_PORT       9000      UDP listen port
//	-tcp-listen-port      TCP_LISTEN_PORT       0         TCP ingress port (0 = disabled)
//	-iface                MULTICAST_IF          eth0      Comma-separated NICs for multicast egress
//	-egress-port          EGRESS_PORT           9001      Destination port on groups
//	-shard-bits           SHARD_BITS            2         Key bit width (1–15)
//	-scope                MC_SCOPE              site      Multicast scope
//	-mc-group-id          MC_GROUP_ID           0x000B    IANA group-id (default Bitcoin = 0x000B)
//	-workers              NUM_WORKERS           NumCPU    Worker goroutine count
//	-debug                DEBUG                 false     Per-packet logging + loopback
//	-metrics-addr         METRICS_ADDR          :9100     HTTP bind for /metrics, /healthz, /readyz
//	-drain-timeout        DRAIN_TIMEOUT         0s        Pre-drain delay before closing sockets (0 = disabled)
//	-instance             INSTANCE_ID           hostname  OTel service.instance.id
//	-otlp-endpoint        OTLP_ENDPOINT         ""        OTLP gRPC endpoint (empty = disabled)
//	-otlp-interval        OTLP_INTERVAL         30s       OTLP push interval
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
	ListenAddr    string   // e.g. "[::]"
	UDPListenPort int      // UDP port to receive BSV BRC-124/BRC-128 transaction frames
	TCPListenPort int      // TCP ingress port; 0 = disabled
	EgressIfaces  []string // NIC names for multicast egress, e.g. ["eth0", "eth1"]
	EgressPort    int      // Destination UDP port written into outgoing multicast datagrams

	// Sharding
	ShardBits uint   // Number of txid prefix bits used as the group key (1–15)
	NumGroups uint32 // Derived: 1 << ShardBits — total distinct multicast groups
	MCScope   string // Human name; one of the keys in Scopes
	MCPrefix  uint16 // Derived from MCScope — upper 16 bits of the IPv6 group address
	MCGroupID uint16 // IANA group-id occupying bytes 12–13 (default 0x000B)

	// Runtime
	NumWorkers   int           // Worker goroutine count; defaults to runtime.NumCPU()
	Debug        bool          // Enables per-packet debug logging and multicast loopback
	DrainTimeout time.Duration // Pre-drain delay before closing ingress sockets; 0 = disabled

	// Observability
	MetricsAddr  string        // HTTP bind address for /metrics, /healthz, /readyz
	InstanceID   string        // OTel service.instance.id for federation; defaults to hostname
	OTLPEndpoint string        // gRPC OTLP endpoint (empty = disabled)
	OTLPInterval time.Duration // OTLP push interval
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
	flag.IntVar(&c.UDPListenPort, "udp-listen-port", envInt("UDP_LISTEN_PORT", 9000),
		"UDP listen port for incoming BSV BRC-124/BRC-128 transaction frames")
	flag.IntVar(&c.TCPListenPort, "tcp-listen-port", envInt("TCP_LISTEN_PORT", 0),
		"TCP ingress port for reliable frame delivery (0 = disabled)")
	ifaceFlag := flag.String("iface", envStr("MULTICAST_IF", "eth0"),
		"comma-separated NIC names for multicast egress (e.g. eth0,eth1)")
	flag.IntVar(&c.EgressPort, "egress-port", envInt("EGRESS_PORT", 9001),
		"destination UDP port written into outgoing multicast datagrams")
	flag.IntVar(&c.NumWorkers, "workers", envInt("NUM_WORKERS", runtime.NumCPU()),
		"number of worker goroutines (0 = runtime.NumCPU)")
	flag.StringVar(&c.MCScope, "scope", envStr("MC_SCOPE", "site"),
		"multicast scope: link | site | org | global")
	groupIDFlag := flag.String("mc-group-id", envStr("MC_GROUP_ID", "0x000B"),
		"IANA group-id (bytes 12–13 of the IPv6 multicast address); default 0x000B (IANA Bitcoin)")
	flag.BoolVar(&c.Debug, "debug", envBool("DEBUG", false),
		"enable per-packet debug logging and multicast loopback (single-host testing)")
	flag.DurationVar(&c.DrainTimeout, "drain-timeout", envDuration("DRAIN_TIMEOUT", 0),
		"pre-drain delay before closing ingress sockets; /readyz returns 503 during this window (0 = disabled)")
	flag.StringVar(&c.MetricsAddr, "metrics-addr", envStr("METRICS_ADDR", ":9100"),
		"HTTP bind address for /metrics, /healthz, /readyz")
	flag.StringVar(&c.InstanceID, "instance", envStr("INSTANCE_ID", ""),
		"OTel service.instance.id for federation (default: hostname)")
	flag.StringVar(&c.OTLPEndpoint, "otlp-endpoint", envStr("OTLP_ENDPOINT", ""),
		"OTLP gRPC endpoint for metric push (empty = disabled)")

	otlpInterval := flag.Duration("otlp-interval", envDuration("OTLP_INTERVAL", 30*time.Second),
		"OTLP push interval")

	bits := flag.Uint("shard-bits", uint(envInt("SHARD_BITS", 2)),
		"txid prefix bit width used as the shard key (1–15)")

	flag.Parse()

	// Validate shard bit width. Top of the 16-bit shard space is reserved for
	// control-plane groups (0xFFFC–0xFFFE), so practical bits is bounded at 15.
	if *bits < 1 || *bits > 15 {
		return nil, fmt.Errorf("shard-bits must be in [1, 15], got %d", *bits)
	}
	c.ShardBits = *bits
	c.NumGroups = 1 << c.ShardBits
	c.OTLPInterval = *otlpInterval

	// Resolve multicast scope.
	prefix, ok := Scopes[c.MCScope]
	if !ok {
		return nil, fmt.Errorf("unknown scope %q; valid values: link, site, org, global", c.MCScope)
	}
	c.MCPrefix = prefix

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
