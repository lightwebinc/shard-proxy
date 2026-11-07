// Command latency-sink joins one or more IPv6 multicast groups (ASM, or SSM
// when -source is given), receives BRC-124 frames, and reports one-way latency
// percentiles from latency stamps written by perf-test -latency-stamp (or any
// sender using the latstamp layout, e.g. the lab harness ssm_tx.py --stamp).
//
// One-way figures are only meaningful when sender and sink clocks are
// disciplined (chrony/NTP) or share a host. On the VM lab the authoritative
// timing is host-side pcap; this tool is the deployment probe and a join/count
// helper there.
//
// Usage:
//
//	latency-sink -iface mc-local -port 9001 -groups ff3e::b:0,ff3e::b:1 \
//	  -source fd00:5::1 -duration 70s -json /tmp/lat.json
package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-proxy/cmd/internal/latstamp"
)

type report struct {
	Datagrams int     `json:"datagrams"`
	Stamped   int     `json:"stamped"`
	Unique    int     `json:"unique"`
	Dups      int     `json:"dups"`
	DecodeErr int     `json:"decode_errors"`
	P50us     float64 `json:"p50_us"`
	P95us     float64 `json:"p95_us"`
	P99us     float64 `json:"p99_us"`
	MaxUs     float64 `json:"max_us"`
	MinUs     float64 `json:"min_us"`
	Note      string  `json:"note"`
}

// probeState keeps a rolling window of latency samples for probe mode.
type probeState struct {
	mu     sync.Mutex
	window time.Duration
	ts     []int64 // arrival ns
	lat    []int64 // one-way ns
	total  uint64
}

func newProbeState(w time.Duration) *probeState { return &probeState{window: w} }

func (p *probeState) record(nowNs, latNs int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.total++
	p.ts = append(p.ts, nowNs)
	p.lat = append(p.lat, latNs)
	cut := nowNs - p.window.Nanoseconds()
	i := 0
	for i < len(p.ts) && p.ts[i] < cut {
		i++
	}
	p.ts = p.ts[i:]
	p.lat = p.lat[i:]
}

// snapshot returns sorted window latencies and the all-time count.
func (p *probeState) snapshot() ([]int64, uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]int64, len(p.lat))
	copy(out, p.lat)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, p.total
}

// serveMetrics exposes Prometheus text-format gauges: rolling-window p50/p95/
// p99/max one-way latency (seconds) + counters. Hand-rolled to keep the probe
// dependency-free.
func serveMetrics(addr string, p *probeState) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		v, total := p.snapshot()
		pc := func(q float64) float64 {
			if len(v) == 0 {
				return 0
			}
			i := int(q * float64(len(v)))
			if i >= len(v) {
				i = len(v) - 1
			}
			return float64(v[i]) / 1e9
		}
		var maxS float64
		if len(v) > 0 {
			maxS = float64(v[len(v)-1]) / 1e9
		}
		_, _ = fmt.Fprintf(w, "# TYPE bsl_probe_latency_seconds gauge\n")
		for q, val := range map[string]float64{"0.5": pc(0.5), "0.95": pc(0.95), "0.99": pc(0.99)} {
			_, _ = fmt.Fprintf(w, "bsl_probe_latency_seconds{quantile=%q} %g\n", q, val)
		}
		_, _ = fmt.Fprintf(w, "# TYPE bsl_probe_latency_max_seconds gauge\nbsl_probe_latency_max_seconds %g\n", maxS)
		_, _ = fmt.Fprintf(w, "# TYPE bsl_probe_window_samples gauge\nbsl_probe_window_samples %d\n", len(v))
		_, _ = fmt.Fprintf(w, "# TYPE bsl_probe_stamped_total counter\nbsl_probe_stamped_total %d\n", total)
	})
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("metrics server: %v", err)
	}
}

// groupSourceReq packs a Linux struct group_source_req by hand (this x/sys
// version has no helper): u32 ifindex + 4B pad + 2×sockaddr_storage(128B)
// holding sockaddr_in6 {family, port, flowinfo, addr, scope}, zero-padded.
// Mirrors the lab harness ssm_rx.py.
func groupSourceReq(ifindex uint32, group, source net.IP) string {
	storage := func(ip net.IP) []byte {
		b := make([]byte, 128)
		binary.NativeEndian.PutUint16(b[0:], unix.AF_INET6)
		copy(b[8:24], ip.To16())
		return b
	}
	req := make([]byte, 0, 264)
	hdr := make([]byte, 8)
	binary.NativeEndian.PutUint32(hdr, ifindex)
	req = append(req, hdr...)
	req = append(req, storage(group)...)
	req = append(req, storage(source)...)
	return string(req)
}

func main() {
	iface := flag.String("iface", "mc-local", "interface to join groups on")
	port := flag.Int("port", 9001, "UDP port to listen on (proxy EGRESS_PORT)")
	groupsFlag := flag.String("groups", "", "comma-separated multicast groups to join")
	source := flag.String("source", "", "SSM source address (empty = ASM join)")
	duration := flag.Duration("duration", 70*time.Second, "receive window (0 = run forever; probe mode)")
	jsonOut := flag.String("json", "", "write JSON report to this path")
	metricsAddr := flag.String("metrics-addr", "",
		"expose Prometheus metrics on this addr (e.g. :9300), probe mode: rolling-window percentiles")
	window := flag.Duration("window", 60*time.Second, "rolling window for probe-mode percentiles")
	flag.Parse()

	ifi, err := net.InterfaceByName(*iface)
	if err != nil {
		log.Fatalf("interface %q: %v", *iface, err)
	}

	fd, err := unix.Socket(unix.AF_INET6, unix.SOCK_DGRAM, unix.IPPROTO_UDP)
	if err != nil {
		log.Fatalf("socket: %v", err)
	}
	defer func() { _ = unix.Close(fd) }()
	// Share the port with a deployed shard-listener (both receive a copy).
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
		log.Fatalf("SO_REUSEADDR: %v", err)
	}
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEPORT, 1); err != nil {
		log.Fatalf("SO_REUSEPORT: %v", err)
	}
	if err := unix.Bind(fd, &unix.SockaddrInet6{Port: *port}); err != nil {
		log.Fatalf("bind :%d: %v", *port, err)
	}

	srcIP := net.ParseIP(*source)
	if *source != "" && srcIP == nil {
		log.Fatalf("invalid -source %q", *source)
	}
	for _, g := range strings.Split(*groupsFlag, ",") {
		g = strings.TrimSpace(g)
		if g == "" {
			continue
		}
		gip := net.ParseIP(g)
		if gip == nil {
			log.Fatalf("invalid group %q", g)
		}
		if srcIP != nil { // SSM (S,G) join
			req := groupSourceReq(uint32(ifi.Index), gip, srcIP)
			if err := unix.SetsockoptString(fd, unix.IPPROTO_IPV6,
				unix.MCAST_JOIN_SOURCE_GROUP, req); err != nil {
				log.Fatalf("SSM join (%s,%s) on %s: %v", *source, g, ifi.Name, err)
			}
			log.Printf("joined SSM (%s, %s) on %s", *source, g, ifi.Name)
		} else { // ASM join
			mreq := &unix.IPv6Mreq{Interface: uint32(ifi.Index)}
			copy(mreq.Multiaddr[:], gip.To16())
			if err := unix.SetsockoptIPv6Mreq(fd, unix.IPPROTO_IPV6,
				unix.IPV6_JOIN_GROUP, mreq); err != nil {
				log.Fatalf("ASM join %s on %s: %v", g, ifi.Name, err)
			}
			log.Printf("joined ASM %s on %s", g, ifi.Name)
		}
	}

	// Probe mode: serve rolling-window percentiles as Prometheus text.
	var probe *probeState
	if *metricsAddr != "" {
		probe = newProbeState(*window)
		go serveMetrics(*metricsAddr, probe)
		log.Printf("probe metrics on %s (window %s)", *metricsAddr, *window)
	}

	// Receive until the window closes.
	tv := unix.Timeval{Sec: 1}
	_ = unix.SetsockoptTimeval(fd, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &tv)
	buf := make([]byte, 65536)
	forever := *duration == 0
	deadline := time.Now().Add(*duration)
	rep := report{Note: "one-way; requires disciplined clocks (chrony/NTP) or same host"}
	seen := make(map[uint32]struct{})
	var lats []int64

	for forever || time.Now().Before(deadline) {
		n, _, err := unix.Recvfrom(fd, buf, 0)
		if err != nil {
			continue // timeout tick or transient
		}
		now := time.Now().UnixNano()
		rep.Datagrams++
		var txNs int64
		var seq uint32
		var ok bool
		if f, derr := frame.Decode(buf[:n]); derr == nil {
			txNs, seq, ok = latstamp.Get(f.Payload)
		} else {
			rep.DecodeErr++
			txNs, seq, ok = latstamp.Scan(buf[:n]) // raw harness datagrams
		}
		if !ok {
			continue
		}
		rep.Stamped++
		if _, dup := seen[seq]; dup {
			rep.Dups++
			continue
		}
		seen[seq] = struct{}{}
		lats = append(lats, now-txNs)
		if probe != nil {
			probe.record(now, now-txNs)
		}
	}

	rep.Unique = len(seen)
	if len(lats) > 0 {
		sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
		pct := func(p float64) float64 {
			i := int(p / 100 * float64(len(lats)))
			if i >= len(lats) {
				i = len(lats) - 1
			}
			return float64(lats[i]) / 1e3
		}
		rep.P50us, rep.P95us, rep.P99us = pct(50), pct(95), pct(99)
		rep.MinUs = float64(lats[0]) / 1e3
		rep.MaxUs = float64(lats[len(lats)-1]) / 1e3
	}

	fmt.Printf("SINK datagrams=%d stamped=%d unique=%d dups=%d decode_err=%d\n",
		rep.Datagrams, rep.Stamped, rep.Unique, rep.Dups, rep.DecodeErr)
	if len(lats) > 0 {
		fmt.Printf("LAT n=%d p50=%.0fus p95=%.0fus p99=%.0fus max=%.0fus (%s)\n",
			len(lats), rep.P50us, rep.P95us, rep.P99us, rep.MaxUs, rep.Note)
	}
	if *jsonOut != "" {
		b, _ := json.MarshalIndent(&rep, "", "  ")
		if err := os.WriteFile(*jsonOut, b, 0o644); err != nil {
			log.Fatalf("write %s: %v", *jsonOut, err)
		}
	}
}
