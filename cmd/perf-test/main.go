// Command perf-test is a throughput performance measurement tool for
// shard-proxy. It sends BRC-124 frames at a configurable PPS rate,
// collects proxy Prometheus metrics, interface statistics, and per-group
// per-receiver delivery data, then produces a markdown report.
//
// Usage:
//
//	perf-test -proxy-addr [fd20::2]:9000 -metrics-url http://10.10.10.20:9100 \
//	  -shard-bits 2 -pps 10000 -duration 5m -payload-min 256 -payload-max 512 \
//	  -lxd -receivers recv1,recv2,recv3 -output report.md
package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	mrand "math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/lightwebinc/shard-common/frame"
)

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

type config struct {
	ProxyAddr  string
	MetricsURL string
	ShardBits  uint
	PPS        int
	Duration   time.Duration
	PayloadMin int
	PayloadMax int
	LXD        bool
	Receivers  []string
	Output     string
	Senders    int
}

func parseFlags() *config {
	c := &config{}
	flag.StringVar(&c.ProxyAddr, "proxy-addr", "[::1]:9000", "proxy listen address (host:port)")
	flag.StringVar(&c.MetricsURL, "metrics-url", "http://10.10.10.20:9100", "proxy metrics base URL")
	bits := flag.Uint("shard-bits", 2, "shard-bits the proxy is configured with")
	flag.IntVar(&c.PPS, "pps", 10000, "target packets per second")
	flag.DurationVar(&c.Duration, "duration", 5*time.Minute, "test duration")
	flag.IntVar(&c.PayloadMin, "payload-min", 256, "minimum payload size in bytes")
	flag.IntVar(&c.PayloadMax, "payload-max", 512, "maximum payload size in bytes")
	flag.BoolVar(&c.LXD, "lxd", false, "enable lxc exec for interface stats, tcpdump, recv-test-frames collection")
	recvFlag := flag.String("receivers", "recv1,recv2,recv3", "comma-separated receiver VM names")
	flag.StringVar(&c.Output, "output", "report.md", "output report file path")
	flag.IntVar(&c.Senders, "senders", 1, "number of concurrent sender goroutines (each targets pps/senders)")
	flag.Parse()

	c.ShardBits = *bits
	for _, r := range strings.Split(*recvFlag, ",") {
		r = strings.TrimSpace(r)
		if r != "" {
			c.Receivers = append(c.Receivers, r)
		}
	}
	return c
}

// ---------------------------------------------------------------------------
// Metrics snapshot (Prometheus text format parser)
// ---------------------------------------------------------------------------

// lineRe matches a Prometheus text-format metric line: name{labels} value
var lineRe = regexp.MustCompile(`^([a-zA-Z_:][a-zA-Z0-9_:]*)({[^}]*})?\s+([0-9eE.+\-]+)`)

type metricsSnapshot struct {
	Timestamp time.Time
	Counters  map[string]float64            // simple counters: metric_name -> value
	Labeled   map[string]map[string]float64 // metric_name -> {label_combo -> value}
	Raw       string
}

func scrapeMetrics(baseURL string) (*metricsSnapshot, error) {
	resp, err := http.Get(baseURL + "/metrics")
	if err != nil {
		return nil, fmt.Errorf("scrape %s/metrics: %w", baseURL, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read metrics body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("scrape %s/metrics: HTTP %d: %s", baseURL, resp.StatusCode, body)
	}

	snap := &metricsSnapshot{
		Timestamp: time.Now(),
		Counters:  make(map[string]float64),
		Labeled:   make(map[string]map[string]float64),
		Raw:       string(body),
	}

	// Metrics we care about as simple counters (sum across all label combos).
	simpleMetrics := map[string]bool{
		"bsp_packets_received_total":  true,
		"bsp_packets_forwarded_total": true,
		"bsp_packets_dropped_total":   true,
		"bsp_bytes_received_total":    true,
		"bsp_bytes_forwarded_total":   true,
		"bsp_egress_errors_total":     true,
		"bsp_ingress_errors_total":    true,
		"bsp_active_groups":           true,
	}

	// Metrics we need per-label breakdown for.
	labeledMetrics := map[string]bool{
		"bsp_flow_packets_total": true,
		"bsp_flow_bytes_total":   true,
	}

	scanner := bufio.NewScanner(strings.NewReader(string(body)))
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "#") {
			continue
		}
		m := lineRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		name := m[1]
		labels := m[2]
		val, err := strconv.ParseFloat(m[3], 64)
		if err != nil {
			continue
		}

		if simpleMetrics[name] {
			snap.Counters[name] += val
		}
		if labeledMetrics[name] && labels != "" {
			if snap.Labeled[name] == nil {
				snap.Labeled[name] = make(map[string]float64)
			}
			snap.Labeled[name][labels] += val
		}
	}

	return snap, nil
}

func metricsDelta(before, after *metricsSnapshot) map[string]float64 {
	delta := make(map[string]float64)
	for k, v := range after.Counters {
		delta[k] = v - before.Counters[k]
	}
	return delta
}

// flowDelta returns per-group deltas for a labeled metric.
func flowDelta(before, after *metricsSnapshot, metric string) map[string]float64 {
	delta := make(map[string]float64)
	afterMap := after.Labeled[metric]
	beforeMap := before.Labeled[metric]
	if afterMap == nil {
		return delta
	}
	for k, v := range afterMap {
		delta[k] = v
		if beforeMap != nil {
			delta[k] -= beforeMap[k]
		}
	}
	return delta
}

// extractGroupFromLabels extracts the group value from a Prometheus label string
// like {group="0002",network.interface.name="enp6s0"}
func extractGroupFromLabels(labels string) string {
	re := regexp.MustCompile(`group="([^"]+)"`)
	m := re.FindStringSubmatch(labels)
	if len(m) > 1 {
		return m[1]
	}
	return labels
}

// ---------------------------------------------------------------------------
// Interface stats (via lxc exec)
// ---------------------------------------------------------------------------

type ifaceStats struct {
	RXPackets uint64
	RXBytes   uint64
	RXErrors  uint64
	RXDropped uint64
	TXPackets uint64
	TXBytes   uint64
	TXErrors  uint64
	TXDropped uint64
}

func lxcExec(vm string, args ...string) (string, error) {
	cmdArgs := append([]string{"exec", vm, "--"}, args...)
	cmd := exec.Command("lxc", cmdArgs...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("lxc exec %s %v: %w\n%s", vm, args, err, out)
	}
	return string(out), nil
}

func collectIfaceStats(vm, iface string) (*ifaceStats, error) {
	out, err := lxcExec(vm, "ip", "-s", "link", "show", iface)
	if err != nil {
		return nil, err
	}
	return parseIfaceStats(out), nil
}

func parseIfaceStats(output string) *ifaceStats {
	s := &ifaceStats{}
	lines := strings.Split(output, "\n")
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "RX:") || strings.HasPrefix(trimmed, "RX: ") {
			// Next line has the values: bytes packets errors dropped ...
			if i+1 < len(lines) {
				fields := strings.Fields(strings.TrimSpace(lines[i+1]))
				if len(fields) >= 4 {
					s.RXBytes, _ = strconv.ParseUint(fields[0], 10, 64)
					s.RXPackets, _ = strconv.ParseUint(fields[1], 10, 64)
					s.RXErrors, _ = strconv.ParseUint(fields[2], 10, 64)
					s.RXDropped, _ = strconv.ParseUint(fields[3], 10, 64)
				}
			}
		}
		if strings.HasPrefix(trimmed, "TX:") || strings.HasPrefix(trimmed, "TX: ") {
			if i+1 < len(lines) {
				fields := strings.Fields(strings.TrimSpace(lines[i+1]))
				if len(fields) >= 4 {
					s.TXBytes, _ = strconv.ParseUint(fields[0], 10, 64)
					s.TXPackets, _ = strconv.ParseUint(fields[1], 10, 64)
					s.TXErrors, _ = strconv.ParseUint(fields[2], 10, 64)
					s.TXDropped, _ = strconv.ParseUint(fields[3], 10, 64)
				}
			}
		}
	}
	return s
}

func saturatingSub(a, b uint64) uint64 {
	if a < b {
		return 0
	}
	return a - b
}

func ifaceStatsDelta(before, after *ifaceStats) *ifaceStats {
	return &ifaceStats{
		RXPackets: saturatingSub(after.RXPackets, before.RXPackets),
		RXBytes:   saturatingSub(after.RXBytes, before.RXBytes),
		RXErrors:  saturatingSub(after.RXErrors, before.RXErrors),
		RXDropped: saturatingSub(after.RXDropped, before.RXDropped),
		TXPackets: saturatingSub(after.TXPackets, before.TXPackets),
		TXBytes:   saturatingSub(after.TXBytes, before.TXBytes),
		TXErrors:  saturatingSub(after.TXErrors, before.TXErrors),
		TXDropped: saturatingSub(after.TXDropped, before.TXDropped),
	}
}

// ---------------------------------------------------------------------------
// tcpdump / tshark helpers
// ---------------------------------------------------------------------------

type tcpdumpProc struct {
	vm        string
	cmd       *exec.Cmd
	stdinPipe io.WriteCloser
}

func startTcpdump(vm, iface string) (*tcpdumpProc, error) {
	// Remove any stale pcap from a previous run so tshark never reads stale data.
	_, _ = lxcExec(vm, "rm", "-f", "/tmp/perf-capture.pcap")

	cmd := exec.Command("lxc", "exec", vm, "--",
		"tcpdump", "-p", "-i", iface, "-n", "ip6 and udp", "-w", "/tmp/perf-capture.pcap")
	// Provide a live stdin pipe — without it, lxc exec may kill the container
	// process when it detects /dev/null stdin (EOF).
	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("stdin pipe for tcpdump on %s: %w", vm, err)
	}
	cmd.Stdout = nil
	cmd.Stderr = os.Stderr // surface tcpdump errors immediately
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start tcpdump on %s: %w", vm, err)
	}
	return &tcpdumpProc{vm: vm, cmd: cmd, stdinPipe: stdinPipe}, nil
}

func stopTcpdump(t *tcpdumpProc) {
	// Kill tcpdump inside the VM.
	_, _ = lxcExec(t.vm, "pkill", "-f", "tcpdump.*perf-capture")
	// Close stdin pipe so lxc exec can exit cleanly.
	_ = t.stdinPipe.Close()
	// Wait for our lxc exec process to finish.
	_ = t.cmd.Wait()
}

// tsharkGroupCounts parses the pcap on a receiver VM and returns per-group packet counts.
func tsharkGroupCounts(vm string) (map[string]int64, error) {
	cmdArgs := []string{"exec", vm, "--",
		"tshark", "-r", "/tmp/perf-capture.pcap",
		"-T", "fields", "-e", "ipv6.dst", "-Y", "udp"}
	cmd := exec.Command("lxc", cmdArgs...)
	// Capture stdout only; discard stderr to avoid "Running as root" warnings.
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("tshark on %s: %w\nstderr: %s", vm, err, stderr.String())
	}
	counts := make(map[string]int64)
	scanner := bufio.NewScanner(strings.NewReader(stdout.String()))
	for scanner.Scan() {
		addr := strings.TrimSpace(scanner.Text())
		if addr != "" {
			counts[addr]++
		}
	}
	return counts, nil
}

// ---------------------------------------------------------------------------
// recv-test-frames helper
// ---------------------------------------------------------------------------

type recvProc struct {
	vm      string
	cmd     *exec.Cmd
	count   atomic.Int64
	out     strings.Builder
	mu      sync.Mutex
	partial string // incomplete line carried across Write calls
}

func startRecvTestFrames(vm, iface string, groups []string, port int) (*recvProc, error) {
	groupStr := strings.Join(groups, ",")
	cmd := exec.Command("lxc", "exec", vm, "--",
		"recv-test-frames", "-iface", iface, "-port", strconv.Itoa(port),
		"-groups", groupStr)
	rp := &recvProc{vm: vm, cmd: cmd}

	cmd.Stdout = &recvWriter{rp: rp}
	cmd.Stderr = &recvWriter{rp: rp}

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start recv-test-frames on %s: %w", vm, err)
	}
	return rp, nil
}

type recvWriter struct {
	rp *recvProc
}

func (w *recvWriter) Write(p []byte) (int, error) {
	w.rp.mu.Lock()
	w.rp.out.Write(p)

	// Prepend any leftover partial line from the previous Write call.
	data := w.rp.partial + string(p)
	lines := strings.Split(data, "\n")
	// The last element is either "" (if data ended with \n) or an
	// incomplete line that must be carried to the next call.
	w.rp.partial = lines[len(lines)-1]
	lines = lines[:len(lines)-1]
	w.rp.mu.Unlock()

	// Count complete lines starting with "recv " as received frames.
	for _, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "recv ") {
			w.rp.count.Add(1)
		}
	}
	return len(p), nil
}

func stopRecvTestFrames(rp *recvProc) int64 {
	_, _ = lxcExec(rp.vm, "pkill", "-f", "recv-test-frames")
	_ = rp.cmd.Wait()
	return rp.count.Load()
}

// ---------------------------------------------------------------------------
// Sender
// ---------------------------------------------------------------------------

type sendResult struct {
	FramesSent int64
	BytesSent  int64
	Elapsed    time.Duration
	ActualPPS  float64
	Mbps       float64
}

// fillRand fills b with non-cryptographic random bytes from math/rand/v2.
// Uses 8-byte writes from Uint64 to amortise the per-call cost.
func fillRand(b []byte) {
	i := 0
	for ; i+8 <= len(b); i += 8 {
		binary.LittleEndian.PutUint64(b[i:i+8], mrand.Uint64())
	}
	if i < len(b) {
		var tail [8]byte
		binary.LittleEndian.PutUint64(tail[:], mrand.Uint64())
		copy(b[i:], tail[:len(b)-i])
	}
}

func sendFramesWorker(ctx context.Context, cfg *config, workerID int, targetPPS int) *sendResult {
	conn, err := net.Dial("udp6", cfg.ProxyAddr)
	if err != nil {
		log.Fatalf("sender %d: dial %s: %v", workerID, cfg.ProxyAddr, err)
	}
	defer func() { _ = conn.Close() }()

	payloadRange := cfg.PayloadMax - cfg.PayloadMin + 1
	maxFrameSize := frame.HeaderSize + cfg.PayloadMax
	buf := make([]byte, maxFrameSize)

	var framesSent int64
	var bytesSent int64

	// Stagger workers so their pacing bursts don't hit the NIC TX ring simultaneously.
	time.Sleep(time.Duration(workerID) * time.Millisecond)

	start := time.Now()
	deadline := start.Add(cfg.Duration)

	// Pre-allocate a reusable frame.
	f := &frame.Frame{}

	log.Printf("sender %d: sending at %d pps for %s to %s ...", workerID, targetPPS, cfg.Duration, cfg.ProxyAddr)

	// Use a busy-loop with time-based pacing so we can achieve high PPS
	// targets that exceed the Go runtime timer resolution (~1 ms).
	// On each iteration we compute how many packets should have been sent
	// by now and send a burst to catch up.
	for {
		now := time.Now()
		if now.After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			goto done
		default:
		}

		elapsedSec := now.Sub(start).Seconds()
		target := int64(elapsedSec * float64(targetPPS))
		if framesSent >= target {
			// Ahead of schedule; yield briefly to avoid pure busy-wait.
			time.Sleep(100 * time.Microsecond)
			continue
		}

		// Generate random txid. Non-cryptographic PRNG is fine here:
		// frames are throwaway load and the proxy only needs the txid
		// for routing/dedup, not adversarial uniqueness.
		fillRand(f.TxID[:])

		// Generate random payload size in [PayloadMin, PayloadMax].
		payloadSize := cfg.PayloadMin
		if payloadRange > 1 {
			payloadSize += mrand.IntN(payloadRange)
		}

		// Fill payload with random bytes.
		f.Payload = buf[frame.HeaderSize : frame.HeaderSize+payloadSize]
		fillRand(f.Payload)

		n, err := frame.Encode(f, buf)
		if err != nil {
			log.Printf("encode: %v", err)
			continue
		}

		if _, err := conn.Write(buf[:n]); err != nil {
			log.Printf("write: %v", err)
			time.Sleep(10 * time.Microsecond)
			continue
		}

		framesSent++
		bytesSent += int64(n)

		if framesSent%int64(targetPPS) == 0 {
			log.Printf("  sender %d: sent %d frames (%.1f s, %.0f actual pps)",
				workerID, framesSent, elapsedSec, float64(framesSent)/elapsedSec)
		}
	}

done:
	elapsed := time.Since(start)
	var actualPPS, mbps float64
	if elapsed > 0 {
		actualPPS = float64(framesSent) / elapsed.Seconds()
		mbps = float64(bytesSent) * 8 / elapsed.Seconds() / 1e6
	}

	log.Printf("sender %d complete: %d frames, %d bytes, %.1f s, %.0f pps, %.2f Mbps",
		workerID, framesSent, bytesSent, elapsed.Seconds(), actualPPS, mbps)

	return &sendResult{
		FramesSent: framesSent,
		BytesSent:  bytesSent,
		Elapsed:    elapsed,
		ActualPPS:  actualPPS,
		Mbps:       mbps,
	}
}

func sendFrames(ctx context.Context, cfg *config) *sendResult {
	if cfg.Senders <= 1 {
		return sendFramesWorker(ctx, cfg, 0, cfg.PPS)
	}
	ppsEach := cfg.PPS / cfg.Senders
	results := make([]*sendResult, cfg.Senders)
	var wg sync.WaitGroup
	for i := range cfg.Senders {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			results[idx] = sendFramesWorker(ctx, cfg, idx, ppsEach)
		}(i)
	}
	wg.Wait()

	var totalFrames, totalBytes int64
	var maxElapsed time.Duration
	for _, r := range results {
		totalFrames += r.FramesSent
		totalBytes += r.BytesSent
		if r.Elapsed > maxElapsed {
			maxElapsed = r.Elapsed
		}
	}
	var actualPPS, mbps float64
	if maxElapsed > 0 {
		actualPPS = float64(totalFrames) / maxElapsed.Seconds()
		mbps = float64(totalBytes) * 8 / maxElapsed.Seconds() / 1e6
	}
	log.Printf("aggregate: %d frames, %d bytes, %.1f s, %.0f pps, %.2f Mbps",
		totalFrames, totalBytes, maxElapsed.Seconds(), actualPPS, mbps)
	return &sendResult{
		FramesSent: totalFrames,
		BytesSent:  totalBytes,
		Elapsed:    maxElapsed,
		ActualPPS:  actualPPS,
		Mbps:       mbps,
	}
}

// ---------------------------------------------------------------------------
// Health check
// ---------------------------------------------------------------------------

func checkHealth(baseURL string) error {
	for _, ep := range []string{"/healthz", "/readyz"} {
		resp, err := http.Get(baseURL + ep)
		if err != nil {
			return fmt.Errorf("GET %s%s: %w", baseURL, ep, err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("%s returned %d: %s", ep, resp.StatusCode, body)
		}
		log.Printf("  %s: %s", ep, strings.TrimSpace(string(body)))
	}
	return nil
}

// ---------------------------------------------------------------------------
// Known group memberships
// ---------------------------------------------------------------------------

var knownGroupMembership = map[string][]string{
	"recv1": {"ff05::", "ff05::1", "ff05::2", "ff05::3"},
	"recv2": {"ff05::2"},
	"recv3": {"ff05::1", "ff05::3"},
}

func groupsForReceiver(vm string) []string {
	if g, ok := knownGroupMembership[vm]; ok {
		return g
	}
	return nil
}

// ---------------------------------------------------------------------------
// Report generation
// ---------------------------------------------------------------------------

type testResults struct {
	Config        *config
	NumGroups     uint32
	StartTime     time.Time
	EndTime       time.Time
	Send          *sendResult
	MetricsBefore *metricsSnapshot
	MetricsAfter  *metricsSnapshot
	MetricsDelta  map[string]float64
	FlowPktDelta  map[string]float64     // labels -> delta packets
	FlowByteDelta map[string]float64     // labels -> delta bytes
	IfaceBefore   map[string]*ifaceStats // vm -> stats
	IfaceAfter    map[string]*ifaceStats
	RecvCounts    map[string]int64            // vm -> recv-test-frames count
	TsharkCounts  map[string]map[string]int64 // vm -> {group_addr -> count}
}

func generateReport(r *testResults) string {
	var b strings.Builder
	w := func(format string, args ...any) { fmt.Fprintf(&b, format, args...) }

	numGroups := r.NumGroups
	shardBits := r.Config.ShardBits

	w("# Throughput Performance Report\n\n")
	w("## Test Configuration\n\n")
	w("| Parameter | Value |\n")
	w("|-----------|-------|\n")
	w("| Date | %s |\n", r.StartTime.Format(time.RFC3339))
	w("| Proxy address | `%s` |\n", r.Config.ProxyAddr)
	w("| Metrics URL | `%s` |\n", r.Config.MetricsURL)
	w("| Shard bits | %d |\n", shardBits)
	w("| Num groups | %d |\n", numGroups)
	w("| Payload range | %d–%d bytes |\n", r.Config.PayloadMin, r.Config.PayloadMax)
	w("| Target PPS | %d |\n", r.Config.PPS)
	w("| Senders | %d |\n", r.Config.Senders)
	w("| Duration | %s |\n", r.Config.Duration)
	w("| LXD collection | %v |\n", r.Config.LXD)
	w("| Receivers | %s |\n", strings.Join(r.Config.Receivers, ", "))
	w("\n")

	// Results
	w("## Results\n\n")
	w("| Metric | Value |\n")
	w("|--------|-------|\n")
	w("| Target PPS | %d |\n", r.Config.PPS)
	w("| Actual PPS | %.0f |\n", r.Send.ActualPPS)
	w("| Frames sent | %d |\n", r.Send.FramesSent)
	w("| Bytes sent | %s |\n", humanBytes(r.Send.BytesSent))
	w("| TX throughput | %.2f Mbps |\n", r.Send.Mbps)
	w("| Duration | %s |\n", r.Send.Elapsed.Round(time.Millisecond))

	if r.MetricsDelta != nil {
		rxPkts := r.MetricsDelta["bsp_packets_received_total"]
		fwdPkts := r.MetricsDelta["bsp_packets_forwarded_total"]
		dropPkts := r.MetricsDelta["bsp_packets_dropped_total"]
		rxBytes := r.MetricsDelta["bsp_bytes_received_total"]
		fwdBytes := r.MetricsDelta["bsp_bytes_forwarded_total"]
		egressErrs := r.MetricsDelta["bsp_egress_errors_total"]
		ingressErrs := r.MetricsDelta["bsp_ingress_errors_total"]
		dropPct := 0.0
		if rxPkts > 0 {
			dropPct = dropPkts / rxPkts * 100
		}

		w("| Proxy RX packets | %.0f |\n", rxPkts)
		w("| Proxy RX bytes | %s |\n", humanBytes(int64(rxBytes)))
		w("| Proxy forwarded packets | %.0f |\n", fwdPkts)
		w("| Proxy forwarded bytes | %s |\n", humanBytes(int64(fwdBytes)))
		w("| Proxy dropped packets | %.0f |\n", dropPkts)
		w("| Drop rate | %.2f%% |\n", dropPct)
		w("| Egress errors | %.0f |\n", egressErrs)
		w("| Ingress errors | %.0f |\n", ingressErrs)
	}
	w("\n")

	// Summary
	w("## Summary\n\n")
	if r.MetricsDelta != nil {
		rxPkts := r.MetricsDelta["bsp_packets_received_total"]
		fwdPkts := r.MetricsDelta["bsp_packets_forwarded_total"]
		dropPkts := r.MetricsDelta["bsp_packets_dropped_total"]
		dropPct := 0.0
		if rxPkts > 0 {
			dropPct = dropPkts / rxPkts * 100
		}
		w("- **Frames sent:** %d\n", r.Send.FramesSent)
		w("- **Proxy received:** %.0f\n", rxPkts)
		w("- **Proxy forwarded:** %.0f\n", fwdPkts)
		w("- **Proxy dropped:** %.0f (%.2f%%)\n", dropPkts, dropPct)
		w("- **Achieved throughput:** %.0f pps / %.2f Mbps\n", r.Send.ActualPPS, r.Send.Mbps)
	} else {
		w("- **Frames sent:** %d\n", r.Send.FramesSent)
		w("- **Achieved throughput:** %.0f pps / %.2f Mbps\n", r.Send.ActualPPS, r.Send.Mbps)
	}
	w("\n")

	// Multicast Group Distribution (Proxy Egress)
	w("## Multicast Group Distribution (Proxy Egress)\n\n")
	if len(r.FlowPktDelta) > 0 {
		// Build sorted group list.
		type groupStats struct {
			Group   string
			Packets float64
			Bytes   float64
		}
		var groups []groupStats
		var totalPkts, totalBytes float64
		for labels, pkts := range r.FlowPktDelta {
			g := extractGroupFromLabels(labels)
			bytes := r.FlowByteDelta[labels]
			groups = append(groups, groupStats{Group: g, Packets: pkts, Bytes: bytes})
			totalPkts += pkts
			totalBytes += bytes
		}
		sort.Slice(groups, func(i, j int) bool { return groups[i].Group < groups[j].Group })

		w("Packets sent by proxy per shard group (from `bsp_flow_packets_total`):\n\n")
		w("| Group | Packets | Bytes | %% of Total |\n")
		w("|-------|---------|-------|------------|\n")
		var pktValues []float64
		for _, g := range groups {
			pct := 0.0
			if totalPkts > 0 {
				pct = g.Packets / totalPkts * 100
			}
			groupAddr := groupIndexToAddr(g.Group)
			w("| %s | %.0f | %s | %.1f%% |\n", groupAddr, g.Packets, humanBytes(int64(g.Bytes)), pct)
			pktValues = append(pktValues, g.Packets)
		}
		w("| **Total** | **%.0f** | **%s** | **100%%** |\n", totalPkts, humanBytes(int64(totalBytes)))
		w("\n")

		if len(pktValues) > 0 {
			min, max, mean, stddev := stats(pktValues)
			w("**Distribution stats:** min=%.0f, max=%.0f, mean=%.0f, stddev=%.0f\n\n", min, max, mean, stddev)
		}
	} else {
		w("*No flow metrics available (proxy metrics not scraped or no traffic forwarded).*\n\n")
	}

	// Per-Group Per-Receiver Delivery Matrix
	w("## Per-Group Per-Receiver Delivery Matrix\n\n")
	if len(r.TsharkCounts) > 0 {
		// Collect all group addresses seen across all receivers.
		allGroups := make(map[string]bool)
		for _, gc := range r.TsharkCounts {
			for g := range gc {
				allGroups[g] = true
			}
		}
		var sortedGroups []string
		for g := range allGroups {
			sortedGroups = append(sortedGroups, g)
		}
		sort.Strings(sortedGroups)

		receivers := r.Config.Receivers

		// Header
		w("Packets received at each receiver, broken down by destination multicast group (from `tshark` post-processing):\n\n")
		w("| Group |")
		for _, recv := range receivers {
			w(" %s |", recv)
		}
		w(" Subscribed receivers |\n")
		w("|-------|")
		for range receivers {
			w("--------|")
		}
		w("------------------------|\n")

		totals := make(map[string]int64)
		for _, g := range sortedGroups {
			w("| %s |", g)
			var subs []string
			for _, recv := range receivers {
				count := r.TsharkCounts[recv][g]
				if count > 0 {
					w(" %d |", count)
					subs = append(subs, recv)
				} else {
					w(" — |")
				}
				totals[recv] += count
			}
			w(" %s |\n", strings.Join(subs, ", "))
		}

		// Totals row
		w("| **Total** |")
		for _, recv := range receivers {
			w(" **%d** |", totals[recv])
		}
		w(" |\n")
		w("\n")

		// Expected vs actual share
		w("**Expected vs actual traffic share** (due to uneven group subscriptions):\n\n")
		fwdPkts := 0.0
		if r.MetricsDelta != nil {
			fwdPkts = r.MetricsDelta["bsp_packets_forwarded_total"]
		}
		w("| Receiver | Groups | Expected share | Actual packets | Actual share |\n")
		w("|----------|--------|----------------|----------------|--------------|\n")
		for _, recv := range receivers {
			groups := groupsForReceiver(recv)
			expectedPct := float64(len(groups)) / float64(numGroups) * 100
			actual := totals[recv]
			actualPct := 0.0
			if fwdPkts > 0 {
				actualPct = float64(actual) / fwdPkts * 100
			}
			w("| %s | %d/%d | %.0f%% | %d | %.1f%% |\n",
				recv, len(groups), numGroups, expectedPct, actual, actualPct)
		}
		w("\n")
	} else if r.Config.LXD {
		w("*tshark data not available. Ensure tshark is installed on receiver VMs.*\n\n")
	} else {
		w("*LXD collection disabled (`-lxd` not set). No per-group per-receiver data.*\n\n")
	}

	// Interface Statistics
	w("## Interface Statistics\n\n")
	if len(r.IfaceBefore) > 0 && len(r.IfaceAfter) > 0 {
		w("Delta of `ip -s link show enp6s0` before and after test:\n\n")
		w("| VM | Direction | Packets | Bytes | Errors | Dropped |\n")
		w("|----|-----------|---------|-------|--------|---------|\n")
		vms := []string{"proxy"}
		vms = append(vms, r.Config.Receivers...)
		for _, vm := range vms {
			before := r.IfaceBefore[vm]
			after := r.IfaceAfter[vm]
			if before == nil || after == nil {
				continue
			}
			d := ifaceStatsDelta(before, after)
			w("| %s | RX | %d | %s | %d | %d |\n", vm, d.RXPackets, humanBytes(int64(d.RXBytes)), d.RXErrors, d.RXDropped)
			w("| %s | TX | %d | %s | %d | %d |\n", vm, d.TXPackets, humanBytes(int64(d.TXBytes)), d.TXErrors, d.TXDropped)
		}
		w("\n")
	} else if r.Config.LXD {
		w("*Interface stats collection failed.*\n\n")
	} else {
		w("*LXD collection disabled (`-lxd` not set).*\n\n")
	}

	// Receiver Delivery
	if len(r.RecvCounts) > 0 {
		w("## Receiver Delivery (recv-test-frames)\n\n")
		w("| Receiver | Frames received |\n")
		w("|----------|-----------------|\n")
		for _, recv := range r.Config.Receivers {
			w("| %s | %d |\n", recv, r.RecvCounts[recv])
		}
		w("\n")
	}

	// Node Group Membership
	w("## Node Group Membership\n\n")
	w("| Receiver | Groups joined | Expected share |\n")
	w("|----------|---------------|----------------|\n")
	for _, recv := range r.Config.Receivers {
		groups := groupsForReceiver(recv)
		pct := float64(len(groups)) / float64(numGroups) * 100
		w("| %s | %s | %.0f%% |\n", recv, strings.Join(groups, ", "), pct)
	}
	w("\n")

	return b.String()
}

// groupIndexToAddr converts a Prometheus group label like "0002" to the
// corresponding ff05:: multicast address.
func groupIndexToAddr(groupHex string) string {
	idx, err := strconv.ParseUint(groupHex, 16, 32)
	if err != nil {
		return "ff05::" + groupHex
	}
	return fmt.Sprintf("ff05::%x", idx)
}

func humanBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.2f GiB", float64(b)/float64(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.2f MiB", float64(b)/float64(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.2f KiB", float64(b)/float64(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

func stats(vals []float64) (min, max, mean, stddev float64) {
	if len(vals) == 0 {
		return
	}
	min = vals[0]
	max = vals[0]
	sum := 0.0
	for _, v := range vals {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
		sum += v
	}
	mean = sum / float64(len(vals))
	sumSq := 0.0
	for _, v := range vals {
		sumSq += (v - mean) * (v - mean)
	}
	stddev = math.Sqrt(sumSq / float64(len(vals)))
	return
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	cfg := parseFlags()
	numGroups := uint32(1) << cfg.ShardBits

	log.Printf("=== perf-test ===")
	log.Printf("proxy=%s  metrics=%s  shard_bits=%d  groups=%d  pps=%d  senders=%d  duration=%s",
		cfg.ProxyAddr, cfg.MetricsURL, cfg.ShardBits, numGroups, cfg.PPS, cfg.Senders, cfg.Duration)
	log.Printf("payload=%d–%d bytes  lxd=%v  receivers=%v  output=%s",
		cfg.PayloadMin, cfg.PayloadMax, cfg.LXD, cfg.Receivers, cfg.Output)

	results := &testResults{
		Config:    cfg,
		NumGroups: numGroups,
		StartTime: time.Now(),
	}

	// ── Signal handling ─────────────────────────────────────────────────
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		log.Println("signal received, stopping...")
		cancel()
	}()

	// ── Pre-flight ──────────────────────────────────────────────────────
	log.Println("[pre-flight] checking proxy health...")
	if err := checkHealth(cfg.MetricsURL); err != nil {
		log.Fatalf("pre-flight health check failed: %v", err)
	}

	// LXD pre-flight: interface stats baseline, start tcpdump, start recv-test-frames
	var tcpdumpProcs []*tcpdumpProc
	var recvProcs []*recvProc

	if cfg.LXD {
		log.Println("[pre-flight] collecting baseline interface stats...")
		results.IfaceBefore = make(map[string]*ifaceStats)
		for _, vm := range append([]string{"proxy"}, cfg.Receivers...) {
			s, err := collectIfaceStats(vm, "enp6s0")
			if err != nil {
				log.Printf("  warning: interface stats on %s: %v", vm, err)
			} else {
				results.IfaceBefore[vm] = s
			}
		}

		log.Println("[pre-flight] starting tcpdump on receivers...")
		for _, recv := range cfg.Receivers {
			tp, err := startTcpdump(recv, "enp6s0")
			if err != nil {
				log.Printf("  warning: tcpdump on %s: %v", recv, err)
			} else {
				tcpdumpProcs = append(tcpdumpProcs, tp)
			}
		}
		// Give tcpdump a moment to start capturing.
		time.Sleep(2 * time.Second)

		log.Println("[pre-flight] starting recv-test-frames on receivers...")
		for _, recv := range cfg.Receivers {
			groups := groupsForReceiver(recv)
			if len(groups) == 0 {
				log.Printf("  warning: no known groups for %s, skipping recv-test-frames", recv)
				continue
			}
			rp, err := startRecvTestFrames(recv, "enp6s0", groups, 9001)
			if err != nil {
				log.Printf("  warning: recv-test-frames on %s: %v", recv, err)
			} else {
				recvProcs = append(recvProcs, rp)
			}
		}
		time.Sleep(1 * time.Second)
	}

	// ── Snapshot metrics (before) ───────────────────────────────────────
	log.Println("[metrics] scraping proxy metrics (before)...")
	metricsBefore, err := scrapeMetrics(cfg.MetricsURL)
	if err != nil {
		log.Printf("  warning: metrics scrape failed: %v", err)
	}
	results.MetricsBefore = metricsBefore

	// ── Send ────────────────────────────────────────────────────────────
	log.Println("[send] starting frame transmission...")
	results.Send = sendFrames(ctx, cfg)

	// ── Drain pause ─────────────────────────────────────────────────────
	log.Println("[drain] waiting 2s for pipeline to drain...")
	time.Sleep(2 * time.Second)

	// ── Snapshot metrics (after) ────────────────────────────────────────
	log.Println("[metrics] scraping proxy metrics (after)...")
	metricsAfter, err := scrapeMetrics(cfg.MetricsURL)
	if err != nil {
		log.Printf("  warning: metrics scrape failed: %v", err)
	}
	results.MetricsAfter = metricsAfter

	if metricsBefore != nil && metricsAfter != nil {
		results.MetricsDelta = metricsDelta(metricsBefore, metricsAfter)
		results.FlowPktDelta = flowDelta(metricsBefore, metricsAfter, "bsp_flow_packets_total")
		results.FlowByteDelta = flowDelta(metricsBefore, metricsAfter, "bsp_flow_bytes_total")
	}

	// ── Post-test LXD collection ────────────────────────────────────────
	if cfg.LXD {
		log.Println("[post-test] stopping recv-test-frames...")
		results.RecvCounts = make(map[string]int64)
		for _, rp := range recvProcs {
			count := stopRecvTestFrames(rp)
			results.RecvCounts[rp.vm] = count
			log.Printf("  %s: %d frames received", rp.vm, count)
		}

		log.Println("[post-test] stopping tcpdump...")
		for _, tp := range tcpdumpProcs {
			stopTcpdump(tp)
		}
		// Give filesystem a moment to flush pcap.
		time.Sleep(1 * time.Second)

		log.Println("[post-test] running tshark per-group analysis...")
		results.TsharkCounts = make(map[string]map[string]int64)
		for _, recv := range cfg.Receivers {
			counts, err := tsharkGroupCounts(recv)
			if err != nil {
				log.Printf("  warning: tshark on %s: %v", recv, err)
			} else {
				results.TsharkCounts[recv] = counts
				log.Printf("  %s: %d distinct groups, %d total packets",
					recv, len(counts), sumCounts(counts))
			}
		}

		log.Println("[post-test] collecting final interface stats...")
		results.IfaceAfter = make(map[string]*ifaceStats)
		for _, vm := range append([]string{"proxy"}, cfg.Receivers...) {
			s, err := collectIfaceStats(vm, "enp6s0")
			if err != nil {
				log.Printf("  warning: interface stats on %s: %v", vm, err)
			} else {
				results.IfaceAfter[vm] = s
			}
		}
	}

	results.EndTime = time.Now()

	// ── Generate report ─────────────────────────────────────────────────
	log.Println("[report] generating markdown report...")
	report := generateReport(results)

	if err := os.WriteFile(cfg.Output, []byte(report), 0644); err != nil {
		log.Fatalf("write report: %v", err)
	}
	log.Printf("[done] report written to %s", cfg.Output)

	// Also print summary to stdout.
	fmt.Println()
	fmt.Println(report)
}

func sumCounts(m map[string]int64) int64 {
	var total int64
	for _, v := range m {
		total += v
	}
	return total
}
