// BEEF object-plane ingress rate limiting.
//
// The object plane is an OPEN class: anyone who can reach the public tx port
// may publish, with no account and no identity. Until this file existed the
// only bounds on that publisher were the per-object size ceiling and the
// grammar, so a single source could put unbounded volume onto the fabric at
// no cost to itself and at the cost of every consumer that elected the topic
// (an empty election matches every topic, so an aggregator receives and is
// billed for all of it).
//
// Two tiers live here, and a third lives at the delivery edge:
//
//	tier 1  per-source, at each door        (this file)
//	tier 2  whole-plane, at each door       (this file)
//	tier 3  per-consumer, at the listener   (shard-listener-1bsv)
//
// Tiers 1 and 2 bound what ONE door will emit. They do not bound a flood
// spread across many doors: ingress is anycast, so an attacker reaches every
// door and each one independently permits its own budget. The fabric ceiling
// under a distributed flood is therefore (doors x tier-2 budget), which is a
// number the operator chooses rather than an unbounded one. Tier 3 is what
// protects a consumer, because every door's traffic converges on the single
// BEEF group that each delivery edge joins: a per-consumer budget there sheds
// the aggregate no matter how many doors produced it, and a shed object is
// not metered and therefore not billed.
//
// Relay and spine re-emission is never rate limited. Those frames were
// admitted at the door where they entered and were counted against that
// door's budget; limiting them again would drop legitimately admitted fabric
// traffic in the middle of the network.
//
// Every budget is off by default (zero rate = unlimited). A library must not
// silently start discarding a deployment's traffic on upgrade; the operated
// build sets its own defensive defaults.
package forwarder

import (
	"net"
	"sync"
	"time"
)

// bucket is a token bucket over an arbitrary unit (objects or bytes). A zero
// rate means unlimited and every call is permitted without taking a lock on
// the refill path.
type bucket struct {
	mu     sync.Mutex
	rate   float64 // tokens per second; 0 = unlimited
	burst  float64 // bucket depth
	tokens float64
	last   time.Time
}

func newBucket(ratePerSec, burst float64) *bucket {
	if ratePerSec <= 0 {
		return &bucket{}
	}
	if burst < ratePerSec {
		// A burst below the sustained rate would make the steady state
		// unreachable: a single object larger than the depth could never be
		// admitted no matter how long the caller waited.
		burst = ratePerSec
	}
	return &bucket{rate: ratePerSec, burst: burst, tokens: burst}
}

// allowN reports whether n tokens are available, consuming them if so. An
// unlimited bucket always allows. A request larger than the whole bucket
// depth is admitted when the bucket is full, so an object at the size ceiling
// is never permanently rejected by a smaller byte budget.
func (b *bucket) allowN(now time.Time, n float64) bool {
	if b == nil || b.rate <= 0 {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.last.IsZero() {
		b.last = now
	}
	if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens += elapsed * b.rate
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
		b.last = now
	}
	if n > b.burst {
		n = b.burst
	}
	if b.tokens < n {
		return false
	}
	b.tokens -= n
	return true
}

// BEEFRateConfig bounds BEEF ingress. Zero in any field disables that budget.
type BEEFRateConfig struct {
	// SourceObjectsPerSec and SourceBytesPerSec bound ONE source. The key is
	// the source address masked to SourcePrefixBits, because a single host
	// owns an entire IPv6 prefix: limiting per address is evaded by using the
	// next address in the same /64.
	SourceObjectsPerSec float64
	SourceBytesPerSec   float64
	SourcePrefixBits    int

	// PlaneObjectsPerSec and PlaneBytesPerSec bound this door's whole BEEF
	// plane, across every source. This is the tier that survives a flood from
	// many source prefixes at one door.
	PlaneObjectsPerSec float64
	PlaneBytesPerSec   float64

	// MaxTrackedSources caps the per-source table so the limiter itself
	// cannot be memory-exhausted by a source-address flood. When the table is
	// full the oldest entry is evicted.
	MaxTrackedSources int
}

// beefLimiter enforces a BEEFRateConfig. The zero value permits everything.
type beefLimiter struct {
	cfg BEEFRateConfig

	planeObj *bucket
	planeByt *bucket

	mu   sync.Mutex
	srcs map[string]*srcBuckets
}

type srcBuckets struct {
	obj  *bucket
	byt  *bucket
	seen time.Time
}

func newBEEFLimiter(cfg BEEFRateConfig) *beefLimiter {
	if cfg.SourcePrefixBits <= 0 || cfg.SourcePrefixBits > 128 {
		cfg.SourcePrefixBits = 64
	}
	if cfg.MaxTrackedSources <= 0 {
		cfg.MaxTrackedSources = 65536
	}
	return &beefLimiter{
		cfg:      cfg,
		planeObj: newBucket(cfg.PlaneObjectsPerSec, cfg.PlaneObjectsPerSec),
		planeByt: newBucket(cfg.PlaneBytesPerSec, cfg.PlaneBytesPerSec),
		srcs:     make(map[string]*srcBuckets),
	}
}

// enabled reports whether any budget is set. Callers skip the whole path when
// it is not, so an unconfigured deployment pays nothing.
func (l *beefLimiter) enabled() bool {
	if l == nil {
		return false
	}
	c := l.cfg
	return c.SourceObjectsPerSec > 0 || c.SourceBytesPerSec > 0 ||
		c.PlaneObjectsPerSec > 0 || c.PlaneBytesPerSec > 0
}

// rateDecision names which budget refused an object, for the drop reason.
type rateDecision int

const (
	rateAllow rateDecision = iota
	rateDenySource
	rateDenyPlane
)

func (d rateDecision) reason() string {
	switch d {
	case rateDenySource:
		return "beef_rate_source"
	case rateDenyPlane:
		return "beef_rate_plane"
	default:
		return ""
	}
}

// allow charges one object of n bytes from src against both tiers. A nil src
// is relay or spine re-emission and is never limited (see the file comment).
//
// The plane tier is charged FIRST and only when the source tier permits, so a
// source already over its own budget cannot drain the shared budget and deny
// every other publisher at this door.
func (l *beefLimiter) allow(src net.Addr, n int, now time.Time) rateDecision {
	if !l.enabled() || src == nil {
		return rateAllow
	}
	if !l.allowSource(src, n, now) {
		return rateDenySource
	}
	if !l.planeObj.allowN(now, 1) || !l.planeByt.allowN(now, float64(n)) {
		return rateDenyPlane
	}
	return rateAllow
}

func (l *beefLimiter) allowSource(src net.Addr, n int, now time.Time) bool {
	if l.cfg.SourceObjectsPerSec <= 0 && l.cfg.SourceBytesPerSec <= 0 {
		return true
	}
	key := sourceKey(src, l.cfg.SourcePrefixBits)

	l.mu.Lock()
	sb := l.srcs[key]
	if sb == nil {
		if len(l.srcs) >= l.cfg.MaxTrackedSources {
			l.evictOldestLocked()
		}
		sb = &srcBuckets{
			obj: newBucket(l.cfg.SourceObjectsPerSec, l.cfg.SourceObjectsPerSec),
			byt: newBucket(l.cfg.SourceBytesPerSec, l.cfg.SourceBytesPerSec),
		}
		l.srcs[key] = sb
	}
	sb.seen = now
	l.mu.Unlock()

	return sb.obj.allowN(now, 1) && sb.byt.allowN(now, float64(n))
}

// evictOldestLocked drops the least recently charged source. The caller holds
// l.mu. Eviction is a scan rather than a heap because the table is bounded and
// this path is only reached when it is full.
func (l *beefLimiter) evictOldestLocked() {
	var oldestKey string
	var oldest time.Time
	for k, v := range l.srcs {
		if oldest.IsZero() || v.seen.Before(oldest) {
			oldest, oldestKey = v.seen, k
		}
	}
	if oldestKey != "" {
		delete(l.srcs, oldestKey)
	}
}

// sourceKey masks an address to a prefix so a whole IPv6 allocation shares one
// budget. An address that does not parse falls back to its string form: every
// such source then shares a single budget, which is deliberate. An address we
// cannot attribute is exactly the one we should not hand its own allowance.
func sourceKey(src net.Addr, prefixBits int) string {
	ip := addrToIP(src)
	if ip == nil {
		return src.String()
	}
	if v4 := ip.To4(); v4 != nil {
		// A v4 source is one host; mask to /32 unless the operator asked for
		// something coarser than the v6 default would imply.
		bits := prefixBits - 96
		if bits < 0 || bits > 32 {
			bits = 32
		}
		return v4.Mask(net.CIDRMask(bits, 32)).String()
	}
	return ip.Mask(net.CIDRMask(prefixBits, 128)).String()
}

func addrToIP(a net.Addr) net.IP {
	switch v := a.(type) {
	case *net.UDPAddr:
		return v.IP
	case *net.TCPAddr:
		return v.IP
	case *net.IPAddr:
		return v.IP
	}
	host, _, err := net.SplitHostPort(a.String())
	if err != nil {
		return net.ParseIP(a.String())
	}
	return net.ParseIP(host)
}

// SetBEEFRateLimit installs the ingress budgets. Must be called before workers
// start. Passing a zero config removes every budget.
func (fw *Forwarder) SetBEEFRateLimit(cfg BEEFRateConfig) {
	fw.beefLimit = newBEEFLimiter(cfg)
}
