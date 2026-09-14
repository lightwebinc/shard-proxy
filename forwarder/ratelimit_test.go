package forwarder

import (
	"fmt"
	"net"
	"testing"
	"time"
)

func udp(ip string) *net.UDPAddr { return &net.UDPAddr{IP: net.ParseIP(ip), Port: 9999} }

// TestBEEFLimiter_Disabled: the zero config is the shipped default and must
// permit everything without allocating per-source state. A library that began
// discarding traffic on upgrade would be worse than the gap it closes.
func TestBEEFLimiter_Disabled(t *testing.T) {
	l := newBEEFLimiter(BEEFRateConfig{})
	if l.enabled() {
		t.Fatal("zero config reports enabled")
	}
	now := time.Now()
	for i := 0; i < 1000; i++ {
		if d := l.allow(udp("fd00::1"), 1<<20, now); d != rateAllow {
			t.Fatalf("unlimited limiter denied at %d: %v", i, d)
		}
	}
	if len(l.srcs) != 0 {
		t.Fatalf("unlimited limiter tracked %d sources", len(l.srcs))
	}
}

// TestBEEFLimiter_SourceObjects: a single source is bounded, and refills.
func TestBEEFLimiter_SourceObjects(t *testing.T) {
	l := newBEEFLimiter(BEEFRateConfig{SourceObjectsPerSec: 10})
	now := time.Now()
	src := udp("fd00::1")

	for i := 0; i < 10; i++ {
		if d := l.allow(src, 100, now); d != rateAllow {
			t.Fatalf("object %d denied inside the burst: %v", i, d)
		}
	}
	if d := l.allow(src, 100, now); d != rateDenySource {
		t.Fatalf("11th object in the same instant = %v, want a source denial", d)
	}
	// One second later the bucket has refilled.
	if d := l.allow(src, 100, now.Add(time.Second)); d != rateAllow {
		t.Fatalf("denied after a full refill period: %v", d)
	}
}

// TestBEEFLimiter_SourceIsPerPrefix is the evasion test: a host owns a whole
// IPv6 prefix, so a per-address budget is defeated by using the next address.
// Addresses inside one /64 must share a budget, and a different /64 must not
// be affected by it.
func TestBEEFLimiter_SourceIsPerPrefix(t *testing.T) {
	l := newBEEFLimiter(BEEFRateConfig{SourceObjectsPerSec: 4, SourcePrefixBits: 64})
	now := time.Now()

	for i := 0; i < 4; i++ {
		if d := l.allow(udp(fmt.Sprintf("fd00:1::%d", i+1)), 10, now); d != rateAllow {
			t.Fatalf("address %d inside the burst denied: %v", i, d)
		}
	}
	if d := l.allow(udp("fd00:1::ffff"), 10, now); d != rateDenySource {
		t.Fatalf("a fresh address in the SAME /64 = %v, want a source denial (per-address budgets are evadable)", d)
	}
	if d := l.allow(udp("fd00:2::1"), 10, now); d != rateAllow {
		t.Fatalf("a different /64 was denied by its neighbour's budget: %v", d)
	}
}

// TestBEEFLimiter_PlaneBudget: the whole-plane tier bounds a door across many
// source prefixes, which is the flood the per-source tier cannot see.
func TestBEEFLimiter_PlaneBudget(t *testing.T) {
	l := newBEEFLimiter(BEEFRateConfig{PlaneObjectsPerSec: 5})
	now := time.Now()

	for i := 0; i < 5; i++ {
		if d := l.allow(udp(fmt.Sprintf("fd00:%d::1", i+1)), 10, now); d != rateAllow {
			t.Fatalf("prefix %d denied inside the plane burst: %v", i, d)
		}
	}
	if d := l.allow(udp("fd00:99::1"), 10, now); d != rateDenyPlane {
		t.Fatalf("a sixth distinct prefix = %v, want a plane denial", d)
	}
}

// TestBEEFLimiter_SourceDenialDoesNotDrainPlane: a source already over its own
// budget must not consume the shared budget, or one flooder would deny every
// other publisher at the door.
func TestBEEFLimiter_SourceDenialDoesNotDrainPlane(t *testing.T) {
	l := newBEEFLimiter(BEEFRateConfig{SourceObjectsPerSec: 1, PlaneObjectsPerSec: 10})
	now := time.Now()
	flood := udp("fd00:bad::1")

	if d := l.allow(flood, 10, now); d != rateAllow {
		t.Fatalf("first object from the flooder denied: %v", d)
	}
	for i := 0; i < 50; i++ {
		if d := l.allow(flood, 10, now); d != rateDenySource {
			t.Fatalf("flooder attempt %d = %v, want a source denial", i, d)
		}
	}
	// The plane budget spent exactly one token on the flooder's single
	// admitted object, so nine remain. Each innocent publisher is a distinct
	// prefix so that its own per-source budget (also 1) is not what is being
	// measured here.
	for i := 0; i < 9; i++ {
		src := udp(fmt.Sprintf("fd0d:%d::1", i))
		if d := l.allow(src, 10, now); d != rateAllow {
			t.Fatalf("innocent publisher %d denied: %v - the flooder drained the shared budget", i, d)
		}
	}
	// And the tenth finds the plane budget legitimately spent.
	if d := l.allow(udp("fd0d:9::1"), 10, now); d != rateDenyPlane {
		t.Fatalf("plane budget = %v after 10 admitted objects, want a plane denial", d)
	}
}

// TestBEEFLimiter_RelayExempt: relay and spine re-emission arrives with a nil
// source. Those frames were charged at the door where they entered; charging
// them again would drop admitted traffic mid-fabric.
func TestBEEFLimiter_RelayExempt(t *testing.T) {
	l := newBEEFLimiter(BEEFRateConfig{SourceObjectsPerSec: 1, PlaneObjectsPerSec: 1})
	now := time.Now()
	for i := 0; i < 100; i++ {
		if d := l.allow(nil, 1<<20, now); d != rateAllow {
			t.Fatalf("relay frame %d was rate limited: %v", i, d)
		}
	}
}

// TestBEEFLimiter_TrackedSourcesBounded: the limiter must not become the
// memory-exhaustion vector it exists to prevent.
func TestBEEFLimiter_TrackedSourcesBounded(t *testing.T) {
	const cap = 64
	l := newBEEFLimiter(BEEFRateConfig{SourceObjectsPerSec: 1000, MaxTrackedSources: cap})
	now := time.Now()
	for i := 0; i < cap*8; i++ {
		l.allow(udp(fmt.Sprintf("fd00:%x::1", i)), 10, now.Add(time.Duration(i)*time.Millisecond))
	}
	if len(l.srcs) > cap {
		t.Fatalf("tracked %d sources, cap is %d", len(l.srcs), cap)
	}
}

// TestBEEFLimiter_ObjectLargerThanBurst: an object at the size ceiling must
// not be permanently unpublishable because the byte budget's depth is smaller
// than it. It waits for a full bucket, then goes.
func TestBEEFLimiter_ObjectLargerThanBurst(t *testing.T) {
	l := newBEEFLimiter(BEEFRateConfig{SourceBytesPerSec: 1000})
	now := time.Now()
	if d := l.allow(udp("fd00::1"), 1<<20, now); d != rateAllow {
		t.Fatalf("an object larger than the whole budget was refused from a full bucket: %v", d)
	}
	if d := l.allow(udp("fd00::1"), 1<<20, now); d != rateDenySource {
		t.Fatalf("a second oversized object in the same instant = %v, want a denial", d)
	}
}

// TestProcessBEEF_RateLimitEndToEnd drives the real admission path, not the
// limiter in isolation: a flood from one source is cut off, and a publisher in
// a different prefix still gets through.
func TestProcessBEEF_RateLimitEndToEnd(t *testing.T) {
	fw, _ := makeBEEFForwarder(t)
	conn, _ := openLoopbackUDP(t)
	egr := makeEgress(t, fw, conn)
	fw.SetBEEFRateLimit(BEEFRateConfig{SourceObjectsPerSec: 3})

	flood := udp("fd00:bad::1")
	for i := 0; i < 20; i++ {
		obj := append([]byte{0x01, 0x00, 0xBE, 0xEF}, byte(i))
		fw.ProcessBEEF(egr, buildBEEFFrameBytes(t, "tm_flood", obj), flood, 0)
	}
	frames, _ := captureEnqueued(egr)
	if len(frames) != 3 {
		t.Fatalf("flood put %d frames on the plane, want the 3-object burst", len(frames))
	}

	// A different prefix has its own budget and is unaffected.
	fw.ProcessBEEF(egr, buildBEEFFrameBytes(t, "tm_ok", beefTestObj), udp("fd00:cafe::1"), 0)
	if frames, _ := captureEnqueued(egr); len(frames) != 1 {
		t.Fatalf("innocent publisher got %d frames through, want 1", len(frames))
	}
}
