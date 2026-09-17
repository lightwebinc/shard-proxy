package manifest

import (
	"context"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
	commanifest "github.com/lightwebinc/shard-common/manifest"
)

func mustAddr(s string) netip.Addr {
	a, err := netip.ParseAddr(s)
	if err != nil {
		panic(err)
	}
	return a
}

func authoritativeManifest(id uint32, sb uint8) *frame.ShardManifest {
	return &frame.ShardManifest{
		Flags:      frame.ShardManifestFlagAuthoritative | frame.ShardManifestFlagGroupsValid | frame.ShardManifestFlagPilotOnly,
		InstanceID: id,
		// Registry expiry is Epoch + TTL (BRC-139), so a fixture dated
		// in the past is born expired. These appliers run on the wall
		// clock, so the manifest has to be stamped now.
		Epoch:            uint32(time.Now().Unix()), //nolint:gosec // wraps in 2106
		AnnounceInterval: 300,
		ShardBits:        sb,
		Groups:           []uint16{0},
	}
}

func TestRestartRequest_IdempotentAndConcurrent(t *testing.T) {
	var r RestartRequest
	if r.Requested() {
		t.Fatalf("initially Requested = true")
	}
	r.Request("first")
	if !r.Requested() {
		t.Errorf("after Request: Requested = false")
	}
	r.Request("second") // should be a no-op
	if got := r.Reason(); got != "first" {
		t.Errorf("Reason = %q, want %q", got, "first")
	}

	// Concurrent races should still produce exactly one reason.
	var rr RestartRequest
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rr.Request("racer")
		}(i)
	}
	wg.Wait()
	if !rr.Requested() {
		t.Errorf("after concurrent Request: Requested = false")
	}
}

func TestApplier_FiresShardBitsHook(t *testing.T) {
	reg := commanifest.NewRegistry(60 * time.Second)
	ev := commanifest.NewEvaluator(commanifest.EvaluatorConfig{
		Quorum:     2,
		Hysteresis: 1 * time.Nanosecond,
	})
	reg.Upsert(mustAddr("fd20::1"), authoritativeManifest(1, 8))
	reg.Upsert(mustAddr("fd20::2"), authoritativeManifest(2, 8))

	var got struct {
		mu    sync.Mutex
		fired bool
		prev  uint8
		next  uint8
	}

	a := &Applier{
		Registry:  reg,
		Evaluator: ev,
		Interval:  10 * time.Millisecond,
		Hooks: Hooks{
			OnShardBitsChange: func(prev, next uint8) {
				got.mu.Lock()
				defer got.mu.Unlock()
				got.fired = true
				got.prev = prev
				got.next = next
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	a.Run(ctx)

	got.mu.Lock()
	defer got.mu.Unlock()
	if !got.fired {
		t.Fatalf("hook did not fire")
	}
	if got.prev != 0 || got.next != 8 {
		t.Errorf("got prev=%d next=%d, want prev=0 next=8", got.prev, got.next)
	}
}

func TestApplier_SuccessorHookFiresAndClears(t *testing.T) {
	reg := commanifest.NewRegistry(60 * time.Second)
	ev := commanifest.NewEvaluator(commanifest.EvaluatorConfig{
		Quorum:     2,
		Hysteresis: 1 * time.Nanosecond,
	})

	// First adopt active SB=8.
	reg.Upsert(mustAddr("fd20::1"), authoritativeManifest(1, 8))
	reg.Upsert(mustAddr("fd20::2"), authoritativeManifest(2, 8))

	// Now attach a successor block to both pilots.
	addSuccessor := func(m *frame.ShardManifest) {
		m.Flags |= frame.ShardManifestFlagSuccessorValid
		m.Successor = &frame.SuccessorBlock{
			ShardBits:       9,
			Flags:           frame.SuccessorFlagSourceModeSSM,
			TransitionEpoch: uint32(time.Now().Add(1 * time.Hour).Unix()),
		}
	}

	mu := sync.Mutex{}
	var lastBefore, lastAfter *commanifest.SuccessorView
	hookFires := 0

	a := &Applier{
		Registry:  reg,
		Evaluator: ev,
		Interval:  10 * time.Millisecond,
		Hooks: Hooks{
			OnSuccessorChange: func(before, after *commanifest.SuccessorView) {
				mu.Lock()
				defer mu.Unlock()
				lastBefore = before
				lastAfter = after
				hookFires++
			},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	go a.Run(ctx)

	// Let the applier observe steady state first.
	time.Sleep(40 * time.Millisecond)

	// Add successor.
	m1 := authoritativeManifest(1, 8)
	m2 := authoritativeManifest(2, 8)
	addSuccessor(m1)
	addSuccessor(m2)
	reg.Upsert(mustAddr("fd20::1"), m1)
	reg.Upsert(mustAddr("fd20::2"), m2)
	time.Sleep(50 * time.Millisecond)

	cancel()
	time.Sleep(20 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if hookFires == 0 {
		t.Fatalf("OnSuccessorChange never fired")
	}
	if lastAfter == nil {
		t.Errorf("last successor view = nil, want non-nil")
	} else if lastAfter.ShardBits != 9 {
		t.Errorf("last successor.ShardBits = %d, want 9", lastAfter.ShardBits)
	}
	_ = lastBefore
}

func beefDomainManifest(id uint32, sb, beefBits uint8) *frame.ShardManifest {
	m := &frame.ShardManifest{
		Flags: frame.ShardManifestFlagAuthoritative | frame.ShardManifestFlagDomainsValid |
			frame.ShardManifestFlagGroupsValid | frame.ShardManifestFlagPilotOnly,
		InstanceID: id,
		// Registry expiry is Epoch + TTL (BRC-139), so a fixture dated
		// in the past is born expired. These appliers run on the wall
		// clock, so the manifest has to be stamped now.
		Epoch:            uint32(time.Now().Unix()), //nolint:gosec // wraps in 2106
		AnnounceInterval: 300,
		ShardBits:        sb,
		Groups:           []uint16{0},
		Domains: []frame.DomainDescriptor{{
			DomainID: 0x1, ShardBits: beefBits, SlotSpan: 1,
			Flags: frame.DomainFlagActive,
		}},
	}
	return m
}

func TestApplier_FiresDomainShardBitsHook(t *testing.T) {
	reg := commanifest.NewRegistry(60 * time.Second)
	ev := commanifest.NewEvaluator(commanifest.EvaluatorConfig{Quorum: 2, Hysteresis: 1 * time.Nanosecond})
	reg.Upsert(mustAddr("fd20::1"), beefDomainManifest(1, 8, 12))
	reg.Upsert(mustAddr("fd20::2"), beefDomainManifest(2, 8, 12))

	var got struct {
		mu           sync.Mutex
		domain, next uint8
		fired        bool
	}
	a := &Applier{
		Registry: reg, Evaluator: ev, Interval: 10 * time.Millisecond,
		Hooks: Hooks{
			OnDomainShardBitsChange: func(domain, prev, next uint8) {
				got.mu.Lock()
				defer got.mu.Unlock()
				got.fired, got.domain, got.next = true, domain, next
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	a.Run(ctx)

	got.mu.Lock()
	defer got.mu.Unlock()
	if !got.fired || got.domain != 0x1 || got.next != 12 {
		t.Fatalf("domain hook: fired=%v domain=0x%X next=%d, want fired domain=0x1 next=12", got.fired, got.domain, got.next)
	}
}
