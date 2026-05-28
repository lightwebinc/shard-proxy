package forwarder

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
)

// TestNextSeq_ConcurrentSameFlow_MonotonicNoGaps verifies that concurrent
// invocations of nextSeq for the same (sender, group, subtree) flow produce
// each integer in [1, N] exactly once — i.e. the atomic counter never
// double-issues or skips, even though many goroutines race through the
// stripe lock and the post-lock atomic increment.
func TestNextSeq_ConcurrentSameFlow_MonotonicNoGaps(t *testing.T) {
	const goroutines = 32
	const perGoroutine = 1000
	const totalCalls = goroutines * perGoroutine

	fw := makeForwarder()
	src := &net.UDPAddr{IP: net.ParseIP("fd20::1"), Port: 12345}
	ip := addrToIPv6(src)
	var sub [32]byte
	const groupIdx uint32 = 7

	seen := make([]atomic.Uint32, totalCalls+1) // index 0 unused; seqNums start at 1
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				_, seq := fw.nextSeq(ip, groupIdx, sub)
				if seq < 1 || seq > totalCalls {
					t.Errorf("seqNum %d out of range [1, %d]", seq, totalCalls)
					return
				}
				seen[seq].Add(1)
			}
		}()
	}
	wg.Wait()

	// Every integer in [1, totalCalls] must have been issued exactly once.
	for i := 1; i <= totalCalls; i++ {
		if got := seen[i].Load(); got != 1 {
			t.Errorf("seqNum %d issued %d times, want 1", i, got)
			break
		}
	}
}

// TestNextSeq_ConcurrentDistinctFlows_IndependentCounters runs many
// goroutines each driving a *distinct* flow (different sender IPs) so they
// land on different stripes and never interfere. Each flow must end at
// exactly perGoroutine.
func TestNextSeq_ConcurrentDistinctFlows_IndependentCounters(t *testing.T) {
	const goroutines = 64
	const perGoroutine = 500

	fw := makeForwarder()
	var sub [32]byte
	const groupIdx uint32 = 11

	finalSeqs := make([]uint64, goroutines)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			// Encode the goroutine id into the sender IP so each
			// goroutine owns a distinct flow.
			var ip [16]byte
			ip[0] = 0xFD
			ip[1] = 0x20
			ip[14] = byte(idx >> 8)
			ip[15] = byte(idx)

			var last uint64
			for i := 0; i < perGoroutine; i++ {
				_, seq := fw.nextSeq(ip, groupIdx, sub)
				last = seq
			}
			finalSeqs[idx] = last
		}(g)
	}
	wg.Wait()

	for i, got := range finalSeqs {
		if got != perGoroutine {
			t.Errorf("flow %d final seq = %d, want %d", i, got, perGoroutine)
		}
	}
}

// TestStripeIndex_NotAllSameBucket sanity-checks that distinct IPs map to a
// variety of stripes — if every IP landed in the same bucket the lock
// striping would be ineffective. We require at least 16 distinct stripes
// across 256 sender IPs.
func TestStripeIndex_NotAllSameBucket(t *testing.T) {
	seen := make(map[uint8]struct{})
	for i := 0; i < 256; i++ {
		var ip [16]byte
		ip[0] = 0xFD
		ip[1] = 0x20
		ip[15] = byte(i)
		seen[stripeIndex(ip)] = struct{}{}
	}
	if len(seen) < 16 {
		t.Errorf("stripeIndex distributed across only %d/%d stripes; expected ≥16",
			len(seen), chainStripes)
	}
}
