package forwarder

import (
	"fmt"
	"testing"
)

// boundedSet is a dedup store with a hard capacity and FIFO eviction: the
// shape of every real store's local tier. Capacity is what makes the shared
// namespace a hazard, so the fake models capacity and nothing else.
type boundedSet struct {
	cap   int
	order []string
	seen  map[string]bool
}

func newBoundedSet(capacity int) *boundedSet {
	return &boundedSet{cap: capacity, seen: map[string]bool{}}
}

func (b *boundedSet) Claim(prefix string, txid [32]byte) (bool, error) {
	k := prefix + string(txid[:])
	if b.seen[k] {
		return false, nil // already claimed: suppress
	}
	if len(b.order) >= b.cap {
		delete(b.seen, b.order[0])
		b.order = b.order[1:]
	}
	b.seen[k] = true
	b.order = append(b.order, k)
	return true, nil
}

func key(n int) [32]byte {
	var k [32]byte
	copy(k[:], fmt.Sprintf("%032d", n))
	return k
}

// TestBEEFFloodEvictsTxClaims_WhenShared is the hazard itself, stated as a
// test: a shared bounded store means an open-class object flood evicts
// TRANSACTION claims, and an evicted claim is a lost claim, so the next copy
// of that transaction wins again and is re-emitted onto the fabric. The flood
// arrives on the object plane and the damage lands on the paying one.
func TestBEEFFloodEvictsTxClaims_WhenShared(t *testing.T) {
	fw := &Forwarder{}
	shared := newBoundedSet(100)
	fw.SetTxidDedup(shared, "bsp:")
	// No SetBEEFDedup: the historical shared-store shape.

	tx := key(1)
	if !fw.claimIngress(tx, "brc124", "eth0", 0) {
		t.Fatal("first sighting of a transaction was suppressed")
	}
	// An object-plane flood, well inside what a single source may publish.
	for i := 0; i < 500; i++ {
		fw.claimBEEFIngress(key(1000+i), "brc148", "eth0", 0)
	}
	// The transaction's claim is gone, so a duplicate now re-emits.
	if fw.claimIngress(tx, "brc124", "eth0", 0) {
		t.Log("CONFIRMED: the BEEF flood evicted the transaction claim, so a " +
			"duplicate transaction would be re-stamped and re-emitted")
	} else {
		t.Fatal("shared store unexpectedly retained the claim; the fixture no longer models the hazard")
	}
}

// TestBEEFFloodCannotEvictTxClaims_WhenSplit is the fix: with its own store the
// object plane's churn is bounded to its own capacity, and the transaction
// plane's claims survive a flood many times its size.
func TestBEEFFloodCannotEvictTxClaims_WhenSplit(t *testing.T) {
	fw := &Forwarder{}
	fw.SetTxidDedup(newBoundedSet(100), "bsp:tx:")
	fw.SetBEEFDedup(newBoundedSet(100), "bsp:beef:")

	tx := key(1)
	if !fw.claimIngress(tx, "brc124", "eth0", 0) {
		t.Fatal("first sighting of a transaction was suppressed")
	}
	for i := 0; i < 5000; i++ {
		fw.claimBEEFIngress(key(1000+i), "brc148", "eth0", 0)
	}
	if fw.claimIngress(tx, "brc124", "eth0", 0) {
		t.Fatal("a BEEF flood evicted a transaction claim despite separate stores")
	}
}

// TestBEEFDedupUnsetFallsBackToTxStore keeps the upgrade safe: a build that
// never calls SetBEEFDedup must still dedup BEEF, not silently stop.
func TestBEEFDedupUnsetFallsBackToTxStore(t *testing.T) {
	fw := &Forwarder{}
	fw.SetTxidDedup(newBoundedSet(10), "bsp:")

	k := key(7)
	if !fw.claimBEEFIngress(k, "brc148", "eth0", 0) {
		t.Fatal("first sighting suppressed")
	}
	if fw.claimBEEFIngress(k, "brc148", "eth0", 0) {
		t.Fatal("duplicate BEEF object was admitted: dedup stopped working when the BEEF store was unset")
	}
}

// TestBEEFDedupSeparateNamespace: the two planes must not be able to suppress
// each other by key, even if a value ever coincided.
func TestBEEFDedupSeparateNamespace(t *testing.T) {
	fw := &Forwarder{}
	shared := newBoundedSet(1000)
	fw.SetTxidDedup(shared, "bsp:tx:")
	fw.SetBEEFDedup(shared, "bsp:beef:")

	same := key(42)
	if !fw.claimIngress(same, "brc124", "eth0", 0) {
		t.Fatal("transaction claim refused")
	}
	if !fw.claimBEEFIngress(same, "brc148", "eth0", 0) {
		t.Fatal("an object was suppressed by a TRANSACTION's claim on the same value")
	}
}
