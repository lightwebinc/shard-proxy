package forwarder

import (
	"net"
	"sync/atomic"
	"testing"
)

// fakeDedup implements TxidDedup. claimsByTxid maps TxID[0] → callable that
// returns (claimed, err) so each test can program the desired race outcome.
type fakeDedup struct {
	claimed atomic.Int32
	denied  atomic.Int32
	allow   bool // true: first caller wins; subsequent callers denied
	seen    map[[32]byte]struct{}
}

func newFakeDedup(allow bool) *fakeDedup {
	return &fakeDedup{allow: allow, seen: make(map[[32]byte]struct{})}
}

func (f *fakeDedup) Claim(_ string, txid [32]byte) (bool, error) {
	if f.allow {
		if _, ok := f.seen[txid]; ok {
			f.denied.Add(1)
			return false, nil
		}
		f.seen[txid] = struct{}{}
		f.claimed.Add(1)
		return true, nil
	}
	f.denied.Add(1)
	return false, nil
}

func TestProcessV2_DedupClaimWins_ForwardProceeds(t *testing.T) {
	fw := makeForwarder()
	d := newFakeDedup(true)
	fw.SetTxidDedup(d, "test:")

	conn, _ := openLoopbackUDP(t)
	tgts := makeTargets(t, conn)
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}

	raw := buildV2Frame(t, 0xAB, 0, []byte("p1"))
	fw.Process(tgts, raw, src, 0)

	if d.claimed.Load() != 1 {
		t.Errorf("claimed = %d, want 1", d.claimed.Load())
	}
}

func TestProcessV2_DedupClaimLost_FrameDropped(t *testing.T) {
	fw := makeForwarder()
	d := newFakeDedup(false) // always denies
	fw.SetTxidDedup(d, "test:")

	conn, _ := openLoopbackUDP(t)
	tgts := makeTargets(t, conn)
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}

	// Build a frame where SeqNum is zero (proxy would normally stamp it).
	raw := buildV2Frame(t, 0xAB, 0, []byte("p1"))
	fw.Process(tgts, raw, src, 0)

	if d.denied.Load() != 1 {
		t.Errorf("denied = %d, want 1", d.denied.Load())
	}
	// SeqNum must remain zero — frame was dropped before stamping.
	if raw[55] != 0 {
		t.Errorf("frame was stamped despite dedup loss (byte 55 = 0x%x)", raw[55])
	}
}

func TestProcessV2_SecondCallDedups(t *testing.T) {
	fw := makeForwarder()
	d := newFakeDedup(true)
	fw.SetTxidDedup(d, "test:")

	conn, _ := openLoopbackUDP(t)
	tgts := makeTargets(t, conn)
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}

	// Two frames with the same TxID — second must be deduped.
	raw1 := buildV2Frame(t, 0xCD, 0, []byte("p1"))
	raw2 := buildV2Frame(t, 0xCD, 0, []byte("p2"))
	fw.Process(tgts, raw1, src, 0)
	fw.Process(tgts, raw2, src, 0)

	if d.claimed.Load() != 1 {
		t.Errorf("claimed = %d, want 1 (first only)", d.claimed.Load())
	}
	if d.denied.Load() != 1 {
		t.Errorf("denied = %d, want 1 (second)", d.denied.Load())
	}
}

func TestProcessV1_DedupBypassed(t *testing.T) {
	fw := makeForwarder()
	d := newFakeDedup(false) // would deny if asked
	fw.SetTxidDedup(d, "test:")

	conn, _ := openLoopbackUDP(t)
	tgts := makeTargets(t, conn)
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}

	raw := buildV1Frame(t, 0xFF, []byte("legacy"))
	fw.Process(tgts, raw, src, 0)

	if d.claimed.Load() != 0 || d.denied.Load() != 0 {
		t.Errorf("BRC-12 frame should bypass dedup, got claimed=%d denied=%d",
			d.claimed.Load(), d.denied.Load())
	}
}
