package forwarder

import (
	"net"
	"testing"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/shard-common/shard"
	"github.com/lightwebinc/shard-proxy/metrics"
)

// buildV2FrameTxID builds a V2 frame carrying payload under an explicit TxID,
// so a test can stamp the honest id or a forged one.
func buildV2FrameTxID(t *testing.T, txid [32]byte, seqNum uint64, payload []byte) []byte {
	t.Helper()
	f := &frame.Frame{
		Version: frame.FrameVerV2,
		SeqNum:  seqNum,
		TxID:    txid,
		Payload: payload,
	}
	buf := make([]byte, frame.HeaderSize+len(payload))
	n, err := frame.Encode(f, buf)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return buf[:n]
}

// honestEFFrame is an EF transaction framed under its own canonical TxID.
func honestEFFrame(t *testing.T, seqNum uint64) ([]byte, []byte, [32]byte) {
	t.Helper()
	tx := buildBareEF()
	id, err := objfmt.TxID(tx)
	if err != nil {
		t.Fatalf("TxID: %v", err)
	}
	return buildV2FrameTxID(t, id, seqNum, tx), tx, id
}

// makeForwarderRec is makeForwarder with a recorder attached, so drop-reason
// counters are observable.
func makeForwarderRec(rec *metrics.Recorder) *Forwarder {
	engine := shard.New(0xFF05, shard.DefaultGroupID, 8)
	return New(engine, 0xFF05, shard.DefaultGroupID, 9001, false, rec)
}

// dropReasonCount scrapes bsp_packets_dropped_total for one reason label.
func dropReasonCount(t *testing.T, rec *metrics.Recorder, reason string) float64 {
	t.Helper()
	mfs, err := rec.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	total := 0.0
	for _, mf := range mfs {
		if mf.GetName() != "bsp_packets_dropped_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "reason" && l.GetValue() == reason {
					total += m.GetCounter().GetValue()
				}
			}
		}
	}
	return total
}

// TestVerifyPayloadHash covers the gate's admit/drop matrix. The EF case is
// the one that matters most: the canonical id excludes the EF extras, so a
// gate that hashed the raw payload bytes would drop every honest EF frame on
// an EF-native fabric rather than none.
func TestVerifyPayloadHash(t *testing.T) {
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}

	t.Run("default_off_mismatch_forwards", func(t *testing.T) {
		fw := makeForwarder() // verifyPayloadHash off
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		fw.Process(egr, buildV2FrameTxID(t, [32]byte{0xAB}, 0, buildBareEF()), src, 0)
		if got := countEnqueued(egr); got != 1 {
			t.Errorf("%d queued, want 1 (gate off: mismatch forwarded verbatim)", got)
		}
	})

	t.Run("honest_ef_forwards", func(t *testing.T) {
		fw := makeForwarder()
		fw.SetVerifyPayloadHash(true)
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		raw, _, _ := honestEFFrame(t, 0)
		fw.Process(egr, raw, src, 0)
		if got := countEnqueued(egr); got != 1 {
			t.Errorf("%d queued, want 1 (honest EF frame: id excludes EF extras)", got)
		}
	})

	t.Run("honest_raw_forwards", func(t *testing.T) {
		fw := makeForwarder()
		fw.SetVerifyPayloadHash(true)
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		tx := buildBareTx() // standard (non-EF) serialization
		id, err := objfmt.TxID(tx)
		if err != nil {
			t.Fatalf("TxID: %v", err)
		}
		fw.Process(egr, buildV2FrameTxID(t, id, 0, tx), src, 0)
		if got := countEnqueued(egr); got != 1 {
			t.Errorf("%d queued, want 1 (honest raw payload)", got)
		}
	})

	t.Run("mismatch_dropped", func(t *testing.T) {
		fw := makeForwarder()
		fw.SetVerifyPayloadHash(true)
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		fw.Process(egr, buildV2FrameTxID(t, [32]byte{0xAB}, 0, buildBareEF()), src, 0)
		if got := countEnqueued(egr); got != 0 {
			t.Errorf("%d queued, want 0 (TxID does not match payload)", got)
		}
	})

	t.Run("unwalkable_payload_dropped", func(t *testing.T) {
		fw := makeForwarder()
		fw.SetVerifyPayloadHash(true)
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		// EF-marked but truncated mid-input: TxID cannot be derived at all.
		bad := buildBareEF()[:20]
		fw.Process(egr, buildV2FrameTxID(t, [32]byte{0xAB}, 0, bad), src, 0)
		if got := countEnqueued(egr); got != 0 {
			t.Errorf("%d queued, want 0 (payload does not walk as a transaction)", got)
		}
	})

	// TxID walks the transaction and ignores whatever follows it, so without
	// an explicit length bound "honest tx ‖ junk" verifies against the honest
	// id and the tail rides onto the fabric billed as payload.
	t.Run("trailing_bytes_dropped", func(t *testing.T) {
		fw := makeForwarder()
		fw.SetVerifyPayloadHash(true)
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		tx := buildBareEF()
		id, err := objfmt.TxID(tx)
		if err != nil {
			t.Fatalf("TxID: %v", err)
		}
		padded := append(append([]byte(nil), tx...), 0xFF, 0xFF, 0xFF)
		if got, err := objfmt.TxID(padded); err != nil || got != id {
			t.Fatalf("fixture assumption broken: padded id %x != %x (err %v)", got, id, err)
		}
		fw.Process(egr, buildV2FrameTxID(t, id, 0, padded), src, 0)
		if got := countEnqueued(egr); got != 0 {
			t.Errorf("%d queued, want 0 (payload must be exactly one transaction)", got)
		}
	})

	t.Run("v1_forwarded_regardless", func(t *testing.T) {
		fw := makeForwarder()
		fw.SetVerifyPayloadHash(true)
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		fw.Process(egr, buildV1Frame(t, 0xAB, []byte("legacy")), src, 0)
		if got := countEnqueued(egr); got != 1 {
			t.Errorf("%d queued, want 1 (BRC-12 carries no payload-bound id)", got)
		}
	})

	// SeqNum is chosen by whoever sent the frame. requireEF exempts stamped
	// frames as a relay optimisation; this gate must not, or setting a
	// non-zero SeqNum is a one-byte bypass.
	t.Run("stamped_frame_not_exempt", func(t *testing.T) {
		fw := makeForwarder()
		fw.SetVerifyPayloadHash(true)
		// Admit stamped input, so the drop below is the verify gate's doing
		// and not the stamped-ingress gate's.
		fw.SetAllowStampedIngress(true)
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		fw.Process(egr, buildV2FrameTxID(t, [32]byte{0xAB}, 42, buildBareEF()), src, 0)
		if got := countEnqueued(egr); got != 0 {
			t.Errorf("%d queued, want 0 (a pre-stamped frame must still be verified)", got)
		}
	})

	// The proxy derives a bare submission's TxID itself, so the gate has
	// nothing to check and must not charge a second hash for it.
	t.Run("bare_submission_unaffected", func(t *testing.T) {
		fw := makeForwarder()
		fw.SetVerifyPayloadHash(true)
		fw.SetRequireEF(true)
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		fw.DispatchClass(egr, buildBareEF(), src, 0, IngressTransaction)
		if got := countEnqueued(egr); got != 1 {
			t.Errorf("%d queued, want 1 (bare submission is self-framed)", got)
		}
	})

	t.Run("mismatch_records_drop_reason", func(t *testing.T) {
		rec, err := metrics.New("test", 1, "", 0)
		if err != nil {
			t.Fatalf("metrics.New: %v", err)
		}
		fw := makeForwarderRec(rec)
		fw.SetVerifyPayloadHash(true)
		conn, _ := openLoopbackUDP(t)
		egr := NewEgress(fw, makeTargets(t, conn), 8, rec)
		t.Cleanup(egr.Flush)
		fw.Process(egr, buildV2FrameTxID(t, [32]byte{0xAB}, 0, buildBareEF()), src, 0)
		if got := dropReasonCount(t, rec, "payload_hash_mismatch"); got != 1 {
			t.Errorf("payload_hash_mismatch = %v, want 1", got)
		}
	})
}

// TestVerifyPayloadHash_GateRunsBeforeDedupClaim is the reason the gate exists
// on the proxy at all rather than only on the listener. The ingress dedup
// claim is keyed on the wire TxID, so a forged frame that reached the claim
// first would burn the honest transaction's slot and the honest frame would be
// suppressed here, never reaching a listener, where the equivalent check
// could no longer help it.
func TestVerifyPayloadHash_GateRunsBeforeDedupClaim(t *testing.T) {
	fw := makeForwarder()
	fw.SetVerifyPayloadHash(true)
	d := newFakeDedup(true) // first claimant wins
	fw.SetTxidDedup(d, "test:")

	conn, _ := openLoopbackUDP(t)
	egr := makeEgress(t, fw, conn)
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}

	honest, tx, id := honestEFFrame(t, 0)

	// Attacker: the honest TxID over a genuinely different transaction (same
	// shape, different prev-txid: offset 11 is the first prev-txid byte).
	other := append([]byte(nil), tx...)
	other[11] = 0x99
	if otherID, err := objfmt.TxID(other); err != nil || otherID == id {
		t.Fatalf("fixture is not a distinct transaction: id=%x err=%v", otherID, err)
	}
	forged := buildV2FrameTxID(t, id, 0, other)
	fw.Process(egr, forged, src, 0)
	if got := countEnqueued(egr); got != 0 {
		t.Fatalf("forged frame: %d queued, want 0", got)
	}
	if got := d.claimed.Load(); got != 0 {
		t.Fatalf("forged frame took %d dedup claims, want 0, the gate must run before the claim", got)
	}

	// The honest transaction must still be able to claim its own slot.
	fw.Process(egr, honest, src, 0)
	if got := countEnqueued(egr); got != 1 {
		t.Errorf("honest frame: %d queued, want 1 (its dedup slot must not have been squatted)", got)
	}
	if got := d.claimed.Load(); got != 1 {
		t.Errorf("honest frame claims = %d, want 1", got)
	}
}
