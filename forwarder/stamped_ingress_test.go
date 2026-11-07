package forwarder

import (
	"net"
	"testing"

	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/shard-proxy/metrics"
)

// TestAllowStampedIngress covers the submission-lane posture: a frame that
// already carries a SeqNum was admitted and emitted by some other proxy, so a
// submitter presenting one is either relaying (a different deployment role) or
// probing for the gates that key off SeqNum.
func TestAllowStampedIngress(t *testing.T) {
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}

	t.Run("stamped_rejected_by_default", func(t *testing.T) {
		fw := makeForwarder()
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		fw.Process(egr, buildV2Frame(t, 0xAB, 7, []byte("tx")), src, 0)
		if got := countEnqueued(egr); got != 0 {
			t.Errorf("%d queued, want 0 (stamped input is not a submission)", got)
		}
	})

	t.Run("unstamped_admitted", func(t *testing.T) {
		fw := makeForwarder()
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		fw.Process(egr, buildV2Frame(t, 0xAB, 0, []byte("tx")), src, 0)
		if got := countEnqueued(egr); got != 1 {
			t.Errorf("%d queued, want 1 (an unstamped submission is the normal case)", got)
		}
	})

	t.Run("stamped_admitted_when_allowed", func(t *testing.T) {
		fw := makeForwarder()
		fw.SetAllowStampedIngress(true)
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		fw.Process(egr, buildV2Frame(t, 0xAB, 7, []byte("tx")), src, 0)
		if got := countEnqueued(egr); got != 1 {
			t.Errorf("%d queued, want 1 (relay/spine posture admits stamped frames)", got)
		}
	})

	// V1 decodes with a zero SeqNum, so the legacy lane is untouched either way.
	t.Run("v1_unaffected", func(t *testing.T) {
		fw := makeForwarder()
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		fw.Process(egr, buildV1Frame(t, 0xAB, []byte("legacy")), src, 0)
		if got := countEnqueued(egr); got != 1 {
			t.Errorf("%d queued, want 1 (BRC-12 carries no SeqNum)", got)
		}
	})

	t.Run("rejection_records_drop_reason", func(t *testing.T) {
		rec, err := metrics.New("test", 1, "", 0)
		if err != nil {
			t.Fatalf("metrics.New: %v", err)
		}
		fw := makeForwarderRec(rec)
		conn, _ := openLoopbackUDP(t)
		egr := NewEgress(fw, makeTargets(t, conn), 8, rec)
		t.Cleanup(egr.Flush)
		fw.Process(egr, buildV2Frame(t, 0xAB, 7, []byte("tx")), src, 0)
		if got := dropReasonCount(t, rec, "stamped_ingress"); got != 1 {
			t.Errorf("stamped_ingress = %v, want 1", got)
		}
	})
}

// TestRequireEF_AppliesToStampedFrames closes the exemption that made SeqNum a
// one-byte opt-out of the EF posture. Before, -require-ef only checked
// unstamped frames, so a non-EF payload rode in under any non-zero SeqNum.
// Enabling -allow-stamped-ingress declares the lane a relay; it must not also
// waive EF.
func TestRequireEF_AppliesToStampedFrames(t *testing.T) {
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}
	raw := buildBareTx() // standard serialization, no EF marker

	t.Run("stamped_non_ef_rejected_on_a_relay_lane", func(t *testing.T) {
		fw := makeForwarder()
		fw.SetRequireEF(true)
		fw.SetAllowStampedIngress(true)
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		fw.Process(egr, buildV2FrameTxID(t, [32]byte{0xAB}, 7, raw), src, 0)
		if got := countEnqueued(egr); got != 0 {
			t.Errorf("%d queued, want 0 (SeqNum must not waive -require-ef)", got)
		}
	})

	t.Run("stamped_ef_still_admitted_on_a_relay_lane", func(t *testing.T) {
		fw := makeForwarder()
		fw.SetRequireEF(true)
		fw.SetAllowStampedIngress(true)
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		ef := buildBareEF()
		id, err := objfmt.TxID(ef)
		if err != nil {
			t.Fatalf("TxID: %v", err)
		}
		fw.Process(egr, buildV2FrameTxID(t, id, 7, ef), src, 0)
		if got := countEnqueued(egr); got != 1 {
			t.Errorf("%d queued, want 1 (relayed EF traffic is the normal case)", got)
		}
	})
}
