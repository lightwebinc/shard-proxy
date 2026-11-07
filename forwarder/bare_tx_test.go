package forwarder

import (
	"net"
	"testing"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/shard"
)

// buildBareTx is a minimal 1-in/1-out standard (BRC-12 raw) transaction:
// no Extended Format marker.
func buildBareTx() []byte {
	b := []byte{0x01, 0, 0, 0, 0x01} // version + input count
	b = append(b, make([]byte, 32)...)
	b = append(b, 0xFF, 0xFF, 0xFF, 0xFF) // prev index
	b = append(b, 0x00)                   // unlocking script length
	b = append(b, 0xFF, 0xFF, 0xFF, 0xFF) // sequence
	b = append(b, 0x01)                   // output count
	b = append(b, make([]byte, 8)...)     // value
	b = append(b, 0x00)                   // locking script length
	b = append(b, 0, 0, 0, 0)             // locktime
	return b
}

// buildBareEF is the same transaction in BRC-30 Extended Format: the 6-byte
// marker after the version, plus per-input spent value + locking script.
func buildBareEF() []byte {
	b := []byte{0x01, 0, 0, 0, 0x00, 0x00, 0x00, 0x00, 0x00, 0xEF, 0x01} // version + EF marker + input count
	b = append(b, make([]byte, 32)...)                                   // prev txid
	b = append(b, 0xFF, 0xFF, 0xFF, 0xFF)                                // prev index
	b = append(b, 0x00)                                                  // unlocking script length
	b = append(b, 0xFF, 0xFF, 0xFF, 0xFF)                                // sequence
	b = append(b, make([]byte, 8)...)                                    // spent value (EF extension)
	b = append(b, 0x00)                                                  // spent locking script length (EF extension)
	b = append(b, 0x01)                                                  // output count
	b = append(b, make([]byte, 8)...)                                    // value
	b = append(b, 0x00)                                                  // locking script length
	b = append(b, 0, 0, 0, 0)                                            // locktime
	return b
}

// TestDispatchBareTx_EFOnly verifies the bare-tx ingress and the EF-native gate:
// a bare EF tx is admitted (reframed + forwarded); a bare raw (BRC-12) tx is
// rejected only when requireEF is set (admitted by default); a framed frame is
// untouched by the magic pre-check.
func TestDispatchBareTx_EFOnly(t *testing.T) {
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}

	t.Run("bare_ef_admitted", func(t *testing.T) {
		fw := makeForwarder()
		fw.SetRequireEF(true)
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		fw.DispatchClass(egr, buildBareEF(), src, 0, IngressTransaction)
		if got := countEnqueued(egr); got != 1 {
			t.Errorf("bare EF: %d queued, want 1 (admitted + reframed)", got)
		}
	})

	t.Run("bare_raw_rejected_when_require_ef", func(t *testing.T) {
		fw := makeForwarder()
		fw.SetRequireEF(true)
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		fw.DispatchClass(egr, buildBareTx(), src, 0, IngressTransaction)
		if got := countEnqueued(egr); got != 0 {
			t.Errorf("bare raw: %d queued, want 0 (rejected, EF-native ingress)", got)
		}
	})

	t.Run("bare_raw_admitted_by_default", func(t *testing.T) {
		fw := makeForwarder() // requireEF off
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		fw.DispatchClass(egr, buildBareTx(), src, 0, IngressTransaction)
		if got := countEnqueued(egr); got != 1 {
			t.Errorf("bare raw (default): %d queued, want 1 (admitted, EF not required)", got)
		}
	})

	t.Run("framed_unaffected", func(t *testing.T) {
		fw := makeForwarder()
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		fw.DispatchClass(egr, buildV2Frame(t, 0xCD, 0, []byte("tx")), src, 0, IngressTransaction)
		if got := countEnqueued(egr); got != 1 {
			t.Errorf("framed tx: %d queued, want 1 (magic present → framed path)", got)
		}
	})
}

// BenchmarkProcessSubmitEF measures the submission hot path with EF-native
// ingress enabled: an unstamped Extended Format frame runs the requireEF gate
// (the marker compare) before decode→dedup→stamp→forward. Compare against
// BenchmarkProcessForward (relay, requireEF off) to see the gate is negligible.
func BenchmarkProcessSubmitEF(b *testing.B) {
	engine := shard.New(0xFF05, shard.DefaultGroupID, 8)
	fw := New(engine, 0xFF05, shard.DefaultGroupID, 9001, false, nil)
	fw.SetRequireEF(true)

	conn, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.IPv6loopback})
	if err != nil {
		b.Skipf("listen: %v", err)
	}
	defer func() { _ = conn.Close() }()
	egr := NewEgress(fw, []Target{{Iface: &net.Interface{Index: 1, Name: "lo"}, Conn: conn}}, 32, nil)

	// Unstamped (SeqNum 0) EF frame, the submission path the gate guards.
	f := &frame.Frame{SeqNum: 0, Payload: buildBareEF()}
	f.TxID[0] = 0x42
	buf := make([]byte, frame.HeaderSize+len(f.Payload))
	n, err := frame.Encode(f, buf)
	if err != nil {
		b.Fatalf("encode: %v", err)
	}
	raw := buf[:n]
	src := &net.UDPAddr{IP: net.ParseIP("fd00:25::2"), Port: 5000}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fw.Process(egr, raw, src, 0)
		egr.meta = egr.meta[:0]
		for j := range egr.msgs {
			egr.msgs[j] = egr.msgs[j][:0]
		}
	}
}
