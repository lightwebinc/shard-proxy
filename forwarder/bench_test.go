package forwarder

import (
	"net"
	"testing"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/shard"
)

// BenchmarkProcessForward measures the per-frame forward hot path (decode →
// derive group → enqueue) in allocs/op. Validates the per-group/per-target
// destination-address caches: steady-state should be allocation-free on this
// path. Pre-stamped (SeqNum != 0) so frames forward verbatim: stable across
// iterations and exercising the Engine.Addr + Egress.enqueue addressing path.
func BenchmarkProcessForward(b *testing.B) {
	engine := shard.New(0xFF05, shard.DefaultGroupID, 8)
	fw := New(engine, 0xFF05, shard.DefaultGroupID, 9001, false, nil)

	conn, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.IPv6loopback})
	if err != nil {
		b.Skipf("listen: %v", err)
	}
	defer func() { _ = conn.Close() }()
	egr := NewEgress(fw, []Target{{Iface: &net.Interface{Index: 1, Name: "lo"}, Conn: conn}}, 32, nil)

	f := &frame.Frame{SeqNum: 5, Payload: make([]byte, 256)}
	f.TxID[0] = 0x42
	buf := make([]byte, frame.HeaderSize+256)
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
		// Drain the per-batch queues without sending (mimic Flush's reset).
		egr.meta = egr.meta[:0]
		for j := range egr.msgs {
			egr.msgs[j] = egr.msgs[j][:0]
		}
	}
}
