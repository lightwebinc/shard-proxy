package worker

import (
	"net"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/shard-common/shard"
	"github.com/lightwebinc/shard-proxy/forwarder"
)

var beefLaneObj = []byte{0x01, 0x00, 0xBE, 0xEF, 0x11, 0x22, 0x33}

func makeBEEFTestForwarder(t *testing.T) *forwarder.Forwarder {
	t.Helper()
	fwd := makeTestForwarder()
	pe, err := shard.NewPlane(0xFF05, shard.DefaultGroupID, 4, shard.DomainBEEF)
	if err != nil {
		t.Fatalf("NewPlane: %v", err)
	}
	fwd.SetBEEF(pe, 1<<20)
	return fwd
}

func mustRecord(t *testing.T, topics []string) []byte {
	t.Helper()
	rec, err := objfmt.EncodeBEEFRecord(topics, beefLaneObj)
	if err != nil {
		t.Fatalf("EncodeBEEFRecord: %v", err)
	}
	return rec
}

// runTCPConn drives TCPIngress.handleConn (grammar auto-detect) over a
// net.Pipe and returns every admitted frame from the FlushVia sink.
func runTCPConn(t *testing.T, stream []byte) [][]byte {
	t.Helper()
	fwd := makeBEEFTestForwarder(t)
	ti := NewTCPIngress(fwd, []*net.Interface{{Index: 1, Name: "lo"}}, nil)
	var sunk [][]byte
	ti.SetFlushVia(func(_ int, raw []byte, _ *net.UDPAddr) error {
		sunk = append(sunk, append([]byte(nil), raw...))
		return nil
	}, nil)
	egr := makeLoopbackEgress(t, fwd)
	srv, cli := net.Pipe()
	done := make(chan struct{})
	go func() { ti.handleConn(srv, egr); close(done) }()
	go func() { _, _ = cli.Write(stream); _ = cli.Close() }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		_ = srv.Close()
		t.Fatal("handleConn did not return")
	}
	return sunk
}

// TestTCPIngressBEEFRecordStream proves the tx port's third grammar: a
// 0xBEEF-tagged record stream splits per record into stamped FrameVer 0x09
// frames (one per single-topic record). Multi-topic records are rejected by
// the OSS single-topic admission gate.
func TestTCPIngressBEEFRecordStream(t *testing.T) {
	stream := append(mustRecord(t, []string{"tm_a"}), mustRecord(t, []string{"tm_b"})...)
	sunk := runTCPConn(t, stream)
	if len(sunk) != 2 {
		t.Fatalf("admitted %d frames, want 2 (two single-topic records)", len(sunk))
	}
	for i, raw := range sunk {
		if !frame.IsBEEFFrame(raw) {
			t.Fatalf("frame %d is not FrameVer 0x09", i)
		}
		bf, err := frame.DecodeBEEF(raw)
		if err != nil || bf.SeqNum == 0 {
			t.Fatalf("frame %d not stamped: %v", i, err)
		}
	}

	// A multi-topic record is rejected by the OSS single-topic gate — no frames
	// reach the sink (multi-topic requires an authenticated submit policy).
	if rejected := runTCPConn(t, mustRecord(t, []string{"tm_b", "tm_c"})); len(rejected) != 0 {
		t.Fatalf("multi-topic record admitted %d frames, want 0 (rejected)", len(rejected))
	}
}

// TestTCPIngressFramedV9 proves a pre-framed BEEF object rides the framed
// TCP grammar (92-byte header family) unchanged.
func TestTCPIngressFramedV9(t *testing.T) {
	raw, err := objfmt.BEEFMulticastBytes(objfmt.TopicID("tm_framed"), beefLaneObj)
	if err != nil {
		t.Fatalf("BEEFMulticastBytes: %v", err)
	}
	sunk := runTCPConn(t, raw)
	if len(sunk) != 1 || !frame.IsBEEFFrame(sunk[0]) {
		t.Fatalf("framed V9 over TCP: admitted %d, want 1 BEEF frame", len(sunk))
	}
}

// runBEEFLane drives ObjectIngress.handleConn with ClassBEEF (the dedicated
// 8728 lane) and returns admitted frames.
func runBEEFLane(t *testing.T, stream []byte) [][]byte {
	t.Helper()
	fwd := makeBEEFTestForwarder(t)
	oi := NewObjectIngress(fwd, []*net.Interface{{Index: 1, Name: "lo"}}, nil, objfmt.ClassBEEF)
	var sunk [][]byte
	oi.SetFlushVia(func(_ int, raw []byte, _ *net.UDPAddr) error {
		sunk = append(sunk, append([]byte(nil), raw...))
		return nil
	}, nil)
	egr := makeLoopbackEgress(t, fwd)
	srv, cli := net.Pipe()
	done := make(chan struct{})
	go func() { oi.handleConn(srv, egr); close(done) }()
	go func() { _, _ = cli.Write(stream); _ = cli.Close() }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		_ = srv.Close()
		t.Fatal("handleConn did not return")
	}
	return sunk
}

// TestObjectIngressBEEFLane proves the dedicated lane splits records into one
// frame per single-topic record like the shared port; multi-topic records are
// rejected by the OSS single-topic admission gate (any open port is single-topic).
func TestObjectIngressBEEFLane(t *testing.T) {
	stream := append(mustRecord(t, []string{"tm_x"}), mustRecord(t, []string{"tm_z"})...)
	sunk := runBEEFLane(t, stream)
	if len(sunk) != 2 {
		t.Fatalf("beef lane admitted %d frames, want 2", len(sunk))
	}
	if rejected := runBEEFLane(t, mustRecord(t, []string{"tm_x", "tm_y"})); len(rejected) != 0 {
		t.Fatalf("beef lane admitted %d frames from a multi-topic record, want 0 (rejected)", len(rejected))
	}
}

// TestObjectIngressBEEFLaneRejectsBareTx proves the single-class lane drops
// a non-record stream instead of admitting it as transactions.
func TestObjectIngressBEEFLaneRejectsBareTx(t *testing.T) {
	if sunk := runBEEFLane(t, bareEFTx()); len(sunk) != 0 {
		t.Fatalf("beef lane admitted %d frames from a bare tx stream, want 0", len(sunk))
	}
}

// consumedBeforeClose streams to handle in 64 KiB writes and returns how many
// bytes the handler read before closing the connection.
func consumedBeforeClose(t *testing.T, handle func(net.Conn), stream []byte) int {
	t.Helper()
	srv, cli := net.Pipe()
	done := make(chan struct{})
	go func() { handle(srv); close(done) }()
	consumed := 0
	for off := 0; off < len(stream); off += 64 << 10 {
		n, err := cli.Write(stream[off:min(off+64<<10, len(stream))])
		consumed += n
		if err != nil {
			break
		}
	}
	_ = cli.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = srv.Close()
		t.Fatal("handler did not return")
	}
	return consumed
}

// oversizeRecord is a single-topic record whose object is n bytes.
func oversizeRecord(t *testing.T, n int) []byte {
	t.Helper()
	obj := make([]byte, n)
	copy(obj, beefLaneObj[:4])
	rec, err := objfmt.EncodeBEEFRecord([]string{"tm_big"}, obj)
	if err != nil {
		t.Fatalf("EncodeBEEFRecord: %v", err)
	}
	return rec
}

// An object far over the bound must not be buffered whole: the connection
// closes once the reader holds the submitter's bound without a record
// boundary, not after the reader's 64 MiB default.
func TestTCPIngressBEEFOversizeClosesEarly(t *testing.T) {
	fwd := makeBEEFTestForwarder(t) // 1 MiB object bound
	ti := NewTCPIngress(fwd, []*net.Interface{{Index: 1, Name: "lo"}}, nil)
	sunk := 0
	ti.SetFlushVia(func(int, []byte, *net.UDPAddr) error { sunk++; return nil }, nil)
	egr := makeLoopbackEgress(t, fwd)

	consumed := consumedBeforeClose(t, func(c net.Conn) { ti.handleConn(c, egr) }, oversizeRecord(t, 8<<20))
	if consumed >= 2<<20 {
		t.Fatalf("read %d bytes of an 8 MiB record against a 1 MiB bound; want the close near the bound", consumed)
	}
	if sunk != 0 {
		t.Fatalf("admitted %d frames from an oversize record, want 0", sunk)
	}
}

// A record only just over the bound fits the reader's envelope allowance, so it
// is read whole and rejected by SubmitBEEF; the stream stays in sync and the
// next record is admitted.
func TestTCPIngressBEEFJustOversizeKeepsStream(t *testing.T) {
	stream := append(oversizeRecord(t, 1<<20+1), mustRecord(t, []string{"tm_after"})...)
	if sunk := runTCPConn(t, stream); len(sunk) != 1 {
		t.Fatalf("admitted %d frames, want 1 (oversize dropped, next record admitted)", len(sunk))
	}
}

// The dedicated BEEF lane reads records under the same bound, not the 256 MiB
// push-object ceiling sized for subtrees.
func TestObjectIngressBEEFOversizeClosesEarly(t *testing.T) {
	fwd := makeBEEFTestForwarder(t)
	oi := NewObjectIngress(fwd, []*net.Interface{{Index: 1, Name: "lo"}}, nil, objfmt.ClassBEEF)
	sunk := 0
	oi.SetFlushVia(func(int, []byte, *net.UDPAddr) error { sunk++; return nil }, nil)
	egr := makeLoopbackEgress(t, fwd)

	consumed := consumedBeforeClose(t, func(c net.Conn) { oi.handleConn(c, egr) }, oversizeRecord(t, 8<<20))
	if consumed >= 2<<20 {
		t.Fatalf("beef lane read %d bytes of an 8 MiB record against a 1 MiB bound", consumed)
	}
	if sunk != 0 {
		t.Fatalf("beef lane admitted %d frames from an oversize record, want 0", sunk)
	}
}
