package worker

import (
	"encoding/binary"
	"net"
	"testing"
	"time"
)

// bareRawTx is a minimal 1-in/1-out BRC-12 raw transaction (no EF marker).
func bareRawTx() []byte {
	b := []byte{0x01, 0, 0, 0, 0x01}
	b = append(b, make([]byte, 32)...)
	b = append(b, 0xFF, 0xFF, 0xFF, 0xFF)
	b = append(b, 0x00)
	b = append(b, 0xFF, 0xFF, 0xFF, 0xFF)
	b = append(b, 0x01)
	b = append(b, make([]byte, 8)...)
	b = append(b, 0x00)
	b = append(b, 0, 0, 0, 0)
	return b
}

// bareEFTx is the same transaction in BRC-30 Extended Format.
func bareEFTx() []byte {
	b := []byte{0x01, 0, 0, 0, 0x00, 0x00, 0x00, 0x00, 0x00, 0xEF, 0x01}
	b = append(b, make([]byte, 32)...)
	b = append(b, 0xFF, 0xFF, 0xFF, 0xFF)
	b = append(b, 0x00)
	b = append(b, 0xFF, 0xFF, 0xFF, 0xFF)
	b = append(b, make([]byte, 8)...)
	b = append(b, 0x00)
	b = append(b, 0x01)
	b = append(b, make([]byte, 8)...)
	b = append(b, 0x00)
	b = append(b, 0, 0, 0, 0)
	return b
}

// runBareConn drives handleConn (which must grammar-detect a bare stream) over
// a net.Pipe with the given bytes, capturing every admitted frame via the
// FlushVia sink. Returns the sunk frames.
func runBareConn(t *testing.T, requireEF bool, stream []byte) [][]byte {
	t.Helper()
	fwd := makeTestForwarder()
	fwd.SetRequireEF(requireEF)
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
		t.Fatal("handleConn hang on bare stream")
	}
	return sunk
}

// TestBareConn_EFStreamAdmitted: two back-to-back bare EF txs on one TCP
// connection are delimited, reframed to stamped BRC-128 frames, and admitted
// under EF-native.
func TestBareConn_EFStreamAdmitted(t *testing.T) {
	stream := append(bareEFTx(), bareEFTx()...)
	sunk := runBareConn(t, true, stream)
	if len(sunk) != 2 {
		t.Fatalf("admitted %d frames, want 2", len(sunk))
	}
	for i, f := range sunk {
		if f[6] != 0x02 {
			t.Errorf("frame %d: ver 0x%02x, want V2", i, f[6])
		}
		if binary.BigEndian.Uint64(f[48:56]) == 0 {
			t.Errorf("frame %d: SeqNum not stamped", i)
		}
	}
	// Same tx → same flow: SeqNum increments across the stream.
	if s0, s1 := binary.BigEndian.Uint64(sunk[0][48:56]), binary.BigEndian.Uint64(sunk[1][48:56]); s1 != s0+1 {
		t.Errorf("SeqNums %d,%d: want consecutive", s0, s1)
	}
}

// TestBareConn_RawRejectedUnderEF: a bare raw (BRC-12) tx is rejected when
// EF-native is on, and admitted when it is off.
func TestBareConn_RawRejectedUnderEF(t *testing.T) {
	if sunk := runBareConn(t, true, bareRawTx()); len(sunk) != 0 {
		t.Fatalf("raw bare tx admitted under require-ef: %d frames", len(sunk))
	}
	if sunk := runBareConn(t, false, bareRawTx()); len(sunk) != 1 {
		t.Fatalf("raw bare tx with EF off: %d frames, want 1", len(sunk))
	}
}

// TestBareConn_FramedStillFramed: a magic-led stream on the same lane still
// takes the framed path (grammar detection must not misroute it).
func TestBareConn_FramedStillFramed(t *testing.T) {
	fwd := makeTestForwarder()
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
	go func() { _, _ = cli.Write(buildTCPHeaderFrame(0x02, 0, 0x77, bareEFTx())); _ = cli.Close() }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleConn hang on framed stream")
	}
	if len(sunk) != 1 || sunk[0][8] != 0x77 {
		t.Fatalf("framed frame not admitted verbatim: %d frames", len(sunk))
	}
}
