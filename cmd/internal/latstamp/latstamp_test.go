package latstamp

import (
	"bytes"
	"testing"
)

// TestMagic pins the marker: perf-test writes it, latency-sink and any
// pcap analyzer match it, so a change here is a wire change for all three.
func TestMagic(t *testing.T) {
	if Magic != "BSVLAT01" {
		t.Errorf("Magic = %q, want %q", Magic, "BSVLAT01")
	}
	if len(Magic) != 8 {
		t.Errorf("len(Magic) = %d, want 8 (the stamp layout reserves 8 bytes)", len(Magic))
	}
}

func TestPutGetRoundTrip(t *testing.T) {
	p := make([]byte, Size+4)
	if !Put(p, 1_700_000_000_123_456_789, 42) {
		t.Fatal("Put on a large enough payload = false")
	}
	if !bytes.HasPrefix(p, []byte(Magic)) {
		t.Errorf("payload head = %q, want the magic", p[:8])
	}
	ns, seq, ok := Get(p)
	if !ok || ns != 1_700_000_000_123_456_789 || seq != 42 {
		t.Errorf("Get = (%d, %d, %v), want (1700000000123456789, 42, true)", ns, seq, ok)
	}
	if Put(make([]byte, Size-1), 1, 1) {
		t.Error("Put on a short payload = true, want false")
	}
	if _, _, ok := Get(bytes.Repeat([]byte{'x'}, Size)); ok {
		t.Error("Get accepted a stamp without the magic")
	}
}

func TestScanFindsStampAtOffset(t *testing.T) {
	p := make([]byte, Size)
	Put(p, 7, 9)
	b := append([]byte("encapsulation header"), p...)
	ns, seq, ok := Scan(b)
	if !ok || ns != 7 || seq != 9 {
		t.Errorf("Scan = (%d, %d, %v), want (7, 9, true)", ns, seq, ok)
	}
	if _, _, ok := Scan(b[:len(b)-1]); ok {
		t.Error("Scan accepted a truncated stamp")
	}
}
