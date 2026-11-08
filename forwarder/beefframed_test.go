package forwarder

import (
	"bytes"
	"math/rand"
	"net"
	"runtime"
	"testing"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/objfmt"
)

// framedBuf lays rec out as a lane reading into a pooled buffer would: the
// frame header's room first, filled with stale bytes (a recycled buffer holds
// whatever its last record left), then the record.
func framedBuf(rec []byte, stale byte) []byte {
	buf := make([]byte, BEEFFrameHeadroom+len(rec))
	for i := range BEEFFrameHeadroom {
		buf[i] = stale
	}
	copy(buf[BEEFFrameHeadroom:], rec)
	return buf
}

// beefObj is a BEEF object of n bytes (a valid leading marker, then random).
func beefObj(n int, seed int64) []byte {
	obj := make([]byte, n)
	rand.New(rand.NewSource(seed)).Read(obj)
	copy(obj, []byte{0x01, 0x00, 0xBE, 0xEF})
	return obj
}

// TestPrepareBEEFFramed_ParityWithPrepareBEEF: for every record shape the
// framed preparation is byte for byte PrepareBEEF's, whatever stale bytes the
// header's room held, and the frame IS the caller's buffer (built in place,
// not copied).
func TestPrepareBEEFFramed_ParityWithPrepareBEEF(t *testing.T) {
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}
	cases := []struct {
		name    string
		topics  []string
		obj     []byte
		deliver int // a policy's DeliverCount for src (0 = no policy)
	}{
		{"one topic, small", []string{"tm_alpha"}, beefTestObj, 0},
		{"one topic, 64 KiB", []string{"tm_alpha"}, beefObj(64<<10, 1), 0},
		{"one topic, 4 MiB", []string{"tm_alpha"}, beefObj(4<<20, 2), 0},
		{"three topics, open path", []string{"tm_x", "tm_y", "tm_z"}, beefObj(9000, 3), 0},
		{"three topics, policy delivers 3", []string{"tm_x", "tm_y", "tm_z"}, beefObj(9000, 4), 3},
		{"two topics, policy over the count", []string{"tm_x", "tm_y"}, beefObj(9000, 5), 40},
	}
	for _, c := range cases {
		for _, stale := range []byte{0x00, 0xA5, 0xFF} {
			fw, _ := makeBEEFForwarder(t)
			fw.beefMaxObject = 8 << 20
			if c.deliver > 0 {
				fw.SetBEEFSubmitPolicy(&boundPolicy{auth: net.ParseIP("::1"), deliver: c.deliver})
			}
			rec := buildBEEFRecordBytes(t, c.topics, c.obj)
			want := fw.PrepareBEEF(rec, src)
			buf := framedBuf(rec, stale)
			got := fw.PrepareBEEFFramed(buf, src)
			if got.result != want.result || got.topics != want.topics || got.deliver != want.deliver {
				t.Fatalf("%s (stale %02X): result %q topics %d deliver %d, want %q %d %d",
					c.name, stale, got.result, got.topics, got.deliver, want.result, want.topics, want.deliver)
			}
			if !bytes.Equal(got.frame, want.frame) {
				t.Fatalf("%s (stale %02X): the framed frame differs from PrepareBEEF's", c.name, stale)
			}
			if &got.frame[0] != &buf[0] || len(got.frame) != len(buf) {
				t.Fatalf("%s: the frame is not the caller's buffer (copied, or a different length)", c.name)
			}
			if got.Len() != want.Len() {
				t.Fatalf("%s: Len %d, want %d", c.name, got.Len(), want.Len())
			}
		}
	}
}

// TestPrepareBEEFFramed_RefusalsMatch: every refusal is PrepareBEEF's, with
// no frame, and a buffer shorter than the header's room is malformed.
func TestPrepareBEEFFramed_RefusalsMatch(t *testing.T) {
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}
	good := buildBEEFRecordBytes(t, []string{"tm_x"}, beefTestObj)
	badVer := append([]byte(nil), good...)
	badVer[2] = 0x7F
	badMarker := buildBEEFRecordBytes(t, []string{"tm_x"}, []byte{0xDE, 0xAD, 0xBE, 0xAA, 0x01})
	trailing := append(append([]byte(nil), good...), 0x00)
	cases := map[string]struct {
		rec      []byte
		maxBytes int
		disabled bool
	}{
		"malformed":       {badVer, 1 << 20, false},
		"trailing byte":   {trailing, 1 << 20, false},
		"bad_marker":      {badMarker, 1 << 20, false},
		"oversize":        {good, 4, false},
		"disabled":        {good, 1 << 20, true},
		"empty record":    {nil, 1 << 20, false},
		"truncated":       {good[:len(good)-1], 1 << 20, false},
		"envelope only":   {good[:4], 1 << 20, false},
		"one byte record": {good[:1], 1 << 20, false},
	}
	for name, c := range cases {
		fw, _ := makeBEEFForwarder(t)
		fw.beefMaxObject = c.maxBytes
		if c.disabled {
			fw.beefEngine = nil
		}
		want := fw.PrepareBEEF(c.rec, src)
		got := fw.PrepareBEEFFramed(framedBuf(c.rec, 0xA5), src)
		if got.result != want.result {
			t.Errorf("%s: result %q, want PrepareBEEF's %q", name, got.result, want.result)
		}
		if want.frame == nil && got.frame != nil {
			t.Errorf("%s: a refused record has a frame", name)
		}
	}
	fw, _ := makeBEEFForwarder(t)
	if p := fw.PrepareBEEFFramed(make([]byte, BEEFFrameHeadroom-1), src); p.result != "malformed" || p.frame != nil {
		t.Errorf("short buffer: result %q, frame %v; want malformed, none", p.result, p.frame != nil)
	}
}

// TestPrepareBEEFFramed_AdmitAndFragmentParity: a framed record admitted and
// fragmented gives the datagrams the copied one gives, byte for byte, with the
// same SeqNums (two forwarders in the same state), whole frames and BRC-130
// fragments alike.
func TestPrepareBEEFFramed_AdmitAndFragmentParity(t *testing.T) {
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}
	for _, size := range []int{len(beefTestObj), 4096, 64 << 10, 1 << 20} {
		var datagrams [2][][]byte
		for i := range 2 {
			fw, _ := makeBEEFForwarder(t)
			fw.beefMaxObject = 8 << 20
			fw.SetFragMTU(1280)
			rec := buildBEEFRecordBytes(t, []string{"tm_big"}, beefObj(size, int64(size)))
			var p *PreparedBEEF
			if i == 0 {
				p = fw.PrepareBEEF(rec, src)
			} else {
				p = fw.PrepareBEEFFramed(framedBuf(rec, 0xA5), src)
			}
			a := fw.AdmitBEEF(nil, p, src, 0)
			if a == nil {
				t.Fatalf("size %d, path %d: not admitted", size, i)
			}
			out, err := fw.FragmentBEEFInto(a, func(n int) []byte { return make([]byte, n) })
			if err != nil {
				t.Fatalf("size %d, path %d: fragment: %v", size, i, err)
			}
			datagrams[i] = out
		}
		if len(datagrams[0]) != len(datagrams[1]) {
			t.Fatalf("size %d: %d datagrams copied, %d framed", size, len(datagrams[0]), len(datagrams[1]))
		}
		for j := range datagrams[0] {
			if !bytes.Equal(datagrams[0][j], datagrams[1][j]) {
				t.Fatalf("size %d: datagram %d differs between the copied and the framed path", size, j)
			}
		}
		if size > 1280 && datagrams[1][0][6] != frame.FrameVerV3 {
			t.Fatalf("size %d: not fragmented", size)
		}
	}
}

// BenchmarkPrepareBEEF and BenchmarkPrepareBEEFFramed: the copied preparation
// allocates and zeroes a frame the size of the record and copies the record
// into it; the framed one writes the 92-byte header into the room the caller
// left. Both hash the record (ContentID), which is the same in both.
func BenchmarkPrepareBEEF(b *testing.B) {
	benchPrepare(b, false)
}

func BenchmarkPrepareBEEFFramed(b *testing.B) {
	benchPrepare(b, true)
}

func benchPrepare(b *testing.B, framed bool) {
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}
	fw, _ := makeBEEFForwarder(&testing.T{})
	fw.beefMaxObject = 8 << 20
	rec, err := objfmt.EncodeBEEFRecord([]string{"tm_bench"}, beefObj(4<<20, 9))
	if err != nil {
		b.Fatal(err)
	}
	buf := framedBuf(rec, 0)
	b.SetBytes(int64(len(rec)))
	b.ReportAllocs()
	var m0, m1 runtime.MemStats
	runtime.ReadMemStats(&m0)
	b.ResetTimer()
	for range b.N {
		var p *PreparedBEEF
		if framed {
			p = fw.PrepareBEEFFramed(buf, src)
		} else {
			p = fw.PrepareBEEF(rec, src)
		}
		if p.frame == nil {
			b.Fatal("refused")
		}
	}
	b.StopTimer()
	runtime.ReadMemStats(&m1)
	// Bytes the preparation allocates (and so zeroes, and the copied path then
	// writes the record into) for each byte of the lane's records.
	b.ReportMetric(float64(m1.TotalAlloc-m0.TotalAlloc)/float64(int64(len(rec))*int64(b.N)), "alloc-B/lane-B")
}
