package worker

import (
	"encoding/binary"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/bundle"
	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/shard"
	"github.com/lightwebinc/shard-proxy/forwarder"
)

// captureSink collects FlushVia output under a lock so tests can observe
// delivery WHILE the connection is still open (the liveness assertions below
// read mid-stream, unlike the close-then-assert tests).
type captureSink struct {
	mu   sync.Mutex
	sunk [][]byte
}

func (c *captureSink) fn(_ int, raw []byte, _ *net.UDPAddr) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sunk = append(c.sunk, append([]byte(nil), raw...))
	return nil
}

func (c *captureSink) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.sunk)
}

func (c *captureSink) frames() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([][]byte, len(c.sunk))
	copy(out, c.sunk)
	return out
}

func waitCount(t *testing.T, c *captureSink, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.count() >= want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("sink has %d frames, want %d (batch stranded?)", c.count(), want)
}

func encodeTestTx(t *testing.T, txidByte byte) []byte {
	t.Helper()
	payload := []byte{0x01, 0, 0, 0, 0x00, 0x00, 0x00, 0x00, 0x00, 0xEF}
	f := &frame.Frame{Payload: payload}
	f.TxID[0] = txidByte
	buf := make([]byte, frame.HeaderSize+len(payload))
	n, err := frame.Encode(f, buf)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return buf[:n]
}

func startCapturedConn(t *testing.T) (*captureSink, net.Conn, chan struct{}) {
	t.Helper()
	fwd := makeTestForwarder()
	ti := NewTCPIngress(fwd, []*net.Interface{{Index: 1, Name: "lo"}}, nil)
	sink := &captureSink{}
	ti.SetFlushVia(sink.fn, nil)
	egr := makeLoopbackEgress(t, fwd)
	srv, cli := net.Pipe()
	done := make(chan struct{})
	go func() { ti.handleConn(srv, egr); ti.flushEgr(egr); close(done) }()
	return sink, cli, done
}

// More frames than one batch, streamed back-to-back: every frame must egress
// and per-flow SeqNums must stay strictly monotonic in egress order, the
// batched cadence changes WHEN flushes happen, never order or completeness.
func TestTCPBatchedCadenceDeliversAllFramesInOrder(t *testing.T) {
	const n = maxTCPBatch*3 + 7 // several full batches + a partial tail
	sink, cli, done := startCapturedConn(t)
	go func() {
		for i := 0; i < n; i++ {
			if _, err := cli.Write(encodeTestTx(t, byte(i))); err != nil {
				return
			}
		}
		_ = cli.Close()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleConn hang")
	}
	frames := sink.frames()
	if len(frames) != n {
		t.Fatalf("egressed %d frames, want %d: batching lost frames", len(frames), n)
	}
	// SeqNum is per-FLOW (HashKey = sender ∥ group ∥ subtree, bytes 40:48):
	// distinct TxIDs shard to distinct groups, so monotonicity holds within
	// each flow, in egress order.
	lastSeq := map[uint64]uint64{}
	for i, fr := range frames {
		hk := binary.BigEndian.Uint64(fr[40:48])
		seq := binary.BigEndian.Uint64(fr[48:56])
		if seq <= lastSeq[hk] {
			t.Fatalf("frame %d flow %x: seq %d not monotonic after %d", i, hk, seq, lastSeq[hk])
		}
		lastSeq[hk] = seq
	}
}

// LIVENESS: one frame then an idle-but-open connection. The frame must egress
// promptly (flushingReader drains before the blocking header read), not sit
// enqueued until more traffic or close.
func TestTCPSingleFrameThenIdleFlushes(t *testing.T) {
	sink, cli, done := startCapturedConn(t)
	if _, err := cli.Write(encodeTestTx(t, 0x01)); err != nil {
		t.Fatalf("write: %v", err)
	}
	waitCount(t, sink, 1) // must arrive while the connection stays open
	_ = cli.Close()
	<-done
}

// LIVENESS, partial-next-frame: a complete frame followed by a PARTIAL header
// in the same segment. The buffer is non-empty (so a Buffered()==0 gate would
// NOT fire) yet the next read blocks, the completed frame must still egress.
func TestTCPPartialNextFrameStillFlushes(t *testing.T) {
	sink, cli, done := startCapturedConn(t)
	full := encodeTestTx(t, 0x02)
	partial := encodeTestTx(t, 0x03)[:20] // 20 bytes of a 44-byte min header
	go func() { _, _ = cli.Write(append(append([]byte(nil), full...), partial...)) }()
	waitCount(t, sink, 1) // completed frame egresses despite buffered partial
	_ = cli.Close()
	<-done
}

// Two control frames in one batch must not alias: the enqueued reference
// outlives the loop iteration under batched cadence, so each must be a fresh
// heap buffer, not a reused stack slot.
func TestTCPControlFramesInOneBatchDoNotAlias(t *testing.T) {
	sink, cli, done := startCapturedConn(t)
	mk := func(tag byte) []byte {
		buf := make([]byte, frame.SubtreeGroupAnnounceSize)
		binary.BigEndian.PutUint32(buf[0:4], frame.MagicBSV)
		binary.BigEndian.PutUint16(buf[4:6], frame.ProtoVer)
		buf[6] = frame.MsgTypeSubtreeGroupAnnounce
		buf[50] = tag // distinct byte inside the announce body
		return buf
	}
	go func() {
		_, _ = cli.Write(append(mk(0xAA), mk(0xBB)...)) // one segment, one batch
		_ = cli.Close()
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleConn hang")
	}
	frames := sink.frames()
	if len(frames) != 2 {
		t.Fatalf("egressed %d control frames, want 2", len(frames))
	}
	if frames[0][50] != 0xAA || frames[1][50] != 0xBB {
		t.Fatalf("control frames aliased: got tags 0x%02X 0x%02X, want 0xAA 0xBB",
			frames[0][50], frames[1][50])
	}
}

// With -coalesce ON, the batched TCP lane packs same-flow txns into BRC-142
// bundle datagrams (fewer egress packets, the fabric-fanout/softirq relief)
// with ZERO member loss, and drains via flushEgr->FlushCoalesced on close (no
// stranding, the hazard the old per-frame path could never satisfy).
func TestTCPCoalesce_PacksSameFlowNoLoss(t *testing.T) {
	// shard-bits 0 => a single group, so every tx lands in one (group,subtree)
	// flow and is eligible to pack together.
	engine := shard.New(0xFF05, shard.DefaultGroupID, 0)
	fwd := forwarder.New(engine, 0xFF05, shard.DefaultGroupID, 9001, false, nil)
	fwd.SetCoalesce(true, 9000, 0, false)
	fwd.SetRequireEF(false)
	ti := NewTCPIngress(fwd, []*net.Interface{{Index: 1, Name: "lo"}}, nil)
	var sunk [][]byte
	ti.SetFlushVia(func(_ int, raw []byte, _ *net.UDPAddr) error {
		sunk = append(sunk, append([]byte(nil), raw...))
		return nil
	}, nil)

	// coal is armed only at batchHint>1 (the real accept loop uses maxTCPBatch).
	conn, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.ParseIP("::1")})
	if err != nil {
		t.Fatalf("loopback: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	egr := forwarder.NewEgress(fwd, []forwarder.Target{{Iface: &net.Interface{Index: 1, Name: "lo"}, Conn: conn}}, maxTCPBatch, nil)

	const n = 40
	srv, cli := net.Pipe()
	done := make(chan struct{})
	go func() { ti.handleConn(srv, egr); ti.flushEgr(egr); close(done) }()
	// One segment carrying all txns: they buffer together so the batch
	// accumulates before flushingReader drains it (net.Pipe delivers one Write
	// as one read; a frame-per-Write would give 1-member bundles, the
	// documented "coalescing only packs when the batch FILLS" behaviour).
	var stream []byte
	for i := 0; i < n; i++ {
		stream = append(stream, encodeTestTx(t, byte(i))...)
	}
	go func() { _, _ = cli.Write(stream); _ = cli.Close() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleConn hang")
	}

	// Fewer egress datagrams than txns (packing happened), every one a bundle,
	// and the member count sums to exactly n (no member stranded or lost).
	if len(sunk) == 0 || len(sunk) >= n {
		t.Fatalf("egressed %d datagrams for %d txns: expected fewer (bundling)", len(sunk), n)
	}
	members := 0
	for i, d := range sunk {
		if !frame.IsBundle(d) {
			t.Fatalf("datagram %d is not a BRC-142 bundle (ver 0x%02x)", i, d[6])
		}
		b, err := bundle.Decode(d)
		if err != nil {
			t.Fatalf("bundle %d decode: %v", i, err)
		}
		members += len(b.Members)
	}
	if members != n {
		t.Fatalf("recovered %d members across %d bundles, want %d (member loss/strand)", members, len(sunk), n)
	}
}

// The coalesce linger packs frames that arrive as SEPARATE writes (the
// subtx-gen-style one-tx-per-segment pattern that otherwise yields ~1-member
// bundles) by holding the batch open up to the linger window: trading a
// bounded latency for density on a fast network. Same-flow, arrivals within
// the window => one packed batch, not N singleton bundles.
func TestTCPCoalesceLinger_PacksAcrossSeparateWrites(t *testing.T) {
	engine := shard.New(0xFF05, shard.DefaultGroupID, 0) // single group => one flow
	fwd := forwarder.New(engine, 0xFF05, shard.DefaultGroupID, 9001, false, nil)
	fwd.SetCoalesce(true, 9000, 0, false)
	fwd.SetRequireEF(false)
	ti := NewTCPIngress(fwd, []*net.Interface{{Index: 1, Name: "lo"}}, nil)
	ti.SetCoalesceLinger(100 * time.Millisecond) // >> the inter-write gap below
	var sunk [][]byte
	ti.SetFlushVia(func(_ int, raw []byte, _ *net.UDPAddr) error {
		sunk = append(sunk, append([]byte(nil), raw...))
		return nil
	}, nil)
	conn, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.ParseIP("::1")})
	if err != nil {
		t.Fatalf("loopback: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	egr := forwarder.NewEgress(fwd, []forwarder.Target{{Iface: &net.Interface{Index: 1, Name: "lo"}, Conn: conn}}, maxTCPBatch, nil)

	const n = 30
	srv, cli := net.Pipe()
	done := make(chan struct{})
	go func() { ti.handleConn(srv, egr); ti.flushEgr(egr); close(done) }()
	go func() {
		for i := 0; i < n; i++ {
			if _, err := cli.Write(encodeTestTx(t, byte(i))); err != nil {
				return
			}
			time.Sleep(1 * time.Millisecond) // << the 100ms linger
		}
		_ = cli.Close()
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("handleConn hang")
	}

	members := 0
	for i, d := range sunk {
		if !frame.IsBundle(d) {
			t.Fatalf("datagram %d not a bundle", i)
		}
		b, err := bundle.Decode(d)
		if err != nil {
			t.Fatalf("bundle %d decode: %v", i, err)
		}
		members += len(b.Members)
	}
	if members != n {
		t.Fatalf("recovered %d members, want %d (loss)", members, n)
	}
	// Density: the linger must have packed the separate writes well below the
	// singleton-bundle count a no-linger frame-per-write would produce (== n).
	if len(sunk) >= n/2 {
		t.Fatalf("linger produced %d bundles for %d frame-per-write txns, not packing", len(sunk), n)
	}
	t.Logf("linger packed %d separate writes into %d bundles (R=%.1f)", n, len(sunk), float64(members)/float64(len(sunk)))
}
