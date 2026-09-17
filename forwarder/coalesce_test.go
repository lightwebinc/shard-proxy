package forwarder

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"

	"github.com/lightwebinc/shard-common/bundle"
	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/shard"
)

// drain captures the datagrams currently queued on egr (one entry per enqueued
// datagram for a single-target Egress) by routing Flush through a capture func.
func drain(egr *Egress) [][]byte {
	var sent [][]byte
	egr.FlushVia(func(_ int, raw []byte, _ *net.UDPAddr) error {
		sent = append(sent, append([]byte(nil), raw...))
		return nil
	})
	return sent
}

func TestCoalesce_BuffersAndPacksBundle(t *testing.T) {
	engine := shard.New(0xFF05, shard.DefaultGroupID, 8)
	fw := New(engine, 0xFF05, shard.DefaultGroupID, 9001, false, nil)
	fw.SetCoalesce(true, 9000, 0, true) // carry TxIDs

	conn, _ := openLoopbackUDP(t)
	egr := NewEgress(fw, makeTargets(t, conn), 8, nil)
	if egr.coal == nil {
		t.Fatal("coal buffer not created when coalescing enabled")
	}

	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}
	payloads := [][]byte{[]byte("tx-one"), []byte("tx-two"), []byte("tx-three")}
	for i, p := range payloads {
		raw := buildV2Frame(t, 0xAB, 0, p) // same top byte → same group; zero subtree
		raw[9] = byte(i + 1)               // unique TxID beyond byte 0
		fw.Process(egr, raw, src, 0)
	}

	// Buffered, not yet enqueued.
	if len(egr.coal.buckets) != 1 {
		t.Fatalf("buckets = %d, want 1", len(egr.coal.buckets))
	}
	if len(egr.coal.buckets[0].members) != 3 {
		t.Fatalf("members = %d, want 3", len(egr.coal.buckets[0].members))
	}
	if got := drain(egr); len(got) != 0 {
		t.Fatalf("nothing should be enqueued before FlushCoalesced, got %d", len(got))
	}

	fw.FlushCoalesced(egr, 0)
	sent := drain(egr)
	if len(sent) != 1 {
		t.Fatalf("sent %d datagrams, want 1 bundle", len(sent))
	}
	if !frame.IsBundle(sent[0]) {
		t.Fatal("enqueued datagram is not a bundle")
	}
	b, err := bundle.Decode(sent[0])
	if err != nil {
		t.Fatalf("decode bundle: %v", err)
	}
	if len(b.Members) != 3 {
		t.Errorf("bundle members = %d, want 3", len(b.Members))
	}
	if b.SeqNum != 1 {
		t.Errorf("bundle SeqNum = %d, want 1", b.SeqNum)
	}
	if b.HashKey == 0 {
		t.Error("bundle HashKey = 0, want stamped")
	}
	if !b.TxIDsPresent() {
		t.Error("expected carried TxIDs")
	}
	want := engine.GroupIndex(&[32]byte{0xAB})
	if uint32(b.GroupIdx) != want {
		t.Errorf("bundle GroupIdx = %d, want %d", b.GroupIdx, want)
	}
	for i, m := range b.Members {
		if !bytes.Equal(m.Tx, payloads[i]) {
			t.Errorf("member %d payload = %q, want %q", i, m.Tx, payloads[i])
		}
	}
	if len(egr.coal.buckets) != 0 {
		t.Error("coal buffer not reset after flush")
	}
}

func TestCoalesce_DisabledByDefault(t *testing.T) {
	fw := makeForwarder() // no SetCoalesce
	conn, _ := openLoopbackUDP(t)
	egr := NewEgress(fw, makeTargets(t, conn), 8, nil)
	if egr.coal != nil {
		t.Fatal("coal buffer should be nil when coalescing disabled")
	}
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}
	fw.Process(egr, buildV2Frame(t, 0xAB, 0, []byte("p")), src, 0)
	fw.FlushCoalesced(egr, 0) // must be a no-op
	sent := drain(egr)
	if len(sent) != 1 {
		t.Fatalf("sent %d, want 1 (normal individual frame)", len(sent))
	}
	if frame.IsBundle(sent[0]) {
		t.Error("frame should be forwarded individually, not bundled")
	}
}

func TestCoalesce_OversizeMemberFallsThrough(t *testing.T) {
	fw := makeForwarder()
	fw.SetCoalesce(true, 1500, 0, false) // 1500 byte budget, fragmentation disabled
	conn, _ := openLoopbackUDP(t)
	egr := NewEgress(fw, makeTargets(t, conn), 8, nil)
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}

	// A 1500-byte payload cannot fit a bundle (66 header + 2 + 1500 > 1500),
	// so it must take the normal individual-frame path, not be buffered.
	fw.Process(egr, buildV2Frame(t, 0xCD, 0, make([]byte, 1500)), src, 0)
	if len(egr.coal.buckets) != 0 {
		t.Fatalf("oversize member should not be buffered, buckets = %d", len(egr.coal.buckets))
	}
	sent := drain(egr)
	if len(sent) != 1 || frame.IsBundle(sent[0]) {
		t.Fatalf("oversize tx should be forwarded as one individual frame; sent=%d", len(sent))
	}
}

// batchHint=1 (the TCP ingress and legacy per-packet UDP paths) must NOT get a
// coal buffer, so Process forwards individually and no member can be stranded
// (those paths never call FlushCoalesced). Locks in the TCP exclusion.
func TestCoalesce_ExcludedWhenBatchCapOne(t *testing.T) {
	fw := makeForwarder()
	fw.SetCoalesce(true, 9000, 0, false)
	conn, _ := openLoopbackUDP(t)
	egr := NewEgress(fw, makeTargets(t, conn), 1, nil)
	if egr.coal != nil {
		t.Fatal("coal must be nil for batchHint=1 (TCP / per-packet path excluded from coalescing)")
	}
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}
	fw.Process(egr, buildV2Frame(t, 0xAB, 0, []byte("p")), src, 0)
	fw.FlushCoalesced(egr, -1) // no-op: coal is nil
	sent := drain(egr)
	if len(sent) != 1 || frame.IsBundle(sent[0]) {
		t.Fatalf("batchHint=1 frame must be forwarded individually (not stranded/bundled); sent=%d", len(sent))
	}
}

// A relay (the spine) receiving an already-coalesced bundle must re-emit it
// verbatim to the bundle's group — not drop it as a decode error, not
// re-coalesce, not re-stamp. Dispatch is called with src=nil (the spine
// re-emit), and coalescing is OFF on the relay (it forwards origin bundles).
func TestProcessBundle_RelaysVerbatim(t *testing.T) {
	fw := makeForwarder() // coalescing OFF: a relay forwards, it does not originate
	conn, _ := openLoopbackUDP(t)
	egr := NewEgress(fw, makeTargets(t, conn), 8, nil)

	in := &bundle.Bundle{
		Flags:     bundle.FlagTxIDsPresent,
		GroupIdx:  7,
		ShardBits: 8,
		HashKey:   0xDEADBEEF,
		SeqNum:    42,
		Members: []bundle.Member{
			{TxID: [32]byte{0x01}, Tx: []byte("tx-a")},
			{TxID: [32]byte{0x02}, Tx: []byte("tx-b")},
		},
	}
	raw, err := in.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	fw.Dispatch(egr, raw, nil, 0) // src=nil: the spine re-emit
	sent := drain(egr)
	if len(sent) != 1 {
		t.Fatalf("relay sent %d datagrams, want 1", len(sent))
	}
	if !bytes.Equal(sent[0], raw) {
		t.Fatal("relayed bundle is not byte-identical to the input (must be verbatim)")
	}
	out, err := bundle.Decode(sent[0])
	if err != nil {
		t.Fatalf("decode relayed: %v", err)
	}
	if out.GroupIdx != 7 || out.HashKey != 0xDEADBEEF || out.SeqNum != 42 {
		t.Errorf("relay re-stamped the bundle: group=%d hashkey=%#x seq=%d", out.GroupIdx, out.HashKey, out.SeqNum)
	}
	if len(out.Members) != 2 {
		t.Errorf("relayed members = %d, want 2", len(out.Members))
	}
}

// A bundle with corrupted magic or a payload length exceeding the datagram is
// dropped by the relay (not mis-routed), since members are forwarded opaquely.
func TestProcessBundle_DropsMalformed(t *testing.T) {
	fw := makeForwarder()
	conn, _ := openLoopbackUDP(t)
	egr := NewEgress(fw, makeTargets(t, conn), 8, nil)

	good := &bundle.Bundle{GroupIdx: 3, Members: []bundle.Member{{Tx: []byte("x")}}}
	raw, err := good.Encode()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	bad := append([]byte(nil), raw...)
	bad[0] ^= 0xFF // corrupt the magic
	fw.Dispatch(egr, bad, nil, 0)
	if got := drain(egr); len(got) != 0 {
		t.Fatalf("bad-magic bundle should be dropped, relayed %d", len(got))
	}

	trunc := append([]byte(nil), raw...)
	binary.BigEndian.PutUint32(trunc[62:66], 0xFFFF) // declared payload > datagram
	fw.Dispatch(egr, trunc, nil, 0)
	if got := drain(egr); len(got) != 0 {
		t.Fatalf("truncated bundle should be dropped, relayed %d", len(got))
	}
}

func TestCoalesce_SeqNumContiguousAcrossBatches(t *testing.T) {
	fw := makeForwarder()
	fw.SetCoalesce(true, 9000, 0, false)
	conn, _ := openLoopbackUDP(t)
	egr := NewEgress(fw, makeTargets(t, conn), 8, nil)
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}

	seqOf := func() uint64 {
		fw.Process(egr, buildV2Frame(t, 0xEE, 0, []byte("a")), src, 0)
		fw.Process(egr, buildV2Frame(t, 0xEE, 0, []byte("b")), src, 0)
		fw.FlushCoalesced(egr, 0)
		sent := drain(egr)
		if len(sent) != 1 {
			t.Fatalf("want 1 bundle per batch, got %d", len(sent))
		}
		b, err := bundle.Decode(sent[0])
		if err != nil {
			t.Fatal(err)
		}
		return b.SeqNum
	}
	if s1, s2 := seqOf(), seqOf(); s1 != 1 || s2 != 2 {
		t.Errorf("bundle SeqNums across batches = %d,%d, want 1,2 (contiguous per flow)", s1, s2)
	}
}

// A bundle packed at the default cap must leave as a datagram that fits the
// path MTU. -coalesce-max-bytes is an ON-THE-WIRE budget: the IPv6 (40) and
// UDP (8) headers ride outside the bundle body, so budgeting the body alone
// put a 1548-byte datagram on a 1500-byte path.
func TestCoalesce_BundleDatagramFitsPathMTU(t *testing.T) {
	fw := makeForwarder()
	fw.SetCoalesce(true, DefaultCoalesceMaxBytes, 0, true) // carried TxIDs = worst case
	conn, _ := openLoopbackUDP(t)
	egr := NewEgress(fw, makeTargets(t, conn), 8, nil)
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}

	// 40 × 200-byte members into one (group, subtree) flow: several bundles,
	// each of which the packer fills right up to the budget.
	for i := 0; i < 40; i++ {
		raw := buildV2Frame(t, 0xAB, 0, make([]byte, 200))
		raw[9] = byte(i + 1) // unique TxID
		fw.Process(egr, raw, src, 0)
	}
	fw.FlushCoalesced(egr, 0)

	sent := drain(egr)
	if len(sent) < 2 {
		t.Fatalf("sent %d datagrams; expected the budget to bind and split the flow", len(sent))
	}
	for i, raw := range sent {
		if !frame.IsBundle(raw) {
			t.Fatalf("datagram %d is not a bundle", i)
		}
		if got := len(raw) + ipv6UDPHeaderSize; got > DefaultCoalesceMaxBytes {
			t.Errorf("datagram %d is %d bytes on the wire (bundle %d + %d IPv6/UDP), exceeds the %d-byte path MTU",
				i, got, len(raw), ipv6UDPHeaderSize, DefaultCoalesceMaxBytes)
		}
	}
}

// The single-member eligibility gate uses the same wire budget: a member that
// would only fit once the IPv6/UDP headers are ignored must fall through to
// the individual-frame path rather than become an oversize datagram.
func TestCoalesce_EligibilityUsesWireBudget(t *testing.T) {
	fw := makeForwarder()
	fw.SetCoalesce(true, DefaultCoalesceMaxBytes, 0, false)
	conn, _ := openLoopbackUDP(t)
	egr := NewEgress(fw, makeTargets(t, conn), 8, nil)
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}

	// Body would be 66 + 2 + 1400 = 1468 ≤ 1500, but the datagram would be
	// 1516 on the wire.
	fw.Process(egr, buildV2Frame(t, 0xCD, 0, make([]byte, 1400)), src, 0)
	if len(egr.coal.buckets) != 0 {
		t.Fatalf("member that only fits without IPv6/UDP headers was buffered: buckets = %d", len(egr.coal.buckets))
	}

	// One byte under the true budget still coalesces.
	fw.Process(egr, buildV2Frame(t, 0xCD, 0, make([]byte, DefaultCoalesceMaxBytes-ipv6UDPHeaderSize-bundle.HeaderSize-bundle.MemberOverhead(false))), src, 0)
	if len(egr.coal.buckets) != 1 {
		t.Fatalf("member exactly at the wire budget should coalesce: buckets = %d", len(egr.coal.buckets))
	}
}
