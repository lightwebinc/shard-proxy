package forwarder

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/shard"
	"github.com/lightwebinc/shard-proxy/metrics"
)

// admitRec is one submission for the admission tests: its record, source
// and the interface the drop metrics name.
type admitRec struct {
	rec []byte
	src net.Addr
}

func admitForwarder(t *testing.T, rec *metrics.Recorder) *Forwarder {
	t.Helper()
	fw := makeForwarderRec(rec)
	pe, err := shard.NewPlane(0xFF05, shard.DefaultGroupID, 4, shard.DomainBEEF)
	if err != nil {
		t.Fatalf("NewPlane: %v", err)
	}
	fw.SetBEEF(pe, 1<<20)
	fw.SetFragMTU(1280)
	fw.SetBEEFSubmitPolicy(&boundPolicy{auth: net.ParseIP("::1"), deliver: 3})
	fw.SetTxidDedup(&pairDedup{seen: map[[32]byte]bool{}}, "bsp:beef:")
	return fw
}

// admitCorpus mixes fragmented and whole records, three sources, two topics
// and a duplicate, so admission order, flows and a drop are all exercised.
func admitCorpus(t *testing.T) []admitRec {
	t.Helper()
	src1 := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 1}
	src2 := &net.UDPAddr{IP: net.ParseIP("fd00::2"), Port: 2}
	src3 := &net.UDPAddr{IP: net.ParseIP("fd00::3"), Port: 3}
	big := func(i, n int) []byte {
		b := bytes.Repeat([]byte{byte(i)}, n)
		copy(b, []byte{0x01, 0x00, 0xBE, 0xEF})
		return b
	}
	var out []admitRec
	add := func(src net.Addr, topics []string, obj []byte) {
		out = append(out, admitRec{rec: buildBEEFRecordBytes(t, topics, obj), src: src})
	}
	add(src1, []string{"tm_a", "tm_b", "tm_c"}, big(1, 4096))
	add(src1, []string{"tm_a"}, big(2, 8))
	add(src2, []string{"tm_a"}, big(3, 3000))
	add(src1, []string{"tm_b"}, big(4, 5000))
	add(src3, []string{"tm_b"}, big(5, 1148)) // over the fragment data size (1128 at MTU 1280): two fragments
	add(src1, []string{"tm_a"}, big(6, 1000)) // one whole frame
	add(src2, []string{"tm_a"}, big(3, 3000)) // duplicate pair: dropped
	add(src1, []string{"tm_a"}, big(7, 2256)) // exactly two full fragments
	add(src2, []string{"tm_b"}, big(8, 16))
	return out
}

// viaDispatch runs recs through DispatchBEEF, the in-line path, and returns
// the datagrams it enqueued with their destinations.
func viaDispatch(t *testing.T, fw *Forwarder, recs []admitRec) (frames [][]byte, dsts []string) {
	t.Helper()
	conn, _ := openLoopbackUDP(t)
	for _, r := range recs {
		egr := makeEgress(t, fw, conn)
		fw.DispatchBEEF(egr, fw.PrepareBEEF(r.rec, r.src), r.src, 0)
		fs, ds := captureEnqueued(egr)
		frames = append(frames, fs...)
		for _, d := range ds {
			dsts = append(dsts, d.String())
		}
	}
	return frames, dsts
}

// viaAdmit runs recs through AdmitBEEF then FragmentBEEFInto, each record
// fragmented into one contiguous buffer, and returns the datagrams.
func viaAdmit(t *testing.T, fw *Forwarder, recs []admitRec) (frames [][]byte, dsts []string) {
	t.Helper()
	conn, _ := openLoopbackUDP(t)
	for _, r := range recs {
		egr := makeEgress(t, fw, conn)
		a := fw.AdmitBEEF(egr, fw.PrepareBEEF(r.rec, r.src), r.src, 0)
		if a == nil {
			continue
		}
		buf := make([]byte, 0, a.Bytes())
		alloc := func(n int) []byte {
			if cap(buf)-len(buf) < n {
				t.Fatalf("alloc %d past Bytes() %d", n, a.Bytes())
			}
			buf = buf[:len(buf)+n]
			return buf[len(buf)-n:]
		}
		dgs, err := fw.FragmentBEEFInto(a, alloc)
		if err != nil {
			t.Fatalf("FragmentBEEFInto: %v", err)
		}
		if len(dgs) != a.Datagrams() || len(buf) != a.Bytes() {
			t.Fatalf("%d datagrams in %d bytes, admission said %d in %d", len(dgs), len(buf), a.Datagrams(), a.Bytes())
		}
		dst := &net.UDPAddr{IP: a.Dst().IP, Port: a.Dst().Port, Zone: "lo"}
		for _, d := range dgs {
			frames = append(frames, d)
			dsts = append(dsts, dst.String())
		}
	}
	return frames, dsts
}

func sameDatagrams(t *testing.T, what string, aF [][]byte, aD []string, bF [][]byte, bD []string) {
	t.Helper()
	if len(aF) != len(bF) {
		t.Fatalf("%s: %d datagrams vs %d", what, len(aF), len(bF))
	}
	for i := range aF {
		if !bytes.Equal(aF[i], bF[i]) {
			t.Fatalf("%s: datagram %d differs", what, i)
		}
		if aD[i] != bD[i] {
			t.Fatalf("%s: datagram %d dst %s vs %s", what, i, aD[i], bD[i])
		}
	}
}

// AdmitBEEF + FragmentBEEFInto emit exactly what DispatchBEEF enqueues for
// the same records: bytes, SeqNums, DeliverCount, destinations, and the
// dropped duplicate.
func TestFragmentBEEFInto_MatchesDispatch(t *testing.T) {
	recs := admitCorpus(t)
	oldF, oldD := viaDispatch(t, admitForwarder(t, nil), recs)
	newF, newD := viaAdmit(t, admitForwarder(t, nil), recs)
	if len(oldF) < 12 {
		t.Fatalf("corpus produced only %d datagrams", len(oldF))
	}
	sameDatagrams(t, "admit vs dispatch", oldF, oldD, newF, newD)
}

// The two paths share the flow counters: records admitted through one and
// the other, interleaved, stamp the SeqNums one path alone would have, and
// every flow's SeqNums are consecutive in emission order.
func TestAdmitBEEF_SeqContinuityAcrossPaths(t *testing.T) {
	recs := admitCorpus(t)
	refF, refD := viaDispatch(t, admitForwarder(t, nil), recs)

	fw := admitForwarder(t, nil)
	var mixF [][]byte
	var mixD []string
	for i := range recs {
		var f [][]byte
		var d []string
		if i%2 == 0 {
			f, d = viaAdmit(t, fw, recs[i:i+1])
		} else {
			f, d = viaDispatch(t, fw, recs[i:i+1])
		}
		mixF = append(mixF, f...)
		mixD = append(mixD, d...)
	}
	sameDatagrams(t, "mixed vs dispatch", refF, refD, mixF, mixD)

	// Per flow (HashKey), SeqNums step by one in emission order.
	last := map[uint64]uint64{}
	for i, raw := range mixF {
		hk := binary.BigEndian.Uint64(raw[40:48])
		seq := binary.BigEndian.Uint64(raw[48:56])
		if prev, ok := last[hk]; ok && seq != prev+1 {
			t.Fatalf("datagram %d: flow %x SeqNum %d after %d", i, hk, seq, prev)
		}
		last[hk] = seq
	}
	if len(last) != 5 {
		t.Fatalf("%d flows, want 5 (three sources over two topics, one source on both)", len(last))
	}
}

// ReserveBEEFSeqs hands out ranges that neither overlap nor leave gaps
// against the per-frame stamp, under concurrent reservation.
func TestReserveBEEFSeqs(t *testing.T) {
	fw := admitForwarder(t, nil)
	src := &net.UDPAddr{IP: net.ParseIP("fd00::9"), Port: 1}
	hk0, first := fw.ReserveBEEFSeqs(src, 7, 3)
	if first != 1 {
		t.Fatalf("first reservation starts at %d, want 1", first)
	}
	hk, seq := fw.nextSeq(addrToIPv6(src), 7, beefZeroIngredient)
	if hk != hk0 || seq != 4 {
		t.Fatalf("stamp after a reservation of 3: hashKey %x/%x, seq %d, want 4", hk, hk0, seq)
	}
	var mu sync.Mutex
	seen := map[uint64]bool{}
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				_, f := fw.ReserveBEEFSeqs(src, 7, g+1)
				mu.Lock()
				for s := f; s < f+uint64(g+1); s++ {
					if seen[s] {
						t.Errorf("SeqNum %d reserved twice", s)
					}
					seen[s] = true
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	want := 4 + 100*(1+2+3+4+5+6+7+8)
	if len(seen) != want-4 {
		t.Fatalf("%d SeqNums reserved, want %d", len(seen), want-4)
	}
	for s := uint64(5); s <= uint64(want); s++ {
		if !seen[s] {
			t.Fatalf("SeqNum %d never reserved", s)
		}
	}
}

// Fragmenting admissions on several goroutines gives the same datagrams as
// fragmenting them in turn: an admission is read-only.
func TestFragmentBEEFInto_Parallel(t *testing.T) {
	recs := admitCorpus(t)
	refF, _ := viaAdmit(t, admitForwarder(t, nil), recs)

	fw := admitForwarder(t, nil)
	var adm []*BEEFAdmission
	for _, r := range recs {
		if a := fw.AdmitBEEF(nil, fw.PrepareBEEF(r.rec, r.src), r.src, 0); a != nil {
			adm = append(adm, a)
		}
	}
	out := make([][][]byte, len(adm))
	var wg sync.WaitGroup
	for i, a := range adm {
		wg.Add(1)
		go func() {
			defer wg.Done()
			dgs, err := fw.FragmentBEEFInto(a, func(n int) []byte { return make([]byte, n) })
			if err != nil {
				t.Error(err)
			}
			out[i] = dgs
		}()
	}
	wg.Wait()
	var got [][]byte
	for _, dgs := range out {
		got = append(got, dgs...)
	}
	if len(got) != len(refF) {
		t.Fatalf("%d datagrams, want %d", len(got), len(refF))
	}
	for i := range got {
		if !bytes.Equal(got[i], refF[i]) {
			t.Fatalf("datagram %d differs", i)
		}
	}
}

// A refused record, an alloc that comes up short, and a nil admission.
func TestAdmitBEEF_Refusals(t *testing.T) {
	fw := admitForwarder(t, nil)
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 1}
	if a := fw.AdmitBEEF(nil, fw.PrepareBEEF([]byte("not a record"), src), src, 0); a != nil {
		t.Fatal("a malformed record was admitted")
	}
	if a := fw.AdmitBEEF(nil, nil, src, 0); a != nil {
		t.Fatal("a nil prepared record was admitted")
	}
	if _, err := fw.FragmentBEEFInto(nil, nil); err == nil {
		t.Fatal("a nil admission fragmented")
	}
	big := bytes.Repeat([]byte{9}, 4096)
	copy(big, []byte{0x01, 0x00, 0xBE, 0xEF})
	a := fw.AdmitBEEF(nil, fw.PrepareBEEF(buildBEEFRecordBytes(t, []string{"tm_x"}, big), src), src, 0)
	if a == nil {
		t.Fatal("not admitted")
	}
	if _, err := fw.FragmentBEEFInto(a, func(n int) []byte { return make([]byte, n-1) }); err == nil {
		t.Fatal("a short alloc was accepted")
	}
	// The SeqNums were spent all the same: the next record on the flow
	// follows the dropped one's range.
	hk, first := a.Seqs()
	_, next := fw.ReserveBEEFSeqs(src, a.GroupIdx(), 1)
	if next != first+uint64(a.Datagrams()) {
		t.Fatalf("next SeqNum %d, want %d after a %d-fragment record (flow %x)", next, first+uint64(a.Datagrams()), a.Datagrams(), hk)
	}
}

// gatherCounters flattens a recorder's counter families to "name{labels}"
// = value, for the families given.
func gatherCounters(t *testing.T, rec *metrics.Recorder, families ...string) map[string]float64 {
	t.Helper()
	mfs, err := rec.Gatherer().Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	want := map[string]bool{}
	for _, f := range families {
		want[f] = true
	}
	out := map[string]float64{}
	for _, mf := range mfs {
		if !want[mf.GetName()] {
			continue
		}
		for _, m := range mf.GetMetric() {
			var ls []string
			for _, l := range m.GetLabel() {
				ls = append(ls, l.GetName()+"="+l.GetValue())
			}
			sort.Strings(ls)
			v := m.GetGauge().GetValue()
			if c := m.GetCounter(); c != nil {
				v = c.GetValue()
			} else if h := m.GetHistogram(); h != nil {
				v = h.GetSampleSum()
			}
			out[mf.GetName()+"{"+strings.Join(ls, ",")+"}"] = v
		}
	}
	return out
}

// The admission path records what the in-line path records: submissions,
// topics, ingress metering, fragmentation and drops.
func TestAdmitBEEF_MetricsMatchDispatch(t *testing.T) {
	families := []string{
		"bsp_beef_submissions_total", "bsp_beef_topics_total",
		"bsp_frames_fragmented_total", "bsp_fragments_emitted_total",
		"bsp_packets_dropped_total", "bsp_ingress_deduped_total",
	}
	recs := admitCorpus(t)
	recs = append(recs, admitRec{rec: []byte("junk"), src: recs[0].src})

	recOld, err := metrics.New("old", 1, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	recNew, err := metrics.New("new", 1, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	viaDispatch(t, admitForwarder(t, recOld), recs)
	viaAdmit(t, admitForwarder(t, recNew), recs)
	old, cur := gatherCounters(t, recOld, families...), gatherCounters(t, recNew, families...)
	if len(old) == 0 {
		t.Fatal("no counters gathered")
	}
	for k, v := range old {
		if cur[k] != v {
			t.Errorf("%s: admit %v, dispatch %v", k, cur[k], v)
		}
	}
	for k := range cur {
		if _, ok := old[k]; !ok {
			t.Errorf("%s: recorded by admit only", k)
		}
	}
	if !strings.Contains(fmt.Sprint(old), "bsp_fragments_emitted_total") || !strings.Contains(fmt.Sprint(old), "bsp_ingress_deduped_total") {
		t.Fatalf("corpus recorded no fragmentation or dedup drop: %v", old)
	}
}

// A frame that fragments carries its DeliverCount on every fragment through
// this path too, and a whole frame comes out stamped.
func TestFragmentBEEFInto_Stamping(t *testing.T) {
	fw := admitForwarder(t, nil)
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 1}
	big := bytes.Repeat([]byte{9}, 4096)
	copy(big, []byte{0x01, 0x00, 0xBE, 0xEF})
	a := fw.AdmitBEEF(nil, fw.PrepareBEEF(buildBEEFRecordBytes(t, []string{"tm_a", "tm_b", "tm_c"}, big), src), src, 0)
	dgs, err := fw.FragmentBEEFInto(a, func(n int) []byte { return make([]byte, n) })
	if err != nil {
		t.Fatal(err)
	}
	hk, first := a.Seqs()
	for i, raw := range dgs {
		ff, err := frame.DecodeFragment(raw)
		if err != nil {
			t.Fatalf("fragment %d: %v", i, err)
		}
		if ff.MsgType != 3 || ff.HashKey != hk || ff.SeqNum != first+uint64(i) || int(ff.FragTotal) != len(dgs) {
			t.Fatalf("fragment %d: DeliverCount %d, HashKey %x/%x, SeqNum %d/%d, total %d/%d",
				i, ff.MsgType, ff.HashKey, hk, ff.SeqNum, first+uint64(i), ff.FragTotal, len(dgs))
		}
	}
	small := fw.AdmitBEEF(nil, fw.PrepareBEEF(buildBEEFRecordBytes(t, []string{"tm_a"}, beefTestObj), src), src, 0)
	dgs, err = fw.FragmentBEEFInto(small, func(n int) []byte { return make([]byte, n) })
	if err != nil || len(dgs) != 1 {
		t.Fatalf("whole frame: %d datagrams, %v", len(dgs), err)
	}
	hk, first = small.Seqs()
	if binary.BigEndian.Uint64(dgs[0][40:48]) != hk || binary.BigEndian.Uint64(dgs[0][48:56]) != first || first == 0 {
		t.Fatalf("whole frame stamped %x/%d, admission says %x/%d", binary.BigEndian.Uint64(dgs[0][40:48]), binary.BigEndian.Uint64(dgs[0][48:56]), hk, first)
	}
}
