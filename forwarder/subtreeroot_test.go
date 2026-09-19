package forwarder

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net"
	"testing"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/objfmt"
)

// rootNodeHash is node i of a test subtree: SHA256d of i as a little-endian
// uint64, or the all-0xFF coinbase placeholder at index 0 when placeholder.
func rootNodeHash(i int, placeholder bool) [32]byte {
	var h [32]byte
	if placeholder && i == 0 {
		for k := range h {
			h[k] = 0xFF
		}
		return h
	}
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(i))
	a := sha256.Sum256(b[:])
	return sha256.Sum256(a[:])
}

// teranodeRoots are the roots Teranode's own merkle code
// (go-subtree BuildMerkleTreeStoreFromBytes, v1.4.2, the version Teranode
// pins) computes for the rootNodeHash subtrees. Sizes cover the one-node
// case, odd counts at several levels, powers of two and their neighbours,
// and a subtree wide enough to take the parallel path.
var teranodeRoots = []struct {
	n           int
	placeholder bool
	root        string
}{
	{1, false, "7ef0ca626bbb058dd443bb78e33b888bdec8295c96e51f5545f96370870c10b9"},
	{2, false, "26c582bf7254e2d2e10d8ab644fef80bbf39b557fc944ae1798bc37b31e93983"},
	{3, false, "61b203381a170e2f9a785cf90c413c47cc22bd072898548e59d01e696a3de235"},
	{4, false, "97c51bcdae046246237b8722f91f2ae7d5f93eff9f6fb4b810841b8e827509c5"},
	{5, false, "98e9dddb89c125d04810eb92f7922ad1c5498060b53640a6bb62d80c9de1ff71"},
	{7, false, "c68cc53399886774be88421238cce6a758550226ea0f1f3f2eee25418164b68f"},
	{8, false, "49530aaf8ad16fbb9290e96a9f4856a82de1279460083b3b3ff6e66caa8bf3ad"},
	{9, false, "fa463804891c147e332b9489e068c0b730baf09c21124f071913e20efc20e33f"},
	{1023, false, "caec9759ccddde84eb5e0ce5ba249e21f82d57b09fb80de2bfffc2af57851c5b"},
	{1024, false, "ae0e0589d6c5d0851b51ab5fd8fc4cf9330e87c9e0c183664ae5f5c025a27043"},
	{1025, false, "a82169a9321adec842d9d3d27b99fb3cc22b74d639d6c6b84bd1433addf8a3ac"},
	{4097, false, "99824ede7c04699ff44f0b098a16d153ef092707035af080ba82419b586b0a5f"},
	{70001, false, "08706b5bd5d4ff3bd470a752180119cf5f2b8d66cdc420002796a098b53efcb6"},
	{1, true, "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
	{2, true, "5f886cc409fc6ae4c9fcf2748f020577a62a4d0b68785257479a327187c142aa"},
	{3, true, "7da98fe7dcdba0a04c2d5c9517479dbc6c1fc295ccdd6912e17667fe07211557"},
	{4, true, "37f4b083dc4b89d9dc9fb227a24fccca9f3ec9db30e43f5763d1345fcf395770"},
	{5, true, "14ec8df078840411716dc55c6aa84923f8a67b3e8cd5e0206d4be55907ebe741"},
	{7, true, "4865b7186a8d474625e02a2548de667cee12688a0fe6080a5a74d2ed5c8af52a"},
	{8, true, "1386a8acfe5a46dc02e8bfcec7447c9437f31df250395f5daff15374bc17708f"},
	{9, true, "7fcc31775462c95206647a1ee7ce1a9774e6faf072c2d069d0a46684c3b2fee7"},
	{1023, true, "398f6d27a4c7260fc092dc8534635d68f8896286aaafa6b612cb5571202d0dd4"},
	{1024, true, "ce11e231f30779cc461def447f0faa7543604c054f19555d0df5603a544929aa"},
	{1025, true, "252a5232c82a0250163c312dcb42e9bfa6d72deebaa37c895b227f019aa91583"},
	{4097, true, "e64d97ef6b261efd5cb1c1f7f1f26b64b7dfba2a6a53b2d31af523f34a4f618b"},
	{70001, true, "cac44ca4b963a71cd810ea52e7cbd5f7a0712a4f72afef7f1c1e777000a74b59"},
}

// rootNodes packs the n test node hashes at the given stride, filling the
// fee and size bytes of a 48-byte record with non-zero values.
func rootNodes(n, stride int, placeholder bool) []byte {
	b := make([]byte, n*stride)
	for i := 0; i < n; i++ {
		h := rootNodeHash(i, placeholder)
		off := i * stride
		copy(b[off:], h[:])
		for k := off + 32; k < off+stride; k++ {
			b[k] = byte(i + k)
		}
	}
	return b
}

func TestSubtreeRoot_MatchesTeranode(t *testing.T) {
	for _, tc := range teranodeRoots {
		for _, stride := range []int{frame.SubtreeNodeHashSize, frame.SubtreeNodeFullSize} {
			got := subtreeRoot(rootNodes(tc.n, stride, tc.placeholder), stride, tc.n)
			if hex.EncodeToString(got[:]) != tc.root {
				t.Errorf("n=%d placeholder=%v stride=%d: root %x, want %s", tc.n, tc.placeholder, stride, got, tc.root)
			}
		}
	}
}

// subtreeObject is a BRC-143 object for the first n test nodes under root.
func subtreeObject(root [32]byte, n int) []byte {
	obj := make([]byte, 0, objfmt.SubtreeHeaderSize+n*32)
	obj = append(obj, root[:]...)
	obj = binary.BigEndian.AppendUint64(obj, uint64(n))
	return append(obj, rootNodes(n, 32, true)...)
}

// goldenRoot returns the Teranode root for n placeholder-led test nodes.
func goldenRoot(t *testing.T, n int) [32]byte {
	t.Helper()
	for _, tc := range teranodeRoots {
		if tc.n == n && tc.placeholder {
			var r [32]byte
			b, _ := hex.DecodeString(tc.root)
			copy(r[:], b)
			return r
		}
	}
	t.Fatalf("no golden root for n=%d", n)
	return [32]byte{}
}

// subtreeFrame reframes a BRC-143 object as the proxy's push lane does.
func subtreeFrame(t *testing.T, obj []byte) []byte {
	t.Helper()
	raw, err := objfmt.MulticastBytes(objfmt.ClassSubtree, obj)
	if err != nil {
		t.Fatalf("MulticastBytes: %v", err)
	}
	return raw
}

func TestVerifySubtreePayload(t *testing.T) {
	root := goldenRoot(t, 9)
	good := subtreeFrame(t, subtreeObject(root, 9))
	sf, err := frame.DecodeSubtreeData(good)
	if err != nil {
		t.Fatalf("DecodeSubtreeData: %v", err)
	}
	if err := verifySubtreePayload(sf.SubtreeID, sf.MsgType, sf.Payload); err != nil {
		t.Fatalf("honest subtree: %v", err)
	}

	t.Run("altered_node", func(t *testing.T) {
		p := append([]byte(nil), sf.Payload...)
		p[frame.SubtreeDataPayloadHeaderSize+5*32] ^= 0x01
		if err := verifySubtreePayload(sf.SubtreeID, sf.MsgType, p); err != errSubtreeRootMismatch {
			t.Errorf("err = %v, want mismatch", err)
		}
	})
	t.Run("reordered_nodes", func(t *testing.T) {
		p := append([]byte(nil), sf.Payload...)
		a := frame.SubtreeDataPayloadHeaderSize + 1*32
		b := frame.SubtreeDataPayloadHeaderSize + 2*32
		var tmp [32]byte
		copy(tmp[:], p[a:a+32])
		copy(p[a:a+32], p[b:b+32])
		copy(p[b:b+32], tmp[:])
		if err := verifySubtreePayload(sf.SubtreeID, sf.MsgType, p); err != errSubtreeRootMismatch {
			t.Errorf("err = %v, want mismatch", err)
		}
	})
	t.Run("dropped_last_node", func(t *testing.T) {
		p := append([]byte(nil), sf.Payload...)
		binary.BigEndian.PutUint64(p[16:24], 8)
		if err := verifySubtreePayload(sf.SubtreeID, sf.MsgType, p); err != errSubtreeRootMismatch {
			t.Errorf("err = %v, want mismatch", err)
		}
	})
	t.Run("zero_nodes", func(t *testing.T) {
		p := append([]byte(nil), sf.Payload...)
		binary.BigEndian.PutUint64(p[16:24], 0)
		if err := verifySubtreePayload(sf.SubtreeID, sf.MsgType, p); err != errSubtreeMalformed {
			t.Errorf("err = %v, want malformed", err)
		}
	})
	t.Run("count_past_payload", func(t *testing.T) {
		p := append([]byte(nil), sf.Payload...)
		binary.BigEndian.PutUint64(p[16:24], 1<<40)
		if err := verifySubtreePayload(sf.SubtreeID, sf.MsgType, p); err != errSubtreeMalformed {
			t.Errorf("err = %v, want malformed", err)
		}
	})
	t.Run("short_payload", func(t *testing.T) {
		if err := verifySubtreePayload(sf.SubtreeID, sf.MsgType, sf.Payload[:10]); err != errSubtreeMalformed {
			t.Errorf("err = %v, want malformed", err)
		}
	})
	t.Run("full_nodes", func(t *testing.T) {
		nodes := make([]frame.SubtreeNode, 9)
		for i := range nodes {
			nodes[i] = frame.SubtreeNode{TxHash: rootNodeHash(i, true), Fee: uint64(100 + i), Size: 226}
		}
		p, err := frame.EncodeSubtreeDataPayload(&frame.SubtreeDataPayload{Nodes: nodes}, frame.SubtreeMsgFullNodes)
		if err != nil {
			t.Fatalf("EncodeSubtreeDataPayload: %v", err)
		}
		if err := verifySubtreePayload(root, frame.SubtreeMsgFullNodes, p); err != nil {
			t.Errorf("honest FullNodes subtree: %v", err)
		}
		// Fee and size are outside the root: altering them still verifies.
		p[frame.SubtreeDataPayloadHeaderSize+32] ^= 0xFF
		if err := verifySubtreePayload(root, frame.SubtreeMsgFullNodes, p); err != nil {
			t.Errorf("FullNodes with altered fee: %v, want nil (the root does not commit to fees)", err)
		}
		p[frame.SubtreeDataPayloadHeaderSize] ^= 0x01
		if err := verifySubtreePayload(root, frame.SubtreeMsgFullNodes, p); err != errSubtreeRootMismatch {
			t.Errorf("FullNodes with altered hash: %v, want mismatch", err)
		}
	})
}

// TestVerifySubtreeRoot_Gate covers the forwarder gate: off by default, drops
// a failing subtree before the dedup claim so it cannot squat the root, and
// exempts no stamped frame even on a relay lane.
func TestVerifySubtreeRoot_Gate(t *testing.T) {
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}
	root := goldenRoot(t, 9)
	good := func() []byte { return subtreeFrame(t, subtreeObject(root, 9)) }
	bad := func() []byte {
		obj := subtreeObject(root, 9)
		obj[objfmt.SubtreeHeaderSize+3*32] ^= 0x01
		return subtreeFrame(t, obj)
	}

	t.Run("default_off_forwards_mismatch", func(t *testing.T) {
		fw := makeForwarder()
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		fw.ProcessSubtreeData(egr, bad(), src, 0)
		if got := countEnqueued(egr); got != 1 {
			t.Errorf("gate off: %d queued, want 1", got)
		}
	})

	t.Run("mismatch_dropped_before_claim", func(t *testing.T) {
		fw := makeForwarder()
		fw.SetVerifySubtreeRoot(true)
		d := newFakeDedup(true)
		fw.SetTxidDedup(d, "test:")
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)

		fw.ProcessSubtreeData(egr, bad(), src, 0)
		if got := countEnqueued(egr); got != 0 {
			t.Fatalf("mismatched subtree: %d queued, want 0", got)
		}
		if got := d.claimed.Load(); got != 0 {
			t.Fatalf("mismatched subtree took %d dedup claims, want 0: the gate must run before the claim", got)
		}

		fw.ProcessSubtreeData(egr, good(), src, 0)
		if got := countEnqueued(egr); got != 1 {
			t.Errorf("honest subtree after a bad copy: %d queued, want 1", got)
		}
		if got := d.claimed.Load(); got != 1 {
			t.Errorf("honest subtree claims = %d, want 1", got)
		}
	})

	t.Run("stamped_not_exempt_on_relay_lane", func(t *testing.T) {
		fw := makeForwarder()
		fw.SetVerifySubtreeRoot(true)
		fw.SetAllowStampedIngress(true)
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		raw := bad()
		binary.BigEndian.PutUint64(raw[48:56], 7) // sender-set SeqNum
		fw.ProcessSubtreeData(egr, raw, src, 0)
		if got := countEnqueued(egr); got != 0 {
			t.Errorf("stamped mismatched subtree: %d queued, want 0", got)
		}
	})

	t.Run("honest_forwarded", func(t *testing.T) {
		fw := makeForwarder()
		fw.SetVerifySubtreeRoot(true)
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		fw.ProcessSubtreeData(egr, good(), src, 0)
		if got := countEnqueued(egr); got != 1 {
			t.Errorf("honest subtree: %d queued, want 1", got)
		}
	})
}

func BenchmarkSubtreeRoot(b *testing.B) {
	for _, n := range []int{1024, 16384, 131072, 1048576} {
		nodes := rootNodes(n, 32, true)
		b.Run(fmt.Sprintf("nodes=%d", n), func(b *testing.B) {
			b.SetBytes(int64(len(nodes)))
			for b.Loop() {
				subtreeRoot(nodes, 32, n)
			}
		})
	}
}
