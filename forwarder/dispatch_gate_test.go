package forwarder

import (
	"net"
	"testing"

	"github.com/lightwebinc/shard-common/frame"
)

// countEnqueued drains egr through FlushVia and returns how many datagrams
// were queued for egress, so a test can assert whether a frame was forwarded
// or dropped by the ingress-class gate.
func countEnqueued(egr *Egress) int {
	n := 0
	egr.FlushVia(func(_ int, _ []byte, _ *net.UDPAddr) error {
		n++
		return nil
	})
	return n
}

// dispatchCase is one (frame builder, expected-forward-on-transaction-socket)
// pair. The frame is built per use, never shared: forwarding stamps HashKey and
// SeqNum into the buffer in place, so a case reused across subtests would hand
// the second one an already-stamped frame, which the stamped-ingress gate
// rejects, and which is not the input the subtest means to exercise.
type dispatchCase struct {
	name           string
	build          func(*testing.T) []byte
	privilegedOnly bool // dropped on a transaction-only socket
}

func gateCases() []dispatchCase {
	return []dispatchCase{
		{"brc124_tx", func(t *testing.T) []byte { return buildV2Frame(t, 0xAB, 0, []byte("tx")) }, false},
		{"brc12_legacy", func(t *testing.T) []byte { return buildV1Frame(t, 0xAB, []byte("tx")) }, false},
		{"brc134_anchor", func(t *testing.T) []byte { return buildAnchorFrame(t, 0xAB, 0, []byte("anchor")) }, false},
		{"brc131_block", func(t *testing.T) []byte { return buildBlockBufForwarder(t, 0xBB, []byte("blk")) }, true},
		{"brc133_coinbase", func(t *testing.T) []byte { return buildCoinbaseBufForwarder(t, 0xBB, []byte("cb")) }, true},
		{"brc132_subtree", func(t *testing.T) []byte { return buildSubtreeDataFrame(t, 0xAA, []byte("sub")) }, true},
	}
}

// TestDispatchClass_TransactionGate verifies that a transaction-only socket
// forwards tx/anchor frames but drops privileged block/coinbase/subtree-data
// frames, while a privileged socket forwards everything.
func TestDispatchClass_TransactionGate(t *testing.T) {
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}

	for _, tc := range gateCases() {
		t.Run(tc.name+"/transaction", func(t *testing.T) {
			fw := makeForwarder()
			conn, _ := openLoopbackUDP(t)
			egr := makeEgress(t, fw, conn)
			fw.DispatchClass(egr, tc.build(t), src, 0, IngressTransaction)
			got := countEnqueued(egr)
			if tc.privilegedOnly && got != 0 {
				t.Errorf("transaction socket forwarded privileged frame (%d queued), want 0 (dropped)", got)
			}
			if !tc.privilegedOnly && got == 0 {
				t.Errorf("transaction socket dropped a transaction frame, want forwarded")
			}
		})

		t.Run(tc.name+"/privileged", func(t *testing.T) {
			fw := makeForwarder()
			conn, _ := openLoopbackUDP(t)
			egr := makeEgress(t, fw, conn)
			fw.DispatchClass(egr, tc.build(t), src, 0, IngressPrivileged)
			if got := countEnqueued(egr); got == 0 {
				t.Errorf("privileged socket dropped %s, want forwarded", tc.name)
			}
		})
	}
}

// TestDispatch_DefaultAcceptsAll verifies the legacy Dispatch entry point
// behaves as a privileged socket (accepts every frame class).
func TestDispatch_DefaultAcceptsAll(t *testing.T) {
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}
	for _, tc := range gateCases() {
		fw := makeForwarder()
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		fw.Dispatch(egr, tc.build(t), src, 0)
		if got := countEnqueued(egr); got == 0 {
			t.Errorf("Dispatch dropped %s, want forwarded (legacy accept-all)", tc.name)
		}
	}
}

// TestRejectPrivileged_FrameTypeLabel covers the metric frame-type
// classification branch for both block-announce and coinbase V4 frames plus
// V5 subtree data, exercising rejectPrivileged via a transaction socket.
func TestRejectPrivileged_FrameTypeLabel(t *testing.T) {
	fw := makeForwarder()
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 1}
	conn, _ := openLoopbackUDP(t)

	// Each must be dropped (no panic, no enqueue) on a transaction socket.
	for _, raw := range [][]byte{
		buildBlockBufForwarder(t, 0x01, nil),
		buildCoinbaseBufForwarder(t, 0x02, nil),
		buildSubtreeDataFrame(t, 0x03, nil),
	} {
		egr := makeEgress(t, fw, conn)
		fw.DispatchClass(egr, raw, src, 0, IngressTransaction)
		if got := countEnqueued(egr); got != 0 {
			t.Errorf("privileged frame ver=%d enqueued %d, want 0", raw[6], got)
		}
	}

	// Guard the coinbase classification path directly.
	cb := buildCoinbaseBufForwarder(t, 0x02, nil)
	if cb[7] != frame.BlockMsgCoinbase {
		t.Fatalf("coinbase fixture MsgType=%d, want %d", cb[7], frame.BlockMsgCoinbase)
	}
}
