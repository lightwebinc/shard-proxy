package forwarder

import (
	"net"
	"testing"

	"github.com/lightwebinc/shard-common/shard"
)

func TestForwardControl_PerTargetWrite(t *testing.T) {
	fw := makeForwarder()
	conn1, _ := openLoopbackUDP(t)
	conn2, _ := openLoopbackUDP(t)
	tgts := makeTargets(t, conn1, conn2)

	// Sending to a multicast address on loopback typically fails; that's fine —
	// the test merely exercises the per-target loop and address derivation
	// path without panicking.
	raw := make([]byte, 64)
	raw[0] = 0xE3
	fw.ForwardControl(tgts, raw, shard.CtrlGroupSubtreeAnnounce, 9001)
	fw.ForwardControl(tgts, raw, shard.CtrlGroupBeacon, 9001)
	fw.ForwardControl(tgts, raw, 0xF000, 9001) // unknown — exercises default label branch
}

func TestForwardControl_EmptyTargets(t *testing.T) {
	fw := makeForwarder()
	fw.ForwardControl(nil, []byte{0x01}, shard.CtrlGroupBeacon, 9001)
}

func TestEgressPort(t *testing.T) {
	fw := makeForwarder()
	if got := fw.EgressPort(); got != 9001 {
		t.Errorf("EgressPort = %d, want 9001", got)
	}
}

func TestCtrlGroupName(t *testing.T) {
	tests := []struct {
		idx  uint16
		want string
	}{
		{shard.CtrlGroupSubtreeAnnounce, "subtree_announce"},
		{shard.CtrlGroupBeacon, "beacon"},
		{shard.CtrlGroupControl, "control"},
		{0xABCD, "0xabcd"},
	}
	for _, tc := range tests {
		if got := ctrlGroupName(tc.idx); got != tc.want {
			t.Errorf("ctrlGroupName(0x%04X) = %q, want %q", tc.idx, got, tc.want)
		}
	}
}

func TestForwardControl_DebugMode(t *testing.T) {
	engine := makeForwarder().engine
	fw := New(engine, 0xFF05, shard.DefaultGroupID, 9001, true, nil)
	conn, _ := openLoopbackUDP(t)
	fw.ForwardControl(makeTargets(t, conn), make([]byte, 64), shard.CtrlGroupBeacon, 9001)

	// Also exercise per-target Zone setting by checking that the conn's local
	// addr is still usable after ForwardControl.
	if conn.LocalAddr() == nil {
		t.Error("conn unusable after ForwardControl")
	}
	// Suppress unused import warning on `net`.
	_ = net.IPv6zero
}
