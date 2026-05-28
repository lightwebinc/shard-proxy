package worker

import (
	"net"
	"testing"
)

func TestNew_DefaultRecvBatch(t *testing.T) {
	fwd := makeTestForwarder()
	ifaces := []*net.Interface{{Index: 1, Name: "lo"}}
	w := New(0, fwd, ifaces, nil)
	if w.recvBatch != DefaultRecvBatch {
		t.Errorf("recvBatch = %d after New, want %d", w.recvBatch, DefaultRecvBatch)
	}
}

func TestSetRecvBatch_OverridesDefault(t *testing.T) {
	fwd := makeTestForwarder()
	w := New(0, fwd, nil, nil)
	w.SetRecvBatch(64)
	if w.recvBatch != 64 {
		t.Errorf("recvBatch = %d after SetRecvBatch(64), want 64", w.recvBatch)
	}
}

func TestSetRecvBatch_ZeroClampsToOne(t *testing.T) {
	fwd := makeTestForwarder()
	w := New(0, fwd, nil, nil)
	w.SetRecvBatch(0)
	if w.recvBatch != 1 {
		t.Errorf("recvBatch = %d after SetRecvBatch(0), want 1 (clamp floor)", w.recvBatch)
	}
}

func TestSetRecvBatch_NegativeClampsToOne(t *testing.T) {
	fwd := makeTestForwarder()
	w := New(0, fwd, nil, nil)
	w.SetRecvBatch(-5)
	if w.recvBatch != 1 {
		t.Errorf("recvBatch = %d after SetRecvBatch(-5), want 1 (clamp floor)", w.recvBatch)
	}
}
