package worker

import (
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// The admit hook must fire on the TCP submission lane. This is the regression
// that motivated it: an ingress-side attribution metric wired only into the UDP
// worker loop reads ZERO on a fabric whose submissions all arrive over TCP,
// which is the supported transport.
func TestAdmitHook_BareTCPStream(t *testing.T) {
	fwd := makeTestForwarder()
	fwd.SetRequireEF(true)
	ti := NewTCPIngress(fwd, []*net.Interface{{Index: 1, Name: "lo"}}, nil)
	var frames, bytes atomic.Int64
	ti.SetAdmitHook(func(f, b int) { frames.Add(int64(f)); bytes.Add(int64(b)) })
	ti.SetFlushVia(func(_ int, _ []byte, _ *net.UDPAddr) error { return nil }, nil)

	egr := makeLoopbackEgress(t, fwd)
	tx := bareEFTx()
	srv, cli := net.Pipe()
	done := make(chan struct{})
	go func() { ti.handleConn(srv, egr); close(done) }()
	go func() { _, _ = cli.Write(append(append([]byte(nil), tx...), tx...)); _ = cli.Close() }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleConn hang")
	}
	if got := frames.Load(); got != 2 {
		t.Fatalf("admitted frames = %d, want 2", got)
	}
	if got, want := bytes.Load(), int64(2*len(tx)); got != want {
		t.Fatalf("admitted bytes = %d, want %d", got, want)
	}
}

// No hook installed must behave exactly as before, the lane still admits, and
// nothing panics on the nil func.
func TestAdmitHook_NilIsInert(t *testing.T) {
	if sunk := runBareConn(t, true, append(bareEFTx(), bareEFTx()...)); len(sunk) != 2 {
		t.Fatalf("admitted %d frames without a hook, want 2", len(sunk))
	}
}
