package worker

import (
	"net"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
)

// TestHandleConn_UnsupportedFrameVersion exercises the default switch arm
// that closes the connection on unknown FrameVer values.
func TestHandleConn_UnsupportedFrameVersion(t *testing.T) {
	hdr := make([]byte, frame.HeaderSizeLegacy)
	hdr[0], hdr[1], hdr[2], hdr[3] = 0xE3, 0xE1, 0xF3, 0xE8
	hdr[4], hdr[5] = 0x02, 0xBF
	hdr[6] = 0x77 // unknown frame version
	dialHandleConn(t, func(conn net.Conn) {
		_, _ = conn.Write(hdr)
	})
}

// TestHandleConn_TruncatedPayload writes a complete BRC-124 header declaring
// a 10-byte payload but sends only 2 bytes before closing. handleConn must
// return cleanly.
func TestHandleConn_TruncatedPayload(t *testing.T) {
	raw := buildTCPFrame(t, 0xAB, 1, []byte("0123456789"))
	dialHandleConn(t, func(conn net.Conn) {
		// Send header + 2 bytes of payload, then close.
		_, _ = conn.Write(raw[:frame.HeaderSize+2])
	})
}

// TestRun_AcceptLoop_StartsAndStops verifies the Run accept loop terminates
// cleanly when done is closed.
func TestRun_AcceptLoop_StartsAndStops(t *testing.T) {
	fwd := makeTestForwarder()
	ifaces := []*net.Interface{{Index: 1, Name: "lo"}}
	ti := NewTCPIngress(fwd, ifaces, nil)

	// Pick a free port.
	ln, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skipf("tcp6 unavailable: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()

	done := make(chan struct{})
	errCh := make(chan error, 1)
	go func() { errCh <- ti.Run("[::1]", port, done) }()

	// Give Run a moment to bind, then dial to make sure it's accepting.
	time.Sleep(100 * time.Millisecond)
	c, err := net.DialTimeout("tcp6", net.JoinHostPort("::1", itoa(port)), 500*time.Millisecond)
	if err == nil {
		_ = c.Close()
	}

	close(done)
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("Run returned err: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after done close")
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
