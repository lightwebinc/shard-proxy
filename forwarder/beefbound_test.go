package forwarder

import (
	"net"
	"testing"
)

type boundPolicy struct {
	auth net.IP
	lift int
}

func (p *boundPolicy) AdmitTopics(net.IP, int) bool { return false }
func (p *boundPolicy) OnFanout(net.IP, int, int)    {}
func (p *boundPolicy) MaxObjectBytes(src net.IP) int {
	if src != nil && src.Equal(p.auth) {
		return p.lift
	}
	return 0
}

// D9: an authenticated source may exceed the open bound, but a policy must
// never TIGHTEN below the operator's floor — that would let a policy bug
// silently reject traffic the operator configured as acceptable.
func TestBEEFObjectBoundPerSource(t *testing.T) {
	fw := &Forwarder{beefMaxObject: 1 << 20}
	auth := net.ParseIP("fd00:57::5")
	open := net.ParseIP("2001:db8::9")

	if got := fw.beefObjectBoundFor(&net.UDPAddr{IP: open}); got != 1<<20 {
		t.Errorf("no policy: bound = %d, want the open bound", got)
	}

	fw.SetBEEFSubmitPolicy(&boundPolicy{auth: auth, lift: 8 << 20})
	if got := fw.beefObjectBoundFor(&net.UDPAddr{IP: auth}); got != 8<<20 {
		t.Errorf("authenticated bound = %d, want 8 MiB uplift", got)
	}
	if got := fw.beefObjectBoundFor(&net.UDPAddr{IP: open}); got != 1<<20 {
		t.Errorf("open source bound = %d, want the open bound (no uplift)", got)
	}

	// A policy returning LESS than the operator's bound must be ignored.
	fw.SetBEEFSubmitPolicy(&boundPolicy{auth: auth, lift: 1024})
	if got := fw.beefObjectBoundFor(&net.UDPAddr{IP: auth}); got != 1<<20 {
		t.Errorf("policy tightened the bound to %d — must never go below the operator floor", got)
	}
}

// The stream-read bound follows the per-source object bound, plus room for the
// largest record envelope, so an authenticated submitter's larger objects are
// still readable while an open one is cut off near its own bound.
func TestBEEFRecordBoundPerSource(t *testing.T) {
	if got := (&Forwarder{}).BEEFRecordBound(&net.UDPAddr{}); got != 0 {
		t.Errorf("no object bound: record bound = %d, want 0 (caller keeps its ceiling)", got)
	}

	fw := &Forwarder{beefMaxObject: 1 << 20}
	auth := net.ParseIP("fd00:57::5")
	fw.SetBEEFSubmitPolicy(&boundPolicy{auth: auth, lift: 8 << 20})

	if got := fw.BEEFRecordBound(&net.TCPAddr{IP: net.ParseIP("2001:db8::9")}); got != 1<<20+beefRecordMaxEnvelope {
		t.Errorf("open record bound = %d, want %d", got, 1<<20+beefRecordMaxEnvelope)
	}
	if got := fw.BEEFRecordBound(&net.TCPAddr{IP: auth}); got != 8<<20+beefRecordMaxEnvelope {
		t.Errorf("authenticated record bound = %d, want %d", got, 8<<20+beefRecordMaxEnvelope)
	}
	if beefRecordMaxEnvelope != 983 {
		t.Errorf("envelope = %d, want 983 (4 + 15 x 65 + 4)", beefRecordMaxEnvelope)
	}
}
