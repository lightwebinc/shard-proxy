package manifest

import (
	"net/netip"
	"testing"
)

func TestPosture(t *testing.T) {
	if got := posture(nil); got != "asm" {
		t.Errorf("empty sources → %q, want asm", got)
	}
	if got := posture([]netip.Addr{}); got != "asm" {
		t.Errorf("zero-len sources → %q, want asm", got)
	}
	if got := posture([]netip.Addr{mustAddr("2001:db8::1")}); got != "ssm" {
		t.Errorf("non-empty sources → %q, want ssm", got)
	}
}

func TestReasonForChange(t *testing.T) {
	if got := reasonForChange(true); got != "quorum-shift" {
		t.Errorf("hadPrev=true → %q, want quorum-shift", got)
	}
	if got := reasonForChange(false); got != "bootstrap" {
		t.Errorf("hadPrev=false → %q, want bootstrap", got)
	}
}

func TestRestartRequest_ReasonEmptyBeforeRequest(t *testing.T) {
	var r RestartRequest
	if got := r.Reason(); got != "" {
		t.Errorf("Reason before Request = %q, want empty", got)
	}
}
