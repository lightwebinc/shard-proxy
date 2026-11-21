package manifest

import (
	"net/netip"
	"testing"

	commanifest "github.com/lightwebinc/shard-common/manifest"
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

func TestRunningPin(t *testing.T) {
	r := Running{ShardBits: 8, SourceModeSSM: true, DomainShardBits: map[uint8]uint8{1: 4}}
	p := r.Pin()
	if !p.HasShardBitsPin || p.ShardBits != 8 {
		t.Errorf("ShardBits pin = (%v, %d), want (true, 8)", p.HasShardBitsPin, p.ShardBits)
	}
	if !p.HasSourceModePin || !p.SourceModeSSM {
		t.Errorf("source mode pin = (%v, ssm %v), want (true, ssm true)", p.HasSourceModePin, p.SourceModeSSM)
	}
	if p.DomainShardBits[1] != 4 || len(p.DomainShardBits) != 1 {
		t.Errorf("domain pins = %v, want map[1:4]", p.DomainShardBits)
	}
	p.DomainShardBits[1] = 9
	if r.DomainShardBits[1] != 4 {
		t.Errorf("Pin shares its domain map with Running")
	}
}

func TestSettle(t *testing.T) {
	a := &Applier{Running: Running{ShardBits: 8, DomainShardBits: map[uint8]uint8{1: 4}}}
	prev := a.Running.adopted()

	// Nothing adopted yet: ShardBits 0 and an absent plane keep the
	// previous values, so they never read as transitions.
	got := a.settle(prev, commanifest.Adopted{})
	if got.ShardBits != 8 || len(got.Domains) != 1 || got.Domains[1].ShardBits != 4 {
		t.Errorf("settle(nothing adopted) = bits %d, domains %v; want 8, map[1:4]", got.ShardBits, got.Domains)
	}

	// Adopted values pass through; a plane the process does not run is dropped.
	got = a.settle(prev, commanifest.Adopted{ShardBits: 9, Domains: map[uint8]commanifest.DomainAdoption{
		1: {ShardBits: 5}, 2: {ShardBits: 6},
	}})
	if got.ShardBits != 9 || len(got.Domains) != 1 || got.Domains[1].ShardBits != 5 {
		t.Errorf("settle(adopted) = bits %d, domains %v; want 9, map[1:5]", got.ShardBits, got.Domains)
	}
}
