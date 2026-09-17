package config

import (
	"net"
	"testing"

	"github.com/lightwebinc/shard-common/shard"
)

// groupAddr renders the control-plane group the way main.go builds it, so
// these tests assert on the address an operator would see on the wire.
func groupAddr(prefix uint16) string {
	return shard.GroupAddr(prefix, shard.DefaultGroupID, shard.GroupBeacon).String()
}

// THE DEFECT. BRC-126 §Beacon Scopes and BRC-129 §Source Mode and Address
// Range both require the 0xFFFD control groups to take the source-specific
// FF3x prefix under SSM. Every release before this one derived FF05::B:FFFD
// whatever -source-mode said, so an SSM fabric — which only forwards
// ff35::/16 and ff3e::/16 — carried beacons on a group outside its own
// mroute ranges.
func TestControlGroupPrefixes_SSMUsesSourceSpecificPrefix(t *testing.T) {
	for _, tc := range []struct {
		scope string
		want  uint16
		addr  string
	}{
		{"site", 0xFF35, "ff35::b:fffd"},
		{"global", 0xFF3E, "ff3e::b:fffd"},
	} {
		got, err := ControlGroupPrefixes(ControlGroupDerived, "ssm", tc.scope)
		if err != nil {
			t.Fatalf("scope %s: %v", tc.scope, err)
		}
		if len(got) != 1 || got[0] != tc.want {
			t.Fatalf("scope %s under ssm: prefixes = %#04x, want [%#04x]", tc.scope, got, tc.want)
		}
		if addr := groupAddr(got[0]); !net.ParseIP(addr).Equal(net.ParseIP(tc.addr)) {
			t.Errorf("scope %s under ssm: group = %s, want %s", tc.scope, addr, tc.addr)
		}
	}
}

// ASM is unchanged in every compat mode: there is nothing to derive, so all
// four scope names keep the prefixes they always had and the flag is inert.
func TestControlGroupPrefixes_ASMUnchanged(t *testing.T) {
	for scope, want := range Scopes {
		for _, compat := range ControlGroupCompatValues {
			got, err := ControlGroupPrefixes(compat, "asm", scope)
			if err != nil {
				t.Fatalf("compat %s scope %s: %v", compat, scope, err)
			}
			if len(got) != 1 || got[0] != want {
				t.Errorf("compat %s scope %s under asm: prefixes = %#04x, want [%#04x]",
					compat, scope, got, want)
			}
		}
	}
}

// The sender-safe default. "asm-only" must never consult -source-mode: it is
// the pre-fix wire, kept so an un-upgraded peer is still reachable.
func TestControlGroupPrefixes_ASMOnlyIgnoresSourceMode(t *testing.T) {
	got, err := ControlGroupPrefixes(ControlGroupASMOnly, "ssm", "site")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != 0xFF05 {
		t.Fatalf("asm-only under ssm: prefixes = %#04x, want [0xff05]", got)
	}
}

// The receiver-safe default. "both" under SSM yields the legacy prefix FIRST
// and the conformant one second, so a listener joins the group an
// un-upgraded retry endpoint still advertises into as well as the one an
// upgraded endpoint will move to.
func TestControlGroupPrefixes_BothSpansTheFlagDay(t *testing.T) {
	got, err := ControlGroupPrefixes(ControlGroupBoth, "ssm", "site")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != 0xFF05 || got[1] != 0xFF35 {
		t.Fatalf("both under ssm: prefixes = %#04x, want [0xff05 0xff35]", got)
	}
}

// BRC-129 tables an SSM control group at site and global scope only.
// "derived" is an explicit request for the conformant address, so asking
// for one at link or org scope is a config error, not a silent fall back to
// the any-source prefix under a flag named "derived".
func TestControlGroupPrefixes_DerivedRejectsScopesWithNoSSMGroup(t *testing.T) {
	for _, scope := range []string{"link", "org"} {
		if _, err := ControlGroupPrefixes(ControlGroupDerived, "ssm", scope); err == nil {
			t.Errorf("derived scope %s under ssm: want error, got none", scope)
		}
	}
}

// "both" is the receiver-safe default, so it must never turn a binary
// upgrade into a startup failure: at a scope with no SSM control group it
// degrades to the any-source prefix alone — today's behaviour exactly.
func TestControlGroupPrefixes_BothDegradesWhereNoSSMGroupExists(t *testing.T) {
	for scope, want := range map[string]uint16{"link": 0xFF02, "org": 0xFF08} {
		got, err := ControlGroupPrefixes(ControlGroupBoth, "ssm", scope)
		if err != nil {
			t.Fatalf("both scope %s under ssm: %v", scope, err)
		}
		if len(got) != 1 || got[0] != want {
			t.Errorf("both scope %s under ssm: prefixes = %#04x, want [%#04x]", scope, got, want)
		}
	}
}

func TestControlGroupPrefixes_Invalid(t *testing.T) {
	if _, err := ControlGroupPrefixes("sometimes", "ssm", "site"); err == nil {
		t.Error("unknown compat mode: want error, got none")
	}
	if _, err := ControlGroupPrefixes(ControlGroupDerived, "ssm", "nonsense"); err == nil {
		t.Error("unknown scope: want error, got none")
	}
}

// The derived prefix must come from the shared helper the data plane uses,
// not a second table: if shard.Prefix ever changes, this follows it.
func TestControlGroupPrefixes_MatchesSharedHelper(t *testing.T) {
	for scopeName, scope := range map[string]shard.Scope{
		"site":   shard.ScopeSite,
		"global": shard.ScopeGlobal,
	} {
		want, err := shard.Prefix(shard.SourceModeSSM, scope)
		if err != nil {
			t.Fatal(err)
		}
		got, err := ControlGroupPrefixes(ControlGroupDerived, "ssm", scopeName)
		if err != nil {
			t.Fatal(err)
		}
		if got[0] != want {
			t.Errorf("scope %s: derived %#04x, shard.Prefix says %#04x", scopeName, got[0], want)
		}
	}
}

// A second, separate defect the same helper closes. BRC-127 subtree group
// announcements sit on control-plane index 0xFFFC, so BRC-129's prefix rule
// covers them too — but here the SENDER was already conformant
// (shard-proxy emits via its -source-mode-derived prefix), so an FF0x-only
// join meant no announcement ever arrived on an SSM fabric. Not a flag day:
// a live mismatch.

// The default joins both, so a listener works against a proxy on either
// prefix without the operator having to know which.
