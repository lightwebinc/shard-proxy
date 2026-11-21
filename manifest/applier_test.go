package manifest

import (
	"context"
	"fmt"
	"log/slog"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
	commanifest "github.com/lightwebinc/shard-common/manifest"
	"github.com/lightwebinc/shard-common/shard"
	"github.com/prometheus/common/expfmt"

	"github.com/lightwebinc/shard-proxy/metrics"
)

func mustAddr(s string) netip.Addr {
	a, err := netip.ParseAddr(s)
	if err != nil {
		panic(err)
	}
	return a
}

func authoritativeManifest(id uint32, sb uint8) *frame.ShardManifest {
	return &frame.ShardManifest{
		Flags:      frame.ShardManifestFlagAuthoritative | frame.ShardManifestFlagGroupsValid | frame.ShardManifestFlagPilotOnly,
		InstanceID: id,
		// Registry expiry is Epoch + TTL (BRC-139), so a fixture dated
		// in the past is born expired. These appliers run on the wall
		// clock, so the manifest has to be stamped now.
		Epoch:            uint32(time.Now().Unix()), //nolint:gosec // wraps in 2106
		AnnounceInterval: 300,
		ShardBits:        sb,
		Groups:           []uint16{0},
	}
}

// pilotManifest is an authoritative pilot announcing ShardBits sb, the
// source mode, and the given object planes.
func pilotManifest(id uint32, sb uint8, ssm bool, planes ...frame.DomainDescriptor) *frame.ShardManifest {
	m := authoritativeManifest(id, sb)
	if ssm {
		m.Flags |= frame.ShardManifestFlagSourceModeSSM
	}
	if len(planes) > 0 {
		m.Flags |= frame.ShardManifestFlagDomainsValid
		m.Domains = planes
	}
	return m
}

func plane(id, bits uint8) frame.DomainDescriptor {
	return frame.DomainDescriptor{DomainID: id, ShardBits: bits, SlotSpan: 1, Flags: frame.DomainFlagActive}
}

// twoPilots is a registry holding two agreeing pilots built by mk (quorum 2).
func twoPilots(mk func(id uint32) *frame.ShardManifest) *commanifest.Registry {
	reg := commanifest.NewRegistry(60 * time.Second)
	reg.Upsert(mustAddr("2001:db8::1"), mk(1))
	reg.Upsert(mustAddr("2001:db8::2"), mk(2))
	return reg
}

func evaluator(pin commanifest.Pin) *commanifest.Evaluator {
	return commanifest.NewEvaluator(commanifest.EvaluatorConfig{Quorum: 2, Hysteresis: time.Nanosecond, Pin: pin})
}

var discard = slog.New(slog.DiscardHandler)

// hookLog records every value-hook call. With restartOnChange set, each call
// also requests a restart, which is how restart-mode wiring used these hooks:
// a call there was a process exit, and on the next start the same call again.
type hookLog struct {
	mu      sync.Mutex
	calls   []string
	restart RestartRequest
}

func (h *hookLog) hooks(restartOnChange bool) Hooks {
	note := func(s string) {
		h.mu.Lock()
		h.calls = append(h.calls, s)
		h.mu.Unlock()
		if restartOnChange {
			h.restart.Request(s)
		}
	}
	return Hooks{
		OnShardBitsChange: func(prev, next uint8) { note(fmt.Sprintf("shard_bits %d->%d", prev, next)) },
		OnSourceModeChange: func(prev, next bool) {
			note(fmt.Sprintf("source_mode_ssm %v->%v", prev, next))
		},
		OnDomainShardBitsChange: func(d, prev, next uint8) {
			note(fmt.Sprintf("domain_%d_shard_bits %d->%d", d, prev, next))
		},
	}
}

func (h *hookLog) snapshot() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.calls)
}

// assertQuiet fails if any hook fired or a restart was requested.
func (h *hookLog) assertQuiet(t *testing.T) {
	t.Helper()
	if got := h.snapshot(); len(got) != 0 {
		t.Errorf("hooks fired %q, want none", got)
	}
	if h.restart.Requested() {
		t.Errorf("restart requested (%q), want none: the restarted process would re-read the same flags and loop", h.restart.Reason())
	}
}

// runFor runs a for d, which at a 5 ms interval is dozens of evaluations.
func runFor(a *Applier, d time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	a.Run(ctx)
}

// runUntil runs a until cond holds (or 5 s pass), then for settle more, so a
// hook that would fire again on a later evaluation gets the chance to.
func runUntil(t *testing.T, a *Applier, cond func() bool, settle time.Duration) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); a.Run(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(settle)
	cancel()
	<-done
}

func TestRestartRequest_IdempotentAndConcurrent(t *testing.T) {
	var r RestartRequest
	if r.Requested() {
		t.Fatalf("initially Requested = true")
	}
	r.Request("first")
	if !r.Requested() {
		t.Errorf("after Request: Requested = false")
	}
	r.Request("second") // should be a no-op
	if got := r.Reason(); got != "first" {
		t.Errorf("Reason = %q, want %q", got, "first")
	}

	// Concurrent races should still produce exactly one reason.
	var rr RestartRequest
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rr.Request("racer")
		}(i)
	}
	wg.Wait()
	if !rr.Requested() {
		t.Errorf("after concurrent Request: Requested = false")
	}
}

// TestApplier_StableManifestEqualToRunningFiresNothing is the restart-loop
// regression. Pilots that agree with the running configuration must fire no
// hook, so restart-mode wiring never requests an exit. Diffed against a zero
// value instead, the first evaluation after every start reported ShardBits
// 0->8 and the BEEF plane 0->4 (and, under SSM, source mode ASM->SSM): an
// exit on every start, forever.
func TestApplier_StableManifestEqualToRunningFiresNothing(t *testing.T) {
	for _, tc := range []struct {
		name   string
		ssm    bool
		pinned bool
	}{
		{"asm pinned", false, true},
		{"ssm pinned", true, true},
		// Without the pin, adoption takes an evaluation or two; what it
		// adopts equals the running values, so it is still no transition.
		{"asm unpinned", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			running := Running{ShardBits: 8, SourceModeSSM: tc.ssm, DomainShardBits: map[uint8]uint8{shard.DomainBEEF: 4}}
			reg := twoPilots(func(id uint32) *frame.ShardManifest {
				return pilotManifest(id, 8, tc.ssm, plane(shard.DomainBEEF, 4))
			})
			var pin commanifest.Pin
			if tc.pinned {
				pin = running.Pin()
			}
			var h hookLog
			a := &Applier{Registry: reg, Evaluator: evaluator(pin), Hooks: h.hooks(true),
				Log: discard, Interval: 5 * time.Millisecond, Running: running}
			runFor(a, 150*time.Millisecond)
			h.assertQuiet(t)
		})
	}
}

// TestApplier_PlaneNotRunNeverFires: a plane the process does not run has no
// running width to move away from, and no restart could make the process run
// it. Announcing one must not fire the domain hook.
func TestApplier_PlaneNotRunNeverFires(t *testing.T) {
	running := Running{ShardBits: 8, DomainShardBits: map[uint8]uint8{shard.DomainBEEF: 4}}
	reg := twoPilots(func(id uint32) *frame.ShardManifest {
		return pilotManifest(id, 8, false, plane(shard.DomainBEEF, 4), plane(0x2, 6))
	})
	var h hookLog
	a := &Applier{Registry: reg, Evaluator: evaluator(running.Pin()), Hooks: h.hooks(true),
		Log: discard, Interval: 5 * time.Millisecond, Running: running}
	runFor(a, 150*time.Millisecond)
	h.assertQuiet(t)
}

// TestApplier_RealChangeFiresOnce: an evaluator free to adopt (no pin) that
// adopts values other than the running ones reports each transition exactly
// once, from the running value, however many evaluations follow. The 0 it
// reports before adopting is not a transition.
func TestApplier_RealChangeFiresOnce(t *testing.T) {
	running := Running{ShardBits: 8, DomainShardBits: map[uint8]uint8{shard.DomainBEEF: 4}}
	reg := twoPilots(func(id uint32) *frame.ShardManifest {
		return pilotManifest(id, 9, false, plane(shard.DomainBEEF, 5))
	})
	var h hookLog
	a := &Applier{Registry: reg, Evaluator: evaluator(commanifest.Pin{}), Hooks: h.hooks(false),
		Log: discard, Interval: 5 * time.Millisecond, Running: running}
	want := []string{"shard_bits 8->9", "domain_1_shard_bits 4->5"}
	runUntil(t, a, func() bool { return len(h.snapshot()) >= len(want) }, 100*time.Millisecond)
	if got := h.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("hooks fired %q, want exactly %q", got, want)
	}
}

// logCapture is a slog.Handler keeping every record.
type logCapture struct {
	mu   sync.Mutex
	recs []slog.Record
}

func (c *logCapture) Enabled(context.Context, slog.Level) bool { return true }
func (c *logCapture) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	c.recs = append(c.recs, r.Clone())
	c.mu.Unlock()
	return nil
}
func (c *logCapture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *logCapture) WithGroup(string) slog.Handler      { return c }

// divergenceWarnings counts the divergence warnings per field.
func (c *logCapture) divergenceWarnings() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[string]int{}
	for _, r := range c.recs {
		if r.Level != slog.LevelWarn || !strings.HasPrefix(r.Message, "manifest divergence") {
			continue
		}
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "field" {
				out[a.Value.String()]++
			}
			return true
		})
	}
	return out
}

// divergenceCounted returns the fields multicast_manifest_divergence_total
// carries in rec's exposition.
func divergenceCounted(t *testing.T, rec *metrics.Recorder) []string {
	t.Helper()
	mfs, err := rec.Gatherer().Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	var sb strings.Builder
	enc := expfmt.NewEncoder(&sb, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, mf := range mfs {
		_ = enc.Encode(mf)
	}
	var fields []string
	for _, line := range strings.Split(sb.String(), "\n") {
		if !strings.HasPrefix(line, "multicast_manifest_divergence_total{") {
			continue
		}
		if _, rest, ok := strings.Cut(line, `field="`); ok {
			if f, _, ok := strings.Cut(rest, `"`); ok {
				fields = append(fields, f)
			}
		}
	}
	slices.Sort(fields)
	return fields
}

// TestApplier_DivergenceNeverRestarts: pilots that disagree with every value
// the process runs (ShardBits, source mode, BEEF plane) fire no hook and
// request no restart, because every running value is pinned. The
// disagreement surfaces as divergence instead: counted on every evaluation,
// logged once per field. The second start is what a restart would be: the
// same flags against the same pilots. It behaves the same, so nothing loops.
func TestApplier_DivergenceNeverRestarts(t *testing.T) {
	running := Running{ShardBits: 8, DomainShardBits: map[uint8]uint8{shard.DomainBEEF: 4}}
	reg := twoPilots(func(id uint32) *frame.ShardManifest {
		return pilotManifest(id, 9, true, plane(shard.DomainBEEF, 5))
	})
	wantFields := []string{"domain_1_shard_bits", "shard_bits", "source_mode"}
	for start := 1; start <= 2; start++ {
		t.Run(fmt.Sprintf("start %d", start), func(t *testing.T) {
			rec, err := metrics.New("t", 1, "", 0)
			if err != nil {
				t.Fatalf("metrics: %v", err)
			}
			logs := &logCapture{}
			var h hookLog
			a := &Applier{Registry: reg, Evaluator: evaluator(running.Pin()), Hooks: h.hooks(true),
				Rec: rec, Log: slog.New(logs), Interval: 5 * time.Millisecond, Running: running}
			// Run until every field has warned, then long enough for a
			// repeat warning, a hook or a restart to show up.
			runUntil(t, a, func() bool {
				w := logs.divergenceWarnings()
				return w["shard_bits"] > 0 && w["source_mode"] > 0 && w["domain_1_shard_bits"] > 0
			}, 100*time.Millisecond)

			h.assertQuiet(t)
			if got := divergenceCounted(t, rec); !slices.Equal(got, wantFields) {
				t.Errorf("divergence counted for %q, want %q", got, wantFields)
			}
			warned := logs.divergenceWarnings()
			for _, f := range wantFields {
				if warned[f] != 1 {
					t.Errorf("divergence warning for %s logged %d times, want once", f, warned[f])
				}
			}
		})
	}
}

func TestApplier_SuccessorHookFiresAndClears(t *testing.T) {
	reg := commanifest.NewRegistry(60 * time.Second)
	ev := commanifest.NewEvaluator(commanifest.EvaluatorConfig{
		Quorum:     2,
		Hysteresis: 1 * time.Nanosecond,
	})

	// First adopt active SB=8.
	reg.Upsert(mustAddr("2001:db8::1"), authoritativeManifest(1, 8))
	reg.Upsert(mustAddr("2001:db8::2"), authoritativeManifest(2, 8))

	// Now attach a successor block to both pilots.
	addSuccessor := func(m *frame.ShardManifest) {
		m.Flags |= frame.ShardManifestFlagSuccessorValid
		m.Successor = &frame.SuccessorBlock{
			ShardBits:       9,
			Flags:           frame.SuccessorFlagSourceModeSSM,
			TransitionEpoch: uint32(time.Now().Add(1 * time.Hour).Unix()),
		}
	}

	mu := sync.Mutex{}
	var lastBefore, lastAfter *commanifest.SuccessorView
	hookFires := 0

	a := &Applier{
		Registry:  reg,
		Evaluator: ev,
		Interval:  10 * time.Millisecond,
		Log:       discard,
		Running:   Running{ShardBits: 8},
		Hooks: Hooks{
			OnSuccessorChange: func(before, after *commanifest.SuccessorView) {
				mu.Lock()
				defer mu.Unlock()
				lastBefore = before
				lastAfter = after
				hookFires++
			},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	go a.Run(ctx)

	// Let the applier observe steady state first.
	time.Sleep(40 * time.Millisecond)

	// Add successor.
	m1 := authoritativeManifest(1, 8)
	m2 := authoritativeManifest(2, 8)
	addSuccessor(m1)
	addSuccessor(m2)
	reg.Upsert(mustAddr("2001:db8::1"), m1)
	reg.Upsert(mustAddr("2001:db8::2"), m2)
	time.Sleep(50 * time.Millisecond)

	cancel()
	time.Sleep(20 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if hookFires == 0 {
		t.Fatalf("OnSuccessorChange never fired")
	}
	if lastAfter == nil {
		t.Errorf("last successor view = nil, want non-nil")
	} else if lastAfter.ShardBits != 9 {
		t.Errorf("last successor.ShardBits = %d, want 9", lastAfter.ShardBits)
	}
	_ = lastBefore
}
