package manifest

import (
	"context"
	"log/slog"
	"maps"
	"reflect"
	"slices"
	"sync/atomic"
	"time"

	commanifest "github.com/lightwebinc/shard-common/manifest"

	"github.com/lightwebinc/shard-proxy/metrics"
)

// RestartRequest is a goroutine-safe, idempotent latch recording that the
// process should exit, with the first reason given.
//
// Deprecated: do not request a restart to apply an adopted manifest value.
// A restart re-reads the same flags, so the process comes back running the
// same configuration, sees the same adoption and, restarting again, loops.
// The proxy never requests one; see [Running].
type RestartRequest struct {
	requested atomic.Bool
	reason    atomic.Value // string
}

// Request marks a restart as needed with the given reason. Idempotent.
func (r *RestartRequest) Request(reason string) {
	if r.requested.CompareAndSwap(false, true) {
		r.reason.Store(reason)
	}
}

// Requested reports whether a restart has been requested.
func (r *RestartRequest) Requested() bool { return r.requested.Load() }

// Reason returns the recorded reason (empty if not yet requested).
func (r *RestartRequest) Reason() string {
	if v := r.reason.Load(); v != nil {
		return v.(string)
	}
	return ""
}

// Running is the configuration the process runs with: the values it was
// started from (CLI/env). The [Applier] compares the adopted view with it,
// never with a zero value, so a manifest that agrees with the running
// configuration fires nothing.
//
// A restart re-reads the same flags and comes back with the same Running
// view: nothing adopted at run time survives it. Build the evaluator with
// [Running.Pin] so adoption never moves a value the process cannot apply,
// and announcers that disagree surface as divergence instead.
type Running struct {
	ShardBits     uint8 // transaction plane (domain 0x0) shard-bit width
	SourceModeSSM bool  // true when the data plane runs SSM

	// DomainShardBits is the shard-bit width of each BRC-148 object plane
	// the process runs, keyed by DomainID. Per-domain transitions are
	// reported only for these planes.
	DomainShardBits map[uint8]uint8
}

// Pin returns an evaluator pin that holds every running value. The CLI
// value is the local authority (BRC-139 manual pin precedence): adoption
// never moves it, and an authoritative announcer that disagrees with it is
// reported in [commanifest.Adopted.DivergenceFields].
func (r Running) Pin() commanifest.Pin {
	return commanifest.Pin{
		ShardBits:        r.ShardBits,
		HasShardBitsPin:  true,
		SourceModeSSM:    r.SourceModeSSM,
		HasSourceModePin: true,
		DomainShardBits:  maps.Clone(r.DomainShardBits),
	}
}

// adopted returns the running configuration as an adopted view: the
// baseline the first evaluation is compared with.
func (r Running) adopted() commanifest.Adopted {
	v := commanifest.Adopted{ShardBits: r.ShardBits, SourceModeSSM: r.SourceModeSSM}
	if len(r.DomainShardBits) > 0 {
		v.Domains = make(map[uint8]commanifest.DomainAdoption, len(r.DomainShardBits))
		for id, bits := range r.DomainShardBits {
			v.Domains[id] = commanifest.DomainAdoption{ShardBits: bits}
		}
	}
	return v
}

// Hooks are pluggable change handlers for the proxy applier. Each fires
// once per transition: the first evaluation is compared with
// [Applier.Running], each later one with the evaluation before it.
//
// A hook must not request a process restart to apply the new value: the
// restarted process re-reads the same flags, runs the old value, sees the
// same transition and, restarting again, loops. Apply the value in place or
// leave it to the divergence telemetry. With the evaluator built from
// [Running.Pin] the three value hooks never fire, because adoption cannot
// move a pinned value.
type Hooks struct {
	// OnShardBitsChange fires when the adopted ShardBits moves away from
	// the running value, then from the previous one. The evaluator reports
	// 0 until it adopts a value; 0 never reads as a transition.
	OnShardBitsChange func(prev, next uint8)

	// OnSourceModeChange fires on a SourceModeSSM transition. The
	// evaluator reports ASM (false) until it adopts a mode, so a process
	// running SSM must pin the mode ([Running.Pin]) or this can report a
	// transition that is only the absence of adoption.
	OnSourceModeChange func(prevSSM, nextSSM bool)

	// OnSuccessorChange fires when a Successor view appears, changes,
	// or disappears. After is nil when the Successor is no longer
	// adopted.
	OnSuccessorChange func(before, after *commanifest.SuccessorView)

	// OnDomainShardBitsChange fires when the adopted ShardBits of an object
	// plane the process runs ([Running.DomainShardBits]) moves away from
	// its running width, then from the previous one, keyed by DomainID
	// (domain 0x0 is OnShardBitsChange). Planes the process does not run
	// never fire, and a plane the evaluator has not adopted (absent, or
	// width 0) keeps its previous width.
	OnDomainShardBitsChange func(domain, prev, next uint8)
}

// Applier is the periodic evaluator+notifier. The proxy applier does
// NOT diff PilotGroups (the proxy is not a subscriber) and does NOT
// diff SourceSet (sources affect listeners, not the proxy emitter
// path); those concerns belong to the listener. It never restarts the
// process: divergence from the running configuration is logged when it
// starts and when it clears, and counted on every tick.
type Applier struct {
	Registry  *commanifest.Registry
	Evaluator *commanifest.Evaluator
	Hooks     Hooks
	Rec       *metrics.Recorder
	Log       *slog.Logger
	Interval  time.Duration // default 1s

	// Running is the configuration this process runs with: the baseline
	// the first evaluation is compared with. See [Running].
	Running Running
}

// Run blocks until ctx is cancelled, firing hooks on adoption changes.
func (a *Applier) Run(ctx context.Context) {
	interval := a.Interval
	if interval == 0 {
		interval = 1 * time.Second
	}
	log := a.Log
	if log == nil {
		log = slog.Default().With("component", "manifest-applier")
	}

	t := time.NewTicker(interval)
	defer t.Stop()

	prev := a.Running.adopted()
	var diverging []string
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		a.Registry.Evict()
		snap := a.Registry.Snapshot()
		next := a.Evaluator.Evaluate(snap)
		a.publishMetrics(next)
		next = a.settle(prev, next)

		if a.Hooks.OnShardBitsChange != nil && prev.ShardBits != next.ShardBits {
			log.Info("ShardBits change", "prev", prev.ShardBits, "next", next.ShardBits)
			a.Rec.ManifestAdoption("shard_bits", reasonForChange(prev.ShardBits != 0))
			a.Hooks.OnShardBitsChange(prev.ShardBits, next.ShardBits)
		}
		if a.Hooks.OnSourceModeChange != nil && prev.SourceModeSSM != next.SourceModeSSM {
			log.Info("SourceMode change", "prev_ssm", prev.SourceModeSSM, "next_ssm", next.SourceModeSSM)
			a.Rec.ManifestAdoption("source_mode", reasonForChange(prev.PilotsKnown > 0))
			a.Hooks.OnSourceModeChange(prev.SourceModeSSM, next.SourceModeSSM)
		}
		if a.Hooks.OnSuccessorChange != nil && !reflect.DeepEqual(prev.Successor, next.Successor) {
			log.Info("Successor change", "prev", prev.Successor, "next", next.Successor)
			a.Hooks.OnSuccessorChange(prev.Successor, next.Successor)
		}
		if a.Hooks.OnDomainShardBitsChange != nil {
			for _, d := range domainShardBitsChanges(prev.Domains, next.Domains) {
				log.Info("domain ShardBits change", "domain", d.domain, "prev", d.prev, "next", d.next)
				a.Rec.ManifestAdoption("domain_shard_bits", reasonForChange(d.prev != 0))
				a.Hooks.OnDomainShardBitsChange(d.domain, d.prev, d.next)
			}
		}
		// Divergence, per-domain included, surfaces via the shared
		// DivergenceFields list: counted on every tick, logged on change.
		for _, f := range next.DivergenceFields {
			a.Rec.ManifestDivergence(f, "peer-disagree")
		}
		diverging = a.logDivergence(log, diverging, next.DivergenceFields)
		prev = next
	}
}

// settle returns next as the applier compares it. A ShardBits of 0 means
// nothing adopted yet (the transaction plane runs 1-12) and keeps the
// previous value. Domains holds exactly the planes the process runs, each
// keeping its previous width until the evaluator adopts a non-zero one.
func (a *Applier) settle(prev, next commanifest.Adopted) commanifest.Adopted {
	if next.ShardBits == 0 {
		next.ShardBits = prev.ShardBits
	}
	var doms map[uint8]commanifest.DomainAdoption
	if len(a.Running.DomainShardBits) > 0 {
		doms = make(map[uint8]commanifest.DomainAdoption, len(a.Running.DomainShardBits))
		for id := range a.Running.DomainShardBits {
			d := next.Domains[id]
			if d.ShardBits == 0 {
				d.ShardBits = prev.Domains[id].ShardBits
			}
			doms[id] = d
		}
	}
	next.Domains = doms
	return next
}

// logDivergence logs each divergence field once when it appears and once
// when it clears (the counter records every tick) and returns the fields
// now diverging. The running configuration stays in effect either way.
func (a *Applier) logDivergence(log *slog.Logger, was, now []string) []string {
	for _, f := range now {
		if !slices.Contains(was, f) {
			log.Warn("manifest divergence: authoritative announcers disagree with each other or with the running configuration",
				"field", f,
				"running_shard_bits", a.Running.ShardBits,
				"running_source_mode_ssm", a.Running.SourceModeSSM,
				"running_domain_shard_bits", a.Running.DomainShardBits,
				"effect", "none: the running configuration stays in effect, and a restart would re-read the same flags")
		}
	}
	for _, f := range was {
		if !slices.Contains(now, f) {
			log.Info("manifest divergence cleared", "field", f)
		}
	}
	return slices.Clone(now)
}

func (a *Applier) publishMetrics(v commanifest.Adopted) {
	a.Rec.ManifestSetPilotsKnown(v.PilotsKnown)
	var bits int32
	if v.QuorumMet["shard_bits"] {
		bits |= 1 << 0
	}
	if v.QuorumMet["source_mode"] {
		bits |= 1 << 1
	}
	if v.QuorumMet["successor"] {
		bits |= 1 << 2
	}
	a.Rec.ManifestSetQuorumMetBits(bits)

	state := int32(0)
	window := int64(0)
	if v.Successor != nil {
		now := time.Now().Unix()
		remaining := int64(v.Successor.TransitionEpoch) - now
		window = remaining
		if remaining > 0 {
			state = 1
		} else {
			state = 2
		}
	}
	a.Rec.ManifestSetReshardState(state)
	a.Rec.ManifestSetReshardWindowSeconds(window)
}

func reasonForChange(hadPrev bool) string {
	if hadPrev {
		return "quorum-shift"
	}
	return "bootstrap"
}

// domainChange is one per-domain ShardBits transition.
type domainChange struct {
	domain     uint8
	prev, next uint8
}

// domainShardBitsChanges returns the per-domain ShardBits transitions between
// two settled views, in ascending DomainID order. Both hold the same planes
// (the ones the process runs), so a domain present in only one map, which
// would read as a change from or to its zero value, does not occur.
func domainShardBitsChanges(prev, next map[uint8]commanifest.DomainAdoption) []domainChange {
	var out []domainChange
	for id := uint8(0); id <= 0x0E; id++ {
		pv := prev[id].ShardBits
		nv := next[id].ShardBits
		if pv != nv {
			out = append(out, domainChange{domain: id, prev: pv, next: nv})
		}
	}
	return out
}
