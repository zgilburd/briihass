// Package presence is the per-beacon state machine: per-(beacon, AP)
// EWMA RSSI with linear decay, closest-AP-wins zone resolution, and a
// sticky-arrival window. See ADR-0006 for the design.
//
// Hard invariants (do not weaken):
//  1. not_home -> <any zone> publishes on the first qualifying sighting.
//  2. <any zone> -> not_home is suppressed for sticky_after_arrival_s
//     after each arrival.
package presence

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"briihass/internal/clock"
	"briihass/internal/config"
	"briihass/internal/ids"
)

// Engine owns the per-beacon state and emits PresenceEvents to out.
// All public methods are safe for concurrent callers; internal state
// is mutex-protected.
type Engine struct {
	mu  sync.Mutex
	top *config.Topology
	tun *config.Tunables
	clk clock.Clock
	out chan<- PresenceEvent

	beacons map[BeaconKey]*beaconState

	// resolved tunables cached per beacon name; refreshed on
	// ApplyTunables. The presence-engine path is hot, so we avoid
	// re-resolving on every sighting.
	resolved map[string]config.Resolved

	// Metric hooks (optional — nil-safe).
	OnUnknownBeacon func(b BeaconKey, ap string)
	OnUnknownAP     func(b BeaconKey, beaconName, ap string) // fired when an AP is in presence but missing from the zones map
	OnEventEmitted  func(PresenceEvent)
	OnEventDropped  func(PresenceEvent) // fired when the out channel was full

	// dropLogLast is the unix-nanos of the last drop-Warn log. Used to
	// rate-limit the log to one entry per minute so a wedged downstream
	// doesn't flood the logs but the operator still gets an in-process
	// signal even when no OnEventDropped hook is wired.
	dropLogLast atomic.Int64

	// Logger is used for the internal drop-Warn. Defaults to slog.Default
	// if unset; not exported to keep the API surface stable for now.
	Logger *slog.Logger
}

// dropLogMinInterval is the minimum gap between internal drop-Warn
// log entries. Set high enough that a stalled downstream cannot flood
// the log even if the engine emits at full rate, but low enough that
// the operator sees a fresh entry within the time-to-investigate.
const dropLogMinInterval = time.Minute

// NewEngine constructs an Engine. Beacons from topology are
// pre-registered so Submit can reject unknown beacons cheaply.
func NewEngine(top *config.Topology, tun *config.Tunables, clk clock.Clock, out chan<- PresenceEvent) *Engine {
	tracked := top.Beacons()
	e := &Engine{
		top:      top,
		tun:      tun,
		clk:      clk,
		out:      out,
		beacons:  make(map[BeaconKey]*beaconState, len(tracked)),
		resolved: make(map[string]config.Resolved, len(tracked)),
	}
	for _, b := range tracked {
		id := b.ID()
		e.beacons[id] = newBeaconState(id, b.Name())
		e.resolved[b.Name()] = tun.ResolveFor(b.Name())
	}
	return e
}

// Submit processes one sighting. Synchronous; returns when state is
// updated. Any resulting event is sent to the out channel
// non-blockingly: if the channel is full, the event is dropped (and
// counted via OnEventDropped + a rate-limited internal Warn) rather
// than stalling the ingest path.
// Safe to call from many ingest goroutines.
func (e *Engine) Submit(s Sighting) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.handleSighting(s, e.clk.Now())
}

// Tick runs the fade-out / sticky-expiry checks. Run() calls this on
// a periodic schedule; callers can also drive it manually (admin UI
// "force recompute" button, tests).
func (e *Engine) Tick() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.recomputeAll(e.clk.Now(), "")
}

// ApplyTunables swaps in a new tunables document and refreshes the
// per-beacon Resolved cache. Called by the admin UI after a successful
// POST /admin/tunables (Postgres SaveAll). Nil-safe (no-op on nil).
func (e *Engine) ApplyTunables(tun *config.Tunables) {
	if tun == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.tun = tun
	for _, bs := range e.beacons {
		e.resolved[bs.name] = tun.ResolveFor(bs.name)
	}
}

// ApplyTopology swaps in a new allowlist + zone map. Called by the
// admin UI after Promote/Demote/Upsert/DeleteZone. Newly added beacons
// get a fresh empty state; removed beacons have their state evicted
// (so a later re-promote starts clean). Existing beacon state is
// preserved in place — the EWMA values, sticky-arrival timestamp, and
// currently-published zone all remain so a topology refresh mid-arrival
// doesn't flap not_home (ADR-0006 sticky-arrival regression scenario).
//
// Calling with nil is a no-op (defensive guard for paths that build
// topology from store errors).
func (e *Engine) ApplyTopology(top *config.Topology) {
	if top == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.top = top
	tracked := top.Beacons()
	keep := make(map[BeaconKey]struct{}, len(tracked))
	keepNames := make(map[string]struct{}, len(tracked))
	for _, b := range tracked {
		id := b.ID()
		keep[id] = struct{}{}
		keepNames[b.Name()] = struct{}{}
		if _, ok := e.beacons[id]; !ok {
			e.beacons[id] = newBeaconState(id, b.Name())
		} else {
			e.beacons[id].name = b.Name()
		}
		e.resolved[b.Name()] = e.tun.ResolveFor(b.Name())
	}
	for id := range e.beacons {
		if _, ok := keep[id]; !ok {
			delete(e.beacons, id)
		}
	}
	// Prune stale per-name entries from resolved so the map doesn't grow
	// unbounded across rename + demote cycles.
	for name := range e.resolved {
		if _, ok := keepNames[name]; !ok {
			delete(e.resolved, name)
		}
	}
	// Synthetic emit for any tracked beacon whose currently-published
	// zone label no longer matches its closest-AP-by-effective-RSSI
	// lookup against the new topology. This fires the moment the
	// operator applies a zone-label rename so HA doesn't have to wait
	// for the next sighting or Tick to see the new label.
	//
	// Precondition: only runs against beacons already in a published
	// zone (the `bs.currentZone == ""` continue below guards this).
	// We never resolve to "" here, so the sticky-arrival invariant
	// (not_home is suppressed during the window) is preserved by
	// construction.
	now := e.clk.Now()
	for _, bs := range e.beacons {
		if bs.currentZone == "" {
			continue
		}
		tun := e.resolved[bs.name]
		closestAP, closestKey, closestEff, runnerUpDiff, ok := bs.closestInPresence(tun, now)
		if !ok {
			continue
		}
		newZone := e.top.Zone(closestKey)
		if newZone == "" || newZone == bs.currentZone {
			continue
		}
		bs.currentZone = newZone
		bs.currentAP = closestKey
		bs.candidateAP = ""
		bs.candidateCount = 0
		stickyActive := now.Sub(bs.lastArrivalTs) < time.Duration(tun.StickyAfterArrivalS)*time.Second
		e.emit(buildEvent(bs, closestAP, closestKey, closestEff, runnerUpDiff, now, stickyActive))
	}
}

// RepublishAll emits a current-state PresenceEvent for every tracked
// beacon WITHOUT changing any state. It is read-only w.r.t. the state
// machine (no recompute), so the sticky-arrival / decay invariants
// (ADR-0006) are untouched.
//
// Two callers: the periodic telemetry pump (continuous RSSI/battery/temp
// cadence between zone transitions) and the HA-repave path (birth message
// or admin "Resync HA") — after the publisher clears its seen set, the
// re-emitted events re-assert discovery config + state for every device.
func (e *Engine) RepublishAll() {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.clk.Now()
	for _, bs := range e.beacons {
		tun := e.resolved[bs.name]
		sticky := bs.currentZone != "" &&
			now.Sub(bs.lastArrivalTs) < time.Duration(tun.StickyAfterArrivalS)*time.Second
		if bs.currentZone != "" {
			if ap, ok := bs.perAP[bs.currentAP]; ok {
				eff := effectiveRSSI(ap, now, tun)
				e.emit(buildEvent(bs, ap, bs.currentAP, eff, 0, now, sticky))
				continue
			}
			// Published zone but the backing AP state was evicted: re-assert
			// the zone label without AP/RSSI detail rather than flip to
			// not_home (which would be a false departure).
			e.emit(PresenceEvent{
				Beacon: bs.id, BeaconName: bs.name,
				State: ids.PresenceState(bs.currentZone), StickyActive: sticky,
				EmittedAt: now, BatteryMV: bs.lastBatteryMV, TemperatureC: bs.lastTemperatureC,
			})
			continue
		}
		e.emit(PresenceEvent{
			Beacon: bs.id, BeaconName: bs.name, State: NotHome,
			EmittedAt: now, BatteryMV: bs.lastBatteryMV, TemperatureC: bs.lastTemperatureC,
		})
	}
}

// RestoreState rehydrates published zones from a persisted snapshot so a
// fresh pod boots warm. For each snapshot whose beacon is still tracked
// and whose zone is non-empty, it re-asserts the published zone + backing
// AP. Snapshots for not_home beacons or beacons no longer on the
// allowlist are ignored (a cold beacon is already not_home; a stale row
// would re-create state for something we no longer track). Returns the
// number of beacons restored.
//
// The restart is treated as a FRESH arrival (lastArrivalTs = now), not the
// persisted arrival time. A new pod's per-AP map starts empty, so the
// first Tick sees no AP in presence; if we trusted a persisted arrival
// time that had already aged past the sticky-arrival window, that Tick
// would publish a false not_home before the first live scan lands — the
// mid-deploy away-flap this whole feature exists to prevent. Granting a
// full sticky window (ADR-0006) holds the zone until live scans (~1–2/s)
// rebuild per-AP presence; a beacon that genuinely never reappears still
// ages out to not_home once the window expires.
//
// Must be called after NewEngine and before the ingest listener starts,
// while no other goroutine touches the engine.
func (e *Engine) RestoreState(snaps []BeaconSnapshot) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.clk.Now()
	n := 0
	for _, sn := range snaps {
		bs, ok := e.beacons[sn.Beacon]
		if !ok || sn.CurrentZone == "" {
			continue
		}
		bs.currentZone = sn.CurrentZone
		bs.currentAP = sn.CurrentAP
		bs.lastArrivalTs = now
		n++
	}
	return n
}

// IsTracked reports whether b is on the current allowlist. Used by the
// ingest path to decide whether to forward the sighting to Submit (and
// whether the observation row should be flagged tracked=true).
func (e *Engine) IsTracked(b BeaconKey) (string, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	bs, ok := e.beacons[b]
	if !ok {
		return "", false
	}
	return bs.name, true
}

// Snapshot returns a point-in-time copy of every tracked beacon's
// state for read-only consumers (the admin UI). Safe to call from
// any goroutine; briefly acquires the engine mutex.
func (e *Engine) Snapshot() EngineSnapshot {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.clk.Now()
	out := EngineSnapshot{AsOf: now, Beacons: make([]BeaconSnapshot, 0, len(e.beacons))}
	for id, bs := range e.beacons {
		tun := e.resolved[bs.name]
		bsnap := BeaconSnapshot{
			Beacon:       id,
			Name:         bs.name,
			CurrentZone:  bs.currentZone,
			CurrentAP:    bs.currentAP,
			LastArrival:  bs.lastArrivalTs,
			StickyActive: bs.currentZone != "" && now.Sub(bs.lastArrivalTs) < time.Duration(tun.StickyAfterArrivalS)*time.Second,
			BatteryMV:    bs.lastBatteryMV,
			TemperatureC: bs.lastTemperatureC,
		}
		for mac, ap := range bs.perAP {
			eff := effectiveRSSI(ap, now, tun)
			bsnap.APs = append(bsnap.APs, APSnapshot{
				Mac:            mac,
				Name:           ap.apName,
				LastRSSI:       ap.lastRSSI,
				EWMARSSI:       ap.ewmaRSSI,
				EffectiveRSSI:  eff,
				LastSightingTs: ap.lastSightingTs,
				InPresence:     eff >= float64(tun.PresenceFloorDbm),
			})
		}
		out.Beacons = append(out.Beacons, bsnap)
	}
	return out
}

// EngineSnapshot is the read-only view of engine state exposed to the
// admin UI. All times are wall clock from the engine's Clock.
type EngineSnapshot struct {
	AsOf    time.Time
	Beacons []BeaconSnapshot
}

// BeaconSnapshot is the per-beacon slice of the engine snapshot.
type BeaconSnapshot struct {
	Beacon       BeaconKey
	Name         string
	CurrentZone  string // "" means not_home
	CurrentAP    string
	LastArrival  time.Time
	StickyActive bool
	APs          []APSnapshot

	// Latest telemetry (nil when never reported).
	BatteryMV    *int
	TemperatureC *float64
}

// APSnapshot is the per-(beacon, AP) slice — what an operator needs
// to see why an AP is/isn't in presence.
type APSnapshot struct {
	Mac            string
	Name           string
	LastRSSI       int
	EWMARSSI       float64
	EffectiveRSSI  float64
	LastSightingTs time.Time
	InPresence     bool
}

// Run drives Tick on a 1-second cadence until ctx is cancelled. Call
// from a goroutine; cancel ctx for graceful shutdown.
func (e *Engine) Run(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			e.Tick()
		}
	}
}

// --- internal -----------------------------------------------------

// handleSighting must be called with the mutex held.
func (e *Engine) handleSighting(s Sighting, now time.Time) {
	bs, ok := e.beacons[s.Beacon]
	if !ok {
		// Unknown beacon — drop with optional metric hook.
		if e.OnUnknownBeacon != nil {
			e.OnUnknownBeacon(s.Beacon, s.APMac)
		}
		return
	}

	apKey := strings.ToLower(s.APMac)
	tun := e.resolved[bs.name]
	ap, fresh := bs.perAP[apKey]
	if !fresh {
		ap = &apState{
			apName:   s.APName,
			ewmaRSSI: float64(s.RSSI),
		}
		bs.perAP[apKey] = ap
	} else {
		// EWMA: new = alpha*raw + (1-alpha)*old
		ap.ewmaRSSI = tun.Alpha*float64(s.RSSI) + (1-tun.Alpha)*ap.ewmaRSSI
		ap.apName = s.APName // keep latest in case the operator renamed the AP
	}
	ap.lastRSSI = s.RSSI
	ap.lastSightingTs = now
	ap.lastSeenAt = s.At

	// Latest telemetry wins; absent fields preserve the last known value so
	// a beacon that interleaves iBeacon and TLM frames keeps its battery
	// reading between TLM frames.
	if s.BatteryMV != nil {
		bs.lastBatteryMV = s.BatteryMV
	}
	if s.TemperatureC != nil {
		bs.lastTemperatureC = s.TemperatureC
	}

	e.recomputeOne(bs, now, apKey)
}

// recomputeAll is the tick path: walk all beacons and re-evaluate
// for fade-outs. Hysteresis confirm counts are NOT advanced by ticks
// (per ADR-0006: K is consecutive sightings, not consecutive ticks).
func (e *Engine) recomputeAll(now time.Time, fromAP string) {
	for _, bs := range e.beacons {
		e.recomputeOne(bs, now, fromAP)
	}
}

// recomputeOne evaluates one beacon's state at time `now`. If
// fromAP != "", this came from a sighting and the hysteresis confirm
// counter can advance; otherwise (fromAP == "") it's a tick and
// confirm counters do not change.
func (e *Engine) recomputeOne(bs *beaconState, now time.Time, fromAP string) {
	tun := e.resolved[bs.name]

	closestAP, closestKey, closestEff, runnerUpDiff, inPresence := bs.closestInPresence(tun, now)

	// Sticky window check: only meaningful when we currently publish a zone.
	stickyActive := bs.currentZone != "" &&
		now.Sub(bs.lastArrivalTs) < time.Duration(tun.StickyAfterArrivalS)*time.Second

	// t_away_max_s safety bound: outside the sticky window, if no AP
	// has reported in this long, force not_home regardless of any
	// odd decay math.
	freshest := bs.freshestSightingTs()
	forceAway := !freshest.IsZero() &&
		now.Sub(freshest) > time.Duration(tun.TAwayMaxS)*time.Second &&
		!stickyActive

	// ---------- Case A: no AP in presence ----------
	if !inPresence || forceAway {
		if bs.currentZone == "" {
			// Already not_home, nothing to publish.
			return
		}
		if stickyActive && !forceAway {
			// Hold last published zone; sticky window is suppressing
			// the departure.
			return
		}
		// Departure edge: publish not_home.
		prevAP := bs.currentAP
		ev := PresenceEvent{
			Beacon:       bs.id,
			BeaconName:   bs.name,
			State:        NotHome,
			LastSeen:     freshest, // best info we have
			StickyActive: false,
			EmittedAt:    now,
		}
		// Carry forward the AP info from the previous published zone
		// for context, plus its last RSSI snapshot if still around.
		if ap, ok := bs.perAP[prevAP]; ok {
			ev.APMac = prevAP
			ev.APName = ap.apName
			ev.RSSIRaw = ap.lastRSSI
			ev.RSSIEWMA = ap.ewmaRSSI
			ev.RSSIEffective = effectiveRSSI(ap, now, tun)
			ev.LastSeen = ap.lastSeenAt
		}
		bs.currentZone = ""
		bs.currentAP = ""
		bs.candidateAP = ""
		bs.candidateCount = 0
		e.emit(ev)
		return
	}

	// At least one AP has presence.
	closestZone := e.top.Zone(closestKey)
	if closestZone == "" {
		// Closest AP is in presence but missing from the zones map —
		// the bridge can't publish a zone it can't name. Surface via
		// OnUnknownAP so an operator sees the misconfiguration; without
		// the hook this becomes a silent fade-out (arrival SLO breaker).
		if e.OnUnknownAP != nil {
			e.OnUnknownAP(bs.id, bs.name, closestKey)
		}
		return
	}

	// ---------- Case B: arrival from not_home — IMMEDIATE ----------
	if bs.currentZone == "" {
		bs.currentZone = closestZone
		bs.currentAP = closestKey
		bs.lastArrivalTs = now
		bs.candidateAP = ""
		bs.candidateCount = 0
		e.emit(buildEvent(bs, closestAP, closestKey, closestEff, runnerUpDiff, now, true))
		return
	}

	// ---------- Case C: already in a zone — check for changes ----------

	// If the published-zone AP is no longer in presence, switch
	// immediately to the closest. No hysteresis: we cannot stay on
	// an AP that's faded out.
	currentAP, currentOK := bs.perAP[bs.currentAP]
	currentEff := -1e9
	if currentOK {
		currentEff = effectiveRSSI(currentAP, now, tun)
	}
	if !currentOK || currentEff < float64(tun.PresenceFloorDbm) {
		bs.currentZone = closestZone
		bs.currentAP = closestKey
		bs.candidateAP = ""
		bs.candidateCount = 0
		e.emit(buildEvent(bs, closestAP, closestKey, closestEff, runnerUpDiff, now, stickyActive))
		return
	}

	// Same AP still closest. Almost always nothing to publish — but
	// check for a zone-label drift first: an operator may have just
	// renamed the zone for this AP via /admin/zones, and Tick is the
	// load-bearing place to surface the new label when the closest
	// AP per RSSI hasn't reported in long enough that ApplyTopology's
	// synthetic emit would have skipped it.
	if closestKey == bs.currentAP {
		bs.candidateAP = ""
		bs.candidateCount = 0
		if closestZone != bs.currentZone {
			bs.currentZone = closestZone
			e.emit(buildEvent(bs, closestAP, closestKey, closestEff, runnerUpDiff, now, stickyActive))
		}
		return
	}

	// Different AP is closest. Compare zones.
	if closestZone == bs.currentZone {
		// Same zone, different AP — no public state change. Reset
		// candidate tracking but keep current AP (no need to flip).
		bs.candidateAP = ""
		bs.candidateCount = 0
		return
	}

	// Different zone. Apply hysteresis. Confirm counter only advances
	// on sightings, never on ticks.
	if fromAP == "" {
		return
	}

	gap := closestEff - currentEff
	if gap < tun.HysteresisDb {
		bs.candidateAP = ""
		bs.candidateCount = 0
		return
	}

	if bs.candidateAP == closestKey {
		bs.candidateCount++
	} else {
		bs.candidateAP = closestKey
		bs.candidateCount = 1
	}
	if bs.candidateCount < tun.ConfirmCount {
		return
	}

	// Confirmed — switch zones.
	bs.currentZone = closestZone
	bs.currentAP = closestKey
	bs.candidateAP = ""
	bs.candidateCount = 0
	e.emit(buildEvent(bs, closestAP, closestKey, closestEff, runnerUpDiff, now, stickyActive))
}

func buildEvent(bs *beaconState, ap *apState, apKey string, eff, runnerUpDiff float64, now time.Time, sticky bool) PresenceEvent {
	return PresenceEvent{
		Beacon:                    bs.id,
		BeaconName:                bs.name,
		State:                     ids.PresenceState(bs.currentZone),
		APMac:                     apKey,
		APName:                    ap.apName,
		RSSIRaw:                   ap.lastRSSI,
		RSSIEWMA:                  ap.ewmaRSSI,
		RSSIEffective:             eff,
		RSSIRunnerUpEffectiveDiff: runnerUpDiff,
		LastSeen:                  ap.lastSeenAt,
		StickyActive:              sticky,
		EmittedAt:                 now,
		BatteryMV:                 bs.lastBatteryMV,
		TemperatureC:              bs.lastTemperatureC,
	}
}

// emit invokes OnEventEmitted / OnEventDropped while the caller holds
// e.mu (every caller — handleSighting, ApplyTopology, Tick — locks
// before calling). Hook implementations MUST NOT re-enter the engine
// (Submit, ApplyTopology, Snapshot, IsTracked) or they will deadlock
// against their own caller. Today the only wired hooks publish to
// Prometheus and are safe; this comment is the contract for future
// additions.
func (e *Engine) emit(ev PresenceEvent) {
	if e.OnEventEmitted != nil {
		e.OnEventEmitted(ev)
	}
	if e.out == nil {
		return
	}
	// Non-blocking send: if downstream is wedged we'd rather drop than
	// stall the ingest path. The drop is recorded via OnEventDropped so
	// /metrics surfaces "events vanishing on the arrival
	// edge" — the alternative was a silent loss with no log or counter.
	//
	// Also emits a rate-limited Warn from inside the engine so a caller
	// that wires the engine without the OnEventDropped hook still gets
	// an in-process signal that events are being dropped.
	select {
	case e.out <- ev:
	default:
		if e.OnEventDropped != nil {
			e.OnEventDropped(ev)
		}
		e.maybeLogDrop(ev)
	}
}

// maybeLogDrop emits a Warn at most once per dropLogMinInterval. Cheap
// atomic-based throttle; under a saturated downstream this still
// produces one log entry per minute, never a flood. Uses the engine's
// injected Clock so tests with a fake clock can advance time
// deterministically across drop events.
func (e *Engine) maybeLogDrop(ev PresenceEvent) {
	now := e.clk.Now().UnixNano()
	prev := e.dropLogLast.Load()
	if prev != 0 && time.Duration(now-prev) < dropLogMinInterval {
		return
	}
	if !e.dropLogLast.CompareAndSwap(prev, now) {
		return // lost race; another goroutine just logged
	}
	logger := e.Logger
	if logger == nil {
		logger = slog.Default()
	}
	logger.Warn("presence engine dropped event (out channel saturated)",
		"beacon", ev.BeaconName,
		"state", ev.State,
		"ap", ev.APMac,
		"throttle", dropLogMinInterval.String())
}

// effectiveRSSI applies the linear-decay model to a single AP's
// current EWMA + last-sighting age.
func effectiveRSSI(ap *apState, now time.Time, tun config.Resolved) float64 {
	if ap.lastSightingTs.IsZero() {
		return -1e9
	}
	age := now.Sub(ap.lastSightingTs).Seconds()
	grace := float64(tun.GracePeriodS)
	if age <= grace {
		return ap.ewmaRSSI
	}
	return ap.ewmaRSSI - tun.DecayRateDbPerS*(age-grace)
}
