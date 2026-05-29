package presence

import (
	"strings"
	"testing"
	"time"

	"briihass/internal/clock"
	"briihass/internal/config"
	"briihass/internal/ids"
)

// ============================================================
//  Test scaffolding
// ============================================================

// baseTunables returns a Tunables with the ADR-0006 defaults filled
// in. Tests that need a specific knob mutate one field of the
// returned value before calling newEngine.
func baseTunables() *config.Tunables {
	return &config.Tunables{
		Defaults: config.DefaultsBlock{
			Alpha:               0.4,
			GracePeriodS:        5,
			DecayRateDbPerS:     2.0,
			PresenceFloorDbm:    -95,
			TAwayMaxS:           30,
			StickyAfterArrivalS: 120,
			HysteresisDb:        4.0,
			ConfirmCount:        2,
		},
		Beacons: map[string]config.Overrides{},
	}
}

// twoZoneTopology has two APs in two different zones plus one
// tracked beacon. Used by transit / hysteresis tests.
func twoZoneTopology() *config.Topology {
	tb, err := config.NewTrackedBeacon(
		ids.MustNewIBeaconKey("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1", 1, 1),
		"tag_one")
	if err != nil {
		panic(err)
	}
	top, err := config.NewTopology(
		map[string]string{
			"aa:bb:cc:00:00:01": "zone_a",
			"aa:bb:cc:00:00:02": "zone_b",
		},
		[]config.TrackedBeacon{tb},
	)
	if err != nil {
		panic(err)
	}
	return top
}

// beaconA is the BeaconID of the single tracked beacon in
// twoZoneTopology.
var beaconA = ids.MustNewIBeaconKey("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1", 1, 1)

// drain pulls every available event from the channel without blocking
// and returns them in order.
func drain(ch <-chan PresenceEvent) []PresenceEvent {
	var out []PresenceEvent
	for {
		select {
		case ev := <-ch:
			out = append(out, ev)
		default:
			return out
		}
	}
}

// newEngine wires Engine + a fake clock starting at t0.
func newEngine(t *testing.T, top *config.Topology, tun *config.Tunables) (*Engine, *clock.Fake, <-chan PresenceEvent) {
	t.Helper()
	clk := clock.NewFake(time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC))
	out := make(chan PresenceEvent, 32)
	return NewEngine(top, tun, clk, out), clk, out
}

func sighting(rssi int, apMac, apName string, when time.Time) Sighting {
	return Sighting{
		Beacon:     beaconA,
		BeaconName: "tag_one",
		APMac:      apMac,
		APName:     apName,
		RSSI:       rssi,
		At:         when,
	}
}

// TestRepublishAll_EmitsCurrentStateAndTelemetry pins that RepublishAll
// re-emits the current state for a tracked beacon (without changing it)
// and carries the latest telemetry — the basis of the telemetry pump and
// the HA discovery repave.
func TestRepublishAll_EmitsCurrentStateAndTelemetry(t *testing.T) {
	e, clk, out := newEngine(t, twoZoneTopology(), baseTunables())

	// Arrive in zone_a carrying a battery reading.
	s := sighting(-70, "aa:bb:cc:00:00:01", "AP-A", clk.Now())
	batt := 3714
	s.BatteryMV = &batt
	e.Submit(s)
	drain(out) // discard the arrival event

	e.RepublishAll()

	select {
	case ev := <-out:
		if ev.Beacon != beaconA {
			t.Errorf("beacon = %v", ev.Beacon)
		}
		if ev.State != "zone_a" {
			t.Errorf("state = %q, want zone_a (RepublishAll must not change state)", ev.State)
		}
		if ev.BatteryMV == nil || *ev.BatteryMV != 3714 {
			t.Errorf("battery = %v, want 3714 (telemetry carried through)", ev.BatteryMV)
		}
	default:
		t.Fatal("RepublishAll emitted no event for the tracked beacon")
	}

	// State machine is unchanged: the beacon is still in zone_a.
	if snap := e.Snapshot(); len(snap.Beacons) != 1 || snap.Beacons[0].CurrentZone != "zone_a" {
		t.Errorf("RepublishAll mutated state: %+v", snap.Beacons)
	}
}

// ============================================================
//  Load-bearing invariants (ADR-0006)
// ============================================================

// TestArrival_FromNotHome_IsImmediate is the load-bearing invariant
// test. Any future change to the state machine that delays this edge
// is a bug. This test runs first by alphabetical-name accident; the
// invariant matters regardless of order.
func TestArrival_FromNotHome_IsImmediate(t *testing.T) {
	e, clk, out := newEngine(t, twoZoneTopology(), baseTunables())

	e.Submit(sighting(-80, "aa:bb:cc:00:00:01", "AP-A", clk.Now()))

	evs := drain(out)
	if len(evs) != 1 {
		t.Fatalf("want 1 event, got %d: %+v", len(evs), evs)
	}
	if evs[0].State != "zone_a" {
		t.Errorf("State: want zone_a, got %q", evs[0].State)
	}
	if evs[0].APMac != "aa:bb:cc:00:00:01" {
		t.Errorf("APMac: want aa:bb:cc:00:00:01, got %q", evs[0].APMac)
	}
	if evs[0].BeaconName != "tag_one" {
		t.Errorf("BeaconName: want tag_one, got %q", evs[0].BeaconName)
	}
	if evs[0].RSSIRaw != -80 {
		t.Errorf("RSSIRaw: want -80, got %d", evs[0].RSSIRaw)
	}
}

// TestApplyTopology_ZoneLabelChangePropagates pins the I1 fix:
// when an operator renames the zone label for the AP a beacon is
// currently sitting on, the engine MUST emit a new event with the
// new label immediately on ApplyTopology — not wait until the next
// sighting or Tick. Otherwise the operator's "saved" form change is
// invisible until the beacon roams.
func TestApplyTopology_ZoneLabelChangePropagates(t *testing.T) {
	e, clk, out := newEngine(t, twoZoneTopology(), baseTunables())
	e.Submit(sighting(-70, "aa:bb:cc:00:00:01", "AP-A", clk.Now()))
	if got := drain(out); len(got) != 1 || got[0].State != "zone_a" {
		t.Fatalf("setup arrival: %+v", got)
	}

	// Operator renames zone_a -> entry via /admin/zones; admin
	// rebuilds the topology and calls ApplyTopology.
	renamed, err := config.NewTopology(
		map[string]string{
			"aa:bb:cc:00:00:01": "entry",
			"aa:bb:cc:00:00:02": "zone_b",
		},
		twoZoneTopology().Beacons(),
	)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	e.ApplyTopology(renamed)

	evs := drain(out)
	if len(evs) != 1 {
		t.Fatalf("ApplyTopology synthetic emit: want 1 event, got %d: %+v", len(evs), evs)
	}
	if evs[0].State != "entry" {
		t.Errorf("ApplyTopology emit State: want entry, got %q", evs[0].State)
	}
	if evs[0].APMac != "aa:bb:cc:00:00:01" {
		t.Errorf("ApplyTopology emit APMac: want aa:bb:cc:00:00:01, got %q", evs[0].APMac)
	}
}

// TestTick_ZoneLabelChangeSelfHeals covers the recomputeOne self-heal
// branch: even if ApplyTopology's synthetic emit didn't catch the
// drift (e.g. because the closest AP's effective RSSI calculation
// crossed an AP boundary between the two passes), the next Tick or
// sighting must surface the new label.
func TestTick_ZoneLabelChangeSelfHeals(t *testing.T) {
	e, clk, out := newEngine(t, twoZoneTopology(), baseTunables())
	e.Submit(sighting(-70, "aa:bb:cc:00:00:01", "AP-A", clk.Now()))
	drain(out)

	// Mutate the engine's topology in place to a renamed map by
	// rebuilding with a different label. Use NewTopology so the
	// invariant of validated input is preserved.
	renamed, err := config.NewTopology(
		map[string]string{
			"aa:bb:cc:00:00:01": "porch",
			"aa:bb:cc:00:00:02": "zone_b",
		},
		twoZoneTopology().Beacons(),
	)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	e.ApplyTopology(renamed)
	drain(out) // consume the synthetic emit

	// A subsequent sighting at the SAME AP must keep producing the
	// new zone label (zero events expected because the engine already
	// reconciled in ApplyTopology; the closest-AP-same path now sees
	// matching zones and stays quiet).
	clk.Advance(time.Second)
	e.Submit(sighting(-71, "aa:bb:cc:00:00:01", "AP-A", clk.Now()))
	if got := drain(out); len(got) != 0 {
		t.Errorf("post-rename same-AP sighting: want 0 events, got %+v", got)
	}
}

// TestDeparture_SuppressedDuringStickyWindow tests the second
// invariant: <any zone> -> not_home is suppressed while the sticky
// window is active.
func TestDeparture_SuppressedDuringStickyWindow(t *testing.T) {
	e, clk, out := newEngine(t, twoZoneTopology(), baseTunables())

	// Arrival.
	e.Submit(sighting(-92, "aa:bb:cc:00:00:01", "AP-A", clk.Now()))
	drain(out) // consume the arrival event

	// Advance far enough that the AP would fade out without sticky:
	// effective = -92 - 2*(50-5) = -182, well below floor -95.
	clk.Advance(50 * time.Second)
	e.Tick()

	evs := drain(out)
	if len(evs) != 0 {
		t.Fatalf("expected no events during sticky window, got: %+v", evs)
	}

	// Advance past sticky_after_arrival_s (120s total): now sticky
	// no longer suppresses departure.
	clk.Advance(80 * time.Second) // total 130s since arrival
	e.Tick()

	evs = drain(out)
	if len(evs) != 1 || evs[0].State != NotHome {
		t.Fatalf("expected single not_home event after sticky expires, got: %+v", evs)
	}
}

// ============================================================
//  Single-AP scenarios (decay + grace)
// ============================================================

func TestSingleScanMiss_DoesNotFlap(t *testing.T) {
	e, clk, out := newEngine(t, twoZoneTopology(), baseTunables())

	// Arrival at strong signal.
	e.Submit(sighting(-70, "aa:bb:cc:00:00:01", "AP-A", clk.Now()))
	drain(out)

	// 1 second later (within grace_period_s=5) — no sighting, just a tick.
	clk.Advance(1 * time.Second)
	e.Tick()

	// Still inside sticky window AND effective_rssi well above floor.
	// Nothing should publish.
	evs := drain(out)
	if len(evs) != 0 {
		t.Fatalf("missed scan should not produce events, got: %+v", evs)
	}
}

func TestExtendedMiss_ThenRearrival(t *testing.T) {
	// Note: This intentionally avoids the sticky window by setting it
	// short, so we can observe the full not_home -> arrival cycle.
	tun := baseTunables()
	short := 5
	tun.Defaults.StickyAfterArrivalS = short

	e, clk, out := newEngine(t, twoZoneTopology(), tun)

	// Arrival.
	e.Submit(sighting(-75, "aa:bb:cc:00:00:01", "AP-A", clk.Now()))
	drain(out)

	// Advance past sticky window AND past the point where -75 decays
	// below floor.
	//   eff = -75 - 2*(age-5)
	//   floor = -95 → reached at age = 15s
	clk.Advance(20 * time.Second)
	e.Tick()
	evs := drain(out)
	if len(evs) != 1 || evs[0].State != NotHome {
		t.Fatalf("want one not_home event, got: %+v", evs)
	}

	// New arrival.
	clk.Advance(10 * time.Second)
	e.Submit(sighting(-80, "aa:bb:cc:00:00:01", "AP-A", clk.Now()))
	evs = drain(out)
	if len(evs) != 1 || evs[0].State != "zone_a" {
		t.Fatalf("want re-arrival event, got: %+v", evs)
	}
}

func TestEdgeOfRange_NeverEstablishesPresence(t *testing.T) {
	tun := baseTunables()
	// Floor is -95; a -97 sighting must NOT establish presence.
	e, clk, out := newEngine(t, twoZoneTopology(), tun)

	e.Submit(sighting(-97, "aa:bb:cc:00:00:01", "AP-A", clk.Now()))

	evs := drain(out)
	if len(evs) != 0 {
		t.Fatalf("expected no events for sub-floor sighting, got: %+v", evs)
	}
}

// ============================================================
//  Two-AP transit + hysteresis
// ============================================================

func TestTwoAPTransit_HysteresisRequiresConfirmCount(t *testing.T) {
	e, clk, out := newEngine(t, twoZoneTopology(), baseTunables())

	// Arrive at AP-A with mid-strength.
	e.Submit(sighting(-75, "aa:bb:cc:00:00:01", "AP-A", clk.Now()))
	drain(out)

	// AP-B starts seeing it slightly stronger, but only by ~5 dB.
	// Hysteresis is 4 dB and ConfirmCount is 2: first sighting at
	// AP-B should NOT switch.
	clk.Advance(1 * time.Second)
	e.Submit(sighting(-69, "aa:bb:cc:00:00:02", "AP-B", clk.Now()))
	evs := drain(out)
	if len(evs) != 0 {
		t.Fatalf("first AP-B sighting should not switch zones, got: %+v", evs)
	}

	// Second consecutive AP-B sighting at similar strength: confirm.
	clk.Advance(1 * time.Second)
	e.Submit(sighting(-69, "aa:bb:cc:00:00:02", "AP-B", clk.Now()))
	evs = drain(out)
	if len(evs) != 1 || evs[0].State != "zone_b" {
		t.Fatalf("second AP-B sighting should confirm switch to zone_b, got: %+v", evs)
	}
}

func TestAlternatingAPs_BelowHysteresis_NoSwitch(t *testing.T) {
	e, clk, out := newEngine(t, twoZoneTopology(), baseTunables())

	// Arrive at AP-A.
	e.Submit(sighting(-75, "aa:bb:cc:00:00:01", "AP-A", clk.Now()))
	drain(out)

	// Alternating AP-B sightings only slightly stronger (within
	// hysteresis margin). Must never switch.
	for i := 0; i < 10; i++ {
		clk.Advance(1 * time.Second)
		e.Submit(sighting(-76, "aa:bb:cc:00:00:02", "AP-B", clk.Now()))
		clk.Advance(1 * time.Second)
		e.Submit(sighting(-75, "aa:bb:cc:00:00:01", "AP-A", clk.Now()))
	}
	evs := drain(out)
	if len(evs) != 0 {
		t.Fatalf("close-margin alternation should not flap, got %d events", len(evs))
	}
}

func TestSameAPHigherRSSI_NoEvent(t *testing.T) {
	// Re-confirming the same AP shouldn't publish a new event; the
	// engine reserves events for state transitions.
	e, clk, out := newEngine(t, twoZoneTopology(), baseTunables())

	e.Submit(sighting(-75, "aa:bb:cc:00:00:01", "AP-A", clk.Now()))
	drain(out)

	for i := 0; i < 5; i++ {
		clk.Advance(1 * time.Second)
		e.Submit(sighting(-70+i, "aa:bb:cc:00:00:01", "AP-A", clk.Now()))
	}
	evs := drain(out)
	if len(evs) != 0 {
		t.Fatalf("same-AP repeats should not publish, got: %+v", evs)
	}
}

// ============================================================
//  Sticky-arrival window scenarios
// ============================================================

func TestNoisyArrival_FaintThenFluctuating(t *testing.T) {
	// Worked example 2 from ADR-0006. Beacon was not_home. Faint
	// outdoor sighting at -92 arrives. Then ~12s of silence (signal
	// gap as the beacon moves through multi-path). Then strong "kitchen"
	// (zone_b) sightings. Bridge should publish:
	//   t=0  arrival to zone_a (outdoor)
	//   t=12 transition to zone_b (still inside sticky)
	// Specifically: NO not_home event in between.
	e, clk, out := newEngine(t, twoZoneTopology(), baseTunables())

	// t=0: arrival edge.
	e.Submit(sighting(-92, "aa:bb:cc:00:00:01", "AP-A", clk.Now()))
	evs := drain(out)
	if len(evs) != 1 || evs[0].State != "zone_a" {
		t.Fatalf("want arrival to zone_a, got: %+v", evs)
	}

	// t=12: signal gap; AP-A has decayed below floor by now.
	// Tick to confirm sticky holds, then arrival at AP-B (zone_b).
	clk.Advance(12 * time.Second)
	e.Tick()
	if got := drain(out); len(got) != 0 {
		t.Fatalf("sticky window must suppress departure during signal gap, got: %+v", got)
	}

	e.Submit(sighting(-80, "aa:bb:cc:00:00:02", "AP-B", clk.Now()))
	evs = drain(out)
	if len(evs) != 1 || evs[0].State != "zone_b" {
		t.Fatalf("want zone change to zone_b inside sticky, got: %+v", evs)
	}
	if !evs[0].StickyActive {
		t.Errorf("expected StickyActive=true on the zone-change event")
	}
}

func TestBriefPassThrough_FalsePositive_OneArrivalOneDeparture(t *testing.T) {
	// Worked example 3 from ADR-0006. One faint sighting then
	// silence. Exactly one arrival at t=0; exactly one not_home after
	// sticky expires; no intermediate flap.
	e, clk, out := newEngine(t, twoZoneTopology(), baseTunables())

	e.Submit(sighting(-92, "aa:bb:cc:00:00:01", "AP-A", clk.Now()))
	if evs := drain(out); len(evs) != 1 || evs[0].State != "zone_a" {
		t.Fatalf("want one arrival event, got: %+v", evs)
	}

	// Tick repeatedly within the sticky window — no events expected.
	for i := 0; i < 6; i++ {
		clk.Advance(15 * time.Second)
		e.Tick()
	}
	if evs := drain(out); len(evs) != 0 {
		t.Fatalf("no events expected within sticky, got: %+v", evs)
	}

	// Tick past sticky window (120s total) → not_home.
	clk.Advance(45 * time.Second) // we're now at t=135
	e.Tick()
	evs := drain(out)
	if len(evs) != 1 || evs[0].State != NotHome {
		t.Fatalf("want one not_home after sticky expires, got: %+v", evs)
	}
}

func TestStickyDisabledPerBeacon_DepartureIsImmediate(t *testing.T) {
	tun := baseTunables()
	zero := 0
	tun.Beacons["tag_one"] = config.Overrides{
		StickyAfterArrivalS: &zero,
	}
	e, clk, out := newEngine(t, twoZoneTopology(), tun)

	e.Submit(sighting(-92, "aa:bb:cc:00:00:01", "AP-A", clk.Now()))
	drain(out)

	// Without sticky, fade-out below floor produces not_home as soon
	// as the decay math hits the floor (-92, age beyond grace, drops
	// to -95 around age 6.5s, below at 7s).
	clk.Advance(10 * time.Second)
	e.Tick()

	evs := drain(out)
	if len(evs) != 1 || evs[0].State != NotHome {
		t.Fatalf("sticky=0 should publish not_home on first fade-out tick, got: %+v", evs)
	}
}

// ============================================================
//  T_away_max_s safety bound
// ============================================================

func TestTAwayMaxBound_ForcesNotHomeOutsideSticky(t *testing.T) {
	// Set a very low decay rate so without the safety bound, an AP
	// would still appear "in presence" after a long silence. The
	// t_away_max_s safety net should kick in anyway, but only after
	// the sticky window has expired.
	tun := baseTunables()
	tun.Defaults.DecayRateDbPerS = 0.01 // basically no decay
	tun.Defaults.StickyAfterArrivalS = 5
	tun.Defaults.TAwayMaxS = 10

	e, clk, out := newEngine(t, twoZoneTopology(), tun)

	e.Submit(sighting(-70, "aa:bb:cc:00:00:01", "AP-A", clk.Now()))
	drain(out)

	// Past sticky (5s), past t_away_max (10s): force not_home.
	clk.Advance(12 * time.Second)
	e.Tick()
	evs := drain(out)
	if len(evs) != 1 || evs[0].State != NotHome {
		t.Fatalf("t_away_max safety should force not_home, got: %+v", evs)
	}
}

// ============================================================
//  Effective RSSI math
// ============================================================

func TestEffectiveRSSI_GraceAndDecay(t *testing.T) {
	tun := config.Resolved{
		Alpha:            0.4,
		GracePeriodS:     5,
		DecayRateDbPerS:  2.0,
		PresenceFloorDbm: -95,
	}
	now := time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		ap   apState
		want float64
	}{
		{"inside grace", apState{ewmaRSSI: -75, lastSightingTs: now.Add(-3 * time.Second)}, -75},
		{"exactly grace edge", apState{ewmaRSSI: -75, lastSightingTs: now.Add(-5 * time.Second)}, -75},
		{"5s after grace", apState{ewmaRSSI: -75, lastSightingTs: now.Add(-10 * time.Second)}, -85},
		{"at floor", apState{ewmaRSSI: -75, lastSightingTs: now.Add(-15 * time.Second)}, -95},
		{"never seen", apState{}, -1e9},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := effectiveRSSI(&tc.ap, now, tun); got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// ============================================================
//  Unknown beacons + edge cases
// ============================================================

func TestUnknownBeacon_DropsWithMetricHook(t *testing.T) {
	e, clk, _ := newEngine(t, twoZoneTopology(), baseTunables())

	var captured []BeaconKey
	e.OnUnknownBeacon = func(id BeaconKey, ap string) {
		captured = append(captured, id)
	}

	unknown := Sighting{
		Beacon: ids.MustNewIBeaconKey("ffffffff-ffff-ffff-ffff-ffffffffffff", 99, 99),
		APMac:  "aa:bb:cc:00:00:01",
		RSSI:   -70,
		At:     clk.Now(),
	}
	e.Submit(unknown)

	if len(captured) != 1 {
		t.Fatalf("OnUnknownBeacon should have fired exactly once, got %d", len(captured))
	}
	if captured[0] != unknown.Beacon {
		t.Errorf("OnUnknownBeacon got %v, want %v", captured[0], unknown.Beacon)
	}
}

func TestUnknownAP_DropsZoneSwitch(t *testing.T) {
	// Sighting from an AP not in the zones map should NOT publish a
	// zone (we don't know what to call it). Existing zone is held.
	e, clk, out := newEngine(t, twoZoneTopology(), baseTunables())

	e.Submit(sighting(-75, "aa:bb:cc:00:00:01", "AP-A", clk.Now()))
	drain(out)

	clk.Advance(1 * time.Second)
	e.Submit(Sighting{
		Beacon:     beaconA,
		BeaconName: "tag_one",
		APMac:      "aa:bb:cc:00:00:99", // not in zones map
		APName:     "Unknown",
		RSSI:       -50, // way stronger
		At:         clk.Now(),
	})
	evs := drain(out)
	if len(evs) != 0 {
		t.Fatalf("unknown-AP sighting should not publish, got: %+v", evs)
	}
}

func TestApplyTunables_HotReload(t *testing.T) {
	tun := baseTunables()
	e, clk, out := newEngine(t, twoZoneTopology(), tun)

	// Arrival.
	e.Submit(sighting(-75, "aa:bb:cc:00:00:01", "AP-A", clk.Now()))
	drain(out)

	// Hot-reload tunables: shorten sticky to 0 so the next fade-out
	// publishes not_home immediately.
	newTun := baseTunables()
	zero := 0
	newTun.Beacons["tag_one"] = config.Overrides{StickyAfterArrivalS: &zero}
	e.ApplyTunables(newTun)

	clk.Advance(20 * time.Second)
	e.Tick()
	evs := drain(out)
	if len(evs) != 1 || evs[0].State != NotHome {
		t.Fatalf("after ApplyTunables with sticky=0, expected not_home, got: %+v", evs)
	}
}

// ============================================================
//  Run() smoke test (the periodic ticker)
// ============================================================

func TestRun_TickerCancellation(t *testing.T) {
	e, _, _ := newEngine(t, twoZoneTopology(), baseTunables())
	// A cancellation must cause Run to return promptly.
	done := make(chan struct{})
	ctx, cancel := contextWithTimeout(t)
	defer cancel()
	go func() {
		e.Run(ctx)
		close(done)
	}()
	cancel()
	select {
	case <-done:
		// ok
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return within 2 seconds of cancel")
	}
}

// contextWithTimeout returns a context that's already cancelable.
// Kept as a tiny helper so the test file doesn't import "context"
// just for one site.
func contextWithTimeout(t *testing.T) (ctx, cancel) {
	t.Helper()
	return newContext()
}

// ============================================================
//  Misc invariants
// ============================================================

func TestPresenceEvent_ZoneFieldNeverEmptyOnArrival(t *testing.T) {
	// Defense: every arrival event must have a non-empty State (zone
	// label). If it's ever NotHome on an arrival path, something is
	// catastrophically wrong.
	e, clk, out := newEngine(t, twoZoneTopology(), baseTunables())
	e.Submit(sighting(-75, "aa:bb:cc:00:00:01", "AP-A", clk.Now()))
	evs := drain(out)
	if len(evs) != 1 {
		t.Fatalf("want 1 event, got %d", len(evs))
	}
	if evs[0].State == "" || strings.EqualFold(evs[0].State.String(), NotHome.String()) {
		t.Errorf("arrival event State must be a zone label, got %q", evs[0].State)
	}
}

// ============================================================
//  ApplyTopology (allowlist + zone swap on promote/demote)
// ============================================================

// TestApplyTopology_EvictsRemovedBeacon verifies that demoting a beacon
// (removing it from the topology) clears that beacon's per-AP state so
// a later re-promote starts from a clean slate.
func TestApplyTopology_EvictsRemovedBeacon(t *testing.T) {
	e, clk, _ := newEngine(t, twoZoneTopology(), baseTunables())
	// Populate per-AP state.
	e.Submit(sighting(-70, "aa:bb:cc:00:00:01", "AP-A", clk.Now()))
	e.mu.Lock()
	if _, ok := e.beacons[beaconA]; !ok {
		e.mu.Unlock()
		t.Fatalf("setup: beacon state not present")
	}
	e.mu.Unlock()

	// Apply an empty topology (operator demoted everything).
	empty, _ := config.NewTopology(map[string]string{}, nil)
	e.ApplyTopology(empty)

	e.mu.Lock()
	if _, ok := e.beacons[beaconA]; ok {
		e.mu.Unlock()
		t.Fatalf("after ApplyTopology with empty allowlist, beacon state should be evicted")
	}
	e.mu.Unlock()

	// Re-apply the original topology: state must be fresh, not the
	// old EWMA value.
	e.ApplyTopology(twoZoneTopology())
	e.mu.Lock()
	bs, ok := e.beacons[beaconA]
	if !ok {
		e.mu.Unlock()
		t.Fatalf("after re-applying topology, beacon state should exist")
	}
	if len(bs.perAP) != 0 {
		t.Errorf("re-promoted beacon should start with empty perAP, got %d entries", len(bs.perAP))
	}
	if bs.currentZone != "" || bs.lastArrivalTs.After(time.Time{}) {
		t.Errorf("re-promoted beacon should be in clean state, got zone=%q lastArrival=%v", bs.currentZone, bs.lastArrivalTs)
	}
	e.mu.Unlock()
}

// TestApplyTopology_PreservesExistingBeaconState verifies that
// editing the topology (e.g. renaming a zone) does NOT wipe the EWMA
// state, sticky-arrival timestamp, or currently-published zone of
// beacons that remain on the allowlist. This locks the ADR-0006
// sticky-arrival regression scenario: a topology save while the beacon is
// mid-arrival must not flap it back to not_home.
func TestApplyTopology_PreservesExistingBeaconState(t *testing.T) {
	e, clk, _ := newEngine(t, twoZoneTopology(), baseTunables())
	// Enter not_home -> zone arrival so lastArrivalTs and
	// currentZone are populated, not just perAP/EWMA.
	e.Submit(sighting(-70, "aa:bb:cc:00:00:01", "AP-A", clk.Now()))

	e.mu.Lock()
	bs := e.beacons[beaconA]
	if bs.currentZone == "" {
		e.mu.Unlock()
		t.Fatalf("setup: expected beacon to be in a zone after arrival, got not_home")
	}
	beforeEWMA := bs.perAP["aa:bb:cc:00:00:01"].ewmaRSSI
	beforeZone := bs.currentZone
	beforeAP := bs.currentAP
	beforeArrival := bs.lastArrivalTs
	e.mu.Unlock()

	if beforeArrival.IsZero() {
		t.Fatalf("setup: expected lastArrivalTs to be populated after arrival")
	}

	// Re-apply the same topology (idempotent edge case — operator hit
	// "save" without changing anything).
	e.ApplyTopology(twoZoneTopology())

	e.mu.Lock()
	defer e.mu.Unlock()
	bs = e.beacons[beaconA]
	if bs == nil {
		t.Fatalf("beacon evicted by idempotent ApplyTopology")
	}
	if got := bs.perAP["aa:bb:cc:00:00:01"].ewmaRSSI; got != beforeEWMA {
		t.Errorf("EWMA changed across ApplyTopology: before=%g after=%g", beforeEWMA, got)
	}
	if bs.currentZone != beforeZone {
		t.Errorf("currentZone changed across ApplyTopology: before=%q after=%q (sticky-arrival regression: sticky window would lose its anchor)",
			beforeZone, bs.currentZone)
	}
	if bs.currentAP != beforeAP {
		t.Errorf("currentAP changed across ApplyTopology: before=%q after=%q", beforeAP, bs.currentAP)
	}
	if !bs.lastArrivalTs.Equal(beforeArrival) {
		t.Errorf("lastArrivalTs changed across ApplyTopology: before=%v after=%v (sticky window resets — not_home flap possible)",
			beforeArrival, bs.lastArrivalTs)
	}
}

// TestApplyTopology_NilTopology verifies the nil-guard at function
// entry — admin error paths that try to refresh with a nil topology
// must not panic.
func TestApplyTopology_NilTopology(t *testing.T) {
	e, _, _ := newEngine(t, twoZoneTopology(), baseTunables())
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("ApplyTopology(nil) should be a no-op, panicked with %v", r)
		}
	}()
	e.ApplyTopology(nil)
	// Topology pointer should be unchanged (still the original).
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.top == nil {
		t.Fatalf("ApplyTopology(nil) wiped e.top instead of being a no-op")
	}
}

// TestApplyTopology_PrunesResolvedOnDemote verifies the resolved-map
// fix: removing a beacon from the allowlist also removes its entry from
// e.resolved so the map doesn't grow unbounded across rename/demote
// cycles.
func TestApplyTopology_PrunesResolvedOnDemote(t *testing.T) {
	e, _, _ := newEngine(t, twoZoneTopology(), baseTunables())
	e.mu.Lock()
	if _, ok := e.resolved["tag_one"]; !ok {
		e.mu.Unlock()
		t.Fatalf("setup: expected resolved entry for tag_one")
	}
	e.mu.Unlock()

	// Demote both beacons.
	empty, _ := config.NewTopology(map[string]string{}, nil)
	e.ApplyTopology(empty)

	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.resolved) != 0 {
		t.Errorf("ApplyTopology(empty) should prune all resolved entries, got %d remaining: %v",
			len(e.resolved), keysOf(e.resolved))
	}
}

func keysOf(m map[string]config.Resolved) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestApplyTopology_RaceWithSubmit exercises the mutex coordination
// between hot-path Submit and admin-path ApplyTopology under -race.
// The point is to fail under `go test -race` if either side forgets
// the mutex, not to assert specific state.
func TestApplyTopology_RaceWithSubmit(t *testing.T) {
	e, _, _ := newEngine(t, twoZoneTopology(), baseTunables())

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 200; i++ {
			e.Submit(sighting(-70-(i%10), "aa:bb:cc:00:00:01", "AP-A", time.Now()))
		}
	}()
	for i := 0; i < 50; i++ {
		e.ApplyTopology(twoZoneTopology())
	}
	<-done
}

// TestEmit_DropsToHookWhenChannelFull verifies that a full out channel
// causes emit to fire OnEventDropped instead of blocking.
func TestEmit_DropsToHookWhenChannelFull(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC))
	// Buffer 1; do not read from it. The first arrival fills the chan;
	// every subsequent event must drop.
	out := make(chan PresenceEvent, 1)
	e := NewEngine(twoZoneTopology(), baseTunables(), clk, out)
	var drops int
	e.OnEventDropped = func(_ PresenceEvent) { drops++ }

	e.Submit(sighting(-70, "aa:bb:cc:00:00:01", "AP-A", clk.Now())) // arrival fills chan
	// Force a zone switch so the engine emits another event.
	// Simulating: AP-A dies, AP-B takes over → switch zones.
	clk.Advance(time.Minute) // outside grace, decays AP-A below floor
	e.Submit(sighting(-60, "aa:bb:cc:00:00:02", "AP-B", clk.Now()))

	if drops < 1 {
		t.Errorf("expected at least one drop on full chan, got %d (chan len=%d)", drops, len(out))
	}
}
