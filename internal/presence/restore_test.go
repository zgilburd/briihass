package presence

import (
	"testing"
	"time"

	"briihass/internal/ids"
)

// TestRestoreState_RepublishesZoneNotNotHome is the core warm-boot
// guarantee: after restoring a beacon that was in zone_a, the telemetry
// pump's RepublishAll re-asserts zone_a — it must NOT publish not_home
// for a beacon that was present before the restart.
func TestRestoreState_RepublishesZoneNotNotHome(t *testing.T) {
	e, _, out := newEngine(t, twoZoneTopology(), baseTunables())

	n := e.RestoreState([]BeaconSnapshot{
		{Beacon: beaconA, CurrentZone: "zone_a", CurrentAP: "aa:bb:cc:00:00:01"},
	})
	if n != 1 {
		t.Fatalf("RestoreState returned %d, want 1", n)
	}

	e.RepublishAll()
	evs := drain(out)
	if len(evs) != 1 {
		t.Fatalf("want 1 event, got %d: %+v", len(evs), evs)
	}
	if evs[0].State != "zone_a" {
		t.Errorf("State = %q, want zone_a (restored beacon must not flap to not_home)", evs[0].State)
	}
}

// TestRestoreState_StickyHoldsUntilLiveScans pins that a restored beacon
// with an EMPTY per-AP map (the real state of a fresh pod) does not get
// force-flipped to not_home by a Tick during the sticky-arrival window —
// the window is what bridges the gap until live scans rebuild presence.
func TestRestoreState_StickyHoldsUntilLiveScans(t *testing.T) {
	e, clk, out := newEngine(t, twoZoneTopology(), baseTunables())
	e.RestoreState([]BeaconSnapshot{
		{Beacon: beaconA, CurrentZone: "zone_a", CurrentAP: "aa:bb:cc:00:00:01"},
	})

	// A Tick well inside the 120s sticky window (and beyond t_away_max_s=30s)
	// must hold the zone, not publish a departure.
	clk.Advance(40 * time.Second)
	e.Tick()
	if evs := drain(out); len(evs) != 0 {
		t.Fatalf("sticky window must suppress not_home, got %+v", evs)
	}
	if snap := e.Snapshot(); snap.Beacons[0].CurrentZone != "zone_a" {
		t.Errorf("zone = %q, want zone_a held through sticky window", snap.Beacons[0].CurrentZone)
	}
}

// TestRestoreState_AgesOutWhenBeaconGone confirms the restore doesn't pin
// a beacon present forever: once the sticky window expires with no live
// scans, the beacon correctly departs to not_home.
func TestRestoreState_AgesOutWhenBeaconGone(t *testing.T) {
	e, clk, out := newEngine(t, twoZoneTopology(), baseTunables())
	e.RestoreState([]BeaconSnapshot{
		{Beacon: beaconA, CurrentZone: "zone_a", CurrentAP: "aa:bb:cc:00:00:01"},
	})

	clk.Advance(121 * time.Second) // past StickyAfterArrivalS=120
	e.Tick()
	evs := drain(out)
	if len(evs) != 1 || evs[0].State != NotHome {
		t.Fatalf("want one not_home departure after sticky expiry, got %+v", evs)
	}
}

// TestRestoreState_IgnoresNotHomeAndUntracked verifies stale/irrelevant
// snapshot rows are skipped: a not_home snapshot leaves the beacon cold,
// and a snapshot for a beacon not on the allowlist is dropped.
func TestRestoreState_IgnoresNotHomeAndUntracked(t *testing.T) {
	e, _, out := newEngine(t, twoZoneTopology(), baseTunables())
	untracked := ids.MustNewIBeaconKey("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb2", 2, 2)

	n := e.RestoreState([]BeaconSnapshot{
		{Beacon: beaconA, CurrentZone: ""},                                         // not_home: skip
		{Beacon: untracked, CurrentZone: "zone_a", CurrentAP: "aa:bb:cc:00:00:01"}, // not tracked: skip
	})
	if n != 0 {
		t.Fatalf("RestoreState returned %d, want 0", n)
	}

	e.RepublishAll()
	for _, ev := range drain(out) {
		if ev.Beacon == beaconA && ev.State != NotHome {
			t.Errorf("beaconA should still be not_home, got %q", ev.State)
		}
		if ev.Beacon == untracked {
			t.Errorf("untracked beacon must not have been restored: %+v", ev)
		}
	}
}
