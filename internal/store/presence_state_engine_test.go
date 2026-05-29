package store_test

import (
	"context"
	"os"
	"testing"
	"time"

	"briihass/internal/clock"
	"briihass/internal/config"
	"briihass/internal/ids"
	"briihass/internal/presence"
	"briihass/internal/store"
)

// TestPresenceState_EngineRoundTrip is the warm-boot integration proof:
// it replicates exactly what cmd/briihass does across a restart — drive
// an engine to a zone, persist its Snapshot via SavePresenceState, then
// (as a fresh pod would) LoadPresenceState into a brand-new engine via
// RestoreState — and asserts the new engine re-asserts the zone instead
// of flapping to not_home. Requires TEST_POSTGRES_DSN.
func TestPresenceState_EngineRoundTrip(t *testing.T) {
	s := storeForEngineTest(t)
	ctx := context.Background()

	beacon := ids.MustNewIBeaconKey("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1", 1, 1)
	tb, err := config.NewTrackedBeacon(beacon, "tag_one")
	if err != nil {
		t.Fatalf("NewTrackedBeacon: %v", err)
	}
	top, err := config.NewTopology(
		map[string]string{"aa:bb:cc:00:00:01": "zone_a"},
		[]config.TrackedBeacon{tb},
	)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	tun := &config.Tunables{
		Defaults: config.DefaultsBlock{
			Alpha: 0.4, GracePeriodS: 5, DecayRateDbPerS: 2.0,
			PresenceFloorDbm: -95, TAwayMaxS: 30, StickyAfterArrivalS: 120,
			HysteresisDb: 4.0, ConfirmCount: 2,
		},
		Beacons: map[string]config.Overrides{},
	}

	// --- "old pod": drive to zone_a, then persist the snapshot ---
	clk := clock.NewFake(time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC))
	out1 := make(chan presence.PresenceEvent, 16)
	e1 := presence.NewEngine(top, tun, clk, out1)
	e1.Submit(presence.Sighting{
		Beacon: beacon, BeaconName: "tag_one",
		APMac: "aa:bb:cc:00:00:01", APName: "AP-A", RSSI: -70, At: clk.Now(),
	})

	snap := e1.Snapshot()
	rows := make([]store.PresenceStateRow, 0, len(snap.Beacons))
	for _, bs := range snap.Beacons {
		rows = append(rows, store.PresenceStateRow{
			Kind:        string(bs.Beacon.Kind()),
			Key:         bs.Beacon.Key(),
			CurrentZone: bs.CurrentZone,
			CurrentAP:   bs.CurrentAP,
			LastArrival: bs.LastArrival,
		})
	}
	if err := s.SavePresenceState(ctx, rows); err != nil {
		t.Fatalf("SavePresenceState: %v", err)
	}

	// --- "new pod": load from DB and restore into a fresh engine ---
	loaded, err := s.LoadPresenceState(ctx)
	if err != nil {
		t.Fatalf("LoadPresenceState: %v", err)
	}
	snaps := make([]presence.BeaconSnapshot, 0, len(loaded))
	for _, r := range loaded {
		bk, ok := ids.FromStoreKey(r.Kind, r.Key)
		if !ok {
			t.Fatalf("FromStoreKey(%q,%q) failed", r.Kind, r.Key)
		}
		snaps = append(snaps, presence.BeaconSnapshot{
			Beacon: bk, CurrentZone: r.CurrentZone, CurrentAP: r.CurrentAP, LastArrival: r.LastArrival,
		})
	}

	out2 := make(chan presence.PresenceEvent, 16)
	e2 := presence.NewEngine(top, tun, clk, out2)
	if n := e2.RestoreState(snaps); n != 1 {
		t.Fatalf("RestoreState restored %d, want 1", n)
	}
	e2.RepublishAll()

	select {
	case ev := <-out2:
		if ev.State != "zone_a" {
			t.Fatalf("warm boot State = %q, want zone_a (must not flap to not_home)", ev.State)
		}
	default:
		t.Fatal("fresh engine emitted no event after restore")
	}
}

// storeForEngineTest mirrors testPostgres (which is in the internal
// package_test) for this external-test-package file.
func storeForEngineTest(t *testing.T) *store.Postgres {
	t.Helper()
	dsn := envOrSkip(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := store.NewPostgres(ctx, dsn, nil, 1)
	if err != nil {
		t.Fatalf("NewPostgres: %v", err)
	}
	if err := s.SavePresenceState(ctx, nil); err != nil {
		s.Close()
		t.Fatalf("clear presence_state: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

func envOrSkip(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set TEST_POSTGRES_DSN to run postgres store integration tests")
	}
	return dsn
}
