package store

import (
	"context"
	"testing"
	"time"

	"briihass/internal/ids"
)

func TestObservations_InsertAndListDevicesSince(t *testing.T) {
	s := testPostgres(t)
	ctx := context.Background()
	now := time.Now()
	// One tracked + one observed-only beacon, two APs each.
	tracked, _ := NewBeacon(ids.MustNewIBeaconKey("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1", 1, 1), "tracked", "")
	if err := s.PromoteBeacon(ctx, tracked); err != nil {
		t.Fatalf("Promote: %v", err)
	}
	obs := []Observation{
		{ObservedAt: now.Add(-30 * time.Second), Kind: "ibeacon", Key: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1_1_1", APMac: "ap-a", RSSI: -50, Tracked: true},
		{ObservedAt: now.Add(-10 * time.Second), Kind: "ibeacon", Key: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1_1_1", APMac: "ap-b", RSSI: -60, Tracked: true},
		{ObservedAt: now.Add(-5 * time.Second), Kind: "ibeacon", Key: "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb2_2_2", APMac: "ap-a", RSSI: -70, Tracked: false},
	}
	if err := s.InsertObservations(ctx, obs); err != nil {
		t.Fatalf("InsertObservations: %v", err)
	}
	devs, err := s.ListDevicesSince(ctx, now.Add(-time.Minute))
	if err != nil {
		t.Fatalf("ListDevicesSince: %v", err)
	}
	if len(devs) != 2 {
		t.Fatalf("ListDevicesSince: want 2, got %d", len(devs))
	}
	var trackedDev, observed *DeviceSummary
	for i := range devs {
		if devs[i].Tracked {
			d := devs[i]
			trackedDev = &d
		} else {
			d := devs[i]
			observed = &d
		}
	}
	if trackedDev == nil || trackedDev.BeaconName != "tracked" || trackedDev.SightingCnt != 2 {
		t.Errorf("tracked: %+v", trackedDev)
	}
	if observed == nil || observed.SightingCnt != 1 || observed.LastAPMac != "ap-a" {
		t.Errorf("observed: %+v", observed)
	}
}

// TestObservations_InsertRejectsEmptyIdentity pins that
// InsertObservations rejects rows with an empty kind or key. Identity is
// packet-derived (kind,key); a row with no identity would never
// associate with an allowlist entry.
func TestObservations_InsertRejectsEmptyIdentity(t *testing.T) {
	s := testPostgres(t)
	ctx := context.Background()
	now := time.Now()
	obs := []Observation{
		{ObservedAt: now, Kind: "", Key: "", APMac: "ap-a", RSSI: -50, Tracked: false},
	}
	err := s.InsertObservations(ctx, obs)
	if err == nil {
		t.Fatal("expected error for empty identity")
	}
	if !contains(err.Error(), "kind, key") {
		t.Errorf("expected kind/key error message; got %v", err)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestObservations_PruneOlderThan(t *testing.T) {
	s := testPostgres(t)
	ctx := context.Background()
	now := time.Now()
	if err := s.InsertObservations(ctx, []Observation{
		{ObservedAt: now.Add(-72 * time.Hour), Kind: "ibeacon", Key: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1_1_1", APMac: "ap-a", RSSI: -50, Tracked: false},
		{ObservedAt: now.Add(-1 * time.Hour), Kind: "ibeacon", Key: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1_1_1", APMac: "ap-a", RSSI: -50, Tracked: false},
	}); err != nil {
		t.Fatalf("InsertObservations: %v", err)
	}
	obs, rp, err := s.PruneOlderThan(ctx, now.Add(-24*time.Hour))
	if err != nil {
		t.Fatalf("PruneOlderThan: %v", err)
	}
	if obs != 1 || rp != 0 {
		t.Errorf("PruneOlderThan: obs=%d raw_posts=%d", obs, rp)
	}
}

func TestObservations_ListAPsSince(t *testing.T) {
	s := testPostgres(t)
	ctx := context.Background()
	now := time.Now()
	if err := s.InsertObservations(ctx, []Observation{
		{ObservedAt: now.Add(-time.Minute), Kind: "ibeacon", Key: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1_1_1", APMac: "ap-a", APName: "entry", RSSI: -50, Tracked: true},
		{ObservedAt: now.Add(-30 * time.Second), Kind: "ibeacon", Key: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1_1_1", APMac: "ap-b", APName: "kitchen", RSSI: -60, Tracked: true},
	}); err != nil {
		t.Fatalf("InsertObservations: %v", err)
	}
	aps, err := s.ListAPsSince(ctx, now.Add(-time.Hour))
	if err != nil {
		t.Fatalf("ListAPsSince: %v", err)
	}
	if aps["ap-a"] != "entry" || aps["ap-b"] != "kitchen" {
		t.Errorf("ListAPsSince: %+v", aps)
	}
}
