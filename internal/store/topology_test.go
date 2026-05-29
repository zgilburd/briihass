package store

import (
	"context"
	"errors"
	"testing"

	"briihass/internal/ids"
)

func TestPromoteListDemoteBeacon(t *testing.T) {
	s := testPostgres(t)
	ctx := context.Background()
	id := ids.MustNewIBeaconKey("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1", 1, 2)
	b, err := NewBeacon(id, "alpha", "first beacon")
	if err != nil {
		t.Fatalf("NewBeacon: %v", err)
	}
	if err := s.PromoteBeacon(ctx, b); err != nil {
		t.Fatalf("PromoteBeacon: %v", err)
	}
	// Duplicate (uuid,major,minor) is rejected as ErrConflict.
	if err := s.PromoteBeacon(ctx, b); !errors.Is(err, ErrConflict) {
		t.Errorf("PromoteBeacon dup: want ErrConflict, got %v", err)
	}
	// Duplicate name with different major is rejected too.
	dupName, _ := NewBeacon(ids.MustNewIBeaconKey("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1", 9, 2), "alpha", "")
	if err := s.PromoteBeacon(ctx, dupName); !errors.Is(err, ErrConflict) {
		t.Errorf("PromoteBeacon dup name: want ErrConflict, got %v", err)
	}
	list, err := s.ListBeacons(ctx)
	if err != nil {
		t.Fatalf("ListBeacons: %v", err)
	}
	if len(list) != 1 || list[0].Name != "alpha" || list[0].Notes != "first beacon" {
		t.Errorf("ListBeacons: %+v", list)
	}
	if err := s.DemoteBeacon(ctx, id); err != nil {
		t.Fatalf("DemoteBeacon: %v", err)
	}
	if err := s.DemoteBeacon(ctx, id); !errors.Is(err, ErrNotFound) {
		t.Errorf("DemoteBeacon missing: want ErrNotFound, got %v", err)
	}
}

func TestNewBeacon_RejectsEmptyName(t *testing.T) {
	id := ids.MustNewIBeaconKey("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1", 1, 2)
	if _, err := NewBeacon(id, "", ""); err == nil {
		t.Fatal("expected error on empty name")
	}
	if _, err := NewBeacon(id, "   ", ""); err == nil {
		t.Fatal("expected error on whitespace-only name")
	}
}

func TestNewBeacon_RejectsZeroID(t *testing.T) {
	if _, err := NewBeacon(ids.BeaconKey{}, "x", ""); err == nil {
		t.Fatal("expected error on zero id")
	}
}

func TestZones_UpsertListDelete(t *testing.T) {
	s := testPostgres(t)
	ctx := context.Background()
	mac := ids.MustNewAPMAC("aa:bb:cc:00:00:01")
	if err := s.UpsertZone(ctx, NewZone(mac, ids.MustNewZoneLabel("zone_a"), "entry")); err != nil {
		t.Fatalf("UpsertZone: %v", err)
	}
	// Re-upsert: should overwrite label without dup.
	if err := s.UpsertZone(ctx, NewZone(mac, ids.MustNewZoneLabel("zone_b"), "")); err != nil {
		t.Fatalf("UpsertZone overwrite: %v", err)
	}
	zones, err := s.ListZones(ctx)
	if err != nil {
		t.Fatalf("ListZones: %v", err)
	}
	if len(zones) != 1 || zones[0].ZoneLabel.String() != "zone_b" {
		t.Errorf("ListZones after overwrite: %+v", zones)
	}
	if err := s.DeleteZone(ctx, mac); err != nil {
		t.Fatalf("DeleteZone: %v", err)
	}
	if err := s.DeleteZone(ctx, mac); !errors.Is(err, ErrNotFound) {
		t.Errorf("DeleteZone missing: want ErrNotFound, got %v", err)
	}
}
