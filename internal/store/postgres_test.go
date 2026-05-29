package store

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"briihass/internal/config"
)

// testPostgres connects to TEST_POSTGRES_DSN (or skips), wipes the
// tunables tables, and returns a fresh Store. Each test gets a clean
// slate; tables are recreated via applySchema after the drop.
//
// To run locally:
//
//	docker run -d --rm --name briihass-pg-test -p 5433:5432 \
//	    -e POSTGRES_PASSWORD=test postgres:16-alpine
//	TEST_POSTGRES_DSN='postgres://postgres:test@localhost:5433/postgres?sslmode=disable' \
//	    go test ./internal/store/...
//	docker rm -f briihass-pg-test
func testPostgres(t *testing.T) *Postgres {
	t.Helper()
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set TEST_POSTGRES_DSN to run postgres store integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	s, err := NewPostgres(ctx, dsn, nil, 1)
	if err != nil {
		t.Fatalf("NewPostgres: %v", err)
	}
	if _, err := s.pool.Exec(ctx, `
		DROP TABLE IF EXISTS observations, raw_posts, beacons, zones, settings,
		                     presence_state, tunables_overrides, tunables_defaults CASCADE`); err != nil {
		s.Close()
		t.Fatalf("drop tables: %v", err)
	}
	if err := s.applySchema(ctx); err != nil {
		s.Close()
		t.Fatalf("re-apply schema: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

func sampleTunables() *config.Tunables {
	stickyOverride := 180
	alphaOverride := 0.5
	return &config.Tunables{
		Defaults: config.DefaultsBlock{
			Alpha: 0.4, GracePeriodS: 5, DecayRateDbPerS: 2,
			PresenceFloorDbm: -95, TAwayMaxS: 30, StickyAfterArrivalS: 120,
			HysteresisDb: 4, ConfirmCount: 2,
		},
		Beacons: map[string]config.Overrides{
			"example_tag_1": {},
			"example_tag_2": {StickyAfterArrivalS: &stickyOverride},
			"example_tag_3": {Alpha: &alphaOverride},
		},
	}
}

func TestEmptyStore_LoadReturnsErrEmpty(t *testing.T) {
	s := testPostgres(t)
	_, err := s.LoadAll(context.Background())
	if !errors.Is(err, ErrEmpty) {
		t.Fatalf("LoadAll: want ErrEmpty, got %v", err)
	}
}

func TestSaveLoadRoundtrip(t *testing.T) {
	s := testPostgres(t)
	want := sampleTunables()
	ctx := context.Background()
	if err := s.SaveAll(ctx, want); err != nil {
		t.Fatalf("SaveAll: %v", err)
	}
	got, err := s.LoadAll(ctx)
	if err != nil {
		t.Fatalf("LoadAll: %v", err)
	}
	if got.Defaults != want.Defaults {
		t.Errorf("defaults mismatch:\n got=%+v\nwant=%+v", got.Defaults, want.Defaults)
	}
	if len(got.Beacons) != len(want.Beacons) {
		t.Fatalf("override count: got %d want %d", len(got.Beacons), len(want.Beacons))
	}
	// example_tag_2 has StickyAfterArrivalS=180; rest are nil pointers.
	o2 := got.Beacons["example_tag_2"]
	if o2.StickyAfterArrivalS == nil || *o2.StickyAfterArrivalS != 180 {
		t.Errorf("example_tag_2.StickyAfterArrivalS: got %v want *180", o2.StickyAfterArrivalS)
	}
	o3 := got.Beacons["example_tag_3"]
	if o3.Alpha == nil || *o3.Alpha != 0.5 {
		t.Errorf("example_tag_3.Alpha: got %v want *0.5", o3.Alpha)
	}
	// example_tag_1 is the "all defaults" case — every Overrides field nil.
	o1 := got.Beacons["example_tag_1"]
	if o1.Alpha != nil || o1.StickyAfterArrivalS != nil {
		t.Errorf("example_tag_1: expected all-nil overrides, got %+v", o1)
	}
}

func TestSaveAll_ReplacesOverrides(t *testing.T) {
	s := testPostgres(t)
	ctx := context.Background()
	if err := s.SaveAll(ctx, sampleTunables()); err != nil {
		t.Fatalf("seed save: %v", err)
	}
	// New tunables drops two beacons and keeps one.
	stickyOverride := 200
	next := &config.Tunables{
		Defaults: sampleTunables().Defaults,
		Beacons: map[string]config.Overrides{
			"example_tag_2": {StickyAfterArrivalS: &stickyOverride},
		},
	}
	if err := s.SaveAll(ctx, next); err != nil {
		t.Fatalf("SaveAll: %v", err)
	}
	got, err := s.LoadAll(ctx)
	if err != nil {
		t.Fatalf("LoadAll: %v", err)
	}
	if _, ok := got.Beacons["example_tag_1"]; ok {
		t.Errorf("example_tag_1 should have been removed")
	}
	if _, ok := got.Beacons["example_tag_3"]; ok {
		t.Errorf("example_tag_3 should have been removed")
	}
	o2 := got.Beacons["example_tag_2"]
	if o2.StickyAfterArrivalS == nil || *o2.StickyAfterArrivalS != 200 {
		t.Errorf("example_tag_2.StickyAfterArrivalS: got %v want *200", o2.StickyAfterArrivalS)
	}
}

func TestApplySchemaIsIdempotent(t *testing.T) {
	s := testPostgres(t)
	ctx := context.Background()
	// applySchema again — must not error.
	if err := s.applySchema(ctx); err != nil {
		t.Fatalf("re-apply: %v", err)
	}
	if err := s.applySchema(ctx); err != nil {
		t.Fatalf("re-apply twice: %v", err)
	}
}

func TestSeedFromYAMLIfEmpty(t *testing.T) {
	s := testPostgres(t)
	ctx := context.Background()
	yamlSeed := []byte(`
defaults:
  alpha: 0.4
  grace_period_s: 5
  decay_rate_db_per_s: 2.0
  presence_floor_dbm: -95
  t_away_max_s: 30
  sticky_after_arrival_s: 120
  hysteresis_db: 4.0
  confirm_count: 2
beacons:
  example_tag_1: {}
`)
	got, err := SeedFromYAMLIfEmpty(ctx, s, yamlSeed)
	if err != nil {
		t.Fatalf("SeedFromYAMLIfEmpty (empty): %v", err)
	}
	if got.Defaults.Alpha != 0.4 {
		t.Errorf("seeded alpha: got %v want 0.4", got.Defaults.Alpha)
	}
	// Second call: store is now populated; should return existing,
	// not re-seed.
	got2, err := SeedFromYAMLIfEmpty(ctx, s, []byte(`defaults:
  alpha: 0.9
  grace_period_s: 5
  decay_rate_db_per_s: 2.0
  presence_floor_dbm: -95
  t_away_max_s: 30
  sticky_after_arrival_s: 120
  hysteresis_db: 4.0
  confirm_count: 2
beacons: {}`))
	if err != nil {
		t.Fatalf("SeedFromYAMLIfEmpty (populated): %v", err)
	}
	if got2.Defaults.Alpha != 0.4 {
		t.Errorf("seeded twice: got alpha=%v want 0.4 (existing should win)", got2.Defaults.Alpha)
	}
}

func TestSaveAll_RejectsNil(t *testing.T) {
	s := testPostgres(t)
	if err := s.SaveAll(context.Background(), nil); err == nil {
		t.Fatalf("SaveAll(nil): expected error, got nil")
	}
}
