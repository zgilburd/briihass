package config

import (
	"strings"
	"testing"

	"briihass/internal/ids"
)

func TestParseTunables_Valid(t *testing.T) {
	const y = `
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
  tag_one: {}
  tag_two:
    sticky_after_arrival_s: 180
    alpha: 0.5
`
	tun, err := ParseTunables([]byte(y))
	if err != nil {
		t.Fatalf("ParseTunables: %v", err)
	}
	// Beacon with empty override -> all defaults.
	r1 := tun.ResolveFor("tag_one")
	if r1.Alpha != 0.4 {
		t.Errorf("tag_one alpha: want 0.4 (default), got %v", r1.Alpha)
	}
	if r1.StickyAfterArrivalS != 120 {
		t.Errorf("tag_one sticky: want 120 (default), got %d", r1.StickyAfterArrivalS)
	}
	// Beacon with overrides.
	r2 := tun.ResolveFor("tag_two")
	if r2.Alpha != 0.5 {
		t.Errorf("tag_two alpha: want 0.5 (override), got %v", r2.Alpha)
	}
	if r2.StickyAfterArrivalS != 180 {
		t.Errorf("tag_two sticky: want 180 (override), got %d", r2.StickyAfterArrivalS)
	}
	if r2.HysteresisDb != 4.0 {
		t.Errorf("tag_two hysteresis: want 4.0 (default — not overridden), got %v", r2.HysteresisDb)
	}
	// Unknown beacon -> all defaults (no panic).
	rn := tun.ResolveFor("never_seen")
	if rn.Alpha != 0.4 {
		t.Errorf("unknown beacon alpha: want 0.4 (default), got %v", rn.Alpha)
	}
}

func TestParseTunables_DefaultsRequired(t *testing.T) {
	// A defaults block with all-zero values trips the lower-bound
	// checks (alpha=0 is out of (0,1], t_away_max_s=0 is too low).
	const y = `
defaults:
  alpha: 0
  grace_period_s: 0
  decay_rate_db_per_s: 0
  presence_floor_dbm: 0
  t_away_max_s: 0
  sticky_after_arrival_s: 0
  hysteresis_db: 0
  confirm_count: 0
beacons: {}
`
	_, err := ParseTunables([]byte(y))
	if err == nil {
		t.Fatal("want validation error, got nil")
	}
	if !strings.Contains(err.Error(), "alpha") {
		t.Errorf("want alpha-range error first, got %v", err)
	}
}

func TestParseTunables_DefaultErrors(t *testing.T) {
	good := `
defaults:
  alpha: 0.4
  grace_period_s: 5
  decay_rate_db_per_s: 2.0
  presence_floor_dbm: -95
  t_away_max_s: 30
  sticky_after_arrival_s: 120
  hysteresis_db: 4.0
  confirm_count: 2
beacons: {}
`
	// Each case mutates one field of the good defaults to break it.
	cases := []struct {
		field string
		bad   string
		want  string
	}{
		{"alpha", "alpha: 1.5", "alpha 1.5"},
		{"grace_period_s", "grace_period_s: -1", "grace_period_s"},
		{"decay_rate_db_per_s", "decay_rate_db_per_s: -1", "decay_rate_db_per_s"},
		{"presence_floor_dbm low", "presence_floor_dbm: -200", "presence_floor_dbm"},
		{"presence_floor_dbm high", "presence_floor_dbm: 5", "presence_floor_dbm"},
		{"t_away_max_s", "t_away_max_s: 0", "t_away_max_s"},
		{"sticky_after_arrival_s", "sticky_after_arrival_s: -1", "sticky_after_arrival_s"},
		{"hysteresis_db", "hysteresis_db: -1", "hysteresis_db"},
		{"confirm_count", "confirm_count: 0", "confirm_count"},
	}
	for _, tc := range cases {
		t.Run(tc.field, func(t *testing.T) {
			lines := strings.Split(good, "\n")
			for i, l := range lines {
				prefix := "  " + strings.SplitN(tc.bad, ":", 2)[0] + ":"
				if strings.HasPrefix(l, prefix) {
					lines[i] = "  " + tc.bad
				}
			}
			_, err := ParseTunables([]byte(strings.Join(lines, "\n")))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestParseTunables_OverrideErrors(t *testing.T) {
	const yamlPrefix = `
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
  tag_one:
`
	cases := []struct {
		field string
		bad   string
		want  string
	}{
		{"alpha", "    alpha: 2.0", "alpha 2"},
		{"grace negative", "    grace_period_s: -3", "grace_period_s"},
		{"decay negative", "    decay_rate_db_per_s: -2", "decay_rate_db_per_s"},
		{"presence_floor low", "    presence_floor_dbm: -200", "presence_floor_dbm"},
		{"presence_floor high", "    presence_floor_dbm: 5", "presence_floor_dbm"},
		{"t_away zero", "    t_away_max_s: 0", "t_away_max_s"},
		{"sticky negative", "    sticky_after_arrival_s: -5", "sticky_after_arrival_s"},
		{"hysteresis negative", "    hysteresis_db: -1", "hysteresis_db"},
		{"confirm zero", "    confirm_count: 0", "confirm_count"},
	}
	for _, tc := range cases {
		t.Run(tc.field, func(t *testing.T) {
			y := yamlPrefix + tc.bad + "\n"
			_, err := ParseTunables([]byte(y))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestParseTunables_UnknownField(t *testing.T) {
	const y = `
defaults:
  alpha: 0.4
  grace_period_s: 5
  decay_rate_db_per_s: 2.0
  presence_floor_dbm: -95
  t_away_max_s: 30
  sticky_after_arrival_s: 120
  hysteresis_db: 4.0
  confirm_count: 2
  typo_field: 1
beacons: {}
`
	_, err := ParseTunables([]byte(y))
	if err == nil || !strings.Contains(err.Error(), "typo_field") {
		t.Fatalf("want unknown-field error, got %v", err)
	}
}

func TestLoadTunables_FileNotFound(t *testing.T) {
	_, err := LoadTunables("/nonexistent/tunables.yaml")
	if err == nil || !strings.Contains(err.Error(), "read") {
		t.Fatalf("want read error, got %v", err)
	}
}

func TestLoadTunables_FromFile(t *testing.T) {
	tun, err := LoadTunables("testdata/tunables_valid.yaml")
	if err != nil {
		t.Fatalf("LoadTunables: %v", err)
	}
	// Spot-check the defaults match the seed file.
	if tun.Defaults.Alpha != 0.4 {
		t.Errorf("alpha: want 0.4, got %v", tun.Defaults.Alpha)
	}
	if tun.Defaults.StickyAfterArrivalS != 120 {
		t.Errorf("sticky: want 120, got %d", tun.Defaults.StickyAfterArrivalS)
	}
}

func TestCrossValidate(t *testing.T) {
	tb1, err := NewTrackedBeacon(ids.MustNewIBeaconKey("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1", 1, 1), "tag_one")
	if err != nil {
		t.Fatalf("NewTrackedBeacon tag_one: %v", err)
	}
	tb2, err := NewTrackedBeacon(ids.MustNewIBeaconKey("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa2", 2, 2), "tag_two")
	if err != nil {
		t.Fatalf("NewTrackedBeacon tag_two: %v", err)
	}
	top, err := NewTopology(
		map[string]string{"aa:bb:cc:dd:ee:01": "zone_a"},
		[]TrackedBeacon{tb1, tb2},
	)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	const tunYAML = `
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
  tag_one: {}
  tag_two: {}
`
	tun, err := ParseTunables([]byte(tunYAML))
	if err != nil {
		t.Fatal(err)
	}
	orphans, err := CrossValidate(top, tun)
	if err != nil {
		t.Errorf("want nil err, got %v", err)
	}
	if len(orphans) != 0 {
		t.Errorf("want no orphans, got %v", orphans)
	}

	// A beacon listed in tunables but not in topology is reported as
	// an orphan — non-fatal so the pod can still start.
	const orphanTun = `
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
  ghost: {}
  tag_one: {}
`
	tunO, err := ParseTunables([]byte(orphanTun))
	if err != nil {
		t.Fatal(err)
	}
	orphans, err = CrossValidate(top, tunO)
	if err != nil {
		t.Errorf("orphan path must not return err, got %v", err)
	}
	if len(orphans) != 1 || orphans[0] != "ghost" {
		t.Errorf("want [ghost], got %v", orphans)
	}

	// A beacon in topology with no entry in tunables is fine.
	const partialTun = `
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
  tag_one: {}
`
	tunP, err := ParseTunables([]byte(partialTun))
	if err != nil {
		t.Fatal(err)
	}
	orphans, err = CrossValidate(top, tunP)
	if err != nil {
		t.Errorf("want nil err (tag_two inherits defaults), got %v", err)
	}
	if len(orphans) != 0 {
		t.Errorf("want no orphans, got %v", orphans)
	}
}

func TestCrossValidate_NilInputs(t *testing.T) {
	if _, err := CrossValidate(nil, nil); err == nil {
		t.Error("want error for nil inputs")
	}
}
