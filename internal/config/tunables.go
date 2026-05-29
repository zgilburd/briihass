package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Tunables is the in-memory representation of the live-editable
// state-machine knobs: a bridge-wide DefaultsBlock plus optional
// per-beacon overrides.
//
// Tunables are Postgres-resident as of Phase 3 (ADR-0007). The admin
// UI calls store.Postgres.SaveAll on every successful POST and the
// engine consumes the result via ApplyTunables. The YAML decode path
// here is used only on cold start by store.SeedFromYAMLIfEmpty, which
// reads the seed at the path set via BRIIHASS_TUNABLES_SEED. See
// internal/config/doc.go for the full lifecycle.
type Tunables struct {
	Defaults DefaultsBlock        `yaml:"defaults"`
	Beacons  map[string]Overrides `yaml:"beacons"`
}

// DefaultsBlock contains the bridge-wide default values for every
// tunable. Used when a beacon has no override (or no entry at all)
// in the Beacons map.
type DefaultsBlock struct {
	Alpha               float64 `yaml:"alpha"`
	GracePeriodS        int     `yaml:"grace_period_s"`
	DecayRateDbPerS     float64 `yaml:"decay_rate_db_per_s"`
	PresenceFloorDbm    int     `yaml:"presence_floor_dbm"`
	TAwayMaxS           int     `yaml:"t_away_max_s"`
	StickyAfterArrivalS int     `yaml:"sticky_after_arrival_s"`
	HysteresisDb        float64 `yaml:"hysteresis_db"`
	ConfirmCount        int     `yaml:"confirm_count"`
}

// Overrides is a partial DefaultsBlock; only non-nil fields override.
// A beacon with no Overrides entry (or an empty entry) inherits all
// defaults.
type Overrides struct {
	Alpha               *float64 `yaml:"alpha,omitempty"`
	GracePeriodS        *int     `yaml:"grace_period_s,omitempty"`
	DecayRateDbPerS     *float64 `yaml:"decay_rate_db_per_s,omitempty"`
	PresenceFloorDbm    *int     `yaml:"presence_floor_dbm,omitempty"`
	TAwayMaxS           *int     `yaml:"t_away_max_s,omitempty"`
	StickyAfterArrivalS *int     `yaml:"sticky_after_arrival_s,omitempty"`
	HysteresisDb        *float64 `yaml:"hysteresis_db,omitempty"`
	ConfirmCount        *int     `yaml:"confirm_count,omitempty"`
}

// LoadTunables reads, parses, and validates a tunables seed YAML file.
// Cold-start use only; runtime mutation goes through the Postgres-backed
// `internal/store/tunables.go` API.
func LoadTunables(path string) (*Tunables, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	return ParseTunables(raw)
}

// ParseTunables is LoadTunables without the file I/O.
func ParseTunables(raw []byte) (*Tunables, error) {
	var t Tunables
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true)
	if err := dec.Decode(&t); err != nil {
		return nil, fmt.Errorf("tunables yaml decode: %w", err)
	}
	if t.Beacons == nil {
		t.Beacons = make(map[string]Overrides)
	}
	if err := t.Validate(); err != nil {
		return nil, err
	}
	return &t, nil
}

// Validate returns an error if the tunables have any structural
// problem. Cross-checks against a Topology (orphan beacon entries,
// missing-name) belong in CrossValidate.
func (t *Tunables) Validate() error {
	if t == nil {
		return errors.New("nil tunables")
	}
	if err := validateDefaults(t.Defaults); err != nil {
		return fmt.Errorf("defaults: %w", err)
	}
	for name, o := range t.Beacons {
		if err := validateOverrides(o); err != nil {
			return fmt.Errorf("beacons[%q]: %w", name, err)
		}
	}
	return nil
}

func validateDefaults(d DefaultsBlock) error {
	if d.Alpha <= 0 || d.Alpha > 1 {
		return fmt.Errorf("alpha %v must be in (0, 1]", d.Alpha)
	}
	if d.GracePeriodS < 0 {
		return fmt.Errorf("grace_period_s %d must be >= 0", d.GracePeriodS)
	}
	if d.DecayRateDbPerS < 0 {
		return fmt.Errorf("decay_rate_db_per_s %v must be >= 0", d.DecayRateDbPerS)
	}
	if d.PresenceFloorDbm > 0 || d.PresenceFloorDbm < -127 {
		return fmt.Errorf("presence_floor_dbm %d must be in [-127, 0]", d.PresenceFloorDbm)
	}
	if d.TAwayMaxS < 1 {
		return fmt.Errorf("t_away_max_s %d must be >= 1", d.TAwayMaxS)
	}
	if d.StickyAfterArrivalS < 0 {
		return fmt.Errorf("sticky_after_arrival_s %d must be >= 0 (0 disables)", d.StickyAfterArrivalS)
	}
	if d.HysteresisDb < 0 {
		return fmt.Errorf("hysteresis_db %v must be >= 0", d.HysteresisDb)
	}
	if d.ConfirmCount < 1 {
		return fmt.Errorf("confirm_count %d must be >= 1", d.ConfirmCount)
	}
	return nil
}

func validateOverrides(o Overrides) error {
	if o.Alpha != nil && (*o.Alpha <= 0 || *o.Alpha > 1) {
		return fmt.Errorf("alpha %v must be in (0, 1]", *o.Alpha)
	}
	if o.GracePeriodS != nil && *o.GracePeriodS < 0 {
		return fmt.Errorf("grace_period_s %d must be >= 0", *o.GracePeriodS)
	}
	if o.DecayRateDbPerS != nil && *o.DecayRateDbPerS < 0 {
		return fmt.Errorf("decay_rate_db_per_s %v must be >= 0", *o.DecayRateDbPerS)
	}
	if o.PresenceFloorDbm != nil && (*o.PresenceFloorDbm > 0 || *o.PresenceFloorDbm < -127) {
		return fmt.Errorf("presence_floor_dbm %d must be in [-127, 0]", *o.PresenceFloorDbm)
	}
	if o.TAwayMaxS != nil && *o.TAwayMaxS < 1 {
		return fmt.Errorf("t_away_max_s %d must be >= 1", *o.TAwayMaxS)
	}
	if o.StickyAfterArrivalS != nil && *o.StickyAfterArrivalS < 0 {
		return fmt.Errorf("sticky_after_arrival_s %d must be >= 0", *o.StickyAfterArrivalS)
	}
	if o.HysteresisDb != nil && *o.HysteresisDb < 0 {
		return fmt.Errorf("hysteresis_db %v must be >= 0", *o.HysteresisDb)
	}
	if o.ConfirmCount != nil && *o.ConfirmCount < 1 {
		return fmt.Errorf("confirm_count %d must be >= 1", *o.ConfirmCount)
	}
	return nil
}

// CrossValidate returns the list of tunables beacon-override names
// that have no matching row in the topology's beacon allowlist
// (orphans). Orphans are non-fatal: the override is inert until a
// matching beacon is promoted, and the /admin/tunables save path
// rebuilds the override map from the topology, so the first admin
// save naturally prunes them. The caller should log/meter the
// orphans and continue startup; the returned error is reserved for
// nil-input programmer error so callers can keep their nil checks.
//
// (Beacons present in the allowlist that have no override here are
// fine — they just inherit defaults.)
func CrossValidate(top *Topology, tun *Tunables) (orphans []string, err error) {
	if top == nil || tun == nil {
		return nil, errors.New("CrossValidate: nil topology or tunables")
	}
	known := top.BeaconNames()
	for name := range tun.Beacons {
		if _, ok := known[name]; !ok {
			orphans = append(orphans, name)
		}
	}
	return orphans, nil
}
