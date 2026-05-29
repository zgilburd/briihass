package config

import (
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"

	"briihass/internal/ids"
)

// Topology is the runtime view of the user's home: AP MAC -> zone
// label, plus the explicit allowlist of tracked beacons. Phase 3
// onward, this is built from the Postgres store (beacons + zones
// tables), not from a YAML file.
//
// Fields are unexported so the only way to construct a Topology with
// content is via NewTopology, which validates. Read access goes
// through Zone, Beacons, and BeaconNames; mutation requires building
// a fresh Topology and swapping it in via Engine.ApplyTopology.
type Topology struct {
	zones   map[string]string
	beacons []TrackedBeacon
}

// TrackedBeacon is one entry in the allowlist. Fields are unexported
// so callers can only construct one via NewTrackedBeacon, which
// validates the operator-visible name.
type TrackedBeacon struct {
	key  ids.BeaconKey
	name string
}

// ID returns the canonical packet-derived identity.
func (b TrackedBeacon) ID() ids.BeaconKey { return b.key }

// Name returns the operator-visible name.
func (b TrackedBeacon) Name() string { return b.name }

// NewTrackedBeacon constructs a TrackedBeacon. Returns an error if
// name is empty after trimming; the key is already validated by its
// constructor.
func NewTrackedBeacon(key ids.BeaconKey, name string) (TrackedBeacon, error) {
	if strings.TrimSpace(name) == "" {
		return TrackedBeacon{}, errors.New("TrackedBeacon: name is empty")
	}
	return TrackedBeacon{key: key, name: name}, nil
}

// NormalizeMAC returns a canonical lowercase MAC form for use as a
// zone-map key. Falls back to a lowercased string when net.ParseMAC
// can't parse the input.
func NormalizeMAC(s string) string {
	if hw, err := net.ParseMAC(s); err == nil && len(hw) == 6 {
		return hw.String()
	}
	return strings.ToLower(s)
}

// NewTopology validates the inputs and returns a usable Topology.
// An empty topology (no zones, no beacons) is allowed — that's the
// bootstrap state on a fresh cluster; the engine just won't emit
// events until the operator promotes from /admin/devices.
//
// zones keys are normalized via NormalizeMAC before insert; callers
// don't need to canonicalize themselves.
func NewTopology(zones map[string]string, beacons []TrackedBeacon) (*Topology, error) {
	out := &Topology{
		zones:   make(map[string]string, len(zones)),
		beacons: make([]TrackedBeacon, 0, len(beacons)),
	}
	for mac, label := range zones {
		if _, err := net.ParseMAC(mac); err != nil {
			return nil, fmt.Errorf("zones: %q is not a valid MAC address: %w", mac, err)
		}
		if strings.TrimSpace(label) == "" {
			return nil, fmt.Errorf("zones: %s has an empty zone label", mac)
		}
		out.zones[NormalizeMAC(mac)] = label
	}
	seen := make(map[ids.BeaconKey]struct{}, len(beacons))
	names := make(map[string]struct{}, len(beacons))
	for i, b := range beacons {
		// TrackedBeacon zero value is rejected: key has been validated
		// by its ids constructor, but the name must not be empty and the
		// key must not be the zero BeaconKey (defensive against direct
		// struct-literal construction from within the package).
		if b.key.IsZero() {
			return nil, fmt.Errorf("beacons[%d]: zero beacon key", i)
		}
		if strings.TrimSpace(b.name) == "" {
			return nil, fmt.Errorf("beacons[%d]: name is empty", i)
		}
		if _, dup := seen[b.key]; dup {
			return nil, fmt.Errorf("beacons[%d]: duplicate beacon %s", i, b.key.Slug())
		}
		seen[b.key] = struct{}{}
		if _, dup := names[b.name]; dup {
			return nil, fmt.Errorf("beacons[%d]: duplicate name %q", i, b.name)
		}
		names[b.name] = struct{}{}
		out.beacons = append(out.beacons, b)
	}
	return out, nil
}

// Zone returns the zone label for an AP MAC, or "" if the MAC is not
// in the zones map. Caller decides how to handle the missing case
// (typically drop the event and increment a counter).
func (t *Topology) Zone(apMAC string) string {
	if t == nil {
		return ""
	}
	return t.zones[NormalizeMAC(apMAC)]
}

// Beacons returns a defensive copy of the allowlist. Callers may
// freely mutate the returned slice without affecting the topology.
func (t *Topology) Beacons() []TrackedBeacon {
	if t == nil {
		return nil
	}
	return slices.Clone(t.beacons)
}

// ZoneCount returns the number of AP -> zone mappings. Cheap; no
// allocation. Useful for /metrics + startup logs.
func (t *Topology) ZoneCount() int {
	if t == nil {
		return 0
	}
	return len(t.zones)
}

// BeaconCount returns the number of tracked beacons. Cheap; no
// allocation.
func (t *Topology) BeaconCount() int {
	if t == nil {
		return 0
	}
	return len(t.beacons)
}

// BeaconNames returns the set of tracked beacon names. Used by the
// cross-validator to catch orphan tunables overrides.
func (t *Topology) BeaconNames() map[string]struct{} {
	if t == nil {
		return nil
	}
	out := make(map[string]struct{}, len(t.beacons))
	for _, b := range t.beacons {
		out[b.name] = struct{}{}
	}
	return out
}
