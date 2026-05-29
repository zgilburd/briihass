package presence

import (
	"time"

	"briihass/internal/config"
	"briihass/internal/ids"
)

// BeaconKey is the packet-derived identity used to look a tracked
// beacon up in the engine. Aliased to the canonical type in
// internal/ids so the store, admin, and ingest packages share a single
// source of truth for validation and slug parsing.
type BeaconKey = ids.BeaconKey

// PresenceState is the published presence value (a zone/area label or
// StateNotHome). Aliased to ids.PresenceState so callers that already
// import presence (the MQTT publisher) don't need to import ids for
// this single type — mirrors the BeaconKey type alias and the NotHome
// const alias.
type PresenceState = ids.PresenceState

// Sighting is one BLE advertisement of one tracked beacon by one AP,
// as observed by the BLE Scan plugin. Ingest constructs and submits these.
type Sighting struct {
	Beacon     BeaconKey
	BeaconName string    // from topology (avoids a lookup in the engine)
	APMac      string    // lowercase, colon-separated
	APName     string    // human-readable, threaded through to MQTT attributes
	RSSI       int       // dBm (negative)
	At         time.Time // observation time per vRIoT payload (informational)

	// Telemetry attributed to this identity within the POST (Eddystone-TLM,
	// correlated by euid in ingest). nil when absent; rides alongside
	// presence to the publisher and never affects zone math.
	BatteryMV    *int
	TemperatureC *float64
}

// PresenceEvent is the result of a state transition that the bridge
// should publish to MQTT. The engine emits one per transition; the
// MQTT publisher consumes them and renders Discovery + state +
// attributes payloads.
type PresenceEvent struct {
	Beacon     BeaconKey
	BeaconName string

	// State is ids.StateNotHome for departure, or a PresenceState
	// derived from the AP's zone label for arrival/zone-change events.
	State ids.PresenceState

	// AP* are empty for not_home; otherwise reflect the AP currently
	// driving the published zone (the closest AP).
	APMac  string
	APName string

	// RSSI snapshot at the moment the event was emitted. Useful for
	// HA attributes and for tuning visibility via /admin/status.
	RSSIRaw                   int     // most recent raw sample seen at this AP
	RSSIEWMA                  float64 // smoothed value before decay
	RSSIEffective             float64 // post-decay value (used for ranking)
	RSSIRunnerUpEffectiveDiff float64 // gap in dB between closest and runner-up; 0 if no runner-up

	LastSeen     time.Time // most recent Sighting.At for this AP
	StickyActive bool      // whether the sticky-arrival window is currently holding
	EmittedAt    time.Time // engine wall clock when the event was emitted

	// Latest telemetry known for this beacon (nil when never reported).
	// Carried through to the MQTT publisher for the voltage/temperature
	// sensors; does not influence presence.
	BatteryMV    *int
	TemperatureC *float64
}

// NotHome is the published state value indicating no AP has presence
// for a beacon (and the sticky window does not apply). Kept as an
// alias of ids.StateNotHome so existing call sites do not need to
// import the ids package for this single constant.
const NotHome = ids.StateNotHome

// apState tracks per-AP EWMA + last-sighting timestamp for one beacon.
type apState struct {
	apName         string
	ewmaRSSI       float64
	lastRSSI       int       // raw value of most recent sighting
	lastSightingTs time.Time // engine wall clock, not the vRIoT payload time
	lastSeenAt     time.Time // vRIoT payload Sighting.At (for MQTT attributes)
}

// beaconState is the per-beacon engine state.
type beaconState struct {
	id    BeaconKey
	name  string
	perAP map[string]*apState

	// Currently-published state:
	currentZone string // "" means not_home
	currentAP   string // AP MAC backing the published zone (empty if not_home)

	// Sticky-arrival window:
	lastArrivalTs time.Time

	// Hysteresis confirm tracking for zone-to-zone transitions
	// (never applied to not_home -> zone).
	candidateAP    string
	candidateCount int

	// Latest telemetry seen for this beacon (latest non-nil wins). Carried
	// to the publisher via PresenceEvent; never affects zone resolution.
	lastBatteryMV    *int
	lastTemperatureC *float64
}

func newBeaconState(id BeaconKey, name string) *beaconState {
	return &beaconState{
		id:    id,
		name:  name,
		perAP: make(map[string]*apState),
	}
}

// closestInPresence walks the per-AP map and returns the AP with the
// highest post-decay effective RSSI that's above the presence floor,
// plus the gap to the runner-up. ok==false when no AP qualifies.
//
// Centralized here so ApplyTopology's synthetic-emit pass and the
// per-sighting recomputeOne can agree on "who is closest right now"
// without duplicating the loop (and the subtle two-best comparison).
func (bs *beaconState) closestInPresence(tun config.Resolved, now time.Time) (closest *apState, key string, eff, runnerUpDiff float64, ok bool) {
	closestEff := -1e9
	runnerUpEff := -1e9
	for k, ap := range bs.perAP {
		e := effectiveRSSI(ap, now, tun)
		if e < float64(tun.PresenceFloorDbm) {
			continue
		}
		ok = true
		switch {
		case e > closestEff:
			runnerUpEff = closestEff
			closestEff = e
			closest = ap
			key = k
		case e > runnerUpEff:
			runnerUpEff = e
		}
	}
	if !ok {
		return nil, "", 0, 0, false
	}
	if runnerUpEff > -1e9 {
		runnerUpDiff = closestEff - runnerUpEff
	}
	return closest, key, closestEff, runnerUpDiff, true
}

// freshestSightingTs returns the most recent lastSightingTs across
// all APs for this beacon, or the zero time if there have been none.
func (bs *beaconState) freshestSightingTs() time.Time {
	var t time.Time
	for _, ap := range bs.perAP {
		if ap.lastSightingTs.After(t) {
			t = ap.lastSightingTs
		}
	}
	return t
}
