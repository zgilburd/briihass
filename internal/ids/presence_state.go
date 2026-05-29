package ids

// PresenceState is the value the bridge publishes as a Home Assistant
// device_tracker state: either StateNotHome (no presence anywhere) or
// a zone label sourced from a Zone. Using a named type makes the
// "either zone or not_home" distinction explicit at call sites so a
// future refactor cannot accidentally pass a raw operator string as
// the state.
type PresenceState string

// StateNotHome is published when no AP currently has presence for a
// beacon (and the sticky-arrival window does not apply). Mirrors the
// Home Assistant convention.
const StateNotHome PresenceState = "not_home"

// PresenceStateFromZone projects a ZoneLabel into a PresenceState. The
// resulting state is whatever the operator chose as the zone label
// (e.g. "entry", "zone_a"), which is exactly what HA renders.
func PresenceStateFromZone(z ZoneLabel) PresenceState {
	return PresenceState(z.String())
}

// String returns the underlying value (so callers can build payloads
// or log entries without an explicit conversion).
func (s PresenceState) String() string { return string(s) }

// IsNotHome reports whether this is the StateNotHome value.
func (s PresenceState) IsNotHome() bool { return s == StateNotHome }
