package ids

import (
	"fmt"
	"regexp"
)

// ZoneLabel is the HA-friendly state name a presence engine publishes
// for a tracker (e.g. "zone_a", "entry"). Construction goes through
// NewZoneLabel so the regex invariant is enforced once at the
// boundary; downstream code can treat ZoneLabel as already-canonical.
//
// The constraint matches the CLAUDE.md convention and keeps MQTT
// topic templates predictable: lowercase ASCII letters/digits/
// underscores, starting with a letter, up to 64 characters.
type ZoneLabel struct {
	label string
}

// zoneLabelRE is the validation regex. Mirrored as a CHECK constraint
// in store/schema.sql for belt-and-suspenders enforcement.
var zoneLabelRE = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

// NewZoneLabel returns a validated ZoneLabel. Errors wrap ErrInvalid.
func NewZoneLabel(s string) (ZoneLabel, error) {
	if s == "" {
		return ZoneLabel{}, fmt.Errorf("ZoneLabel: empty: %w", ErrInvalid)
	}
	if !zoneLabelRE.MatchString(s) {
		return ZoneLabel{}, fmt.Errorf("ZoneLabel %q must match %s: %w", s, zoneLabelRE, ErrInvalid)
	}
	return ZoneLabel{label: s}, nil
}

// MustNewZoneLabel panics if NewZoneLabel would have errored. For
// tests + fixtures only.
func MustNewZoneLabel(s string) ZoneLabel {
	z, err := NewZoneLabel(s)
	if err != nil {
		panic(err)
	}
	return z
}

// ZoneLabelFromStoreRow constructs a ZoneLabel from a row already
// persisted in the zones table. The schema CHECK plus write-path
// validation guarantees the regex; re-validating on every read is
// wasted work. If you have an unvalidated string, call NewZoneLabel
// instead.
func ZoneLabelFromStoreRow(s string) ZoneLabel { return ZoneLabel{label: s} }

// String returns the canonical label.
func (z ZoneLabel) String() string { return z.label }

// IsZero reports whether the value is the zero ZoneLabel.
func (z ZoneLabel) IsZero() bool { return z.label == "" }

// IsValidZoneLabel reports whether s would be accepted by
// NewZoneLabel. Exported for callers that want to check without
// allocating a ZoneLabel value or building an error.
func IsValidZoneLabel(s string) bool {
	return s != "" && zoneLabelRE.MatchString(s)
}
