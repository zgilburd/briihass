package ids

import (
	"fmt"
	"net"
	"strings"
)

// APMAC identifies a single Access Point by its EUI-48 MAC address in
// canonical lowercase colon-separated form. Construction goes through
// NewAPMAC (or MustNewAPMAC for tests) so callers cannot persist a
// non-canonical form. The internal store always reads/writes the
// canonical String() form.
type APMAC struct {
	mac string // canonical lowercase "aa:bb:cc:dd:ee:ff"
}

// NewAPMAC parses and canonicalizes a MAC string. Accepts any form
// net.ParseMAC accepts (colon, dash, dotted) and requires EUI-48
// (6 bytes); EUI-64 and Infiniband forms are rejected because zone
// keys must match the AP MACs the vRIoT plugin reports.
//
// Errors wrap ErrInvalid.
func NewAPMAC(s string) (APMAC, error) {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return APMAC{}, fmt.Errorf("APMAC: empty: %w", ErrInvalid)
	}
	hw, err := net.ParseMAC(trimmed)
	if err != nil {
		return APMAC{}, fmt.Errorf("APMAC %q: %w: %w", s, err, ErrInvalid)
	}
	if len(hw) != 6 {
		return APMAC{}, fmt.Errorf("APMAC %q: expected EUI-48 (6 bytes), got %d: %w", s, len(hw), ErrInvalid)
	}
	return APMAC{mac: hw.String()}, nil
}

// MustNewAPMAC panics if NewAPMAC would have errored. For tests + fixtures only.
func MustNewAPMAC(s string) APMAC {
	m, err := NewAPMAC(s)
	if err != nil {
		panic(err)
	}
	return m
}

// APMACFromStoreRow constructs an APMAC from a row already persisted
// in the zones table. The schema CHECK plus write-path validation
// guarantees canonical form, so re-validating on every read is wasted
// work. If you have an unvalidated string, call NewAPMAC instead.
func APMACFromStoreRow(s string) APMAC { return APMAC{mac: s} }

// String returns the canonical lowercase colon-separated form.
func (m APMAC) String() string { return m.mac }

// IsZero reports whether the value is the uninitialized-Go-struct
// state (empty `mac` field). NOT the same as the all-zero MAC
// `00:00:00:00:00:00`, which is a valid EUI-48 and would canonicalize
// to a non-empty string. Useful for distinguishing "not set" from
// "explicitly set to any value".
func (m APMAC) IsZero() bool { return m.mac == "" }
