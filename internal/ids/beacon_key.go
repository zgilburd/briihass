package ids

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// uuidRE validates canonical lowercase 8-4-4-4-12 UUIDs. Shared by the
// iBeacon key constructor and the store-layer observation validation.
var uuidRE = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// IsCanonicalUUID reports whether s is a canonical lowercase 8-4-4-4-12
// UUID. Exposed so the store layer can validate UUIDs without rebuilding
// a full identity.
func IsCanonicalUUID(s string) bool { return uuidRE.MatchString(s) }

// ErrInvalid is returned (wrapped) when an identity literal fails
// validation. Callers can errors.Is against this sentinel.
var ErrInvalid = errors.New("invalid identity")

// Kind discriminates the source of a packet-derived beacon identity.
// The BLE Scan plugin surfaces every advertisement type, so identity is
// no longer iBeacon-only; each Kind carries a canonical, kind-specific
// Key (see the per-kind table in ADR-0008).
type Kind string

const (
	KindIBeacon      Kind = "ibeacon"       // Apple mfg 4C00 02 15
	KindEddystoneUID Kind = "eddystone_uid" // svc-data FEAA frame 0x00
	KindEddystoneURL Kind = "eddystone_url" // svc-data FEAA frame 0x10
	KindName         Kind = "name"          // AD 0x08/0x09 local name
	KindMfg          Kind = "mfg"           // allowlisted company-id w/ stable payload
)

// knownKinds gates ParseSlug / store reads so an unknown discriminator
// can't silently produce a zero-but-non-IsZero key.
var knownKinds = map[Kind]bool{
	KindIBeacon: true, KindEddystoneUID: true, KindEddystoneURL: true,
	KindName: true, KindMfg: true,
}

// BeaconKey is the canonical, packet-derived identity threaded through
// ingest, presence, store, mqtt, and admin. Unexported fields force
// construction through a validated entry point (mirrors the old BeaconID), so a
// raw struct literal can't smuggle an unvalidated identity across a
// package boundary.
type BeaconKey struct {
	kind Kind
	key  string
}

// Kind returns the identity discriminator.
func (k BeaconKey) Kind() Kind { return k.kind }

// Key returns the canonical, kind-specific key.
func (k BeaconKey) Key() string { return k.key }

// IsZero reports whether this is the zero value (no kind). A valid
// BeaconKey always has a non-empty kind and key.
func (k BeaconKey) IsZero() bool { return k.kind == "" }

// Slug is the stable cross-layer string used as a map key, in logs, and
// in admin URLs: "<kind>.<key>". Inverse of ParseSlug.
func (k BeaconKey) Slug() string { return string(k.kind) + "." + k.key }

// ParseSlug is the inverse of Slug. ok=false for malformed input or an
// unknown kind. The key half is trusted (it round-trips from Slug on a
// key that was validated at construction).
func ParseSlug(s string) (BeaconKey, bool) {
	kStr, key, found := strings.Cut(s, ".")
	if !found || key == "" {
		return BeaconKey{}, false
	}
	kind := Kind(kStr)
	if !knownKinds[kind] {
		return BeaconKey{}, false
	}
	return BeaconKey{kind: kind, key: key}, true
}

// EntityID is the Home Assistant / MQTT entity-id stem:
// "briihass_<kind>_<sanitized key>". It must be a stable, collision-free
// function of the BeaconKey.
//
//   - ibeacon: the key is "<uuid>_<major>_<minor>"; dashes are stripped.
//     The uuid is fixed-width hex, so stripping dashes stays injective and
//     yields the familiar briihass_ibeacon_<32hex>_<maj>_<min> form.
//   - eddystone_uid / mfg: keys are already [a-z0-9_]-safe hex+underscore.
//   - eddystone_url / name (free-form): the readable portion is sanitized
//     and a short hash of the exact key is appended so two keys that
//     sanitize to the same readable form still get distinct entity ids.
func (k BeaconKey) EntityID() string {
	base := "briihass_" + string(k.kind) + "_"
	switch k.kind {
	case KindIBeacon:
		return base + strings.ReplaceAll(k.key, "-", "")
	case KindEddystoneUID, KindMfg:
		return base + k.key
	default:
		return base + sanitizeFree(k.key) + "_" + shortHash(k.key)
	}
}

const freeReadableMax = 32

// sanitizeFree lowercases, maps any char outside [a-z0-9_] to '_',
// collapses runs of '_', trims leading/trailing '_', and caps length.
// It is intentionally lossy — uniqueness is restored by the hash suffix
// EntityID appends for free-form kinds.
func sanitizeFree(s string) string {
	var b strings.Builder
	lastUnderscore := false
	for _, r := range strings.ToLower(s) {
		ok := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_'
		if !ok {
			r = '_'
		}
		if r == '_' {
			if lastUnderscore {
				continue
			}
			lastUnderscore = true
		} else {
			lastUnderscore = false
		}
		b.WriteRune(r)
		if b.Len() >= freeReadableMax {
			break
		}
	}
	return strings.Trim(b.String(), "_")
}

func shortHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:4]) // 8 hex chars
}

// --- constructors -----------------------------------------------------

// NewIBeaconKey validates the UUID (canonical 8-4-4-4-12, lowercased)
// and builds an ibeacon-kind key "<uuid>_<major>_<minor>". Errors wrap
// ErrInvalid.
func NewIBeaconKey(uuid string, major, minor uint16) (BeaconKey, error) {
	u := strings.ToLower(strings.TrimSpace(uuid))
	if !uuidRE.MatchString(u) {
		return BeaconKey{}, fmt.Errorf("BeaconKey: uuid %q is not canonical 8-4-4-4-12 form: %w", uuid, ErrInvalid)
	}
	return BeaconKey{kind: KindIBeacon, key: fmt.Sprintf("%s_%d_%d", u, major, minor)}, nil
}

var (
	edNamespaceRE = regexp.MustCompile(`^[0-9a-f]{20}$`)
	edInstanceRE  = regexp.MustCompile(`^[0-9a-f]{12}$`)
)

// NewEddystoneUIDKey validates the 10-byte namespace + 6-byte instance
// (lowercase hex) and builds an eddystone_uid key "<namespace>_<instance>".
func NewEddystoneUIDKey(namespace, instance string) (BeaconKey, error) {
	ns := strings.ToLower(strings.TrimSpace(namespace))
	in := strings.ToLower(strings.TrimSpace(instance))
	if !edNamespaceRE.MatchString(ns) {
		return BeaconKey{}, fmt.Errorf("BeaconKey: eddystone namespace %q is not 20 hex chars: %w", namespace, ErrInvalid)
	}
	if !edInstanceRE.MatchString(in) {
		return BeaconKey{}, fmt.Errorf("BeaconKey: eddystone instance %q is not 12 hex chars: %w", instance, ErrInvalid)
	}
	return BeaconKey{kind: KindEddystoneUID, key: ns + "_" + in}, nil
}

// NewEddystoneURLKey builds an eddystone_url key from a decoded URL.
// The URL is trimmed and lowercased; an empty URL is rejected.
func NewEddystoneURLKey(url string) (BeaconKey, error) {
	u := strings.ToLower(strings.TrimSpace(url))
	if u == "" {
		return BeaconKey{}, fmt.Errorf("BeaconKey: empty eddystone url: %w", ErrInvalid)
	}
	return BeaconKey{kind: KindEddystoneURL, key: u}, nil
}

// NewNameKey builds a name-kind key from a BLE local name. The name is
// trimmed; an empty name is rejected. (BLE local names are typically
// ASCII; no Unicode normalization is applied — collisions are guarded by
// EntityID's hash suffix and the operator-facing unique name constraint.)
func NewNameKey(name string) (BeaconKey, error) {
	n := strings.TrimSpace(name)
	if n == "" {
		return BeaconKey{}, fmt.Errorf("BeaconKey: empty local name: %w", ErrInvalid)
	}
	return BeaconKey{kind: KindName, key: n}, nil
}

// NewMfgKey builds an mfg-kind key from a company id + a payload slice
// the caller has deemed stable. The key is "<companyid hex>_<sha256_16
// of payload>". Caveat: many vendor payloads embed rotating bytes; mfg
// identity is opt-in (see ADR-0008) and the caller is responsible for
// passing only the stable portion of the payload.
func NewMfgKey(companyID uint16, payload []byte) (BeaconKey, error) {
	if len(payload) == 0 {
		return BeaconKey{}, fmt.Errorf("BeaconKey: empty mfg payload: %w", ErrInvalid)
	}
	sum := sha256.Sum256(payload)
	return BeaconKey{kind: KindMfg, key: fmt.Sprintf("%04x_%s", companyID, hex.EncodeToString(sum[:8]))}, nil
}

// FromStoreKey rebuilds a BeaconKey from a persisted (kind, key) row.
// The store schema and write-path validation guarantee both halves, so
// reads (engine startup, topology rebuild, admin pages) skip
// re-validation. ok=false only for an unknown kind or empty key, which
// indicates a corrupt row.
func FromStoreKey(kind, key string) (BeaconKey, bool) {
	k := Kind(kind)
	if !knownKinds[k] || key == "" {
		return BeaconKey{}, false
	}
	return BeaconKey{kind: k, key: key}, true
}

// MustNewIBeaconKey panics if NewIBeaconKey would have errored. For
// tests + table fixtures only.
func MustNewIBeaconKey(uuid string, major, minor uint16) BeaconKey {
	k, err := NewIBeaconKey(uuid, major, minor)
	if err != nil {
		panic(err)
	}
	return k
}

// ParseSlugMust is a test/fixture helper that panics on malformed input.
func ParseSlugMust(s string) BeaconKey {
	k, ok := ParseSlug(s)
	if !ok {
		panic("ParseSlugMust: " + strconv.Quote(s))
	}
	return k
}
