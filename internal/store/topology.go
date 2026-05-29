package store

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"briihass/internal/ids"
)

// Beacon is one row in the allowlist table. The identity is folded
// into a single ids.BeaconKey field so callers can only build a Beacon
// via NewBeacon (which validates) or via a store read path (which
// trusts the schema).
type Beacon struct {
	Key   ids.BeaconKey
	Name  string
	Notes string
}

// NewBeacon constructs a Beacon after validating the operator-visible
// name. The key is already validated by its own constructor; the name
// must be non-empty after trimming.
func NewBeacon(key ids.BeaconKey, name, notes string) (Beacon, error) {
	if key.IsZero() {
		return Beacon{}, errors.New("beacon: zero key")
	}
	if strings.TrimSpace(name) == "" {
		return Beacon{}, errors.New("beacon name is required")
	}
	return Beacon{Key: key, Name: name, Notes: notes}, nil
}

// Domain returns the canonical ids.BeaconKey. Kept as a method (rather
// than direct field access) so call sites read uniformly with the
// DeviceSummary projection.
func (b Beacon) Domain() ids.BeaconKey { return b.Key }

// Zone is one (AP MAC -> zone label) row with both identity values
// guaranteed canonical at the type level. Build via NewZone after
// validating raw operator input through ids.NewAPMAC and
// ids.NewZoneLabel.
type Zone struct {
	APMac     ids.APMAC
	ZoneLabel ids.ZoneLabel
	APName    string
}

// NewZone wraps pre-validated identity values into a Zone. No error
// return — both inputs are guaranteed canonical by their own
// constructors. The re-check in `UpsertZone` is defensive against
// zero-value `Zone{}` struct literals built by external packages
// that bypass this constructor.
func NewZone(apMac ids.APMAC, label ids.ZoneLabel, apName string) Zone {
	return Zone{APMac: apMac, ZoneLabel: label, APName: apName}
}

// ErrNotFound is returned by store operations that target a specific
// row that does not exist.
var ErrNotFound = errors.New("store: not found")

// ErrConflict is returned when an insert collides with a uniqueness
// constraint (e.g. promoting a beacon whose name is already taken).
var ErrConflict = errors.New("store: conflict")

// PromoteBeacon inserts b into the allowlist. Notes may be empty.
// Returns ErrConflict if (kind,key) is already promoted or the name is
// already taken.
func (s *Postgres) PromoteBeacon(ctx context.Context, b Beacon) error {
	if err := validateBeacon(b); err != nil {
		return err
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO beacons (kind, key, name, notes)
		VALUES ($1, $2, $3, NULLIF($4, ''))
	`, string(b.Key.Kind()), b.Key.Key(), b.Name, b.Notes)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: beacon already promoted or name taken", ErrConflict)
		}
		return fmt.Errorf("insert beacon: %w", err)
	}
	return nil
}

// DemoteBeacon removes the row identified by key. Returns ErrNotFound
// if no such row exists.
func (s *Postgres) DemoteBeacon(ctx context.Context, key ids.BeaconKey) error {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM beacons WHERE kind = $1 AND key = $2
	`, string(key.Kind()), key.Key())
	if err != nil {
		return fmt.Errorf("delete beacon: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ListBeacons returns every promoted beacon, ordered by name.
func (s *Postgres) ListBeacons(ctx context.Context) ([]Beacon, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT kind, key, name, COALESCE(notes, '')
		  FROM beacons ORDER BY name
	`)
	if err != nil {
		return nil, fmt.Errorf("query beacons: %w", err)
	}
	defer rows.Close()
	var out []Beacon
	for rows.Next() {
		var kind, key, name, notes string
		if err := rows.Scan(&kind, &key, &name, &notes); err != nil {
			return nil, fmt.Errorf("scan beacon: %w", err)
		}
		bk, ok := ids.FromStoreKey(kind, key)
		if !ok {
			return nil, fmt.Errorf("scan beacon: corrupt identity row (kind=%q key=%q)", kind, key)
		}
		out = append(out, Beacon{Key: bk, Name: name, Notes: notes})
	}
	return out, rows.Err()
}

// UpsertZone writes the zone row. ap_name may be empty.
func (s *Postgres) UpsertZone(ctx context.Context, z Zone) error {
	if z.APMac.IsZero() {
		return errors.New("UpsertZone: ap_mac required")
	}
	if z.ZoneLabel.IsZero() {
		return errors.New("UpsertZone: zone_label required")
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO zones (ap_mac, zone_label, ap_name, updated_at)
		VALUES ($1, $2, NULLIF($3, ''), now())
		ON CONFLICT (ap_mac) DO UPDATE SET
			zone_label = EXCLUDED.zone_label,
			ap_name    = EXCLUDED.ap_name,
			updated_at = now()
	`, z.APMac.String(), z.ZoneLabel.String(), z.APName)
	if err != nil {
		return fmt.Errorf("upsert zone: %w", err)
	}
	return nil
}

// DeleteZone removes the row identified by ap MAC. Returns ErrNotFound
// when no such row exists.
func (s *Postgres) DeleteZone(ctx context.Context, apMac ids.APMAC) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM zones WHERE ap_mac = $1`, apMac.String())
	if err != nil {
		return fmt.Errorf("delete zone: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ListZones returns every zone row, ordered by ap_mac.
func (s *Postgres) ListZones(ctx context.Context) ([]Zone, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT ap_mac, zone_label, COALESCE(ap_name, '')
		  FROM zones ORDER BY ap_mac
	`)
	if err != nil {
		return nil, fmt.Errorf("query zones: %w", err)
	}
	defer rows.Close()
	var out []Zone
	for rows.Next() {
		var mac, label, apName string
		if err := rows.Scan(&mac, &label, &apName); err != nil {
			return nil, fmt.Errorf("scan zone: %w", err)
		}
		out = append(out, Zone{
			APMac:     ids.APMACFromStoreRow(mac),
			ZoneLabel: ids.ZoneLabelFromStoreRow(label),
			APName:    apName,
		})
	}
	return out, rows.Err()
}

func validateBeacon(b Beacon) error {
	if b.Key.IsZero() {
		return errors.New("beacon: zero key")
	}
	if strings.TrimSpace(b.Name) == "" {
		return errors.New("beacon name is required")
	}
	return nil
}

// isUniqueViolation reports whether err is a Postgres unique_violation
// (SQLSTATE 23505). Used to translate insert collisions into
// ErrConflict for callers.
//
// `fmt.Errorf("…%w", err)` preserves the typed interface so
// `errors.As(err, &pgErr)` succeeds. `errors.New(err.Error())` and
// `fmt.Errorf("…%s", err)` do NOT — they discard the chain. The
// substring fallback below catches the latter wrap shapes so a
// caller refactor that loses the typed wrap doesn't silently break
// the unique-violation→ErrConflict translation.
func isUniqueViolation(err error) bool {
	var pgErr interface{ SQLState() string }
	if errors.As(err, &pgErr) {
		return pgErr.SQLState() == "23505"
	}
	return strings.Contains(err.Error(), "23505")
}
