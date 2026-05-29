// Package store is the Postgres-backed persistence layer for every
// piece of runtime-mutable state in the bridge:
//
//   - tunables: the engine tunables document (defaults + per-beacon
//     overrides). LoadAll/SaveAll under the Store interface.
//   - beacons: the operator allowlist (uuid, major, minor, name, notes).
//     Mutated via /admin/devices promote/demote.
//   - zones: AP MAC -> zone label. Mutated via /admin/zones.
//   - observations: per-event ingest rows, retention-bounded.
//     Inserted via the buffered ObservationsWriter so the ingest hot
//     path never blocks on a DB round trip.
//   - raw_posts: optional captured /ingest and /heartbeat envelopes,
//     retention-bounded.
//   - settings: retention_days + capture toggles. The ingest hot path
//     reads them via SettingsSnapshot (atomic.Pointer, no lock).
//
// The schema in schema.sql is applied idempotently on every boot
// (NewPostgres -> apply). YAML has exactly one cold-start use: the
// tunables_defaults/tunables_overrides tables are seeded from
// tunables-seed.yaml (path set via BRIIHASS_TUNABLES_SEED)
// when LoadAll returns ErrEmpty; see SeedFromYAMLIfEmpty. The
// beacons, zones, and settings tables have no YAML seed — they are
// populated either at first boot via the schema's INSERT-ON-CONFLICT
// defaults (settings) or by operator action via /admin (beacons,
// zones).
package store

import (
	"context"
	"errors"

	"briihass/internal/config"
)

// Store persists and retrieves the full Tunables document atomically.
// The single document is the unit of read/write; there is no per-row
// optimistic locking yet (single writer, single admin operator — add
// only if/when it becomes a real concern).
type Store interface {
	// LoadAll returns the persisted tunables. Returns ErrEmpty when
	// the store has never been written; the caller is expected to
	// seed via SeedFromYAMLIfEmpty.
	LoadAll(ctx context.Context) (*config.Tunables, error)

	// SaveAll atomically replaces the persisted document with t.
	SaveAll(ctx context.Context, t *config.Tunables) error
}

// ErrEmpty is returned by LoadAll when the store has not yet been
// populated with any tunables row.
var ErrEmpty = errors.New("store: tunables empty")
