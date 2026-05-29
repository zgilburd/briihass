// Package config defines the in-memory shapes for the bridge's
// runtime configuration and merges them into a per-beacon Resolved
// view for the presence engine to consume.
//
// Two top-level documents:
//
//   - Topology (zones + tracked beacons) is sourced from the Postgres
//     store (internal/store; tables beacons and zones) and mutated at
//     runtime via /admin/devices (promote/demote) and /admin/zones
//     (upsert/delete). YAML topology is no longer shipped or parsed
//     (Phase 3, ADR-0007).
//
//   - Tunables (defaults block + per-beacon overrides) is the
//     operator-tunable engine knobs. Lives in Postgres
//     (internal/store tunables tables) and is editable via
//     /admin/tunables. A single YAML seed
//     (tunables-seed.yaml, path set via BRIIHASS_TUNABLES_SEED)
//     is consulted only on cold start when the
//     tunables tables are empty; see store.SeedFromYAMLIfEmpty.
//
// The presence engine consumes Resolved (see resolved.go), produced
// by (*Tunables).ResolveFor(beaconName), which merges the document
// defaults with any matching per-beacon override.
//
// All design constraints come from ADR-0006 (presence model) and
// ADR-0007 (postgres-resident persistence).
package config
