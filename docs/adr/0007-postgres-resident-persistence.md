# ADR-0007 — Postgres-resident persistence + observed-device promote/demote

**Status:** accepted (Phase 3)
**Date:** 2026-05-26
**Supersedes:** parts of ADR-0003 (the YAML-topology cold-start half),
and the "Configuration file (not state)" section of ADR-0006 — topology
storage now lives in the `beacons` and `zones` Postgres tables, edited
via `/admin/devices` and `/admin/zones`.

## Context

Phase 2 moved the engine tunables into Postgres but left two other
shapes of operator-edited state behind:

1. The beacon allowlist + zone map lived in a YAML config file mounted
   into the pod. Adding or removing a beacon meant a config reseal, an
   infra-repo PR, and a pod roll. This was the standing operator blocker
   after Phase 2.
2. The bridge had no record of *what it had seen* — unknown beacons
   were dropped with a counter, so operators couldn't promote from
   observed reality and had to know UUIDs out-of-band.

Beacon-driven HA automation made the friction acute: every new tracked beacon,
phone change, or vendor SDK rotation required another seal cycle.

## Decision

All operator-mutable persistence moves into Postgres. YAML topology is
deleted. The bridge keeps a short rolling window of every observation
so operators promote from what they actually see.

Concrete shape:

- **`beacons`** (the allowlist) replaces `briihass.yaml > beacons`.
  Promoted via `/admin/devices`. Triple `(uuid, major, minor)` is the
  primary key; `name` is unique (it's the HA entity suffix).
- **`zones`** replaces `briihass.yaml > zones`. AP MAC → zone label,
  upserted via `/admin/zones` (which shows observed APs that haven't
  been labeled yet).
- **`observations`** is the rolling window of every iBeacon advert
  seen, tracked or not. `tracked` is recorded at observation time so
  the UI can split "already-tracked" from "observed-only" cleanly.
- **`raw_posts`** is the per-request envelope (gzipped body + sha256 +
  endpoint + remote address). Optional capture.
- **`settings`** holds the two capture toggles and `retention_days`.
  The ingest hot path reads from an in-memory snapshot refreshed on
  every save.

Operator-driven mutation:

- Promote: `POST /admin/devices/promote` inserts into `beacons`, then
  signals the presence engine via `ApplyTopology` — no restart. The
  next sighting publishes HA Discovery config + state (idempotent).
- Demote: `POST /admin/devices/demote` deletes the allowlist row,
  signals the engine, **and** calls `mqtt.Publisher.RemoveEntity` to
  publish empty retained payloads to its Discovery config, state, and
  attributes topics. The triple-clear is belt-and-suspenders: HA owns
  entity cleanup once the config goes empty, but a stale retained
  state on the broker would otherwise leak through to the next
  subscribe before the engine re-emits on a future re-promote.

Retention worker:

- Hourly goroutine in `cmd/briihass` runs
  `DELETE FROM observations WHERE observed_at < now() - settings.retention_days`
  and the matching prune on `raw_posts`. `retention_days` is
  operator-configurable (1..30, default 7).
- `observations.raw_post_id` has `ON DELETE SET NULL`, so a pruned
  envelope leaves its referencing observations intact (they still show
  up in the per-device packet view, just without a clickable raw-POST
  link).

Ingest hot path:

- Observations are buffered through a channel + batched-tx writer
  (`internal/store.ObservationsWriter`) so the request goroutine never
  blocks on a DB round trip.
- Raw POST envelopes are written **synchronously** before per-event
  parsing (we need the row ID to stamp on observations). One round
  trip per POST, gated by `settings.capture_full_posts`.

YAML topology is **deleted**, not seed-on-empty. Cold-start on an
empty DB is the supported bootstrap; the operator promotes from
observed reality. The cold-start config shrinks to only
`tunables-seed.yaml`.

## Consequences

Positive:

- The standing reseal blocker is gone. New beacons appear in
  `/admin/devices` and are promoted with a single click.
- HA entity removal is automated and idiomatic; no more orphan
  `device_tracker.briihass_*` entries after a beacon is retired.
- Operators can debug a misbehaving advert without shelling into the
  pod or replaying a capture — the per-device packet view shows raw
  hex, parsed iBeacon fields, and a full TLV walk.

Negative / costs:

- Storage grows with observation rate. At 1–2 ev/s for the default
  7-day window with per-event hex captured, ~70–150 MB. With full POST
  capture, an additional ~70 MB – 700 MB. Both are bounded by the
  retention worker and operator toggles; the 30-day cap is the
  worst-case budget.
- The presence engine + ingest path now coordinate via the engine's
  `ApplyTopology` swap rather than a frozen-at-startup map. Tested via
  the admin promote/demote round-trip test.

Out of scope:

- Long-term historical analysis. HA owns that; briihass keeps a 7–10
  day rolling window.
- Auto-promote on first sighting. We deliberately keep promotion as an
  explicit operator action to avoid surfacing every guest's phone as a
  `device_tracker` entity.

## Verification

The hot-path tests cover:

- `internal/store/*_test.go` — every CRUD pair plus the retention prune.
- `internal/ingest/handler_test.go` — observation recorded for both
  tracked + untracked beacons; raw POST envelope recorded only when
  `capture_full_posts` is on.
- `internal/mqtt/publisher_test.go` — `RemoveEntity` publishes empty
  retained payloads to all three topics and clears the seen-map so a
  later re-promote re-asserts the Discovery config.
- `internal/admin/devices_test.go` — promote/demote round-trip with
  fakes for engine + MQTT remover.

## References

- HA MQTT Discovery removal: https://www.home-assistant.io/integrations/mqtt/#discovery-messages
- ADR-0006 (presence model) — `ApplyTopology` preserves per-beacon
  EWMA state across allowlist edits; added/removed beacons see fresh
  empty / evicted state respectively. Hot-path lookup uses the engine
  via `IsTracked`.
