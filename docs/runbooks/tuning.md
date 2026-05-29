# Runbook — Tuning the presence model

The bridge's state-machine knobs (RSSI smoothing, decay rate, sticky
window, etc.) are persisted in Postgres (the `briihass-postgres`
StatefulSet in the briihass namespace, schema in
`internal/store/schema.sql`). There are no compiled-in
defaults; the bridge refuses to start without a populated
`tunables_defaults` row.

See [ADR-0006](../adr/0006-presence-model-closest-ap-wins.md) for what
each tunable does at the mechanism level. This runbook is the
operator-side how-to.

## Storage

Two tables in the `briihass` Postgres:

| Table | Rows | Purpose |
|---|---|---|
| `tunables_defaults` | exactly 1 (`id = 1` CHECK) | bridge-wide defaults |
| `tunables_overrides` | one per beacon with a per-beacon override | nullable columns; null = inherit default |

The bridge connects via `BRIIHASS_POSTGRES_DSN` (injected from your secret
manager in prod, from `~/.config/briihass/credentials.env` in dev — see
[ADR-0003](../adr/0003-credential-handling.md)).

## Cold-start seeding

On boot the bridge calls `store.SeedFromYAMLIfEmpty`. If
`tunables_defaults` is empty, the bridge parses the YAML file at
`BRIIHASS_TUNABLES_SEED` (default `/etc/briihass/tunables-seed.yaml`;
mount or supply it however your platform provides config) and writes
that as the initial state. Subsequent restarts skip the seed —
whatever's in Postgres wins.

## Live editing

The bridge ships a basic-auth admin UI at `:8082/admin/tunables`
(cluster-internal in prod; reach via `kubectl port-forward
deployment/briihass 8082:8082`). The page renders a form pre-filled
with the current values. Save submits a POST that:

1. Validates the new values (same rules as the loader).
2. Calls `Store.SaveAll(ctx, t)` — a single transaction that upserts
   the defaults row and rewrites the overrides table.
3. Signals the presence engine to recompute Resolved values for
   every affected beacon on the next tick.

No restart is required. If validation fails, the page redisplays the
form with the error message; Postgres is untouched.

See [docs/runbooks/admin-ui.md](admin-ui.md) for the auth setup
(credential source, port-forward, hardening notes).

## Tunable reference (defaults from ADR-0006)

| Knob | Default | What it does |
|---|---|---|
| `alpha` | `0.4` | EWMA smoothing weight in (0, 1]. New sample 40%, history 60%. |
| `grace_period_s` | `5` | Seconds after a sighting during which the EWMA holds without decay. |
| `decay_rate_db_per_s` | `2.0` | After grace, effective RSSI fades at this rate. |
| `presence_floor_dbm` | `-95` | AP loses presence when its effective RSSI drops below this. |
| `t_away_max_s` | `30` | Hard upper bound: no sighting from any AP for this long ⇒ force `not_home` (outside the sticky window). |
| `sticky_after_arrival_s` | `120` | Minimum guaranteed dwell after `not_home → zone`. Bridge will not publish `not_home` during this window. Set to `0` to disable. |
| `hysteresis_db` | `4.0` | Effective-RSSI gap required to switch zones in steady state. Not applied to `not_home → zone`. |
| `confirm_count` | `2` | Consecutive sightings that must agree for a steady-state zone switch. Not applied to `not_home → zone`. |

## When to tune what

| Symptom | Probable knob |
|---|---|
| Beacon shows `not_home` briefly mid-arrival (flap) | Raise `sticky_after_arrival_s`. |
| Departure declared too quickly | Raise `t_away_max_s` and/or `decay_rate_db_per_s`. |
| Zone label flaps between adjacent APs mid-house | Raise `hysteresis_db` (per-beacon override if only one beacon flaps). |
| RSSI graph too jittery in `/admin/status` | Lower `alpha` (e.g., to 0.25). |
| RSSI graph too laggy / slow to update | Raise `alpha` (e.g., to 0.6). |
| Edge-of-range beacons establish presence and never fade | Raise `presence_floor_dbm` toward zero (e.g., -90). |
| Arrival latency too high | This is **never** a tunable problem in the bridge — arrival is always immediate per ADR-0006's invariant. If you see lag, check vRIoT scan cadence and HA automation conditions (`for: 0` on the trigger?). |

## Watch in Grafana while tuning

The bridge exposes these gauges (port 8081 `/metrics`):

- `briihass_per_ap_effective_rssi{beacon, ap}` — the decayed value
  driving closest-AP selection. Graph this when tuning `alpha`,
  `decay_rate_db_per_s`, `grace_period_s`, `presence_floor_dbm`.
- `briihass_beacon_in_sticky_window{beacon}` — 0/1 indicator of when
  the sticky window is suppressing a departure.
- `briihass_arrival_latency_seconds` — histogram from sighting to
  MQTT publish. Should be < 200 ms p99; alert if not.

## Per-beacon overrides

When one beacon needs different values from the rest (e.g., a beacon with a
slower or noisier arrival pattern wants a longer sticky window), the admin UI's
form has a collapsible section per tracked beacon. Leave a field empty to
inherit the default; enter a value to pin it for that beacon. Saving
writes only the non-empty fields into `tunables_overrides` (nullable
columns).

Equivalent YAML representation (used only for the seed file):

```yaml
beacons:
  mobile_beacon:
    sticky_after_arrival_s: 240   # slower/noisier arrival pattern
    decay_rate_db_per_s: 1.5      # weaker beacon signal
```

Empty entries (`mobile_beacon: {}`) mean "use defaults" and produce no
override row.

## Inspecting state directly

`kubectl exec` is **banned** by the global governance rules (all `kubectl`
mutating verbs are blocked at the tool layer, and `exec` opens a shell
session that the policy treats as mutating regardless of the command
that follows). Use `kubectl port-forward` + a local `psql` client
instead:

```bash
# In one terminal:
kubectl -n briihass port-forward sts/briihass-postgres 5432:5432

# In another, with the credentials extracted via dev-creds.sh:
PGPASSWORD="$(./scripts/dev-creds.sh --print POSTGRES_PASSWORD)" \
  psql -h 127.0.0.1 -U briihass -d briihass -c \
  'SELECT alpha, grace_period_s, sticky_after_arrival_s FROM tunables_defaults'
```

The port-forward is a read-only TCP tunnel; the cluster's RBAC governs
which credentials it accepts.

## What if the Postgres tables are wiped?

Restart the bridge. `SeedFromYAMLIfEmpty` will re-seed from
`BRIIHASS_TUNABLES_SEED` (or `docs/examples/tunables.yaml` in dev),
so the bridge comes back with defaults. Per-beacon overrides made
through the admin UI will need to be re-entered.
