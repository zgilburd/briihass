# CLAUDE.md — briihass

Project-scoped guidance for Claude Code and other coding agents.
Read [ARCHITECTURE.md](ARCHITECTURE.md), [INSTALL.md](INSTALL.md), and the ADRs
under `docs/adr/` before making non-trivial changes. Environment-specific
deployment, CI, and secret-management guidance lives in `CLAUDE.local.md`
(gitignored — copy `CLAUDE.local.md.example` to start).

## What this repo is

A Go bridge daemon. **Inbound:** gzipped JSON HTTP POSTs from a Ruckus
vRIoT 3.1.0.0 **BLE Scan** plugin to two endpoints (`/ingest` and
`/heartbeat`). The plugin forwards the **full** BLE advertisement for
every scanned device (iBeacon, Eddystone, named, Apple Continuity, custom
mfg); identity is **packet-derived and polymorphic** — `BeaconKey{kind,key}`,
not iBeacon-only — and never keyed on the (rotating) `device_euid`. See
[ADR-0008](docs/adr/0008-generalized-packet-derived-identity.md). **Outbound:**
MQTT to a Mosquitto broker (configured via `MQTT_BROKER_URL`) using Home
Assistant's device-based MQTT Discovery convention. The `/ingest` payload already
carries AP metadata (ap_name, ap_location, ap_model, lat/lng, …) so the
bridge does NOT call vSmartZone (see [ADR-0005](docs/adr/0005-vsz-enrichment.md)).

**Primary use case:** Home Assistant automation driven by tracked BLE beacons
moving between AP-detected areas and leaving AP coverage entirely (`not_home`).
Arrival latency is the load-bearing metric; the asymmetric debounce model in
ADR-0006 prioritizes fast `not_home → home` publication while HA owns any
"wait before acting" hysteresis.

## Hard rules

### Deployment
- briihass builds to a single static binary / distroless image (see
  `Dockerfile`) and is configured entirely through environment variables —
  see [INSTALL.md](INSTALL.md) for the full contract.
- **Deployment, CI, and secret-management specifics are environment-specific**
  and live in `CLAUDE.local.md` (gitignored). Keep them out of the committed
  tree.

### Ingest is HTTP POST, not polling, not MQTT-subscribe
- The vRIoT **BLE Scan** plugin POSTs (gzipped JSON) to two endpoints on
  the bridge: `/ingest` (beacon events, ~1–2/sec) and `/heartbeat` (gateway
  online/offline list, ~every 75 s, `vendor:"blescan"`). See
  [ADR-0001](docs/adr/0001-http-post-ingest.md) for verified schemas.
- The polling endpoint `/app/v1/beacon` exists on the controller but is a
  diagnostic-only escape hatch. Do not build a polling loop.
- Inbound auth: **`Api-Key: <value>` header**, constant-time compared
  against `INGEST_SHARED_SECRET`. Source IP allowlisted to the vRIoT VM
  (`192.0.2.30`) as belt-and-suspenders.
- Body is always `Content-Encoding: gzip`; bridge must gunzip.
- AP metadata (`ap_name`, `ap_location`, `ap_model`, lat/lng, etc.) is
  carried in every /ingest payload — **no vSmartZone calls needed for v1**
  (see [ADR-0005](docs/adr/0005-vsz-enrichment.md)).
- `events[].data` is a BLE advertisement **TLV**. The parser
  (`internal/parser`) walks the whole TLV and decodes **every** AD
  structure — iBeacon, Eddystone (UID/URL/TLM), local name, mfg, service
  data — then `Identify` derives a `BeaconKey{kind,key}` by precedence
  (`ibeacon > eddystone_uid > eddystone_url > mfg(allowlisted) > name >
  anonymous`). Anonymous/ephemeral adverts (~85%: rotating Apple
  Continuity, Eddystone-TLM/URL) are counted, not stored. **euid is a
  within-POST telemetry-correlation hint only — never an identity key.**
- **Identity is polymorphic.** Don't reintroduce iBeacon-only assumptions:
  everything keys on `ids.BeaconKey{kind,key}` (ADR-0008), not
  `uuid/major/minor`.

### MQTT publish contract
- HA **device-based** MQTT Discovery is the only contract with Home
  Assistant. We do not build a custom HA integration. See
  [ADR-0002](docs/adr/0002-mqtt-discovery-as-ha-contract.md).
- **One config per device:** `homeassistant/device/<entity-id>/config`
  (retained) declares the device + components (a `device_tracker` plus
  `sensor`s for RSSI / battery voltage / temperature, grouped under one HA
  device). Voltage/temperature components are added lazily on first
  telemetry. `source_type: bluetooth_le`.
- **entity-id derivation:** `briihass_<kind>_<key>` via
  `ids.BeaconKey.EntityID()` — e.g. iBeacon →
  `briihass_ibeacon_<uuid-no-dashes>_<major>_<minor>`. See
  `internal/mqtt/discovery.go`. Stable across restarts.
- **device_tracker state is a BARE `home`/`not_home` string; telemetry is
  separate JSON.** HA does NOT apply `value_template` to a device_tracker
  under device-based discovery (it renders the raw payload as a custom
  location), AND HA only treats a device_tracker as *home* when its state is
  exactly `home`. So `briihass/<entity-id>/state` carries the bare `home` /
  `not_home` string (NOT the area label), and `briihass/<entity-id>/telemetry`
  carries the JSON the sensors read via `value_template` (+ the tracker's
  `json_attributes_topic`). Do NOT put JSON, or an area label, on the
  tracker's state topic. See [ADR-0002](docs/adr/0002-mqtt-discovery-as-ha-contract.md)
  Phase 5 addendum.
- **Discovery self-heals:** the bridge subscribes to `homeassistant/status`
  and re-asserts all configs on `online`; a manual "Resync HA" admin
  button does the same. Bridge availability is published via MQTT LWT on
  `briihass/bridge/availability`.
- **Presence is split across two HA entities (Phase 5).** The
  `device_tracker` carries `home`/`not_home` only — any tracked-AP presence
  ⇒ `home`. The AP-derived area label (e.g. `zone_a`, `zone_b`) rides a
  separate, always-present **area sensor** (`<entity-id>_area`, reads
  `value_json.area` from the telemetry topic). The bridge runs a
  closest-AP-wins state machine; AP MAC → area label mapping lives in the
  `zones` Postgres table (Phase 3). Internally the engine still emits the
  label as `PresenceEvent.State`; only the MQTT publish mapping splits it.
  See [ADR-0006](docs/adr/0006-presence-model-closest-ap-wins.md) and
  [ADR-0002](docs/adr/0002-mqtt-discovery-as-ha-contract.md) Phase 5.
- **Primary use case is beacon-driven Home Assistant automation.** Arrival edge
  (`not_home → <zone>`) **must publish immediately** — no K-of-N
  confirms, no window averaging delay. HA owns "wait before acting"
  hysteresis via `for:` qualifiers.
- **Per-AP presence uses EWMA + linear decay, not last-seen-timestamp.**
  Each (beacon, AP) pair carries a smoothed RSSI that linearly decays
  after a short grace period; AP presence drops only when the decayed
  value falls below a floor. Debounces missed scans and edge-of-range
  noise so the published zone doesn't flap when the beacon hasn't moved.
- **Asymmetric debounce: sticky-arrival window.** After `not_home →
  <zone>`, the bridge will NOT publish `not_home` for at least
  `sticky_after_arrival_s` (default 120 s, per-beacon override). Zone
  updates still happen during this window. Rationale: a beacon entering
  AP coverage can produce one faint first sighting followed by signal
  gaps as it moves through multi-path; flipping back to `not_home`
  would delay downstream HA automations. The user explicitly called this
  out: "prevent flipping to not_home." Defaults in ADR-0006.
- Tracked beacons live in the `beacons` Postgres table (Phase 3 — see
  [ADR-0007](docs/adr/0007-postgres-resident-persistence.md)). They
  are mutated only via `/admin/devices` promote/demote, which signals
  the presence engine via `ApplyTopology`. Untracked beacons land in
  `observations` as `tracked=false` for the retention window (default
  7d) so they can be promoted from observed reality; they still drop
  on the unknown-beacon path before reaching the engine. Do NOT
  auto-create HA entities for unknown beacons — promotion is an
  explicit operator action.
- AP → zone mappings live in the `zones` Postgres table. Mutated via
  `/admin/zones`.
- Demote is automated end-to-end: deleting a beacon row publishes an
  empty retained payload to its HA Discovery config topic (idiomatic
  removal per the HA MQTT docs).
- There is no committed config file. A `tunables-seed.yaml` is used on cold
  start only (when the tunables tables are empty); see
  [`docs/examples/tunables.yaml`](docs/examples/tunables.yaml) and
  `BRIIHASS_TUNABLES_SEED` in [INSTALL.md](INSTALL.md).

### Secrets
- Supply all credentials as environment variables; **never commit a secret.**
  Prod: inject them from your platform's secret manager. Dev: a plaintext
  `~/.config/briihass/credentials.env` (mode 600) managed by
  `scripts/dev-creds.sh`, **outside the repo** and never committed. See
  [ADR-0003](docs/adr/0003-credential-handling.md).
- Load-bearing credential sets: **`INGEST_SHARED_SECRET`** (value the bridge
  expects in the `Api-Key` header on every /ingest and /heartbeat POST),
  **Mosquitto** (publish creds), **`BRIIHASS_POSTGRES_DSN`**, and **vRIoT**
  (controller mgmt API, if used). **vSmartZone** creds remain in the schema for
  future use but are not consumed by v1 (see ADR-0005).

### Vendor documentation
- `vendor-docs/` is support-gated material from Ruckus. It is gitignored
  and must stay that way. Read locally only.

## Skill triggers (project-local in `.claude/skills/`)

- **ruckus-apis**: invoke when writing or modifying code that talks to
  vRIoT or vSmartZone. Has the BLE Scan plugin payload schema, the
  full-advert decode recipe (iBeacon + Eddystone UID/URL/TLM + name + mfg),
  the Eddystone-TLM telemetry decode (incl. the `0x8000` temp sentinel),
  auth flows, and the euid-is-not-identity rule.

## Conventions

- **Always PR.** Every change goes through a pull request — never
  push directly to `main`, never merge a feature branch without an
  open PR (even single-reviewer changes). The PR title and body
  document intent; CI runs on the PR and gates merge.
- **Branch:** `feature/<kebab-description>`. PR titles: conventional commits.
- **Go:** module path `briihass` (placeholder; confirm
  on first `go mod init`). Multi-stage build: `golang:1.23` → distroless
  final. Single static binary, no CGO.
- **Layout (Phase 2 plan):**
  - `cmd/briihass/` — main binary
  - `cmd/captured/` — diagnostic capture server (**already exists**;
    used to grab real vRIoT POSTs for testdata + schema verification)
  - `internal/ingest/` — HTTP handler, Api-Key auth, gunzip, JSON parse
  - `internal/parser/` — full BLE advert decoder (`Parse`) + polymorphic
    identity classifier (`Identify`); decodes iBeacon, Eddystone
    UID/URL/TLM, local name, mfg (per ADR-0008)
  - `internal/ids/` — `BeaconKey{kind,key}` polymorphic identity + APMAC +
    ZoneLabel (the `BeaconID` type was removed in Phase 4)
  - `internal/presence/` — per-AP EWMA+linear-decay model, sticky-
    arrival window, closest-AP-wins zone resolution (per ADR-0006)
  - `internal/mqtt/` — HA Discovery publisher
  - `internal/config/` — runtime topology types + tunables YAML
    seeder (the YAML topology parser was deleted in Phase 3)
  - `internal/store/` — Postgres-backed allowlist, zones, observations,
    raw_posts, settings, tunables (renamed from `internal/tunables/store/`
    in Phase 3)
  - **No `internal/vsz/` for v1** — vSmartZone is deferred per ADR-0005.
- **Tests:** Vitest-equivalent for Go is the stdlib `testing` package +
  table-driven tests. Hot-path tests must run under 30s. Ingest handler
  tests are fed a recorded corpus of real vRIoT POST bodies once available.

## Open verification items (kept in this file because they shape design)

- vRIoT plugin behavior on bridge 5xx — retry policy unknown. Test
  with deliberate fault injection during Phase 2.
- `Api-Key` max length / character set the plugin UI accepts (informs
  rotation policy).
- State-machine tunables (window N, min-sightings M, hysteresis H,
  confirm count K, away timeout T) — defaults in ADR-0006 are guesses;
  tune after observing real HA behavior.

## Gotchas

- **HA ignores `value_template` on a device_tracker under device-based
  discovery.** Publish the device_tracker's state as a BARE string; put
  JSON telemetry on a separate topic for the sensors. See ADR-0002
  Phase 4 addendum. Re-introducing JSON on the tracker state topic makes
  HA render the raw blob as a custom location.
- **HA only treats a device_tracker as `home` when its state is exactly
  `home`.** A custom string (area label like `zone_a`) reads as a
  *custom location = not home*, silently breaking every home/away
  automation. The tracker state MUST be
  `home`/`not_home`; the area label lives on the `<entity-id>_area`
  sensor (`value_json.area`). Don't regress the tracker back to publishing
  the area label. See ADR-0002 Phase 5 addendum.
- **`device_euid` is not an identity.** It rotates and is ambiguous
  (one euid carried two iBeacon identities in a 5-min corpus). Use it
  only as a within-POST telemetry-correlation hint.
- **Don't gate the ingest backend's readiness on MQTT.** The ingest
  `readinessProbe` hits `/health` (liveness-grade, always 200 once the
  listener is up). Ingest persists observations to Postgres independently
  of MQTT, so making MQTT a readiness gate would pull the pod from the load
  balancer during a broker outage and **drop BLE ingest** — the opposite of the
  goal. The metrics-port `/ready` (8081) is MQTT-aware for Prometheus/alert
  use only; it must not back the Service endpoint.
- **Warm boot is structural, not probe-driven.** `Engine.RestoreState`
  runs before the ingest listener binds, so `/health` returning 200
  already implies "presence state restored." Don't add a separate
  warm-up readiness gate that withholds traffic until warm — with
  `maxUnavailable:0` a NotReady pod gets zero ingest and could never warm
  (deadlock). Warmth comes from Postgres rehydration at boot, not live
  traffic.

## Changelog

- **2026-05-29 (Phase 5 — HA home/not_home fix + area sensor):** the
  `device_tracker` now publishes a bare `home`/`not_home` string (any
  tracked-AP presence ⇒ `home`) instead of the AP-derived area label,
  fixing home/away automations — HA only treats a device_tracker as home
  when its state is exactly `home`, so the prior label-as-state scheme broke
  every home/away automation. The area label moved to a new
  always-present **area sensor** (`<entity-id>_area`, `value_json.area`).
  The presence engine (EWMA/decay/sticky-arrival, immediate arrival) is
  unchanged; only the MQTT publish mapping splits the emitted label into
  (`home`/`not_home` tracker + area sensor). Telemetry JSON field `state`
  renamed `area`. See [ADR-0002](docs/adr/0002-mqtt-discovery-as-ha-contract.md)
  Phase 5 and [ADR-0006](docs/adr/0006-presence-model-closest-ap-wins.md)
  Phase 5 addenda.
- **2026-05-27 (zero-downtime deploys — warm boot):** presence engine
  now persists each tracked beacon's published zone to a `presence_state`
  Postgres table (periodic ~5 s flush + a final flush in the shutdown
  drain) and rehydrates it via `Engine.RestoreState` **before the ingest
  listener binds**, so a restart boots warm instead of cold. Fixes the
  mid-deploy away-flap where a fresh pod published `not_home` for present
  beacons before re-observing them. Restore treats the restart as a fresh
  arrival (resets the sticky window) so the zone holds until live scans
  rebuild per-AP presence; a genuinely-gone beacon still ages out. Pairs
  with a rolling deploy strategy (`maxUnavailable:0`/`maxSurge:1` + preStop
  drain) that closes the load-balancer no-backend gap. See the [ADR-0006](docs/adr/0006-presence-model-closest-ap-wins.md)
  2026-05-27 addendum.
- **2026-05-27 (Phase 4 — BLE Scan migration):** switched from the vRIoT
  iBeacon plugin to the BLE Scan plugin; replaced iBeacon-only `BeaconID`
  with polymorphic `BeaconKey{kind,key}` across the pipeline (ADR-0008);
  full-advert parser decoding iBeacon/Eddystone/named/mfg; two-pass ingest
  correlating Eddystone-TLM telemetry by euid within a POST; Postgres
  `beacons`/`observations` re-keyed to `(kind,key)` + battery/temperature
  columns; device-based HA discovery with bare-state device_tracker +
  RSSI/voltage/temperature sensors, bridge availability (LWT), and
  HA-birth/admin discovery resync (ADR-0002 Phase 4 addendum). **Resolves
  the prior "no battery for iBeacon" gap** — battery voltage now comes
  from Eddystone-TLM frames the tracked beacons also emit.
