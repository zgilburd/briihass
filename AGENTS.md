# AGENTS.md — briihass

Guidance for non-Claude coding agents (Codex, Aider, Cursor, etc.).
[CLAUDE.md](CLAUDE.md) is the authoritative project guide; this file is a short
orientation that defers to it. When the two disagree, CLAUDE.md wins.

## What this repo is

A Go bridge daemon. **Inbound:** gzipped JSON HTTP POSTs from a Ruckus vRIoT
3.1.0.0 **BLE Scan** plugin to two endpoints — `/ingest` (beacon events,
~1–2/sec) and `/heartbeat` (gateway online/offline list, ~every 75 s).
**Outbound:** MQTT to a Mosquitto broker (configured via `MQTT_BROKER_URL`)
using Home Assistant's device-based MQTT Discovery convention. The `/ingest`
payload already carries AP metadata, so the bridge does **not** call vSmartZone
in v1.

Primary use case is Home Assistant automation driven by tracked BLE beacons
moving between AP-detected areas and leaving AP coverage entirely (`not_home`).
Arrival latency is the load-bearing metric.

## Read these first

- [CLAUDE.md](CLAUDE.md) — hard rules, MQTT contract, gotchas, conventions.
- [ARCHITECTURE.md](ARCHITECTURE.md) — end-to-end data flow.
- [INSTALL.md](INSTALL.md) — build, configure (env vars), run, operate.
- [`docs/adr/`](docs/adr/) — the 0001–0008 decision records.
- The `ruckus-apis` skill in `.claude/skills/` — verified vRIoT/BLE-Scan payload
  schemas and the full-advert decode recipe.

## Load-bearing invariants (do not regress)

1. **Ingest is HTTP POST** (vRIoT pushes; we receive). No polling, no
   MQTT-subscribe ingest. Inbound auth is the `Api-Key` header, constant-time
   compared to `INGEST_SHARED_SECRET`; body is always gzip.
2. **Identity is packet-derived and polymorphic** — `ids.BeaconKey{kind,key}`
   (iBeacon / Eddystone / named / mfg), never the rotating `device_euid`
   (see [ADR-0008](docs/adr/0008-generalized-packet-derived-identity.md)).
3. **The HA `device_tracker` state is a BARE `home`/`not_home` string.** Any
   tracked-AP presence ⇒ `home`. The AP-derived area label rides a separate
   `<entity-id>_area` sensor — never on the tracker's state topic. HA only
   treats a tracker as home when its state is exactly `home`
   (see [ADR-0002](docs/adr/0002-mqtt-discovery-as-ha-contract.md)).
4. **The arrival edge (`not_home → <zone>`) publishes immediately**; the bridge
   suppresses `not_home` for a sticky window after arrival. Per-AP presence uses
   EWMA + linear decay, not a last-seen timestamp
   (see [ADR-0006](docs/adr/0006-presence-model-closest-ap-wins.md)).
5. **Don't auto-create HA entities for unknown beacons** — promotion to the
   `beacons` table is an explicit operator action via the admin UI.
6. **Never commit secrets** — credentials are env vars only
   (see [ADR-0003](docs/adr/0003-credential-handling.md)).

## Conventions

- **Always PR.** Every change goes through a pull request; CI gates merge.
- Branches: `feature/<kebab>`. Commits: conventional (`feat:`, `fix:`, `docs:`…).
- Go, single static binary, distroless image, no CGO
  (see [ADR-0004](docs/adr/0004-go-as-implementation-language.md)).
- `vendor-docs/` (support-gated Ruckus material) and capture output
  (`captures/`, `blecaps/`) are gitignored and must stay that way — captures
  contain real MACs / beacon UUIDs / coordinates.
- Deployment, CI, and secret-management specifics are environment-specific and
  live in a gitignored `CLAUDE.local.md` (see `CLAUDE.local.md.example`).
