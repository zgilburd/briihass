# briihass

**briihass** — **Bri**dge for **R**uckus **I**oT **i**Beacons in Home
Assistant.

A small Go bridge that receives HTTP-POSTed BLE advertisement events from a
Ruckus vRIoT 3.1.0.0 controller (BLE Scan plugin) and republishes presence +
telemetry to Home Assistant via a Mosquitto MQTT broker, using HA's device-based
MQTT Discovery so entities auto-create. Despite the name, identity is
packet-derived and **polymorphic** — iBeacon, Eddystone, named, and mfg beacons,
not iBeacon-only (see
[ADR-0008](docs/adr/0008-generalized-packet-derived-identity.md)). The vRIoT
`/ingest` payload already carries AP metadata, so the bridge does not call
vSmartZone (see [ADR-0005](docs/adr/0005-vsz-enrichment.md)).

Primary use case: Home Assistant automation driven by tracked BLE beacons moving
between AP-detected areas and leaving AP coverage entirely (`not_home`). Arrival
latency is the load-bearing metric. See
[ADR-0006](docs/adr/0006-presence-model-closest-ap-wins.md) for the presence
model (per-AP RSSI with EWMA + linear decay, sticky-arrival window, HA owns
"wait before acting" hysteresis).

## A small bit of security advice

Do not use BLE broadcast information alone to drive security-sensitive
automation outcomes. BLE advertisements are intentionally broadcast, so briihass
presence should be one signal in an `and` chain: for example, require the
relevant Home Assistant mobile-app user or device tracker, with location
tracking enabled, to also be inside the expected zone before unlocking a door.
An iBeacon can help choose which garage door to open, but it must not be the
sole determining factor. Require additional factors, or better yet have the
automation challenge the user with Home Assistant Companion
[actionable notifications](https://companion.home-assistant.io/docs/notifications/actionable-notifications/)
before taking the action.

- **Install / configure / run / operate:** see [INSTALL.md](INSTALL.md).
- **What it does and why (architecture):** see [ARCHITECTURE.md](ARCHITECTURE.md).
- **Admin UI:** see [docs/runbooks/admin-ui.md](docs/runbooks/admin-ui.md).
- **Tuning the presence model:** see [docs/runbooks/tuning.md](docs/runbooks/tuning.md).
- **Capturing real plugin payloads:** see
  [docs/runbooks/capture.md](docs/runbooks/capture.md) and the `cmd/captured`
  diagnostic server.
- **Design decisions:** see [`docs/adr/`](docs/adr/) (ADRs 0001–0008).

## Configuration & secrets

briihass is a single static binary configured entirely through environment
variables (`INGEST_SHARED_SECRET`, `MQTT_USER`/`MQTT_PASS`, `MQTT_BROKER_URL`,
`BRIIHASS_POSTGRES_DSN`, …). See [INSTALL.md](INSTALL.md) for the full contract.
**No secrets are committed** — supply them from your platform's secret manager
in production, or a local `~/.config/briihass/credentials.env` for dev (see
[ADR-0003](docs/adr/0003-credential-handling.md)). Deployment and
secret-management specifics are environment-specific and belong in a gitignored
`CLAUDE.local.md` (copy `CLAUDE.local.md.example` to start).

The `vendor-docs/` directory (support-gated Ruckus PDFs/HTML) and capture output
(`captures/`, `blecaps/`) are gitignored and must stay that way.

## License

MIT — see [LICENSE](LICENSE).
