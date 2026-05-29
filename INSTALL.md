# Installing & operating briihass

**briihass** — **Bri**dge for **R**uckus **I**oT **i**Beacons in Home
Assistant. It receives BLE advertisement events HTTP-POSTed by a Ruckus
vRIoT controller's **BLE Scan** plugin and republishes presence + telemetry to
Home Assistant over MQTT, using HA's device-based MQTT Discovery so entities
auto-create. Despite the name, identity is polymorphic (iBeacon / Eddystone /
named / mfg), not iBeacon-only.

This guide is vendor-neutral. Deployment, CI, and secret-management specifics
for a particular environment belong in a gitignored `CLAUDE.local.md` (copy
`CLAUDE.local.md.example`).

## 1. Prerequisites

- **Go 1.25+** to build from source (or Docker to build the image).
- **A Ruckus vRIoT controller** (validated against 3.1.0.0) running the **BLE
  Scan** plugin, configured to POST to this bridge. See the `ruckus-apis` skill
  in `.claude/skills/` for the verified payload schema and plugin setup.
- **An MQTT broker** (Mosquitto) that Home Assistant also uses.
- **PostgreSQL** — the bridge stores tunables, the beacon allowlist, AP→zone
  mappings, observations, and warm-boot presence state here.
- **Home Assistant** with the MQTT integration enabled.

## 2. Build

```bash
# Static binary into ./bin/
make build            # or: go build -o bin/briihass ./cmd/briihass

# Production container image (distroless, nonroot, CGO-disabled)
make docker-build     # or: docker build -t briihass:dev .
```

The image is a multi-stage `golang:1.25` → `distroless/static` build producing a
single static binary (see `Dockerfile`).

## 3. Configuration

briihass is configured entirely through environment variables. **No secret is
ever read from a committed file** — see
[ADR-0003](docs/adr/0003-credential-handling.md).

### Required

| Variable | Purpose |
|---|---|
| `INGEST_SHARED_SECRET` | Value the bridge expects in the `Api-Key` header on every `/ingest` and `/heartbeat` POST. Generate with `openssl rand -base64 48`. |
| `MQTT_USER` / `MQTT_PASS` | Credentials for publishing to the broker. |
| `BRIIHASS_POSTGRES_DSN` | Postgres connection string, e.g. `postgres://user:pass@host:5432/briihass?sslmode=require`. |

### Optional (defaults shown)

| Variable | Default | Purpose |
|---|---|---|
| `MQTT_BROKER_URL` | `tcp://localhost:1883` | Broker URL (`scheme://host:port`). Set this for any non-local broker. |
| `MQTT_CLIENT_ID` | `briihass` | MQTT client ID. |
| `INGEST_ALLOWED_CIDRS` | _(empty = allow all)_ | Comma-separated CIDR allowlist for ingest source IPs (belt-and-suspenders alongside `Api-Key`). |
| `ADMIN_USER` / `ADMIN_PASS` | _(unset = admin UI disabled)_ | HTTP Basic-Auth for the admin UI. If either is unset the admin listener does not start. |
| `BRIIHASS_TUNABLES_SEED` | `/etc/briihass/tunables-seed.yaml` | Cold-start seed (see §5). |
| `BRIIHASS_INGEST_ADDR` | `:8080` | Ingest / heartbeat / health listener. |
| `BRIIHASS_METRICS_ADDR` | `:8081` | Prometheus `/metrics` + MQTT-aware `/ready` (keep internal-only). |
| `BRIIHASS_ADMIN_ADDR` | `:8082` | Admin UI listener (keep internal-only). |

For local dev, `scripts/dev-creds.sh` manages a mode-600
`~/.config/briihass/credentials.env`; `eval "$(scripts/dev-creds.sh --print)"`
sources it into your shell before `make run`.

## 4. Database

The schema (`internal/store/schema.sql`) is **embedded in the binary and applied
idempotently on every boot** — no manual migration step. Tables include
`tunables_defaults` / `tunables_overrides`, `beacons` (the tracked allowlist),
`zones` (AP MAC → zone label), `observations`, `settings`, and `presence_state`
(warm-boot rehydration). The `beacons` and `zones` tables are **not** seeded —
you populate them at runtime through the admin UI (§7), promoting from observed
reality.

Point `BRIIHASS_POSTGRES_DSN` at a database the bridge can create tables in.

## 5. Cold-start seed (tunables)

On first boot, if `tunables_defaults` is empty, the bridge reads the YAML at
`BRIIHASS_TUNABLES_SEED` and writes it as the initial tunables. After that,
Postgres is the source of truth and the seed is ignored. Copy
[`docs/examples/tunables.yaml`](docs/examples/tunables.yaml), adjust the values
(the defaults are the [ADR-0006](docs/adr/0006-presence-model-closest-ap-wins.md)
recommendations), and mount/point `BRIIHASS_TUNABLES_SEED` at it. Tunables are
editable live afterward via the admin UI with no restart
(see [docs/runbooks/tuning.md](docs/runbooks/tuning.md)).

## 6. Run

```bash
docker run --rm \
  -e INGEST_SHARED_SECRET=... \
  -e MQTT_USER=briihass -e MQTT_PASS=... \
  -e MQTT_BROKER_URL=tcp://mqtt.example.internal:1883 \
  -e BRIIHASS_POSTGRES_DSN='postgres://briihass:...@db.example.internal:5432/briihass?sslmode=require' \
  -e ADMIN_USER=admin -e ADMIN_PASS="$(openssl rand -base64 24)" \
  -v /path/to/tunables-seed.yaml:/etc/briihass/tunables-seed.yaml:ro \
  -p 8080:8080 \
  briihass:dev
```

Ports:

- **8080** — `/ingest`, `/heartbeat`, and `/health`. Expose this (behind your
  ingress / reverse proxy with TLS) so the vRIoT controller can reach it.
- **8081** — Prometheus `/metrics` and `/ready`. Keep internal-only.
- **8082** — admin UI. Keep internal-only; gate with an edge SSO layer if
  exposed.

**Health vs ready (important):** `/health` (port 8080) is liveness-grade — it
returns 200 once the listener is up, and is what should back your load balancer
/ readiness probe. Ingest persists to Postgres independently of MQTT, so do
**not** gate ingest readiness on MQTT. `/ready` (port 8081) is MQTT-aware and is
for metrics/alerting only — it must not back the serving endpoint. Warm boot is
structural: presence state is rehydrated from Postgres before the ingest
listener binds, so a 200 from `/health` already implies "presence restored."

## 7. Point vRIoT at the bridge

In the vRIoT controller UI, configure the **BLE Scan** plugin to POST to
`https://<your-bridge-host>/ingest` (and `/heartbeat`) with the `Api-Key`
header set to your `INGEST_SHARED_SECRET`. Bodies are gzipped JSON; the bridge
gunzips on receipt. To capture and inspect a real payload first, see
[docs/runbooks/capture.md](docs/runbooks/capture.md) and `cmd/captured`.

## 8. Home Assistant

With the MQTT integration connected to the same broker, entities **auto-create**
via device-based MQTT Discovery — no custom integration. Each tracked beacon
becomes an HA device with a `device_tracker` (`home`/`not_home`), an area sensor
(the AP-derived zone label), and RSSI / battery / temperature sensors as
telemetry arrives. The bridge re-asserts discovery on `homeassistant/status`
`online`, and the admin UI has a "Resync HA" button. Bridge availability is
published via MQTT LWT on `briihass/bridge/availability`.

For HA automations, the arrival edge (`not_home → <zone>`) publishes
immediately; let HA own any "wait before acting" hysteresis via `for:`
qualifiers. See [ADR-0002](docs/adr/0002-mqtt-discovery-as-ha-contract.md) and
[ADR-0006](docs/adr/0006-presence-model-closest-ap-wins.md).

## 9. Operate

- **Admin UI** (`/admin`) — view per-beacon state, promote/demote tracked
  beacons, map APs to zones, edit tunables live, adjust capture/retention
  settings. See [docs/runbooks/admin-ui.md](docs/runbooks/admin-ui.md).
- **Tuning** the presence model (EWMA, decay, sticky-arrival window) —
  [docs/runbooks/tuning.md](docs/runbooks/tuning.md).
- **Capturing** real plugin payloads for debugging —
  [docs/runbooks/capture.md](docs/runbooks/capture.md).
- **Metrics** — scrape `/metrics` on port 8081 with Prometheus.
- **Secrets rotation** — rotate in your secret manager (or
  `~/.config/briihass/credentials.env` in dev) and restart the process.

Production deployment topology, CI, and secret wiring are environment-specific —
keep them in a gitignored `CLAUDE.local.md`, not in this repo.
