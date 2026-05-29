# Architecture — briihass

## One-paragraph summary

The Ruckus vRIoT controller scans for BLE beacons via its **BLE Scan**
plugin and HTTP-POSTs (gzipped JSON) two kinds of message to briihass:
`/ingest` carries per-gateway beacon-sighting events (the full BLE
advertisement for every scanned device) with AP metadata already
attached; `/heartbeat` carries the live gateway-MAC list. briihass
authenticates each request via the `Api-Key` header, inflates and parses
the body, decodes **every** BLE advertisement and derives a polymorphic
packet-derived identity (`BeaconKey{kind,key}` — iBeacon, Eddystone,
named, mfg; see ADR-0008), and runs a per-beacon closest-AP-wins state
machine to compute a zone label. Results publish to the in-cluster
Mosquitto MQTT broker using Home Assistant's **device-based** MQTT
Discovery convention, so HA auto-creates one device per beacon — a
`device_tracker` whose state is the zone label (`zone_a`, `zone_b`, …)
plus RSSI / battery-voltage / temperature `sensor`s. Tracked beacons live
in the Postgres `beacons` allowlist (operator-managed via `/admin/devices`)
so guest-phone beacons don't pollute HA's entity list.

## Component diagram

```
+--------------------+   HTTPS POST     +-------------------------------+
| Ruckus vRIoT VM    | --------------->  briihass
| (BLE Scan plugin)  |   /ingest or     +-------------------------------+
| on Proxmox         |   /heartbeat     |  ingest (HTTP handler)        |
| Api-Key header     |   gzip JSON      |    - api-key constant-time    |
+--------------------+                  |    - IP allowlist (vRIoT IP)  |
                                        |    - rate-limit per src IP    |
                                        |    - gunzip + JSON parse      |
                                        +---------------+---------------+
                                                        |
                                                +-------v-------+
                                                | parser        |
                                                | - walk BLE TLV|
                                                | - decode ALL  |
                                                |   advert types|
                                                | - Identify -> |
                                                |   BeaconKey   |
                                                +-------+-------+
                                                        |
                                                +-------v---------+
                                                | filter          |
                                                | - drop beacons  |
                                                |   not in        |
                                                |   allowlist (DB) |
                                                +-------+---------+
                                                        |
                                                +-------v-----------+
                                                | presence state    |
                                                | machine           |
                                                | - sliding window  |
                                                | - closest-AP-wins |
                                                | - hysteresis      |
                                                +-------+-----------+
                                                        |
                                                +-------v-----------+
                                                | zone resolver     |
                                                | - AP MAC -> zone  |
                                                |   label from      |
                                                |   zones (DB)      |
                                                +-------+-----------+
                                                        |
                                                +-------v-----------+        +-------------+
                                                | mqtt publisher    | -----> | Mosquitto   |
                                                | - HA Discovery    |  pub   | (home ns)   |
                                                | - bounded buffer  |        | 192.0.2.203|
                                                +-------------------+        +------+------+
                                                                                    |
                                                                                    v
                                                                            +-------+-------+
                                                                            | Home Assistant |
                                                                            | (home ns)      |
                                                                            | auto-creates   |
                                                                            | device_tracker |
                                                                            +----------------+
```

## Data flow

1. **Receive.** vRIoT POSTs gzipped JSON to `https://${BRIDGE_HOSTNAME}/ingest`
   (or `/heartbeat`). Handler verifies `Api-Key` via constant-time
   compare against `INGEST_SHARED_SECRET`. Optional IP allowlist limits
   accepted source IPs to the configured vRIoT VM. Auth failures → 401,
   logged with source IP. Bodies over 1 MiB are rejected.
2. **Inflate + parse.** Handler gunzips the body and parses JSON.
   Malformed → 400. The /ingest payload carries `gateway_euid`,
   `ap_name`, `ap_location`, `ap_model`, `ap_ip_address`, `ap_serial`,
   `ap_firmware`, `latitude/longitude/altitude`, `version`, `timestamp`,
   and an `events[]` array. AP metadata is already enriched — no
   vSmartZone call needed.
3. **Parse advertisements.** Each `events[].data` is a hex-encoded BLE
   advertisement TLV. The parser walks the **whole** TLV and decodes
   every AD structure (iBeacon, Eddystone UID/URL/TLM, local name, mfg,
   service data), then `Identify` derives a `BeaconKey{kind,key}` by
   precedence (ADR-0008). Anonymous/ephemeral adverts (rotating Apple
   Continuity, Eddystone-TLM/URL — ~85% of events) are counted, not
   stored. Eddystone-TLM battery/temperature is correlated to a stable
   identity by `device_euid` **within a single POST** (euid is never a
   durable key).
4. **Filter.** Only beacons in the Postgres `beacons` allowlist
   (operator-managed via `/admin/devices`) are admitted to the presence
   pipeline. Unknown beacons drop and increment
   `briihass_unknown_beacon_total{kind,key,ap}`; all observed beacons land
   in `observations` so the operator can discover and promote them.
5. **Presence.** For each `(beacon, AP)` pair the bridge maintains
   an EWMA of observed RSSI plus an age-decay function (linear, 2 dB/s
   after a 5 s grace period). An AP "sees" a beacon iff the decayed
   effective RSSI stays above `presence_floor_dbm` (default -95 dBm).
   The closest AP (highest effective RSSI among APs with active
   presence) maps to the published zone label.
   **Arrival from `not_home` publishes immediately** — the first
   qualifying sighting pushes the state machine into the zone, no
   bridge-side gating. **After arrival, a sticky-arrival window
   suppresses the `not_home` transition for `sticky_after_arrival_s`
   (default 120 s)**, so a beacon's faint-then-fluctuating arrival
   signal pattern produces one arrival event, not a flap. Zone
   updates still happen during the sticky window. Steady-state
   zone-to-zone transitions apply mild hysteresis (4 dB margin,
   2-sighting confirm). Departure (outside the sticky window) happens
   naturally as effective RSSI fades; a hard `t_away_max_s = 30`
   safety bound also forces `not_home` if no AP has reported the
   beacon in 30 s. HA owns all "wait before acting" debouncing via
   standard automation qualifiers. See
   [ADR-0006](docs/adr/0006-presence-model-closest-ap-wins.md).
6. **Zone resolve.** The Postgres `zones` table maps AP MAC →
   human-readable zone label (`zone_a`, `zone_b`, …). The closest AP's
   label becomes the published state.
7. **Publish (device-based discovery).** A retained
   `homeassistant/device/briihass_<id>/config` declares the device +
   components on first sight. Each transition publishes the **bare** zone
   string (or `not_home`) to `briihass/<id>/state` (the device_tracker —
   HA ignores `value_template` there) and a JSON document to
   `briihass/<id>/telemetry` (`{state, rssi, voltage, temperature,
   ap_mac, ap_name, last_seen, …}`) that the RSSI/voltage/temperature
   sensors read and that backs the tracker's attributes. A ~20 s pump
   refreshes telemetry continuously. See
   [ADR-0002](docs/adr/0002-mqtt-discovery-as-ha-contract.md) Phase 4 addendum.
8. **Heartbeat passthrough.** `/heartbeat` POSTs (the `{vendor, Online,
   Offline}` payload) update an internal gauge and publish a single
   non-retained `briihass/health` message every 30 s with the gateway
   online/offline list. The bridge does **not** republish per-gateway
   heartbeats to HA — too noisy.

## MQTT topic schema

| Topic | Retained | Payload |
|---|---|---|
| `homeassistant/device/briihass_<id>/config` | yes | Device-based HA Discovery JSON (`dev`+`o`+`cmps`: device_tracker + RSSI/voltage/temperature sensors) |
| `briihass/<id>/state` | yes | **bare** zone label (e.g. `zone_a`) or `not_home` — the device_tracker state |
| `briihass/<id>/telemetry` | yes | JSON: `{state, rssi, voltage, temperature, ap_mac, ap_name, last_seen, …}` — sensors + tracker attributes |
| `briihass/bridge/availability` | yes | `online`/`offline` (LWT-backed); referenced by every device's `availability` |
| `briihass/health` | no | JSON heartbeat (gateways online/offline, queue depths, error counters) |

`<id>` = `briihass_<kind>_<key>` (e.g. iBeacon → `briihass_ibeacon_<uuid_no_dashes>_<major>_<minor>`). See [ADR-0008](docs/adr/0008-generalized-packet-derived-identity.md).

## Inbound networking

- Front the bridge with your ingress / reverse proxy on hostname
  `${BRIDGE_HOSTNAME}` (placeholder), terminating TLS there.
- The vRIoT VM (`192.0.2.30`, placeholder) is the only allowed source IP
  (allowlist enforced in the handler in addition to `Api-Key`).
- Internal-only; no public DNS exposure.

## Failure modes

| Failure | Behavior |
|---|---|
| `Api-Key` missing/wrong | 401, logged with source IP, no body returned. |
| Source IP not on allowlist | 401, logged. |
| Body > 1 MiB | 413, logged. |
| gunzip failure | 400, logged. |
| Malformed JSON | 400, logged. |
| Advert with no stable identity (anonymous/ephemeral) | counted (`briihass_anonymous_adverts_total`), not stored. |
| Beacon (kind,key) not in the allowlist | dropped from engine path, counter `briihass_unknown_beacon_total{kind,key,ap}`; still recorded in `observations`. |
| Mosquitto unreachable | Buffer in memory (bounded, ~5 min worth), then drop oldest. Log loss. |
| Sustained POST flood | Per-source-IP rate limit (configurable). |
| Postgres unreachable at startup | Bridge retries connect (10 attempts) then exits non-zero for K8s restart. |

## What this bridge intentionally does **not** do

- Does not poll vRIoT for beacons (push-only).
- Does not subscribe to any MQTT broker for ingest.
- Does not write a custom Home Assistant integration.
- Does not query vSmartZone (the /ingest payload already carries AP
  metadata — see [ADR-0005](docs/adr/0005-vsz-enrichment.md)).
- Does not persist presence state across restarts. Re-converges from
  the next sighting; retained MQTT messages give HA the last-known
  state in the meantime.
- Does not compute distance from RSSI. RSSI ranking, not absolute
  distance.
- Does not auto-create HA entities for unknown beacons. Tracked beacons must be promoted via /admin/devices (observed beacons are surfaced for discovery).

## Open verification items

See "Open verification items" in [CLAUDE.md](CLAUDE.md). The biggest
remaining unknown is the vRIoT plugin's behavior on bridge 5xx — to be
characterized during Phase 2 with deliberate fault injection.
