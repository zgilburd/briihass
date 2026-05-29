# ADR-0002: MQTT Discovery is the bridge↔Home Assistant contract

- **Status:** Accepted
- **Date:** 2026-05-18 (revised after live capture)

## Context

briihass needs to surface beacon presence into Home Assistant. Options:

1. **Custom HA integration** — Python module in HA's `custom_components/`.
2. **MQTT Discovery** — bridge publishes to well-known topics on the
   already-running Mosquitto broker; HA's MQTT integration auto-creates
   entities.
3. **HA REST API** — bridge POSTs to HA's `/api/states/...`.

## Decision

Use **MQTT Discovery**. Bridge publishes:

- `homeassistant/device_tracker/briihass_<beacon-id>/config` — retained,
  HA Discovery JSON describing the entity.
- `homeassistant/device_tracker/briihass_<beacon-id>/state` — retained,
  the AP-derived zone label (e.g. `zone_a`); `not_home` is the only
  non-zone fallback. See [ADR-0006](0006-presence-model-closest-ap-wins.md)
  for the state machine.
- `homeassistant/device_tracker/briihass_<beacon-id>/attributes` —
  retained, JSON with the rich per-event context.

`<beacon-id>` = `<uuid_without_dashes>_<major>_<minor>`, lowercased.

## Attribute schema (verified field names from /ingest payload)

The /ingest payload carries AP metadata directly, so the attributes
object can mirror it cheaply:

```json
{
  "ap_mac":      "aa:bb:cc:00:00:01",          // from gateway_euid, lowercased
  "ap_name":     "AP-C",                // from ap_name
  "ap_location": "Test Residence",           // from ap_location
  "ap_model":    "R770",                        // from ap_model
  "rssi":        -50,                           // from events[].rssi
  "uuid":        "fda50693-a4e2-4fb1-afcf-c6eb07647825",
  "major":       10065,
  "minor":       26049,
  "tx_power":    -3,
  "last_seen":   "2026-05-18T06:30:43.452Z"    // from event timestamp (ms→ISO)
}
```

Latitude/longitude/altitude are also available and can be added to
attributes if a use case appears (e.g., HA map card). For v1 we keep
them out to avoid leaking precise residence coordinates onto MQTT
unnecessarily.

## Rationale

- **No HA code to deploy or version.** Custom integrations drag HA
  upgrade compatibility into our release cycle. MQTT Discovery is a
  stable HA contract.
- **Mosquitto already runs in the cluster** with HA already wired to it.
- **Loose coupling.** HA can be down for an upgrade and beacon state
  resumes from retained MQTT messages on restart.
- **REST API rejected** because it requires a long-lived HA token
  managed outside the GitOps flow.

## Consequences

**Positive**
- Decouples release cycles of briihass and HA.
- Trivial to test via `mosquitto_sub`.
- Multi-consumer: Node-RED or any other MQTT client can subscribe.

**Negative**
- Runtime dependency on Mosquitto for the publish path. Mitigated by
  in-memory buffering on publisher disconnect (see ARCHITECTURE.md).

## Resolved — gateway_euid ↔ AP MAC mapping

**Identity, no normalization required.** The /ingest payload's
`gateway_euid` (e.g. `AA:BB:CC:00:00:01`) is exactly one of the MACs
that appears in /heartbeat's `Online` list. They are the same string.
The bridge can use `gateway_euid` directly as the AP identifier
internally; we lowercase it only for the outbound MQTT attribute to
match typical HA conventions.

Because the /ingest payload also carries `ap_name`, `ap_location`, and
`ap_model`, **the bridge does not need to ask vSmartZone anything to
enrich a beacon event**. See ADR-0005 for the resulting deferral.

## Addendum (2026-05-26, Phase 3) — Entity removal

The original ADR didn't cover the inverse of "create entity on first
sighting". Phase 3 adds operator-driven demote (`/admin/devices`):
when a beacon is removed from the allowlist, the bridge publishes an
**empty retained payload** to the same Discovery config topic. Per
the official HA docs
(https://www.home-assistant.io/integrations/mqtt/#discovery-messages),
this is the canonical "delete this entity" signal; HA cleans up the
linked state + attributes topics on its own. The bridge also clears
the in-memory `seen` map for that beacon, so a later re-promote
re-asserts a fresh Discovery config on the next sighting.

Implementation: `mqtt.Publisher.RemoveEntity(BeaconID)`. Tested in
`publisher_test.go::TestPublisher_RemoveEntityClearsRetainedTopics`.

## Addendum (2026-05-27, Phase 4 / BLE Scan) — device-based discovery

Phase 4 (BLE Scan migration, see [ADR-0008](0008-generalized-packet-derived-identity.md))
revises the topic scheme to Home Assistant's recommended **device-based
discovery** and corrects a device_tracker pitfall. Verified against the
official HA MQTT docs (https://www.home-assistant.io/integrations/mqtt/
and `.../device_tracker.mqtt/`).

**One config message per device.** Instead of separate
`device_tracker/.../{config,state,attributes}` topics, the bridge
publishes a single retained
`homeassistant/device/<entity_id>/config` whose payload carries `dev`
(device) + `o` (origin) + `cmps` (components): a `device_tracker` plus
`sensor`s for RSSI, battery voltage, and temperature — all grouped under
one HA device. `<entity_id>` = `briihass_<kind>_<key>` (the polymorphic
identity, ADR-0008; e.g. `briihass_ibeacon_<uuid_no_dashes>_<major>_<minor>`).
Sensor components are declared **lazily** — voltage/temperature are added
(config re-published) only once that telemetry first appears, so beacons
without it get no empty sensors. `source_type` is `bluetooth_le`.

**device_tracker reads a BARE state string; sensors read JSON.** HA does
**not** apply `value_template` to a `device_tracker` declared via
device-based discovery (confirmed live: a tracker fed a JSON state with a
correct `value_template` rendered the whole JSON blob as a custom
location). Therefore:

- `briihass/<entity_id>/state` — **bare** zone label / `not_home` (the
  tracker's `state_topic`, no `value_template`). The 004b-proven approach.
- `briihass/<entity_id>/telemetry` — JSON `{state, rssi, voltage,
  temperature, ap_mac, ap_name, …}`; the sensors read it via
  `value_template` (sensors honor it reliably) and it backs the tracker's
  `json_attributes_topic`.

**Availability via Last-Will.** The bridge sets an MQTT LWT on
`briihass/bridge/availability` (`offline`, retained) and publishes
`online` on connect; every device config references it via `availability`,
so HA marks entities unavailable if the bridge dies.

**Discovery self-heal.** The bridge subscribes to HA's birth topic
(`homeassistant/status`); on `online` it clears its `seen` set and
re-asserts every device's config + state (the HA-recommended repave
trigger). A manual **"Resync HA"** admin button does the same for the
case where a single entity was deleted in HA without an HA restart.

**Migration cleanup.** On first config publish per entity the bridge
empty-retains the legacy 004b `homeassistant/device_tracker/<entity_id>/*`
topics (same `unique_id`) so the stale per-component config doesn't
collide with — and mask — the device-based config.

Implementation: `mqtt/discovery.go` (`BuildDeviceDiscovery`, `BuildState`,
topic helpers), `mqtt/publisher.go` (birth subscribe, LWT, lazy growth,
`ResyncDiscovery`). Tested in `publisher_test.go`
(`TestPublisher_TrackerStateIsBareString`, `…LazyTelemetryGrowth`,
`…ResyncDiscovery`).

## Addendum (2026-05-29, Phase 5) — tracker is home/not_home; area is a sensor

The Phase 4 scheme published the AP-derived zone label (`zone_a`, `zone_b`) as
the bare `device_tracker` state. **That is broken by construction.** Verified
against the official HA docs
(https://www.home-assistant.io/integrations/device_tracker.mqtt/,
https://www.home-assistant.io/integrations/device_tracker/): a `device_tracker`
is treated as **home** — for person entities, `zone.home`, and
`from: not_home → home` automations — **only** when its state is exactly `home`
(or a `payload_home` override). Any other string is a *custom location* that
reads as not-home, so the bridge never satisfied a single home/away automation.

HA cannot encode both "home/away" and "which AP/area" in the one tracker state.
The canonical HA pattern (ESPresense / the `mqtt_room` integration,
https://www.home-assistant.io/integrations/mqtt_room/) is a `device_tracker` for
`home`/`not_home` **plus** a separate sensor for the room/area. HA *Areas* are
not a dynamic channel — `suggested_area`/`sa`
(https://www.home-assistant.io/integrations/mqtt/) is read once at device
creation and can't follow a moving beacon.

**Decision (Phase 5):** split presence across two components on the same HA
device, with the presence engine unchanged:

- **`device_tracker`** — bare state `home` (whenever the engine resolved any
  area label, i.e. presence at any AP) or `not_home`. This is the load-bearing
  state automations key on. `briihass/<entity_id>/state`, no `value_template`.
- **`sensor` "Area"** (component `<entity_id>_area`) — the AP-derived area label,
  or `not_home` while away. **Always present** (every tracked beacon resolves an
  area). Reads `{{ value_json.area }}` from the telemetry topic.

The telemetry JSON field `state` was renamed `area` to match. The closest-AP
state machine (ADR-0006) is untouched; only the MQTT publish mapping changed
(`mqtt/discovery.go` `trackerState()` + the area sensor; `mqtt/publisher.go`
writes `trackerState(ev.State)` to the tracker topic). Tested in
`publisher_test.go` (`TestTrackerState`, `TestBuildState`,
`TestBuildDeviceDiscovery`, `TestPublisher_TrackerStateIsBareString`) and
`ingest/integration_test.go` (`TestEnd2End_CapturedCorpus`).
