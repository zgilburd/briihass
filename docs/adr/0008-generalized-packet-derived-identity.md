# ADR-0008: Generalized packet-derived beacon identity (BLE Scan plugin)

- **Status:** Accepted (implemented across phases 004a–004c, 2026-05-27)
- **Date:** 2026-05-26
- **Deciders:** Zack
- **Supersedes (in part):** the iBeacon-only identity assumptions in
  [ADR-0001](0001-http-post-ingest.md), [ADR-0002](0002-mqtt-discovery-as-ha-contract.md),
  [ADR-0006](0006-presence-model-closest-ap-wins.md)

## Context

briihass was built against the Ruckus vRIoT **iBeacon** plugin, which only surfaced
iBeacon advertisements. As a result the whole pipeline keys on the iBeacon-specific
identity `BeaconID{uuid, major, minor}`.

We are switching the controller to the **BLE Scan** plugin. The HTTP envelope is
identical (`gateway_euid`, `ap_name`, `events[].{data, device_euid, rssi, timestamp}`,
heartbeat with `Online`/`Offline`), but two things change materially:

1. **`events[].data` now carries the full BLE advertisement for every scanned device** —
   Eddystone, named peripherals, Apple Continuity, custom manufacturer data, plus iBeacon.
   The heartbeat `vendor` field is now `"blescan"`.
2. **The hardware MAC (`device_euid`) is not a stable identity.** Modern devices rotate
   resolvable private addresses, and in a 5-minute capture corpus a single `device_euid`
   already carried **two distinct iBeacon identities**. Keying on the euid is unsafe.

We want to track more than iBeacons and to use the richer data (notably Eddystone-TLM
telemetry: battery, voltage, temperature), without ever relying on the MAC for identity.

## Decision

Adopt a **polymorphic, packet-derived identity**: replace `BeaconID` with
`BeaconKey{Kind, Key}` threaded through every layer (`ids`, `parser`, `ingest`,
`presence`, `store`, `mqtt`, `admin`, `config`).

- **Kinds:** `ibeacon`, `eddystone_uid`, `eddystone_url`, `name`, `mfg`. Each has a
  canonical, kind-specific `Key` (e.g. iBeacon = `<uuid>_<major>_<minor>`; Eddystone-UID =
  `<namespace>_<instance>`). The full key table lives in the master plan, spec A.
- **Identity precedence** when an advert carries multiple identity-bearing structures:
  `ibeacon > eddystone_uid > eddystone_url > mfg(allowlisted) > name > anonymous`.
- **The parser fully decodes the advertisement** (all AD structures) and a separate
  classifier derives the `BeaconKey`. Nothing is silently dropped at parse time;
  classification decides.
- **Anonymous adverts** (rotating Apple Continuity, Eddystone-TLM/URL with no resolvable
  identity — ~85% of traffic) are **counted via metrics and not persisted**.
- **Telemetry with no identity of its own** (Eddystone-TLM) is attributed to a stable
  `BeaconKey` **only by correlating `device_euid` within a single `/ingest` POST** — the
  one place the euid is touched, and never as a durable key. Attributable enrichment
  (battery/voltage/temperature/txpower) is persisted and published to Home Assistant.
- **MQTT entity IDs** become `briihass_<kind>_<sanitized key>`; telemetry is published as
  HA sensor entities grouped under the tracked device.

The promotion / demotion / zone / observation / MQTT-Discovery model is unchanged in
shape; only the identity it carries is generalized.

## Consequences

**Positive**
- Tracks any stable-identity BLE device, not just iBeacons.
- Enables battery/temperature/voltage telemetry to Home Assistant.
- Identity is robust to MAC randomization (joins are on packet content).

**Negative / costs**
- Touches every layer; a multi-phase migration (plans 004a–004d) with a schema change
  (`beacons` PK `(uuid,major,minor)` → `(kind,key)`, `observations` re-keyed + enrichment
  columns).
- Existing iBeacon MQTT entity IDs change; rollout must clean up old retained configs.
- Some kinds are less trustworthy: `mfg` payloads may embed rotating bytes (kept opt-in);
  local names can collide (entity-id hash-suffix mitigates topic collisions).
- Eddystone-UID decode is currently only exercisable against synthetic fixtures (no
  UID-frame device observed in the corpus yet).

## Alternatives considered

- **Keep `BeaconID`, add parallel per-kind tables/paths.** Rejected: duplicates the
  promote/demote/zone/observation/MQTT machinery per kind and diverges over time.
- **Use `device_euid` as identity.** Rejected: proven unstable/ambiguous (rotation +
  one-euid-to-two-identities in the corpus).
