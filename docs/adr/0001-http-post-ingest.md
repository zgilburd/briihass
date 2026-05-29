# ADR-0001: Ingest beacon events via HTTP POST from the vRIoT iBeacon plugin

- **Status:** Accepted
- **Date:** 2026-05-18 (revised after live capture)
- **Deciders:** Zack

## Context

The Ruckus vRIoT 3.1.0.0 iBeacon plugin can deliver beacon sightings to an
external consumer via HTTP POST or MQTT publish. We picked HTTP POST (see
the "Rationale" section). Initial drafts of this ADR speculated about
auth and payload shape; the verified facts below come from `cmd/captured`
recording the live plugin on 2026-05-18.

## Decision

briihass ingests via HTTP POST. Two endpoints are exposed:

1. **`POST /ingest`** — beacon events. One POST per gateway, per scan
   cycle (roughly 1–2 per second during active scanning).
2. **`POST /heartbeat`** — gateway liveness list (~every 75 seconds).

The polling endpoint `/app/v1/beacon` on the controller is retained
only as a manual diagnostic.

## Verified plugin behavior

- **Transport:** HTTP/1.1, keep-alive, `Content-Type: application/json`,
  **`Content-Encoding: gzip` on every request**. Bridge MUST gunzip.
- **Auth header:** `Api-Key: <shared-secret>`. The plugin sends whatever
  string is configured in its UI; no HMAC, no Basic, no mTLS observed.
- **User-Agent:** `go-resty/2.16.5 (...)` (the plugin is itself a Go service).
- **Cadence:** ~1–2 `/ingest` POSTs per second, ~1 `/heartbeat` per 75 s.

### `/ingest` payload (gunzipped JSON)

Outer object keys: `gateway_euid`, `vendor_code`, `latitude`, `longitude`,
`altitude`, `ap_name`, `ap_ip_address`, `ap_model`, `ap_firmware`,
`ap_serial`, `ap_location`, `version`, `timestamp` (ms epoch),
`events[]`, `meta_data`.

Each `events[].data` is a hex-encoded BLE advertisement TLV (not a flat
iBeacon frame). The bridge parser walks the TLV looking for a manuf-data
section of shape `<len> FF 4C 00 02 15 <16-byte UUID> <2B Major> <2B Minor>
<1B TX>`. Events that aren't iBeacons (Apple Continuity, AirDrop, etc.)
drop silently.

### `/heartbeat` payload

`{"vendor":"ibeacon","Online":[<MAC>,...],"Offline":[<MAC>,...]}` — note
CapitalCase keys. Used for liveness monitoring; the bridge does not
republish to MQTT.

## Auth on the inbound POST

`Api-Key` header compared in constant time to `INGEST_SHARED_SECRET` (supplied
as an env var). Source IP is logged. As belt-and-suspenders, an optional IP
allowlist limits accepted clients to the vRIoT VM (placeholder `192.0.2.30`).
Auth failures → 401, logged with source IP. Rate limit per source IP to defang
stuck-loop scenarios.

This is a plain shared-secret header — not the strongest scheme possible
in theory, but it's the strongest scheme the plugin actually supports
(no HMAC option observed). Terminating TLS at your ingress plus LAN-only
network exposure (the bridge has no public DNS / public IP) keep the attack
surface narrow.

## TLS

Terminate TLS at your ingress / reverse proxy on an internal hostname
(placeholder `${BRIDGE_HOSTNAME}`); no public DNS exposure required.

## Idempotency and replay

The plugin's retry behavior on 5xx is not yet characterized. Assume
at-least-once; the parser + presence state machine are idempotent on
duplicate sightings (same `(beacon, gateway, advert-timestamp)` tuple
produces no extra state transition). In-memory dedup of the most recent
N events per beacon (window equal to the presence-hysteresis window) is
sufficient.

## Rationale (rejecting alternatives)

- **MQTT-subscribe ingest rejected.** Plugin supports it, but it adds
  broker-side ACL complexity and weakens per-request observability.
  HTTP gives standard access logs and clear per-request status codes.
- **Polling rejected.** Push is documented and works; polling would
  add latency and load for no benefit.
- **Custom HA integration rejected (covered in ADR-0002).** MQTT
  Discovery is the right HA contract.

## Consequences

**Positive**
- One inbound surface, one credential, easy to reason about.
- Bridge can be tested with `curl --data-binary @capture.gz | gunzip` of
  any recorded capture — no broker needed.
- Outage isolation: ingest survives Mosquitto failure.
- Most enrichment fields are already in the payload — vSZ is not on the
  steady-state path (see ADR-0005).

**Negative**
- Mandatory gunzip on every request; bounded buffer.
- HTTP retry semantics on 5xx depend on the plugin's behavior; we'll
  characterize this with a deliberate fault injection once Phase 2 lands.

## Verification status

- [x] Confirm push delivery exists. (Yes — captured 161 POSTs in 30s.)
- [x] Identify auth scheme. (`Api-Key` header.)
- [x] Capture real payload for both endpoints. (Both schemas above.)
- [x] Identify content-encoding. (Always gzip.)
- [ ] Determine plugin behavior on bridge returning 5xx. Test
      deliberately during Phase 2.
- [ ] Verify Api-Key max length / character set the plugin accepts in
      its UI (informs rotation policy).

## Addendum (2026-05-27, Phase 4) — BLE Scan plugin

The controller now runs the **BLE Scan** plugin, not the iBeacon plugin
(see [ADR-0008](0008-generalized-packet-derived-identity.md)). The HTTP
envelope, auth, gzip, and two endpoints are **unchanged**; what changes:

- `/heartbeat` `vendor` is now `"blescan"` (was `"ibeacon"`). The handler
  never validated `vendor`, so it accepts either.
- `events[].data` now carries the **full** BLE advertisement for **every**
  scanned device (Eddystone, named peripherals, Apple Continuity, custom
  manufacturer data, plus iBeacon) — not just iBeacon frames. The parser
  decodes all of it; identity is derived per advert type (ADR-0008).
- `device_euid` is **not** a stable identity (it rotates / is ambiguous —
  one euid carried two iBeacon identities in a 5-minute corpus). It is
  used only as a within-POST correlation hint for telemetry, never as a key.
