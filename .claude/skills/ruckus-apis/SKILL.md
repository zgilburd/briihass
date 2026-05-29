---
name: ruckus-apis
description: Use when writing or modifying Go code that talks to a Ruckus vRIoT controller, parses vRIoT BLE Scan plugin POST bodies, or (future) talks to vSmartZone. Provides verified auth flow, endpoint schemas, the full-advert decode + polymorphic-identity recipe (iBeacon + Eddystone UID/URL/TLM + name + mfg), Eddystone-TLM telemetry decode, the euid-is-not-identity rule, and the HA device_tracker bare-state gotcha so you don't have to re-derive them. TRIGGER when editing internal/ingest/, internal/parser/, internal/presence/, internal/ids/, internal/mqtt/, or any code path that constructs HTTP requests to a Ruckus controller or interprets fields from /ingest or /heartbeat payloads. vSmartZone is currently DEFERRED (ADR-0005); skill still carries the reference for when it returns to scope.
---

# Ruckus controller API quick-reference

This skill collapses the load-bearing facts from the vRIoT REST API guide
and (eventually) the vSmartZone API guide so a developer doesn't have to
crawl PDFs in the middle of writing code. Cite this skill when making
implementation decisions about either controller.

The full vendor docs live in `vendor-docs/` (gitignored). Treat the
information here as a working summary, not gospel — when in doubt, open the
vendor doc.

## Ruckus vRIoT 3.1.0.0

### Auth — for management calls only

The bridge does **not** make per-event API calls to vRIoT in the steady-
state path; the controller pushes to us. But for one-off management
(configure the BLE Scan plugin, fetch a diagnostic snapshot), use:

1. **Basic** auth to the token endpoint with `admin:<password>`. Default
   creds are `admin:admin`; rotate them and supply the real value via your
   secret store (alongside `INGEST_SHARED_SECRET`).
2. Receive `ACCESS` and `REFRESH` tokens.
3. Use `Authorization: Token <access-token>` on subsequent requests.
4. Refresh well before expiry; the docs do not document an exact TTL, so
   treat 401 as "re-login" and retry once.

### BLE Scan plugin (the primary integration surface — VERIFIED)

> **Phase 4:** the controller now runs the **BLE Scan** plugin, not the
> iBeacon plugin. The HTTP envelope, auth, gzip, and two endpoints are
> identical; the difference is `events[].data` carries the **full** BLE
> advertisement for **every** scanned device (not just iBeacon), and
> `/heartbeat`'s `vendor` is `"blescan"`. Identity is polymorphic and
> packet-derived (ADR-0008). See the "Full-advert decode" section below.

The plugin POSTs to two endpoints on the configured target URL. All
specifics below were captured live (see project memory note
`vriot_plugin_schema.md` for the full schema).

**Transport:** HTTP/1.1 keep-alive, `Content-Type: application/json`,
**`Content-Encoding: gzip` on every request**. Bridge MUST gunzip.
User-Agent is `go-resty/2.16.5` — the plugin is itself a Go service.

**Auth:** `Api-Key: <value>` header. The value is whatever string the
operator pastes into the plugin's UI auth field. No HMAC, no
Basic, no mTLS observed.

**Endpoints:**

1. `POST /ingest` — beacon events, ~1–2/sec during scanning. Payload:
   ```
   {gateway_euid, vendor_code, latitude, longitude, altitude,
    ap_name, ap_ip_address, ap_model, ap_firmware, ap_serial,
    ap_location, version, timestamp, events[], meta_data}
   ```
   `events[].data` is a hex-encoded **full** BLE advertisement TLV (every
   AD structure for the device, all advert types). Other fields per
   event: `device_euid` (8-byte BLE source — **rotates, NOT an identity**),
   `rssi` (int dBm), `timestamp` (ms epoch).

2. `POST /heartbeat` — gateway liveness, ~1/75 s. Payload:
   `{"vendor": "blescan", "Online": [<MAC>...], "Offline": [<MAC>...]}`
   Note CapitalCase keys. The bridge 200-OKs and tracks an internal
   gauge; no MQTT republish per-gateway.

**AP metadata enrichment:** /ingest already carries ap_name,
ap_location, ap_model, ap_ip_address, ap_serial, ap_firmware,
latitude, longitude, altitude. **No vSmartZone call needed for v1.**
See ADR-0005.

### Full-advert decode + polymorphic identity (Phase 4, ADR-0008)

The BLE advertisement bytes form a sequence of TLV records:
`<len><type><value (len-1 bytes)>...`. `internal/parser.Parse` walks the
**whole** sequence and decodes every AD structure; it does **not** filter
to iBeacon. A separate `Identify` derives a `BeaconKey{kind,key}` by
precedence:

```
ibeacon > eddystone_uid > eddystone_url > mfg(allowlisted) > name > anonymous
```

- **iBeacon** — manuf-data (`0xFF`) starting `4C 00 02 15`; then
  UUID(16) + Major(2 BE) + Minor(2 BE) + TX(1 int8). key =
  `<uuid>_<major>_<minor>`.
  Example: `0201061AFF4C000215FDA50693A4E24FB1AFCFC6EB07647825275165C1FD`
  → UUID `fda50693-a4e2-4fb1-afcf-c6eb07647825`, Major 10065, Minor 26049, TX −3.
- **Eddystone** — service-data (`0x16`) for UUID `0xFEAA`; first payload
  byte is the frame type: `0x00` UID (namespace 10B + instance 6B → key),
  `0x10` URL (scheme byte + expansion table → key), `0x20` TLM (telemetry,
  no identity), `0x30` EID (ephemeral, no identity).
- **name** — complete/short local name (`0x09`/`0x08`).
- **mfg** — opt-in by company id (allowlist empty by default; most vendor
  payloads embed rotating bytes).

**Anonymous adverts** (rotating Apple Continuity `0x10`/`0x0C`,
Eddystone-TLM/URL with no resolvable id, EID) — ~85% of live events —
are **counted** (per-kind metric) and **not stored**. The parser must be
robust; never crash on a non-iBeacon advert.

### Eddystone-TLM telemetry decode

TLM (`0xFEAA` service-data, frame `0x20`) has **no identity of its own**.
Layout after the frame-type byte: `version(1) battery_mV(2 BE)
temp(2 BE, 8.8 fixed signed) adv_cnt(4 BE) sec_cnt(4 BE)`.

- **Temperature `0x8000` is the "not available" sentinel** → leave nil,
  do NOT publish −128.0 °C.
- A real beacon interleaves iBeacon/UID **and** TLM frames from the **same
  `device_euid`** within a POST. Correlate telemetry to a stable identity
  **only within a single `/ingest` POST body** by euid (the one place euid
  is touched). Cross-POST euid correlation is forbidden — euid rotates and
  is ambiguous (one euid carried two iBeacon identities in a 5-min corpus).
  Verified: the tracked iBeacons also emit TLM (~3714 mV / ~29 °C) from
  their own euid, so battery/temperature attach to the iBeacon identity.

### HA device_tracker gotcha (see ADR-0002 Phase 4 addendum)

HA does **not** apply `value_template` to a `device_tracker` declared via
device-based discovery. Publish the tracker's state as a **bare** zone
string; put JSON telemetry on a **separate** topic the sensors read via
`value_template`. Do not regress this.

### Polling endpoint (diagnostic only — do not wire into pipeline)

- `GET /app/v1/beacon?gateway_euid=<MAC>&limit=&offset=` — returns
  recent sightings per gateway. Useful for "what does the controller
  currently see?" but **must not** be the primary ingest path. ADR-0001
  bans it from the pipeline.

### Polling endpoint (diagnostic only — do not wire into pipeline)

- `GET /app/v1/beacon?gateway_euid=<MAC>&limit=&offset=` — returns recent
  sightings per gateway. Useful for "what does the controller currently
  see?" but **must not** be the primary ingest path. ADR-0001 bans it from
  the pipeline.

### iBeacon advertisement parsing (the part of the BLE advert we care about)

A standard iBeacon advert frame is 25 bytes after the company ID:

| Offset | Length | Field |
|---|---|---|
| 0 | 2 | Company ID (`0x004C` = Apple) |
| 2 | 1 | iBeacon type byte (`0x02`) |
| 3 | 1 | iBeacon length (`0x15`) |
| 4 | 16 | **UUID** (128-bit) |
| 20 | 2 | **Major** (uint16 big-endian) |
| 22 | 2 | **Minor** (uint16 big-endian) |
| 24 | 1 | **TX power** (int8, dBm at 1m) |

The vRIoT `data` field is the full advert payload as hex. Parse with
`encoding/hex` + `encoding/binary` (BigEndian). Sanity-check the company
ID and iBeacon type/length bytes before extracting the rest; if they
don't match, the advert isn't an iBeacon (could be Eddystone, AltBeacon,
or random BLE) and should be dropped silently.

### Polling cadence — N/A

We do not poll. If you find yourself wanting to add a polling loop, you
are off the supported path. Re-read [ADR-0001](../../../docs/adr/0001-http-post-ingest.md).

## Ruckus vSmartZone — DEFERRED for v1 (not consumed)

The bridge does **not** query vSmartZone in v1. The vRIoT /ingest
payload already carries the AP metadata (`ap_name`, `ap_location`,
`ap_model`, lat/lng, etc.) that would have justified a vSZ call. See
[ADR-0005](../../../docs/adr/0005-vsz-enrichment.md) for the deferral
rationale and the criteria under which we'd revisit (zone hierarchy,
formal venue metadata, AP groups, etc.).

The reference below stays accurate for when we do consume vSZ later.

**Source of truth:** Ruckus vSZ-E 7.1.1-patch1 Public API Reference Guide
(https://docs.ruckuswireless.com/smartzone/7.1.1-patch1/vsze-public-api-reference-guide-711-patch1.html).
This is the **vSZ Essentials** API, not vSZ-High-Scale — check version
strings on the live controller.

### Base URL

```
https://<vsz-host>:8443/wsg/api/public/v13_1
```

The `v13_1` segment is the API version; it tracks vSZ firmware. Confirm
on first contact via `GET /apiInfo`.

### Logon — `POST /v13_1/serviceTicket`

Acquire a **service ticket** (auth token) by POSTing username/password.
The ticket is valid for **24 hours**. Re-use the same ticket across many
calls during that window. Re-login on 401 (with at most one retry per
request, with jitter).

Subsequent requests pass the ticket as a query parameter
(`?serviceTicket=<ticket>`) — quirk of the vSZ API; not the typical
`Authorization` header.

### AP lookup by MAC — `GET /v13_1/aps/{apMac}`

This is the **primary enrichment call**. Returns the full AP configuration
which includes the AP's name and the zone it belongs to. The `apMac`
path parameter is the AP's MAC. **Normalize to vSZ's format** — typically
colon-separated lowercase (e.g., `aa:bb:cc:dd:ee:ff`). Verify against a
real response on first contact and normalize both sides (vRIoT
`gateway_euid` and vSZ `apMac`) to the same form before caching.

### AP list / cache-warm — `GET /v13_1/aps`

Returns APs in a zone or domain. Use for cache pre-warm at bridge startup
(one batch fetch, cache everything, then per-event lookups are pure cache
hits). Supports query filtering by zone/domain.

### Venue lookup

Venue is associated at the zone level on vSZ-E (a zone belongs to a
venue). After `/v13_1/aps/{apMac}` gives you the zone ID, fetch venue
metadata via the zone configuration endpoint (search the API guide for
`zone-configuration-retrieve`). Worth caching with a long TTL (zones/
venues are nearly static).

### Pagination and querying

vSZ list endpoints accept `?listSize=&index=` for pagination and a POST
body `queryCriteria` for filtering on list endpoints that take filters
(documented in the "Usage for Query Criteria" preamble of the API guide).
For our access pattern (per-MAC lookup, periodic full-list cache warm),
neither matters much in steady state.

### Identifier conventions

- **AP MAC** is the canonical identifier in path params (`{apMac}`).
- Normalize to lowercase with colons before comparison or cache lookup.
- vSZ's internal AP UUID exists but is not what we key on.

### Rate limits

The vSZ docs do not publish explicit rate limits. With caching enabled
(LRU 1024, 1h TTL, stale-while-revalidate), per-AP fetches should be
infrequent enough that limits do not apply. Avoid hammering during cache
warm — sequential paginated fetches, not parallel.

### gateway_euid ↔ AP MAC mapping

The bridge assumes `gateway_euid` from vRIoT equals the AP's primary MAC
in vSZ, possibly with normalization (case, separator). This is
**unverified**; see [ADR-0002](../../../docs/adr/0002-mqtt-discovery-as-ha-contract.md)
for the verification plan. Normalize both sides to lowercase with `:`
separators before comparison.

### Caching

- LRU, size 1024, TTL 1 hour, stale-while-revalidate.
- Cache key: normalized EUID.
- Cache value: `{ap_name, zone, venue, fetched_at}`.
- Fail open on vSZ errors; never block the ingest path on a slow vSZ.

## Common pitfalls

- **Don't trust the `data` field length blindly.** Some non-iBeacon
  adverts will be shorter or longer; bounds-check before slicing.
- **Don't assume `gateway_euid` is uppercase/lowercase.** Normalize.
- **Don't forget that the vRIoT API and the BLE Scan plugin are separate
  surfaces.** Plugin config is set via the REST API but pushes are
  independent.
- **Don't introduce a polling loop "for resilience."** If the plugin
  stops POSTing, the controller is broken or misconfigured — alert on it,
  don't paper over it with polling.
