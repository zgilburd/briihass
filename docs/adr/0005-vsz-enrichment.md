# ADR-0005: vSmartZone enrichment — deferred (not needed for v1)

- **Status:** Deferred (originally Accepted; revised after live capture)
- **Date:** 2026-05-18

## Context

The original draft assumed the bridge would need to query vSmartZone to
enrich each beacon event with `ap_name` / `ap_zone` / `ap_venue` /
`ap_model`, because the vRIoT controller was thought to surface only a
gateway MAC. ADR-0005 proposed an LRU cache, stale-while-revalidate,
fail-open behavior, etc.

The live capture on 2026-05-18 (`cmd/captured` against the configured
plugin) showed that **the /ingest payload already includes the enrichment
fields directly**:

- `ap_name` (e.g. `"AP-C"`)
- `ap_location` (e.g. `"Test Residence"` — used here as the venue analog)
- `ap_model`, `ap_serial`, `ap_firmware`, `ap_ip_address`
- `latitude`, `longitude`, `altitude`

This is the data vSZ would have provided, served back to us by vRIoT for
free on every POST.

## Decision

**Defer the vSmartZone client entirely from v1.** The bridge consumes
the AP metadata directly from the /ingest payload, copies the relevant
fields into MQTT attributes, and ships no vSZ code.

## Rationale

- Zero benefit to making the bridge query vSZ when the same data
  arrives in the inbound payload.
- One less network dependency. One less credential set.
- One less failure mode (no "vSZ down → degraded enrichment").
- vSZ's `VSZ_API_USER` / `VSZ_API_PASS` / `VSZ_HOST` credentials remain
  in the credential schema for future use, but the bridge does not
  consume them.

## When we might revisit

- If we want **zone hierarchy** (vSZ has a Zone → Domain tree that
  isn't surfaced in the /ingest payload).
- If we want **venue metadata** distinct from `ap_location`. (vSZ has a
  formal Venue object with attributes like floor, country, etc.)
- If we want to **correlate beacons with vSZ AP groups or tags** for
  automation rules like "any AP in the OUTDOOR group".

Until at least one of these has a concrete HA-automation use case, we
do not add a vSZ client.

## Consequences

**Positive**
- The bridge gets simpler. No cache. No vSZ session management.
- Steady-state latency drops — no enrichment lookup at all.
- No fail-open path to monitor.

**Negative**
- If the /ingest payload format changes in a future vRIoT release and
  drops `ap_*` fields, we'll need to add the vSZ client at that point.
  Likelihood: low (the fields look intentional and have been stable
  through observed payloads with `version: 3`).

## ruckus-apis skill

The vSmartZone section of `.claude/skills/ruckus-apis/SKILL.md` stays
in place as reference material (the endpoints are correct), but it's
marked "future / not currently consumed."
