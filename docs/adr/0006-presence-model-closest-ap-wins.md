# ADR-0006: Presence model — per-AP RSSI debounce with decay, closest-AP-wins, HA owns hysteresis

- **Status:** Accepted
- **Date:** 2026-05-18 (revised: added sticky-arrival window for asymmetric debounce)

> **Superseded in part by [ADR-0007](0007-postgres-resident-persistence.md) (Phase 3):**
> topology storage moved from `briihass.yaml` to the `beacons` and `zones`
> Postgres tables. The "Configuration file (not state)" section below
> describes the pre-Phase-3 model and is retained for historical context
> only. Current operator paths are `/admin/devices` (allowlist) and
> `/admin/zones` (AP→zone map). The presence-model invariants in the rest
> of this ADR are unchanged.

## Context

A primary use case is Home Assistant automation driven by tracked BLE beacons
moving between AP-detected areas and leaving AP coverage entirely (`not_home`).

- **Arrival edge** must reach HA immediately. Any bridge-side
  debouncing here adds delay before HA can act on arrival.
- **Departure edge** can be a few seconds late.
- **Steady-state zone transitions** are not latency-critical.

Two earlier mistakes informed this revision:

1. An early draft added K-of-N confirmation gating on every transition,
   which would have delayed arrival by 30+ seconds.
2. The next draft replaced that with a simple "last sighting timestamp
   per AP" model. That fails the **per-AP presence debounce**
   requirement: AP reporting cadence varies, and beacons near the
   edge of an AP's range pop in and out of a single scan cycle.
   Treating "last seen <T seconds ago" as binary AP presence makes
   the zone state flap, which produces a noisy HA UI even though the
   beacon hasn't physically moved.

The right model uses **RSSI as a continuous signal** with a **time
decay** component, so per-AP presence fades gracefully and brief
gaps don't flip presence on/off.

## Decision

### Per-AP presence (the debounce layer)

For each `(beacon, AP)` pair the bridge maintains:

- `ewma_rssi` — exponentially weighted moving average of observed
  RSSI. Updated on every sighting with `alpha = 0.4`
  (new sample weighted 40%, history 60%). First sighting initializes
  the EWMA to the raw value (no smoothing on cold start).
- `last_sighting_ts` — wall-clock time of the most recent sighting.

The **effective RSSI** for ranking and presence at time `now` is:

```
age = now - last_sighting_ts
if age <= grace_period_s:
    effective_rssi = ewma_rssi             # no decay yet
else:
    effective_rssi = ewma_rssi - decay_rate_db_per_s * (age - grace_period_s)
```

An AP is considered to currently see a beacon iff
`effective_rssi >= presence_floor_dbm`. Otherwise the AP has no
presence for that beacon.

### Closest-AP selection

Among APs with active presence for a beacon, the closest AP is the one
with the highest `effective_rssi`. Comparisons use the decayed value
so an AP that hasn't reported recently naturally loses out to one
that has, even if the older AP's last raw RSSI was stronger.

### Bridge-wide state for a beacon

- **`not_home`** if no AP has presence for the beacon AND the beacon is
  not currently inside a sticky-arrival window (see below).
- Otherwise, the zone label from the `zones` table corresponding to
  the closest AP's MAC. If all APs lose presence during the sticky window,
  the bridge keeps publishing the last known zone label until either
  (a) the sticky window expires, or (b) a new sighting establishes a
  different closest AP (zone updates ARE permitted during sticky;
  only the `not_home` transition is suppressed).

### Arrival edge — immediate (load-bearing invariant)

When the beacon's bridge-wide state transitions from `not_home` to any
zone, **publish immediately**. The first sighting that pushes any AP's
`ewma_rssi` above `presence_floor_dbm` causes the transition. No
K confirmations on this edge, no window averaging, no waiting.

This invariant is the entire point of the design and must be preserved
across any future tunable change. Any patch that delays
`not_home → <zone>` must be rejected.

### Sticky-arrival window (asymmetric debounce on `not_home`)

After a `not_home → <zone>` transition, the bridge guarantees the
beacon's published state will **not** flip back to `not_home` for at
least `sticky_after_arrival_s` (default **120 s**, configurable per
beacon).

**Why:** the load-bearing scenario is a tracked beacon entering AP coverage.
The first sighting at the edge of coverage is often a single faint reading
(e.g., -92 dBm); the next few seconds can have variable signal as the beacon
moves through multi-path/line-of-sight changes, possibly dropping below the
floor briefly before rising again. Without the sticky window, the bridge would
publish arrival → not_home → arrival in rapid succession. Even if HA's
automation conditions absorbed the flap and didn't fire spuriously, the
user-visible cost is delayed arrival handling while the state churns. The user
has explicitly called out this anti-pattern: "prevent flipping to not_home."

**Mechanism:** when the bridge publishes an arrival, it records
`last_arrival_ts`. The bridge-wide `not_home` decision becomes:

```
if (now - last_arrival_ts) < sticky_after_arrival_s:
    not_home is suppressed; keep publishing last known zone
else:
    apply normal rules (no AP has presence → not_home)
```

During the sticky window:
- Zone-to-zone transitions still happen normally (a beacon may first appear at
  `zone_f`, then transition to `zone_c` as AP signal strength changes — this
  should publish).
- If all APs lose presence, the bridge holds the last published zone.
- If a fresh sighting arrives at a different AP, the closest-AP
  recomputation runs and may switch zones immediately (treat like
  arrival into a new zone — H/K hysteresis is skipped because there's
  no current-AP comparison to make).

**False-positive cost:** a brief pass through AP coverage. The bridge will
publish arrival, hold for `sticky_after_arrival_s` (default 2 min), then
publish `not_home`. Mitigations the user can layer on the HA side:
- `condition: state … not_home for: 5m` — only act if the beacon was away for
  at least 5 minutes
- Require the arrival state to remain stable with an HA-side `for:` qualifier
- Confirm with another HA signal before running any automation

The sticky window is bounded; if the cost is unacceptable for a
specific beacon, set `sticky_after_arrival_s: 30` (or even `0`) for
the beacon via `/admin/tunables`. The default is tuned for cases where brief
AP-coverage gaps during arrival are more costly than a short false-positive
presence window.

### Asymmetric departure

By design, `not_home` is **harder** to enter than to leave. This is
the opposite of conventional hysteresis. Justified by cost
asymmetry: a missed/delayed arrival slows downstream HA automations
(high cost); a brief false-positive presence can be filtered by HA-side
conditions.

### Steady-state zone transitions — light hysteresis

When the beacon is already in zone `X` (closest AP `A`) and a
different AP `B` becomes a candidate (its `effective_rssi` exceeds
`A`'s):

- Require `effective_rssi(B) - effective_rssi(A) >= H` (default
  `H = 4 dB`), **and**
- The condition holds across the last `K` consecutive sightings of
  this beacon (default `K = 2`)

before switching the published zone to `B`. Prevents the HA UI from
flapping when the beacon is right between two APs.

### Departure edge

Outside the sticky-arrival window: pure decay. The bridge publishes
`not_home` the moment the last AP's `effective_rssi` falls below
`presence_floor_dbm`. With default tunables, a strong-signal beacon
(initial -75 dBm) fades out ~15 seconds after the last sighting; a
weak-signal beacon (-85) ~10 seconds. Edge-of-range sightings (-95)
drop almost immediately.

This is correct: strong signals are credible and should be trusted
longer; edge-of-range sightings are noise and should fade fast.

`t_away_max_s` (default 30 s) is a hard upper bound on the "no
sighting at all" case: regardless of the decay math, if no AP has
reported the beacon in 30 s **and** we are outside the sticky-arrival
window, the beacon is `not_home`. Defends against arithmetic
weirdness if EWMA gets pushed to an absurd value.

### HA owns the "wait before acting" hysteresis

The bridge publishes events fast and accurately. HA-side automations
use standard qualifiers to decide when to act:

```yaml
trigger:
  - platform: state
    entity_id: device_tracker.briihass_example_tag
    from: not_home
    to: zone_f
    for: "00:00:02"   # debounce arrival edge slightly
condition:
  - condition: state
    entity_id: device_tracker.briihass_example_tag
    state: not_home
    for: "00:05:00"   # only act if was away for at least 5 minutes
action:
  - service: script.beacon_arrival_action
```

The bridge does not need to know about these qualifiers.

<details>
<summary><strong>Pre-Phase-3 model: YAML configuration file (superseded by ADR-0007)</strong></summary>

## Configuration file (not state)

`briihass.yaml` is **read-only static configuration**, loaded once at
bridge startup. The bridge **never writes** to this file at runtime.
YAML is for config (tunables, topology, whitelist), not for runtime
persistence; if we ever needed runtime persistence (we don't),
the right tool would be sqlite or a proper KV store, not YAML.

What lives in `briihass.yaml`:

- `zones:` — AP MAC → zone label mapping (topology)
- `beacons:` — explicit whitelist of tracked beacons + optional
  per-beacon tunable overrides (`alpha`, `sticky_after_arrival_s`,
  etc.) — these are constants the operator sets, **not** runtime
  values the bridge updates.

What does NOT live in `briihass.yaml`:

- Per-AP EWMA `rssi` values — in-memory only.
- `last_sighting_ts` per (beacon, AP) — in-memory only.
- Current zone per beacon — in-memory only.
- Sticky-arrival timers — in-memory only.

All runtime state is reconstructed from the next sighting on bridge
restart. The bridge does not persist presence across restarts;
retained MQTT messages give HA the last-known state during the
sub-second window before reconstruction begins.

The file paths:

- **Prod:** `/etc/briihass/briihass.yaml`, mounted from your secret store /
  config (separate from the credentials; see ADR-0003).
- **Dev:** `~/.config/briihass/briihass.yaml`. Edit with `$EDITOR`
  directly.

Both files have the same schema. The bridge fails to start on a
malformed config; misconfiguration is loud, not silent.

### Schema

`briihass.yaml`:

```yaml
zones:
  "aa:bb:cc:00:00:01": zone_c         # AP-C
  "aa:bb:cc:00:00:02": zone_d     # AP-D
  "aa:bb:cc:00:00:03": zone_a        # AP-A
  "aa:bb:cc:00:00:04": zone_b     # AP-B
  "AA:BB:CC:00:00:05": zone_e      # AP-E
  "aa:bb:cc:00:00:06": zone_f         # AP-F
  "aa:bb:cc:00:00:07": zone_g          # AP-G

beacons:
  - uuid: aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
    major: 1
    minor: 1
    name: example_tag
    # Optional per-beacon overrides for any tunable below:
    # alpha: 0.6
    # grace_period_s: 8
    # decay_rate_db_per_s: 1.5
    # presence_floor_dbm: -90
    # t_away_max_s: 60
    # sticky_after_arrival_s: 180   # noisier arrival pattern: hold longer
    # hysteresis_db: 6
    # confirm_count: 3
```

Untracked beacons drop silently with a counter increment
(`briihass_unknown_beacon_total{uuid,major,minor,ap}`).

</details>

## MQTT publish topic schema

`<id>` = `<uuid_without_dashes>_<major>_<minor>`, lowercased.

| Topic | Retained | Payload |
|---|---|---|
| `homeassistant/device_tracker/briihass_<id>/config` | yes | HA Discovery JSON |
| `homeassistant/device_tracker/briihass_<id>/state` | yes | zone label or `not_home` |
| `homeassistant/device_tracker/briihass_<id>/attributes` | yes | `{ap_name, ap_mac, zone, rssi_raw, rssi_ewma, rssi_effective, rssi_runner_up_effective_db, last_seen}` |
| `briihass/health` | no | gateways online/offline, queue depths, error counters |

`rssi_effective` is the post-decay value used for ranking. Exposing
both `rssi_raw` (latest sample) and `rssi_ewma` (smoothed) plus
`rssi_effective` lets the user write HA template sensors that
introspect what the bridge is doing without changing the bridge.

## Default tunables

| Param | Default | Per-beacon override | Purpose |
|---|---|---|---|
| `alpha` | **0.4** | yes | EWMA smoothing weight. New sample 40%, history 60%. Two to three samples of memory. |
| `grace_period_s` | **5 s** | yes | Time after last sighting where EWMA holds without decay. |
| `decay_rate_db_per_s` | **2 dB/s** | yes | After grace, effective RSSI linearly decays at this rate. |
| `presence_floor_dbm` | **-95 dBm** | yes | Effective RSSI below this floor → AP loses presence for beacon. |
| `t_away_max_s` | **30 s** | yes | Hard upper bound: no sighting from any AP in this many seconds (and outside sticky window) → force `not_home`. |
| `sticky_after_arrival_s` | **120 s** | yes | Minimum guaranteed dwell after `not_home → <zone>`. Bridge will not publish `not_home` during this window. Set to `0` to disable for a beacon. |
| `H` (steady-state zone hysteresis in dB) | **4 dB** | yes | Effective-RSSI gap required to switch zones. **Not applied to `not_home → zone`.** |
| `K` (confirm count for zone-to-zone) | **2** | yes | Consecutive sightings that must agree. **Not applied to `not_home → zone`.** Skipped inside sticky window when no AP currently has presence. |

**Hard guarantees:**
1. `not_home → <any zone>` always publishes on the first qualifying
   sighting, regardless of any tunable above.
2. `<any zone> → not_home` is suppressed for `sticky_after_arrival_s`
   after each arrival edge, regardless of any tunable above (zone
   updates still allowed during this window).

### Worked example 1 — clean departure with defaults

AP sees beacon at -75 dBm, then stops reporting. Beacon is already
in-zone (sticky window from a much earlier arrival has already expired).

| t (s) | event | ewma | effective | per-AP presence | published state |
|---:|---|---:|---:|:-:|:-:|
| 0 | sighting at -75 | -75 | -75 | yes | (zone) |
| 5 | grace expires | -75 | -75 | yes | (zone) |
| 10 | decay 5 s × 2 dB | -75 | -85 | yes | (zone) |
| 15 | decay 10 s × 2 dB | -75 | -95 | yes (at floor) | (zone) |
| 15.5 | decay 10.5 s × 2 dB | -75 | -96 | **no** | **`not_home`** |

If a second sighting at -78 arrives at t=12, EWMA updates to
`0.4×(-78) + 0.6×(-75) = -76.2`, age resets, presence robust again.

### Worked example 2 — arrival with faint then fluctuating signal

Beacon is `not_home`. It first appears at the edge of AP coverage. First
sighting at the zone_f AP is -92 dBm (edge of range). Then a 12-second gap
(temporary signal shadow), then strong sightings from another AP.

| t (s) | event | per-AP presence | sticky | published state |
|---:|---|:-:|:-:|---|
| 0 | zone_f AP sighting at -92 | yes (-92 ≥ -95 floor) | starts (window 0–120 s) | **`zone_f` published immediately (arrival edge)** |
| 5 | grace expires; -92 holds | yes | active | `zone_f` (no change) |
| 6.5 | decay reaches -95 (floor) | yes (at floor) | active | `zone_f` |
| 7 | decay below floor | **no** at zone_f AP | active | `zone_f` (sticky suppresses `not_home`) |
| 12 | zone_c AP sighting at -80 | yes at zone_c | active | `zone_c` published (zone change inside sticky is fine) |
| 17 | another zone_c sighting at -75 | yes (EWMA strengthens) | active | `zone_c` (no change) |
| 60 | zone_c sightings continuing at -75 | yes | active | `zone_c` |
| 120 | sticky window expires | EWMA at zone_c ~ -75 | ended | `zone_c` (steady state) |

Without the sticky window, the bridge would have published
`zone_f → not_home → zone_c` between t=7 and t=12, exposing HA to a
flap.

### Worked example 3 — brief pass-through false positive

Beacon was `not_home`. It briefly passes through AP coverage; zone_f AP
catches one faint sighting, then nothing.

| t (s) | event | per-AP presence | sticky | published state |
|---:|---|:-:|:-:|---|
| 0 | zone_f AP sighting at -92 | yes | starts | **`zone_f` published** |
| 5 | grace ends; no further sightings | yes (-92 holds) | active | `zone_f` |
| 7 | decay below floor | no | active | `zone_f` (held by sticky) |
| 120 | sticky expires; still no sightings | no | ended | **`not_home` published** |

HA receives one arrival event at t=0 and one departure event at t=120 s.
The user's HA automation should layer additional conditions (e.g., `for: 5m`
on `not_home`, or confirmation from another HA signal) if brief pass-through
false positives are a concern.

## Rationale

- **EWMA + decay handles the "AP reporting varies" problem.** A
  missed scan or two doesn't flip presence off; multiple consecutive
  missed scans naturally fade it. No hard "10 second timeout" cliff.
- **Effective RSSI is signal-strength-weighted age-out.** Edge-of-range
  sightings (-95) fade in under a second; in-room sightings (-70 to
  -80) hold for 10–20 seconds. This matches physical intuition.
- **Closest-AP comparisons use the decayed value**, so an AP that
  hasn't reported recently loses naturally to one that has. The
  steady-state hysteresis (H, K) just prevents flapping at the
  boundary.
- **HA owns the meaningful debounce** (`for: 5m`) for automation
  triggering. The bridge supplies fast, accurate, low-noise events.

## Considered and rejected

- **Last-sighting-timestamp with a single T_away threshold.** What we
  had in the previous revision; user feedback identified it as too
  noisy ("AP reporting can vary slightly"). Replaced.
- **Hard departure cliff (no decay).** Simpler but causes more
  flapping when an AP misses a scan; chosen against.
- **Per-AP rolling window of N raw RSSI samples.** Equivalent
  expressive power to EWMA but more memory per beacon-AP pair, and
  EWMA's recency weighting matches the use case better.
- **Exponential decay instead of linear.** More mathematically
  elegant but harder to reason about ("when will this AP lose
  presence?"). Linear with a floor is the cheapest model that's
  predictable.

## Consequences

**Positive**
- No flapping from single missed scans or edge-of-range sightings.
- Arrival latency stays at "one scan cycle" (the floor
  set by vRIoT's ~1 s scan cadence).
- The attributes payload exposes the bridge's internal RSSI views
  (`rssi_raw`, `rssi_ewma`, `rssi_effective`) so the user can build
  HA template sensors / dashboards for tuning visibility.

**Negative**
- More state per `(beacon, AP)` pair than the simple model. Bounded
  by `(tracked_beacons × APs)`, so trivial for any realistic home
  setup. Still O(1) work per sighting.
- Effective-RSSI decay is wall-clock-time-dependent, so the state
  machine needs a monotonic clock and benefits from a `time.Now()`
  injector for unit tests.

## Verification

- [ ] Implement state machine in Phase 2 with the defaults above.
- [ ] Unit tests: synthetic event streams covering
  - arrival from cold (verify immediate publish)
  - single-scan miss (verify no flap)
  - extended miss → fade-out → re-arrival (verify single not_home
    transition then immediate re-publish)
  - edge-of-range -95 sightings (verify never establish presence)
  - two-AP transit with 4 dB margin (verify K=2 hysteresis works)
  - rapid alternating-AP sightings (verify no flap)
  - **noisy-arrival pattern**: faint -92 first sighting, 12 s signal
    gap, then strong -75 sightings (verify NO `not_home` publish
    during the sticky window; verify zone updates DO publish)
  - **brief pass-through**: single faint sighting then silence (verify the
    bridge publishes exactly one arrival at t=0 and one `not_home`
    at t ≈ `sticky_after_arrival_s`; no intermediate flap)
  - **sticky override**: per-beacon `sticky_after_arrival_s: 0`
    behaves like the pre-sticky model (no asymmetric debounce)
- [ ] Replay the captured 161 POSTs through the state machine and
      confirm published zones match the closest-AP analysis in the
      `target_beacons` project memory.
- [ ] Once deployed, measure end-to-end
      "first sighting after `not_home` → MQTT publish" latency with a
      Prometheus histogram. Alert if p99 > 200 ms (anything beyond
      network + scan cadence).
- [ ] Add Prometheus gauges per (beacon, ap):
  - `briihass_per_ap_effective_rssi` for tuning the decay curves
  - `briihass_beacon_in_sticky_window` (0/1) so users can see when
    sticky is suppressing a departure

## Addendum (2026-05-27, Phase 4) — keyed on BeaconKey

The state machine is unchanged, but its identity key generalized from the
iBeacon-only `BeaconID{uuid,major,minor}` to the polymorphic
`BeaconKey{kind,key}` (see [ADR-0008](0008-generalized-packet-derived-identity.md)).
The engine treats identity opaquely (a comparable map key), so EWMA +
linear decay, closest-AP-wins, hysteresis, and the **sticky-arrival
window** invariants are byte-identical. Telemetry (battery/temperature)
rides alongside on `Sighting`/`PresenceEvent` for the MQTT publisher and
does **not** influence zone resolution. `RepublishAll` re-emits current
state for all beacons (HA repave + telemetry pump) without mutating state.

## Addendum (2026-05-27) — presence state survives restarts

The engine's per-beacon published zone is now persisted so a restart
(deploy, evict, crash) boots **warm** instead of cold. Motivation: with
`replicas: 1`, a cold restart re-initialized every tracked beacon to
`not_home`; the telemetry pump's `RepublishAll` then published `not_home`
before the new pod re-observed the beacon — flapping a present beacon to
away mid-deploy, the exact regression the sticky-arrival window exists to
prevent.

- A `presence_state` row per tracked `(kind, key)` records the last
  published zone, the backing AP, and the arrival timestamp. It is
  full-replaced on a periodic flush (~5 s) and once more in the shutdown
  drain so the freshest zones survive a graceful SIGTERM.
- On boot, `Engine.RestoreState` re-asserts the zone **before the ingest
  listener binds**, so the pod is never Ready-but-cold. A load failure
  degrades to the prior cold start rather than blocking ingest on the
  `presence_state` table.
- **The restart is treated as a fresh arrival** (`lastArrivalTs = now`),
  not the persisted arrival time. A new pod's per-AP map starts empty, so
  the first `Tick` finds no AP in presence; trusting a persisted arrival
  time already past the sticky window would publish a false `not_home`
  before the first live scan. Granting a full sticky window holds the zone
  until live scans (~1–2/s) rebuild per-AP presence. A beacon that
  genuinely never reappears still ages out to `not_home` after the window.
- This pairs with the deploy strategy change (RollingUpdate
  `maxUnavailable: 0`/`maxSurge: 1` + preStop drain) so HTTP ingest also
  never gaps during a rollout. The two together close both mid-deploy
  away-flap causes: the load-balancer no-backend window and cold-start state
  loss.

## Addendum (2026-05-29, Phase 5) — area label moves off the tracker state

The state machine is **unchanged**: it still resolves the closest AP to an area
label and emits `PresenceEvent{State: <label> | NotHome}`. What changed is how
that label is published. HA only treats a `device_tracker` whose state is
exactly `home` as home (see [ADR-0002](0002-mqtt-discovery-as-ha-contract.md)
Phase 5 addendum), so publishing the area label as the tracker state broke every
home/away automation — the entire HA automation use case this ADR exists for.

The published mapping now splits the single `State` value across two HA
components: the `device_tracker` carries `home`/`not_home` (any resolved area
label ⇒ `home`; all APs on-prem ⇒ "any presence is home"), and a separate
always-present **area sensor** carries the label (or `not_home`). The
arrival-latency invariant is preserved verbatim: `not_home → <area>` still
publishes immediately, now as a `not_home → home` tracker edge plus the area
sensor flip. The sticky-arrival window, EWMA + linear decay, hysteresis, and
warm-boot restore are byte-identical — only the outbound string representation
changed.
