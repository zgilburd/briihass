// Package presence holds the per-beacon presence state machine: per-(beacon,
// AP) exponentially-weighted RSSI with linear decay, closest-AP-wins zone
// resolution, and the sticky-arrival window that prevents flap during noisy
// arrival patterns.
//
// All state is in-memory. The bridge does not persist presence across
// restarts; the next sighting re-converges state (see ADR-0006).
//
// Two hard invariants that must be preserved across any future change:
//  1. not_home -> <any zone> publishes on the first qualifying sighting,
//     with no K-of-N gating, no window averaging delay, no hysteresis.
//     Arrival latency is load-bearing for beacon-driven HA automations.
//  2. <any zone> -> not_home is suppressed for sticky_after_arrival_s
//     after each arrival. Zone-to-zone updates still publish during the
//     sticky window; only the not_home transition is held.
package presence
