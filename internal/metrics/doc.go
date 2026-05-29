// Package metrics owns the Prometheus registry and named counters / gauges
// / histograms that briihass exposes on its metrics listener (port 8081 in
// prod, not exposed via IngressRoute).
//
// The metrics catalog is defined in ADR-0006's Verification section plus
// the ARCHITECTURE.md failure-modes table. Notable load-bearing items:
//
//   - briihass_arrival_latency_seconds (histogram): time from a sighting's
//     observation timestamp to the moment its arrival-edge MQTT publish
//     succeeds. p99 target: < 200ms. This is the arrival SLO metric.
//   - briihass_per_ap_effective_rssi{beacon, ap} (gauge): the post-decay
//     RSSI used for closest-AP ranking. Useful for tuning.
//   - briihass_beacon_in_sticky_window{beacon} (gauge 0/1): so the
//     operator can see when sticky is suppressing a departure.
package metrics
