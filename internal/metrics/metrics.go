// Package metrics owns the Prometheus registry and the named
// counters / gauges / histograms exposed on the bridge's metrics
// listener (port 8081 in prod, not exposed via IngressRoute).
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registry bundles every metric briihass emits. One Registry per
// process, constructed at startup and passed by reference into every
// consumer (ingest, presence, mqtt, admin).
type Registry struct {
	reg *prometheus.Registry

	IngestRequests        *prometheus.CounterVec // labels: endpoint, status
	GunzipFailures        prometheus.Counter
	AnonymousAdverts      *prometheus.CounterVec // labels: ap (adverts with no stable packet identity; counted, not stored)
	AdvertsByKind         *prometheus.CounterVec // labels: kind, ap (adverts that resolved to a stable identity)
	ParserErrors          *prometheus.CounterVec // labels: ap (TLV-walk errors, distinct from anonymous)
	UnknownBeacons        *prometheus.CounterVec // labels: kind, key, ap
	UnknownAP             *prometheus.CounterVec // labels: beacon, ap (closest AP is in presence but missing from zones map)
	AuthFailures          *prometheus.CounterVec // labels: endpoint, reason
	HeartbeatGateways     prometheus.Gauge       // count of currently-online gateways
	BeaconZone            *prometheus.GaugeVec   // labels: beacon, zone (value 1 when active)
	BeaconInSticky        *prometheus.GaugeVec   // labels: beacon (value 0/1)
	PerAPEffectiveRSSI    *prometheus.GaugeVec   // labels: beacon, ap
	ArrivalLatencySeconds prometheus.Histogram

	MQTTPublishOK     prometheus.Counter
	MQTTPublishErrors prometheus.Counter
	MQTTQueueDepth    prometheus.Gauge
	MQTTDropped       prometheus.Counter
	MQTTConnected     prometheus.Gauge
	MQTTMarshalFailed *prometheus.CounterVec // labels: kind ("discovery"|"attributes")

	EngineEventsDropped       prometheus.Counter
	RawPostInsertErrors       prometheus.Counter
	ObservationsRowsDropped   prometheus.Counter
	ObservationsSubmitDropped prometheus.Counter
	RetentionSkipped          prometheus.Counter
	RetentionPruneFailed      prometheus.Counter
	MQTTInitialConnectFailed  prometheus.Counter
	MQTTPublisherDropLogs     prometheus.Counter     // increments each time we log a drop (rate-limited; tracks log emissions, not drops)
	IngestBadTimestamp        *prometheus.CounterVec // labels: ap. Increments when ev.Timestamp <= 0 and the handler substitutes server time.
	TunablesOrphanOverrides   prometheus.Gauge       // count of tunables beacon-override rows whose beacon is not in the topology allowlist. Drops to 0 once an /admin/tunables save prunes them or the missing beacons get promoted via /admin/devices.
}

// New creates a Registry with all metrics constructed and
// registered. Call once at process startup.
func New() *Registry {
	reg := prometheus.NewRegistry()

	r := &Registry{
		reg: reg,

		IngestRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "briihass_ingest_requests_total",
			Help: "HTTP requests received by the ingest server.",
		}, []string{"endpoint", "status"}),

		GunzipFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "briihass_gunzip_failures_total",
			Help: "Inbound POSTs whose body could not be gunzipped.",
		}),

		AnonymousAdverts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "briihass_anonymous_adverts_total",
			Help: "events[].data values that parsed but carry no stable packet identity (rotating Apple Continuity, Eddystone-TLM/URL, EID). Counted, not stored (ADR-0008).",
		}, []string{"ap"}),

		AdvertsByKind: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "briihass_adverts_total",
			Help: "Adverts that resolved to a stable packet identity, by kind (ibeacon, eddystone_uid, eddystone_url, name, mfg).",
		}, []string{"kind", "ap"}),

		ParserErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "briihass_parser_errors_total",
			Help: "events[].data values whose BLE TLV walk failed (truncated frame, bad length octet, etc.). Distinct from anonymous — a sustained non-zero rate from one AP suggests a firmware bug.",
		}, []string{"ap"}),

		UnknownBeacons: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "briihass_unknown_beacon_total",
			Help: "Sightings whose (kind, key) is not in the allowlist. Dropped from the engine path (still observed when capture is on).",
		}, []string{"kind", "key", "ap"}),

		UnknownAP: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "briihass_unknown_ap_total",
			Help: "Times the engine's closest AP for a tracked beacon was in presence but not in the zones map. Each increment is a missed zone publish that would otherwise have fired.",
		}, []string{"beacon", "ap"}),

		AuthFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "briihass_ingest_auth_failures_total",
			Help: "Inbound POSTs rejected for bad Api-Key or disallowed source IP.",
		}, []string{"endpoint", "reason"}),

		HeartbeatGateways: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "briihass_heartbeat_gateways_online",
			Help: "Most recent /heartbeat-reported count of gateways in the Online list.",
		}),

		BeaconZone: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "briihass_beacon_zone_active",
			Help: "1 when the beacon's currently-published state matches the labelled zone, else 0.",
		}, []string{"beacon", "zone"}),

		BeaconInSticky: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "briihass_beacon_in_sticky_window",
			Help: "1 when the sticky-arrival window is currently suppressing departure for this beacon, else 0.",
		}, []string{"beacon"}),

		PerAPEffectiveRSSI: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "briihass_per_ap_effective_rssi_dbm",
			Help: "Most recently emitted effective RSSI (post-decay) for the labelled (beacon, ap). Snapshot from PresenceEvent.",
		}, []string{"beacon", "ap"}),

		ArrivalLatencySeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "briihass_arrival_latency_seconds",
			Help:    "Time from a sighting's observed timestamp to the moment its arrival-edge MQTT publish was queued. Arrival SLO metric.",
			Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1, 2, 5},
		}),

		MQTTPublishOK: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "briihass_mqtt_publish_ok_total",
			Help: "Successful MQTT publishes to Mosquitto.",
		}),

		MQTTPublishErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "briihass_mqtt_publish_errors_total",
			Help: "MQTT publish errors (timeouts, broker errors).",
		}),

		MQTTQueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "briihass_mqtt_queue_depth",
			Help: "Current depth of the publisher's in-memory queue.",
		}),

		MQTTDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "briihass_mqtt_dropped_total",
			Help: "Events the MQTT publisher dropped because its queue was full (oldest-drop policy).",
		}),

		MQTTConnected: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "briihass_mqtt_connected",
			Help: "1 when the MQTT client reports connected, else 0.",
		}),

		MQTTMarshalFailed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "briihass_mqtt_marshal_failed_total",
			Help: "JSON marshalling failures for an outbound MQTT payload. kind=\"discovery\" or \"attributes\".",
		}, []string{"kind"}),

		EngineEventsDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "briihass_engine_events_dropped_total",
			Help: "Presence events dropped because the downstream MQTT channel was full. Each drop is an arrival/departure edge that did not reach Home Assistant.",
		}),

		RawPostInsertErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "briihass_raw_post_insert_errors_total",
			Help: "Failures to persist a captured /ingest or /heartbeat envelope while capture_full_posts is on.",
		}),

		ObservationsRowsDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "briihass_observations_rows_dropped_total",
			Help: "Observation rows dropped after retry from the writer because InsertObservations kept failing. Counts individual rows (not batches) so a 256-row batch loss surfaces as 256.",
		}),

		ObservationsSubmitDropped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "briihass_observations_submit_dropped_total",
			Help: "Observations dropped at Submit time because the writer's channel was saturated (Postgres stall or writer goroutine stuck). Each increment is one sighting row that never reached the DB.",
		}),

		RetentionSkipped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "briihass_retention_skipped_total",
			Help: "Retention runs skipped because retention_days was non-positive (settings misconfigured or load failed). Each tick increments — non-zero means observations + raw_posts are growing unbounded.",
		}),

		RetentionPruneFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "briihass_retention_prune_failed_total",
			Help: "Retention prune cycles that errored out before deleting anything. Sustained non-zero means observations + raw_posts are growing unbounded despite settings being valid.",
		}),

		MQTTInitialConnectFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "briihass_mqtt_initial_connect_failed_total",
			Help: "Initial broker Connect() failed at startup. paho's auto-reconnect will keep retrying; this counter increments once per failed first attempt.",
		}),

		MQTTPublisherDropLogs: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "briihass_mqtt_publisher_drop_logs_total",
			Help: "Rate-limited drop-log emissions for the MQTT publisher queue. Drops themselves are counted by briihass_mqtt_dropped_total; this counter tracks how many times we actually wrote a log line about it.",
		}),

		IngestBadTimestamp: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "briihass_ingest_bad_timestamp_total",
			Help: "Events whose ev.Timestamp was missing or <= 0; the handler substituted server time so the observation and arrival edge still land. Sustained non-zero from one AP suggests vRIoT firmware regression.",
		}, []string{"ap"}),

		TunablesOrphanOverrides: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "briihass_tunables_orphan_overrides",
			Help: "Tunables beacon-override rows whose beacon name is absent from the topology allowlist. Inert (no beacon to apply to) but indicates the seed shipped names that were never promoted, or a demote left an override behind. The next /admin/tunables save rebuilds from topology and prunes them.",
		}),
	}

	for _, c := range []prometheus.Collector{
		r.IngestRequests, r.GunzipFailures, r.AnonymousAdverts, r.AdvertsByKind, r.ParserErrors,
		r.UnknownBeacons, r.UnknownAP, r.AuthFailures, r.HeartbeatGateways,
		r.BeaconZone, r.BeaconInSticky, r.PerAPEffectiveRSSI,
		r.ArrivalLatencySeconds, r.MQTTPublishOK, r.MQTTPublishErrors,
		r.MQTTQueueDepth, r.MQTTDropped, r.MQTTConnected, r.MQTTMarshalFailed,
		r.EngineEventsDropped, r.RawPostInsertErrors,
		r.ObservationsRowsDropped, r.ObservationsSubmitDropped,
		r.RetentionSkipped, r.RetentionPruneFailed,
		r.MQTTInitialConnectFailed, r.MQTTPublisherDropLogs,
		r.IngestBadTimestamp, r.TunablesOrphanOverrides,
	} {
		reg.MustRegister(c)
	}
	return r
}

// Handler returns the HTTP handler that exposes the metrics at
// (typically) /metrics on the secondary listener.
func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.reg, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}

// PrometheusRegistry returns the underlying *prometheus.Registry, so
// callers (notably tests) can introspect or scrape it directly.
func (r *Registry) PrometheusRegistry() *prometheus.Registry {
	return r.reg
}
