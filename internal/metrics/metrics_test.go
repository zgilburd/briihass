package metrics

import (
	"io"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNew_CollectorsRegistered(t *testing.T) {
	r := New()
	r.IngestRequests.WithLabelValues("/ingest", "200").Inc()
	r.GunzipFailures.Inc()
	r.ArrivalLatencySeconds.Observe(0.05)
	r.BeaconZone.WithLabelValues("tag_one", "zone_a").Set(1)
	r.MQTTPublishOK.Inc()
	r.MQTTConnected.Set(1)

	srv := httptest.NewServer(r.Handler())
	defer srv.Close()

	res, err := srv.Client().Get(srv.URL)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	out := string(raw)

	for _, want := range []string{
		"briihass_ingest_requests_total",
		"briihass_gunzip_failures_total",
		"briihass_arrival_latency_seconds_bucket",
		"briihass_beacon_zone_active",
		"briihass_mqtt_publish_ok_total",
		"briihass_mqtt_connected",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics output missing %q\n--- output ---\n%s", want, out)
		}
	}
}
