package ingest_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"briihass/internal/config"
	"briihass/internal/ids"
	"briihass/internal/ingest"
	"briihass/internal/metrics"
	"briihass/internal/mqtt"
	"briihass/internal/presence"

	"briihass/internal/clock"

	paho "github.com/eclipse/paho.mqtt.golang"
	mochi "github.com/mochi-mqtt/server/v2"
	mochiauth "github.com/mochi-mqtt/server/v2/hooks/auth"
	mochilisteners "github.com/mochi-mqtt/server/v2/listeners"
)

// startBroker mirrors the helper in internal/mqtt/publisher_test.go.
func startBroker(t *testing.T) (string, func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()

	server := mochi.New(&mochi.Options{InlineClient: true})
	_ = server.AddHook(new(mochiauth.AllowHook), nil)
	if err := server.AddListener(mochilisteners.NewTCP(mochilisteners.Config{ID: "tcp-1", Address: addr})); err != nil {
		t.Fatalf("AddListener: %v", err)
	}
	go func() { _ = server.Serve() }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond); err == nil {
			_ = c.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	return addr, func() { _ = server.Close() }
}

// recorder subscribes to the HA Discovery topic root and captures
// every retained message we publish.
type recorder struct {
	mu     sync.Mutex
	topics map[string][][]byte
	c      paho.Client
}

func newRecorder(t *testing.T, addr string) *recorder {
	t.Helper()
	r := &recorder{topics: make(map[string][][]byte)}
	opts := paho.NewClientOptions().
		AddBroker("tcp://" + addr).
		SetClientID("integ-rec").
		SetUsername("rec").SetPassword("rec").
		SetCleanSession(true).
		SetConnectTimeout(2 * time.Second)
	r.c = paho.NewClient(opts)
	if tok := r.c.Connect(); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		t.Fatalf("recorder connect: %v", tok.Error())
	}
	tok := r.c.Subscribe("#", 0, func(_ paho.Client, m paho.Message) {
		r.mu.Lock()
		r.topics[m.Topic()] = append(r.topics[m.Topic()], append([]byte(nil), m.Payload()...))
		r.mu.Unlock()
	})
	if !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		t.Fatalf("recorder subscribe: %v", tok.Error())
	}
	return r
}

func (r *recorder) close() { r.c.Disconnect(100) }

func (r *recorder) get(topic string) [][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([][]byte, len(r.topics[topic]))
	for i, p := range r.topics[topic] {
		out[i] = append([]byte(nil), p...)
	}
	return out
}

func waitFor(t *testing.T, d time.Duration, fn func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return fn()
}

// makeIngestBody renders a synthetic /ingest payload mirroring the
// shape vRIoT actually sends. Beacon hex is the public well-known
// SDK iBeacon (FDA50693).
func makeIngestBody(t *testing.T, gateway, apName string, rssi int, beaconHex string) []byte {
	t.Helper()
	payload := map[string]any{
		"gateway_euid":  gateway,
		"ap_name":       apName,
		"ap_location":   "Test Residence",
		"ap_model":      "R770",
		"ap_ip_address": "192.0.2.34",
		"timestamp":     time.Now().UnixMilli(),
		"events": []map[string]any{
			{
				"data":        beaconHex,
				"device_euid": "00:00:00:00:00:00:00:01",
				"rssi":        rssi,
				"timestamp":   time.Now().UnixMilli(),
			},
		},
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, err := gw.Write(raw); err != nil {
		t.Fatalf("gzip: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

const realIBeaconHex = "0201061AFF4C000215FDA50693A4E24FB1AFCFC6EB07647825275165C1FD"

// TestEnd2End_CapturedCorpus wires the whole pipeline (ingest HTTP
// server -> parser -> presence engine -> MQTT publisher -> embedded
// broker -> recording subscriber) and feeds a sequence of synthetic
// /ingest POSTs through it. Asserts the expected HA Discovery +
// state + attributes messages land on the broker.
func TestEnd2End_CapturedCorpus(t *testing.T) {
	addr, stopBroker := startBroker(t)
	defer stopBroker()
	rec := newRecorder(t, addr)
	defer rec.close()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	reg := metrics.New()

	tbE2E, err := config.NewTrackedBeacon(
		ids.MustNewIBeaconKey("fda50693-a4e2-4fb1-afcf-c6eb07647825", 10065, 26049),
		"tag_e2e")
	if err != nil {
		t.Fatalf("NewTrackedBeacon: %v", err)
	}
	top, err := config.NewTopology(
		map[string]string{
			"aa:bb:cc:00:00:01": "zone_a",
			"aa:bb:cc:00:00:02": "zone_b",
		},
		[]config.TrackedBeacon{tbE2E},
	)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	tun := &config.Tunables{
		Defaults: config.DefaultsBlock{
			Alpha:               0.4,
			GracePeriodS:        5,
			DecayRateDbPerS:     2.0,
			PresenceFloorDbm:    -95,
			TAwayMaxS:           30,
			StickyAfterArrivalS: 120,
			HysteresisDb:        4.0,
			ConfirmCount:        2,
		},
		Beacons: map[string]config.Overrides{},
	}

	// Wire presence engine to a buffered channel; an adapter pushes
	// each event into the MQTT publisher.
	events := make(chan presence.PresenceEvent, 64)
	engine := presence.NewEngine(top, tun, clock.Real{}, events)
	engine.OnEventEmitted = func(ev presence.PresenceEvent) {}

	pub, err := mqtt.New(mqtt.Options{
		BrokerURL: "tcp://" + addr,
		Username:  "u",
		Password:  "p",
		ClientID:  "integ-pub",
		Logger:    logger,
	})
	if err != nil {
		t.Fatalf("mqtt.New: %v", err)
	}
	if err := pub.Connect(); err != nil {
		t.Fatalf("mqtt.Connect: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pub.Run(ctx)
	defer pub.Close()

	// Drain presence events -> publisher.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-events:
				pub.Publish(ev)
			}
		}
	}()

	srv, err := ingest.New(ingest.Options{
		APIKey:         "secret",
		PresenceSubmit: engine.Submit,
		OnHeartbeat:    func(on, off []string) {},
		BeaconLookup: func(id presence.BeaconKey) (string, bool) {
			for _, b := range top.Beacons() {
				if b.ID() == id {
					return b.Name(), true
				}
			}
			return "", false
		},
		Logger:  logger,
		Metrics: reg,
	})
	if err != nil {
		t.Fatalf("ingest.New: %v", err)
	}
	tsrv := httptest.NewServer(srv.Routes())
	defer tsrv.Close()

	// Helper that POSTs a body to the real httptest server.
	post := func(body []byte) {
		req, _ := http.NewRequest(http.MethodPost, tsrv.URL+"/ingest", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Content-Encoding", "gzip")
		req.Header.Set("Api-Key", "secret")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		if res.StatusCode != http.StatusOK {
			t.Fatalf("post status: %d", res.StatusCode)
		}
		_ = res.Body.Close()
	}

	// --- Scenario: arrive at AP-A (zone_a), transit to AP-B (zone_b).

	// Three consecutive sightings at AP-A (zone_a) to build a stable
	// EWMA.
	for i := 0; i < 3; i++ {
		post(makeIngestBody(t, "AA:BB:CC:00:00:01", "AP-A", -75, realIBeaconHex))
		time.Sleep(5 * time.Millisecond)
	}

	// Wait for the arrival publish. Device-based discovery: one config
	// topic + a BARE-string state topic (device_tracker) + a JSON telemetry
	// topic (sensors + attributes).
	const entity = "briihass_ibeacon_fda50693a4e24fb1afcfc6eb07647825_10065_26049"
	stateTopic := "briihass/" + entity + "/state"
	telemetryTopic := "briihass/" + entity + "/telemetry"
	configTopic := "homeassistant/device/" + entity + "/config"

	bareState := func(b []byte) string { return strings.TrimSpace(string(b)) }

	if !waitFor(t, 2*time.Second, func() bool {
		return len(rec.get(stateTopic)) >= 1 && len(rec.get(configTopic)) >= 1
	}) {
		t.Fatalf("missed arrival publish; state=%v config=%v", rec.get(stateTopic), rec.get(configTopic))
	}
	// The device_tracker reports a bare "home" on arrival (HA only treats
	// "home" as home); the AP-derived area label rides the telemetry topic.
	if got := bareState(rec.get(stateTopic)[0]); got != "home" {
		t.Errorf("first state = %q, want bare home", got)
	}
	lastArea := func() string {
		tels := rec.get(telemetryTopic)
		if len(tels) == 0 {
			return ""
		}
		var st map[string]any
		if err := json.Unmarshal(tels[len(tels)-1], &st); err != nil {
			t.Fatalf("telemetry unmarshal: %v", err)
		}
		s, _ := st["area"].(string)
		return s
	}
	if !waitFor(t, 2*time.Second, func() bool { return lastArea() == "zone_a" }) {
		t.Fatalf("missed zone_a area; last telemetry area=%q", lastArea())
	}

	// Now drive a transit to AP-B with strong signal twice to
	// satisfy hysteresis (4 dB margin, 2-sample confirm). The tracker stays
	// "home"; the area sensor (telemetry.area) reflects the zone change.
	for i := 0; i < 3; i++ {
		post(makeIngestBody(t, "AA:BB:CC:00:00:02", "AP-B", -65, realIBeaconHex))
		time.Sleep(5 * time.Millisecond)
	}

	if !waitFor(t, 2*time.Second, func() bool { return lastArea() == "zone_b" }) {
		t.Fatalf("missed zone_b area; last telemetry area=%q, tracker states=%v",
			lastArea(), asStrings(rec.get(stateTopic)))
	}

	// The tracker must remain a bare "home" across the zone change — only
	// the area sensor reflects the zone, never the tracker state.
	if states := rec.get(stateTopic); bareState(states[len(states)-1]) != "home" {
		t.Errorf("tracker flipped off home on zone change: states=%v", asStrings(states))
	}

	// The JSON telemetry document carries the area + AP context.
	var st map[string]any
	if err := json.Unmarshal(rec.get(telemetryTopic)[len(rec.get(telemetryTopic))-1], &st); err != nil {
		t.Fatalf("telemetry unmarshal: %v", err)
	}
	if st["area"] != "zone_b" {
		t.Errorf("telemetry.area = %v, want zone_b", st["area"])
	}
	if st["ap_mac"] != "aa:bb:cc:00:00:02" {
		t.Errorf("telemetry.ap_mac = %v, want aa:bb:cc:00:00:02", st["ap_mac"])
	}

	// Discovery published exactly once (no republish on subsequent state
	// changes; the iBeacon corpus carries no telemetry, so no lazy growth).
	if got := len(rec.get(configTopic)); got != 1 {
		t.Errorf("config publishes = %d, want 1", got)
	}
}

func asStrings(in [][]byte) []string {
	out := make([]string, len(in))
	for i, b := range in {
		out[i] = string(b)
	}
	return out
}
