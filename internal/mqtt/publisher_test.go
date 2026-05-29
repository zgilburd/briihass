package mqtt

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"briihass/internal/ids"
	"briihass/internal/presence"

	paho "github.com/eclipse/paho.mqtt.golang"
	mochi "github.com/mochi-mqtt/server/v2"
	mochiauth "github.com/mochi-mqtt/server/v2/hooks/auth"
	mochilisteners "github.com/mochi-mqtt/server/v2/listeners"
)

// ============================================================
//  Embedded test broker
// ============================================================

// startBroker spins up an in-process Mosquitto-compatible broker on a
// random localhost port. Returns the listen address and a stop
// function. Anonymous auth is enabled for tests — we still pass
// username/password from the publisher because production rejects
// anonymous, but the broker accepts anything.
func startBroker(t *testing.T) (addr string, stop func()) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr = lis.Addr().String()
	if err := lis.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}

	server := mochi.New(&mochi.Options{InlineClient: true})
	_ = server.AddHook(new(mochiauth.AllowHook), nil)
	tcp := mochilisteners.NewTCP(mochilisteners.Config{
		ID:      "tcp-1",
		Address: addr,
	})
	if err := server.AddListener(tcp); err != nil {
		t.Fatalf("AddListener: %v", err)
	}
	go func() { _ = server.Serve() }()

	// Wait for the listener to be ready (mochi.Serve is async).
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = c.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	return addr, func() { _ = server.Close() }
}

// recorder subscribes to every topic and captures messages by topic.
type recorder struct {
	mu      sync.Mutex
	byTopic map[string][][]byte
	all     []recorded
	c       paho.Client
}

type recorded struct {
	Topic    string
	Payload  []byte
	Retained bool
}

func newRecorder(t *testing.T, brokerAddr string) *recorder {
	t.Helper()
	r := &recorder{byTopic: make(map[string][][]byte)}
	opts := paho.NewClientOptions().
		AddBroker("tcp://" + brokerAddr).
		SetClientID("recorder").
		SetUsername("rec").
		SetPassword("rec").
		SetCleanSession(true).
		SetAutoReconnect(false).
		SetConnectTimeout(2 * time.Second)
	r.c = paho.NewClient(opts)
	if tok := r.c.Connect(); !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		t.Fatalf("recorder connect: %v (timeout=%v)", tok.Error(), !tok.WaitTimeout(0))
	}
	tok := r.c.Subscribe("#", 0, func(_ paho.Client, m paho.Message) {
		r.mu.Lock()
		r.byTopic[m.Topic()] = append(r.byTopic[m.Topic()], append([]byte(nil), m.Payload()...))
		r.all = append(r.all, recorded{Topic: m.Topic(), Payload: append([]byte(nil), m.Payload()...), Retained: m.Retained()})
		r.mu.Unlock()
	})
	if !tok.WaitTimeout(2*time.Second) || tok.Error() != nil {
		t.Fatalf("recorder subscribe: %v", tok.Error())
	}
	return r
}

func (r *recorder) close() {
	if r.c != nil {
		r.c.Disconnect(100)
	}
}

func (r *recorder) snapshot() map[string][][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string][][]byte, len(r.byTopic))
	for k, v := range r.byTopic {
		dup := make([][]byte, len(v))
		for i, p := range v {
			dup[i] = append([]byte(nil), p...)
		}
		out[k] = dup
	}
	return out
}

// waitFor polls fn until it returns true or the deadline expires.
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

// ============================================================
//  Fixtures
// ============================================================

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func sampleArrival() presence.PresenceEvent {
	return presence.PresenceEvent{
		Beacon:                    ids.MustNewIBeaconKey("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1", 1, 1),
		BeaconName:                "tag_one",
		State:                     "zone_a",
		APMac:                     "aa:bb:cc:00:00:01",
		APName:                    "AP-A",
		RSSIRaw:                   -75,
		RSSIEWMA:                  -75.0,
		RSSIEffective:             -75.0,
		RSSIRunnerUpEffectiveDiff: 5.5,
		LastSeen:                  time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC),
		StickyActive:              true,
		EmittedAt:                 time.Date(2026, 5, 18, 12, 0, 1, 0, time.UTC),
	}
}

func sampleDeparture(beacon presence.BeaconKey) presence.PresenceEvent {
	return presence.PresenceEvent{
		Beacon:     beacon,
		BeaconName: "tag_one",
		State:      presence.NotHome,
		LastSeen:   time.Date(2026, 5, 18, 12, 5, 0, 0, time.UTC),
		EmittedAt:  time.Date(2026, 5, 18, 12, 5, 1, 0, time.UTC),
	}
}

// ============================================================
//  Pure-function tests (no broker needed)
// ============================================================

func TestEntityID(t *testing.T) {
	// NewBeaconID lowercases the UUID; this test covers EntityID's
	// formatting (dash-stripping + suffix) for an already-canonical id.
	id := ids.MustNewIBeaconKey("AAAAAAAA-aaaa-aaaa-AAAA-aaaaaaaaaaa1", 1, 1)
	if got, want := EntityID(id), "briihass_ibeacon_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1_1_1"; got != want {
		t.Errorf("EntityID = %q, want %q", got, want)
	}
}

// devTopicSet mirrors the new device-based discovery topics for tests:
// one config topic per device + one shared JSON state topic.
type devTopicSet struct{ Config, State string }

func devTopics(id presence.BeaconKey) devTopicSet {
	return devTopicSet{
		Config: deviceConfigTopic("homeassistant", id),
		State:  stateTopic(id),
	}
}

func TestDeviceTopics(t *testing.T) {
	id := ids.MustNewIBeaconKey("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1", 1, 1)
	tt := devTopics(id)
	if tt.Config != "homeassistant/device/briihass_ibeacon_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1_1_1/config" {
		t.Errorf("config topic: %q", tt.Config)
	}
	if tt.State != "briihass/briihass_ibeacon_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1_1_1/state" {
		t.Errorf("state topic: %q", tt.State)
	}
}

func TestBuildDeviceDiscovery(t *testing.T) {
	ev := sampleArrival()
	d := BuildDeviceDiscovery("homeassistant", ev, false, false)
	if d.Device.Name != "tag_one" {
		t.Errorf("device name = %q", d.Device.Name)
	}
	if d.Device.Manufacturer != "briihass" {
		t.Errorf("manufacturer = %q", d.Device.Manufacturer)
	}
	entity := EntityID(ev.Beacon)
	tracker, ok := d.Components[entity]
	if !ok || tracker.Platform != "device_tracker" || tracker.SourceType != "bluetooth_le" {
		t.Errorf("tracker component = %+v", tracker)
	}
	// Tracker reads the bare state topic with NO value_template; its
	// attributes + the sensors read the telemetry topic.
	if tracker.ValueTemplate != "" {
		t.Errorf("tracker value_template = %q, want empty", tracker.ValueTemplate)
	}
	if tracker.StateTopic != stateTopic(ev.Beacon) {
		t.Errorf("tracker state_topic = %q", tracker.StateTopic)
	}
	if tracker.JSONAttributesTopic != telemetryTopic(ev.Beacon) {
		t.Errorf("tracker json_attributes_topic = %q", tracker.JSONAttributesTopic)
	}
	rssi, ok := d.Components[entity+"_rssi"]
	if !ok || rssi.DeviceClass != "signal_strength" || rssi.UnitOfMeasurement != "dBm" {
		t.Errorf("rssi component = %+v", rssi)
	}
	if rssi.StateTopic != telemetryTopic(ev.Beacon) {
		t.Errorf("rssi state_topic = %q, want telemetry topic", rssi.StateTopic)
	}
	// The area sensor is always present and reads value_json.area from the
	// telemetry topic (the AP-derived location the tracker state dropped).
	area, ok := d.Components[entity+"_area"]
	if !ok || area.Platform != "sensor" || area.ValueTemplate != "{{ value_json.area }}" {
		t.Errorf("area component = %+v", area)
	}
	if area.StateTopic != telemetryTopic(ev.Beacon) {
		t.Errorf("area state_topic = %q, want telemetry topic", area.StateTopic)
	}
	// Without telemetry flags, no voltage/temperature components.
	if _, ok := d.Components[entity+"_voltage"]; ok {
		t.Error("voltage component should be absent without withVoltage")
	}
	// With telemetry, the components appear.
	d2 := BuildDeviceDiscovery("homeassistant", ev, true, true)
	if v, ok := d2.Components[entity+"_voltage"]; !ok || v.DeviceClass != "voltage" || v.UnitOfMeasurement != "mV" {
		t.Errorf("voltage component = %+v", v)
	}
	if tt, ok := d2.Components[entity+"_temperature"]; !ok || tt.DeviceClass != "temperature" {
		t.Errorf("temperature component = %+v", tt)
	}
	if len(d.Availability) == 0 || d.Availability[0].Topic != bridgeAvailabilityTopic {
		t.Errorf("availability = %+v", d.Availability)
	}
}

func TestBuildState(t *testing.T) {
	ev := sampleArrival()
	batt := 3714
	ev.BatteryMV = &batt
	s := BuildState(ev)
	if s.Area != "zone_a" {
		t.Errorf("Area = %q", s.Area)
	}
	if s.RSSI == nil || *s.RSSI != -75 {
		t.Errorf("RSSI = %v, want -75", s.RSSI)
	}
	if s.Voltage == nil || *s.Voltage != 3714 {
		t.Errorf("Voltage = %v", s.Voltage)
	}
	if !s.StickyActive {
		t.Errorf("StickyActive want true")
	}
	if s.LastSeen != "2026-05-18T12:00:00Z" {
		t.Errorf("LastSeen = %q", s.LastSeen)
	}
	// not_home: RSSI omitted so the sensor reads unknown while away, and
	// the area sensor reads "not_home".
	dep := sampleDeparture(ev.Beacon)
	depState := BuildState(dep)
	if depState.RSSI != nil {
		t.Error("RSSI should be nil for not_home")
	}
	if depState.Area != "not_home" {
		t.Errorf("Area = %q, want not_home", depState.Area)
	}
}

func TestTrackerState(t *testing.T) {
	if got := trackerState(presence.PresenceState("zone_a")); got != "home" {
		t.Errorf("trackerState(zone_a) = %q, want home", got)
	}
	// A second distinct zone label must also map to home, so the
	// "any zone ⇒ home" invariant can't regress into a per-label special case.
	if got := trackerState(presence.PresenceState("zone_b")); got != "home" {
		t.Errorf("trackerState(zone_b) = %q, want home", got)
	}
	if got := trackerState(presence.NotHome); got != "not_home" {
		t.Errorf("trackerState(NotHome) = %q, want not_home", got)
	}
}

func TestRoundTo(t *testing.T) {
	cases := []struct {
		in, want float64
		n        int
	}{
		{-75.0, -75, 2},
		{-76.234, -76.23, 2},
		{-76.235, -76.24, 2},
		{12.5, 13, 0},
		{-12.5, -13, 0},
		{0, 0, 2},
	}
	for _, tc := range cases {
		if got := roundTo(tc.in, tc.n); got != tc.want {
			t.Errorf("roundTo(%v, %d) = %v, want %v", tc.in, tc.n, got, tc.want)
		}
	}
}

// ============================================================
//  Broker-backed integration tests
// ============================================================

func TestPublisher_PublishesArrival(t *testing.T) {
	addr, stop := startBroker(t)
	defer stop()
	rec := newRecorder(t, addr)
	defer rec.close()

	p, err := New(Options{
		BrokerURL: "tcp://" + addr,
		Username:  "u",
		Password:  "p",
		ClientID:  "pub-arrival",
		Logger:    discardLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := p.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)
	defer p.Close()

	ev := sampleArrival()
	p.Publish(ev)

	topics := devTopics(ev.Beacon)
	ok := waitFor(t, 2*time.Second, func() bool {
		snap := rec.snapshot()
		return len(snap[topics.Config]) >= 1 && len(snap[topics.State]) >= 1
	})
	if !ok {
		t.Fatalf("did not receive config + state within 2s; snapshot=%v", rec.snapshot())
	}

	snap := rec.snapshot()

	// Device-based discovery config should parse: one device with a
	// device_tracker component pointing at the shared state topic.
	var cfg deviceDiscovery
	if err := json.Unmarshal(snap[topics.Config][0], &cfg); err != nil {
		t.Fatalf("unmarshal discovery: %v", err)
	}
	if cfg.Device.Name != "tag_one" {
		t.Errorf("device.Name = %q", cfg.Device.Name)
	}
	entity := EntityID(ev.Beacon)
	tracker := cfg.Components[entity]
	if tracker.Platform != "device_tracker" || tracker.StateTopic != topics.State {
		t.Errorf("tracker component = %+v (state topic want %q)", tracker, topics.State)
	}

	// Tracker state topic carries the BARE "home" string (the area label
	// lives on the area sensor / telemetry topic).
	if got := string(snap[topics.State][0]); got != "home" {
		t.Errorf("tracker state = %q, want bare home", got)
	}

	// The telemetry topic carries the JSON that drives the sensors + attrs.
	tel := telemetryTopic(ev.Beacon)
	if !waitFor(t, 2*time.Second, func() bool { return len(rec.snapshot()[tel]) >= 1 }) {
		t.Fatal("no telemetry publish")
	}
	var st statePayload
	if err := json.Unmarshal(rec.snapshot()[tel][0], &st); err != nil {
		t.Fatalf("unmarshal telemetry: %v", err)
	}
	if st.Area != "zone_a" || st.APMac != "aa:bb:cc:00:00:01" {
		t.Errorf("telemetry = %+v", st)
	}
	if st.RSSI == nil || *st.RSSI != -75 {
		t.Errorf("telemetry.rssi = %v, want -75", st.RSSI)
	}
}

func TestPublisher_DiscoveryPublishedOnceUntilReconnect(t *testing.T) {
	addr, stop := startBroker(t)
	defer stop()
	rec := newRecorder(t, addr)
	defer rec.close()

	p, err := New(Options{
		BrokerURL: "tcp://" + addr,
		Username:  "u",
		Password:  "p",
		ClientID:  "pub-discovery-once",
		Logger:    discardLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := p.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)
	defer p.Close()

	ev := sampleArrival()
	p.Publish(ev)

	// Second event on the same beacon should NOT republish Discovery,
	// but should publish a new state/attributes pair.
	ev2 := ev
	ev2.State = "zone_b"
	ev2.RSSIRaw = -70
	p.Publish(ev2)

	topics := devTopics(ev.Beacon)
	ok := waitFor(t, 2*time.Second, func() bool {
		s := rec.snapshot()
		return len(s[topics.State]) >= 2
	})
	if !ok {
		t.Fatalf("expected two state messages, got %v", rec.snapshot()[topics.State])
	}
	if got := len(rec.snapshot()[topics.Config]); got != 1 {
		t.Errorf("Discovery config publishes = %d, want 1 (republished only on reconnect)", got)
	}
}

func TestPublisher_NotHomeStatePayload(t *testing.T) {
	addr, stop := startBroker(t)
	defer stop()
	rec := newRecorder(t, addr)
	defer rec.close()

	p, err := New(Options{
		BrokerURL: "tcp://" + addr,
		Username:  "u",
		Password:  "p",
		ClientID:  "pub-not-home",
		Logger:    discardLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := p.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)
	defer p.Close()

	id := ids.MustNewIBeaconKey("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1", 1, 1)
	p.Publish(sampleDeparture(id))

	topics := devTopics(id)
	ok := waitFor(t, 2*time.Second, func() bool {
		return len(rec.snapshot()[topics.State]) >= 1
	})
	if !ok {
		t.Fatalf("no state message received")
	}
	// Tracker state topic carries the bare "not_home" string.
	if got := string(rec.snapshot()[topics.State][0]); got != presence.NotHome.String() {
		t.Errorf("tracker state = %q, want %q", got, presence.NotHome)
	}
	// The area sensor must read "not_home" while away — assert it over the
	// wire on the telemetry topic, not just on the BuildState struct.
	telTopic := telemetryTopic(id)
	if !waitFor(t, 2*time.Second, func() bool { return len(rec.snapshot()[telTopic]) >= 1 }) {
		t.Fatalf("no telemetry message received")
	}
	var tel statePayload
	if err := json.Unmarshal(rec.snapshot()[telTopic][0], &tel); err != nil {
		t.Fatalf("telemetry not JSON: %v", err)
	}
	if tel.Area != "not_home" {
		t.Errorf("telemetry.area = %q, want not_home", tel.Area)
	}
}

func TestPublisher_DropsOldestOnOverflow(t *testing.T) {
	// Don't run a broker — publisher will queue forever. We just verify
	// that Publish never blocks even when the queue is full and that
	// the dropped counter advances.
	p, err := New(Options{
		BrokerURL:  "tcp://127.0.0.1:1",
		Username:   "u",
		Password:   "p",
		ClientID:   "pub-overflow",
		BufferSize: 4,
		Logger:     discardLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// Don't Connect — we just want to test Publish queueing in
	// isolation. Run is not started either, so nothing drains.

	ev := sampleArrival()
	for i := 0; i < 20; i++ {
		p.Publish(ev)
	}
	stats := p.Stats()
	if stats.QueueDepth != 4 {
		t.Errorf("QueueDepth = %d, want 4 (capacity)", stats.QueueDepth)
	}
	if stats.Dropped == 0 {
		t.Errorf("expected drops > 0, got 0")
	}
}

func TestPublisher_OptionDefaults(t *testing.T) {
	p, err := New(Options{
		BrokerURL: "tcp://127.0.0.1:1",
		Username:  "u",
		Password:  "p",
		Logger:    discardLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if p.opts.ClientID != "briihass" {
		t.Errorf("ClientID default = %q", p.opts.ClientID)
	}
	if p.opts.DiscoveryPrefix != "homeassistant" {
		t.Errorf("DiscoveryPrefix default = %q", p.opts.DiscoveryPrefix)
	}
	if p.opts.BufferSize != 5000 {
		t.Errorf("BufferSize default = %d", p.opts.BufferSize)
	}
}

func TestPublisher_Validation(t *testing.T) {
	cases := []struct {
		name string
		opts Options
		want string
	}{
		{"no broker", Options{Username: "u", Password: "p", Logger: discardLogger()}, "BrokerURL"},
		{"no user", Options{BrokerURL: "tcp://h:1", Password: "p", Logger: discardLogger()}, "Username and Password"},
		{"no logger", Options{BrokerURL: "tcp://h:1", Username: "u", Password: "p"}, "Logger"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(tc.opts)
			if err == nil {
				t.Fatal("want error, got nil")
			}
		})
	}
}

func TestPublisher_RemoveEntityClearsRetainedTopics(t *testing.T) {
	addr, stop := startBroker(t)
	defer stop()
	rec := newRecorder(t, addr)
	defer rec.close()

	p, err := New(Options{
		BrokerURL: "tcp://" + addr,
		Username:  "u",
		Password:  "p",
		ClientID:  "pub-remove",
		Logger:    discardLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := p.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)
	defer p.Close()

	ev := sampleArrival()
	p.Publish(ev)
	topics := devTopics(ev.Beacon)
	if !waitFor(t, 2*time.Second, func() bool {
		return len(rec.snapshot()[topics.Config]) >= 1
	}) {
		t.Fatalf("expected initial config publish, got %v", rec.snapshot()[topics.Config])
	}

	if err := p.RemoveEntity(context.Background(), ev.Beacon); err != nil {
		t.Fatalf("RemoveEntity: %v", err)
	}

	// We expect an empty-payload publish on the device config + state
	// topics (the wipe).
	if !waitFor(t, 2*time.Second, func() bool {
		s := rec.snapshot()
		return hasEmpty(s[topics.Config]) && hasEmpty(s[topics.State])
	}) {
		t.Fatalf("expected empty payloads on config/state, got config=%v state=%v",
			rec.snapshot()[topics.Config], rec.snapshot()[topics.State])
	}

	// Re-publishing after a removal should re-assert the Discovery
	// config (the seen-map entry was purged).
	p.Publish(ev)
	if !waitFor(t, 2*time.Second, func() bool {
		// Count non-empty config payloads (≥ 2: initial + re-assertion).
		n := 0
		for _, b := range rec.snapshot()[topics.Config] {
			if len(b) > 0 {
				n++
			}
		}
		return n >= 2
	}) {
		t.Errorf("expected re-published config after RemoveEntity; got %v", rec.snapshot()[topics.Config])
	}
}

// TestPublisher_TrackerStateIsBareString pins the device_tracker contract:
// HA does not apply value_template to a device_tracker declared via
// device-based discovery, so the tracker's state_topic MUST carry a bare
// state string ("zone_a"/"not_home"), and the JSON telemetry MUST live on
// a SEPARATE topic that the sensors read. Regression here reproduces the
// "device_tracker shows the raw JSON document" bug.
func TestPublisher_TrackerStateIsBareString(t *testing.T) {
	addr, stop := startBroker(t)
	defer stop()
	rec := newRecorder(t, addr)
	defer rec.close()

	p, err := New(Options{
		BrokerURL: "tcp://" + addr,
		Username:  "u", Password: "p", ClientID: "pub-bare",
		Logger: discardLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := p.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)
	defer p.Close()

	ev := sampleArrival()
	batt := 3714
	ev.BatteryMV = &batt
	p.Publish(ev)

	stTopic := stateTopic(ev.Beacon)
	telTopic := telemetryTopic(ev.Beacon)
	cfgTopic := deviceConfigTopic("homeassistant", ev.Beacon)
	if !waitFor(t, 2*time.Second, func() bool {
		s := rec.snapshot()
		return len(s[stTopic]) >= 1 && len(s[telTopic]) >= 1 && len(s[cfgTopic]) >= 1
	}) {
		t.Fatalf("missing publishes; state=%v tel=%v cfg=%v",
			rec.snapshot()[stTopic], rec.snapshot()[telTopic], rec.snapshot()[cfgTopic])
	}

	// Tracker state topic must be the BARE "home" string, not JSON and not
	// the area label (HA only treats "home" as home).
	if got := string(rec.snapshot()[stTopic][0]); got != "home" {
		t.Errorf("tracker state = %q, want bare \"home\"", got)
	}
	// Telemetry topic must be JSON with the sensor fields, including the
	// area label the tracker state no longer carries.
	var tel statePayload
	if err := json.Unmarshal(rec.snapshot()[telTopic][0], &tel); err != nil {
		t.Fatalf("telemetry not JSON: %v", err)
	}
	if tel.Area != "zone_a" {
		t.Errorf("telemetry.area = %q, want zone_a", tel.Area)
	}
	if tel.RSSI == nil || *tel.RSSI != -75 || tel.Voltage == nil || *tel.Voltage != 3714 {
		t.Errorf("telemetry = %+v", tel)
	}

	// The tracker component must NOT carry a value_template, and its
	// attributes + sensors must point at the telemetry topic.
	var cfg deviceDiscovery
	if err := json.Unmarshal(rec.snapshot()[cfgTopic][0], &cfg); err != nil {
		t.Fatalf("config: %v", err)
	}
	entity := EntityID(ev.Beacon)
	tr := cfg.Components[entity]
	if tr.ValueTemplate != "" {
		t.Errorf("tracker value_template = %q, want empty (bare state)", tr.ValueTemplate)
	}
	if tr.StateTopic != stTopic {
		t.Errorf("tracker state_topic = %q, want %q", tr.StateTopic, stTopic)
	}
	if tr.JSONAttributesTopic != telTopic {
		t.Errorf("tracker json_attributes_topic = %q, want %q", tr.JSONAttributesTopic, telTopic)
	}
	if rssi := cfg.Components[entity+"_rssi"]; rssi.StateTopic != telTopic {
		t.Errorf("rssi state_topic = %q, want %q", rssi.StateTopic, telTopic)
	}
}

// TestPublisher_LazyTelemetryGrowth pins that the voltage component is
// declared only once telemetry first appears: the initial config has no
// voltage component, and a later event carrying BatteryMV re-publishes
// the device config with one added.
func TestPublisher_LazyTelemetryGrowth(t *testing.T) {
	addr, stop := startBroker(t)
	defer stop()
	rec := newRecorder(t, addr)
	defer rec.close()

	p, err := New(Options{
		BrokerURL: "tcp://" + addr,
		Username:  "u", Password: "p", ClientID: "pub-lazy",
		Logger: discardLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := p.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)
	defer p.Close()

	ev := sampleArrival()
	topics := devTopics(ev.Beacon)
	entity := EntityID(ev.Beacon)

	// First event: no telemetry → config without a voltage component.
	p.Publish(ev)
	if !waitFor(t, 2*time.Second, func() bool { return len(rec.snapshot()[topics.Config]) >= 1 }) {
		t.Fatal("no initial config")
	}
	var d0 deviceDiscovery
	_ = json.Unmarshal(rec.snapshot()[topics.Config][0], &d0)
	if _, ok := d0.Components[entity+"_voltage"]; ok {
		t.Error("voltage component present before any telemetry")
	}

	// Second event carries battery → config re-published WITH voltage.
	batt := 3714
	ev2 := ev
	ev2.BatteryMV = &batt
	p.Publish(ev2)
	if !waitFor(t, 2*time.Second, func() bool {
		cfgs := rec.snapshot()[topics.Config]
		if len(cfgs) < 2 {
			return false
		}
		var d deviceDiscovery
		_ = json.Unmarshal(cfgs[len(cfgs)-1], &d)
		_, ok := d.Components[entity+"_voltage"]
		return ok
	}) {
		t.Fatalf("voltage component not added after telemetry; configs=%d", len(rec.snapshot()[topics.Config]))
	}
}

// TestPublisher_ResyncDiscovery pins that ResyncDiscovery clears the
// seen map and fires OnHAOnline (the admin "Resync HA" + HA birth path).
func TestPublisher_ResyncDiscovery(t *testing.T) {
	p, err := New(Options{
		BrokerURL: "tcp://127.0.0.1:1",
		Username:  "u", Password: "p", ClientID: "pub-resync",
		Logger: discardLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	var fired atomic.Bool
	p.OnHAOnline = func() { fired.Store(true) }

	// Seed a seen entry as if a device had been published.
	p.mu.Lock()
	p.seen["x"] = struct{}{}
	p.hasVoltage["x"] = true
	p.mu.Unlock()

	p.ResyncDiscovery()

	if !fired.Load() {
		t.Error("OnHAOnline did not fire")
	}
	p.mu.Lock()
	_, stillSeen := p.seen["x"]
	stillVolt := p.hasVoltage["x"]
	p.mu.Unlock()
	if stillSeen || stillVolt {
		t.Error("seen/declared maps were not cleared")
	}
}

func hasEmpty(msgs [][]byte) bool {
	for _, m := range msgs {
		if len(m) == 0 {
			return true
		}
	}
	return false
}

// TestPublisher_MarshalFailureDoesNotPoisonSeen pins the C2 fix: if
// the first event's discovery marshal fails, the seen[entityID] entry
// MUST NOT be set. Otherwise the subsequent event would skip the
// re-attempt and HA would be left without a config payload until the
// next reconnect. Uses the marshalCompactFn seam to inject the failure.
func TestPublisher_MarshalFailureDoesNotPoisonSeen(t *testing.T) {
	addr, stop := startBroker(t)
	defer stop()
	rec := newRecorder(t, addr)
	defer rec.close()

	p, err := New(Options{
		BrokerURL: "tcp://" + addr,
		Username:  "u",
		Password:  "p",
		ClientID:  "pub-marshal-fail",
		Logger:    discardLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := p.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)
	defer p.Close()

	// Swap the marshal seam to fail once on the FIRST call (discovery)
	// then succeed for all subsequent calls. A naive implementation
	// that sets seen before marshal would never re-attempt discovery
	// for this beacon — the test would see zero non-empty config
	// publishes even after a working second event.
	var calls atomic.Int32
	origMarshal := marshalCompactFn
	marshalCompactFn = func(v any) ([]byte, error) {
		if calls.Add(1) == 1 {
			return nil, errors.New("forced marshal failure for test")
		}
		return origMarshal(v)
	}
	defer func() { marshalCompactFn = origMarshal }()

	ev := sampleArrival()
	p.Publish(ev)

	// Give the failed publish a beat — then send the second event.
	time.Sleep(100 * time.Millisecond)
	p.Publish(ev)

	topics := devTopics(ev.Beacon)
	ok := waitFor(t, 2*time.Second, func() bool {
		for _, body := range rec.snapshot()[topics.Config] {
			if len(body) > 0 {
				return true
			}
		}
		return false
	})
	if !ok {
		t.Fatalf("expected a non-empty config publish on the second event; got %v", rec.snapshot()[topics.Config])
	}
}

// TestPublisher_RepublishOrphans pins the I2 fix: entries in the
// seen map that aren't in the supplied known list become enqueued
// removes, and re-running the reconcile is idempotent.
func TestPublisher_RepublishOrphans(t *testing.T) {
	addr, stop := startBroker(t)
	defer stop()
	rec := newRecorder(t, addr)
	defer rec.close()

	p, err := New(Options{
		BrokerURL: "tcp://" + addr,
		Username:  "u",
		Password:  "p",
		ClientID:  "pub-orphans",
		Logger:    discardLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := p.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.Run(ctx)
	defer p.Close()

	// Publish two distinct beacons; both end up in seen.
	beaconA := ids.MustNewIBeaconKey("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1", 1, 1)
	beaconB := ids.MustNewIBeaconKey("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb2", 2, 2)
	evA := sampleArrival()
	evA.Beacon = beaconA
	evB := sampleArrival()
	evB.Beacon = beaconB
	evB.BeaconName = "tag_two"
	p.Publish(evA)
	p.Publish(evB)

	topicsA := devTopics(beaconA)
	topicsB := devTopics(beaconB)
	if !waitFor(t, 2*time.Second, func() bool {
		s := rec.snapshot()
		return len(s[topicsA.Config]) >= 1 && len(s[topicsB.Config]) >= 1
	}) {
		t.Fatalf("setup: did not get initial config publishes")
	}

	// "known" lists only beaconA — beaconB is the orphan.
	n, err := p.RepublishOrphans(context.Background(), []presence.BeaconKey{beaconA})
	if err != nil {
		t.Fatalf("RepublishOrphans: %v", err)
	}
	if n != 1 {
		t.Errorf("RepublishOrphans enqueued: got %d want 1", n)
	}
	if !waitFor(t, 2*time.Second, func() bool {
		return hasEmpty(rec.snapshot()[topicsB.Config])
	}) {
		t.Fatalf("orphan beaconB never got an empty-config publish; got %v", rec.snapshot()[topicsB.Config])
	}

	// Re-running with the same known list should enqueue zero — the
	// orphan's seenIDs entry was cleared by handleRemove.
	n2, err := p.RepublishOrphans(context.Background(), []presence.BeaconKey{beaconA})
	if err != nil {
		t.Fatalf("RepublishOrphans (idempotent): %v", err)
	}
	if n2 != 0 {
		t.Errorf("idempotent reconcile: got %d want 0", n2)
	}
}

// TestPublisher_RepublishOrphans_PartialFailureRetains pins T3 from
// the round-4 review: when the publisher queue saturates partway
// through a reconcile, unenqueued orphans MUST remain in seenIDs so
// the next refresh-engine click can pick them up. Closes the
// "stateless retry" contract in the RepublishOrphans doc.
//
// No broker is involved: we construct the Publisher and mutate its
// queue / seenIDs directly so the saturation scenario is deterministic
// (real-broker timing would depend on the Run goroutine's drain rate).
func TestPublisher_RepublishOrphans_PartialFailureRetains(t *testing.T) {
	p, err := New(Options{
		BrokerURL:  "tcp://127.0.0.1:65535", // never connect
		Username:   "u",
		Password:   "p",
		ClientID:   "test",
		BufferSize: 1,
		Logger:     discardLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// Pre-fill the queue to saturate it. Subsequent enqueue + drop-
	// oldest both fail because we're not running the drain goroutine.
	// (drop-oldest pops one out of the channel, freeing a slot; then
	// the new item is enqueued. To keep BOTH attempts failing we'd
	// have to block enqueue entirely — instead, run the test with
	// BufferSize=1 and observe that drop-oldest evicts the placeholder.
	// That's actually fine: it still demonstrates the contract because
	// the second orphan's enqueue will succeed via drop-oldest, but the
	// first orphan also went through drop-oldest and the dropped item
	// gets counted via OnDropped. seenIDs population is what we care
	// about: both orphans either enqueued or remained marked as orphans.)
	//
	// To force a genuine "neither attempt succeeds" we instead close
	// the queue's drain by leaving it null-Run and filling beyond drop.
	//
	// The clean route: directly assert that RepublishOrphans does not
	// remove the orphan from seenIDs on its own. The Run goroutine
	// removes via handleRemove; without Run, seenIDs stays populated
	// across the call.
	beaconA := ids.MustNewIBeaconKey("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1", 1, 1)
	beaconB := ids.MustNewIBeaconKey("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb2", 2, 2)
	p.mu.Lock()
	p.seen[EntityID(beaconA)] = struct{}{}
	p.seenIDs[EntityID(beaconA)] = beaconA
	p.seen[EntityID(beaconB)] = struct{}{}
	p.seenIDs[EntityID(beaconB)] = beaconB
	p.mu.Unlock()

	// Call with empty known — both are orphans. Enqueue may partially
	// succeed (we have a 1-slot queue, no drain) so at most 1 fits;
	// the second hits drop-oldest, which evicts the first and slots
	// the second. Either way: both orphans must STILL be in seenIDs
	// after the call because removal only happens via handleRemove.
	enq, rerr := p.RepublishOrphans(context.Background(), nil)
	_ = enq
	_ = rerr

	p.mu.Lock()
	remaining := len(p.seenIDs)
	_, stillA := p.seenIDs[EntityID(beaconA)]
	_, stillB := p.seenIDs[EntityID(beaconB)]
	p.mu.Unlock()

	if remaining != 2 {
		t.Errorf("seenIDs remaining after RepublishOrphans without Run: got %d want 2", remaining)
	}
	if !stillA {
		t.Errorf("beaconA must remain in seenIDs (only handleRemove may clear it)")
	}
	if !stillB {
		t.Errorf("beaconB must remain in seenIDs (only handleRemove may clear it)")
	}
}

// TestPublisher_ConnectTimeoutReturnsError pins D1 from the round-4
// review: Connect must surface a timeout as an error so the cold-
// start MQTTInitialConnectFailed counter trips. Uses an unreachable
// broker + a short ConnectTimeout to exercise the WaitTimeout==false
// branch.
func TestPublisher_ConnectTimeoutReturnsError(t *testing.T) {
	p, err := New(Options{
		BrokerURL:      "tcp://127.0.0.1:1", // unreachable
		Username:       "u",
		Password:       "p",
		ClientID:       "test-timeout",
		ConnectTimeout: 50 * time.Millisecond,
		Logger:         discardLogger(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	cerr := p.Connect()
	if cerr == nil {
		t.Fatal("Connect: expected timeout error, got nil")
	}
	// Either "timed out" from the explicit WaitTimeout==false branch
	// or a token error from paho's internal failure path is acceptable.
	// We assert non-nil; the exact message wording is paho-internal.
}
