package mqtt

import (
	"encoding/json"
	"time"

	"briihass/internal/presence"
)

// bridgeAvailabilityTopic is the single bridge-wide availability topic.
// Backed by the MQTT Last-Will (set to "offline" retained on connect)
// and published "online" once connected. Every device's discovery config
// references it so HA marks all entities unavailable if briihass dies.
const bridgeAvailabilityTopic = "briihass/bridge/availability"

const (
	payloadOnline  = "online"
	payloadOffline = "offline"
)

const (
	// trackerHome / trackerNotHome are the only two device_tracker states
	// briihass publishes. HA treats a device_tracker as "home" (for person
	// entities, zone.home, and from/to: home automations) ONLY when its
	// state is exactly "home"; any other string is a custom location that
	// reads as not-home. The AP-derived area label therefore lives on a
	// separate "area" sensor (see BuildDeviceDiscovery), not the tracker
	// state. See ADR-0002 Phase 5 addendum.
	trackerHome    = "home"
	trackerNotHome = "not_home"
)

// trackerState maps a presence state to the bare device_tracker payload:
// "home" whenever the beacon is in presence at any AP (the engine resolved
// an area label), "not_home" otherwise. The granular area label is carried
// separately on the telemetry topic for the area sensor.
func trackerState(s presence.PresenceState) string {
	if s.IsNotHome() {
		return trackerNotHome
	}
	return trackerHome
}

// EntityID is the stable identifier used as the discovery object_id, the
// HA device identifier, and the unique_id stem. Delegates to the
// packet-derived identity's own collision-free encoding
// (briihass_<kind>_<sanitized key>).
func EntityID(id presence.BeaconKey) string {
	return id.EntityID()
}

// deviceConfigTopic is the device-based discovery config topic
// (homeassistant/device/<object_id>/config). One retained message per
// beacon declares the whole device (tracker + sensors).
func deviceConfigTopic(prefix string, id presence.BeaconKey) string {
	return prefix + "/device/" + EntityID(id) + "/config"
}

// stateTopic carries the device_tracker's BARE state string
// ("home" or "not_home"). HA does NOT apply value_template to a
// device_tracker declared via device-based discovery, so the tracker
// must read a plain string here — not a JSON document. The AP-derived
// area label rides the telemetry topic (read by the area sensor), not
// this topic.
func stateTopic(id presence.BeaconKey) string {
	return "briihass/" + EntityID(id) + "/state"
}

// telemetryTopic carries the JSON telemetry document that the sensors
// read via value_template (sensors honor value_template reliably) and
// that backs the tracker's json_attributes_topic.
func telemetryTopic(id presence.BeaconKey) string {
	return "briihass/" + EntityID(id) + "/telemetry"
}

// legacyTopics are the 004b-era per-component topics. Cleared (empty
// retained) on demote/resync so retained configs from the old scheme
// don't linger as HA ghost entities after the switch to device-based
// discovery. Remove once every environment has rolled past 004c.
func legacyTopics(prefix string, id presence.BeaconKey) []string {
	base := prefix + "/device_tracker/" + EntityID(id)
	return []string{base + "/config", base + "/state", base + "/attributes"}
}

// --- device-based discovery config (HA "device" schema) ---------------

type deviceDiscovery struct {
	Device       deviceBlock          `json:"dev"`
	Origin       originBlock          `json:"o"`
	Availability []availabilityBlock  `json:"availability,omitempty"`
	Components   map[string]component `json:"cmps"`
}

type deviceBlock struct {
	Identifiers  []string `json:"ids"`
	Name         string   `json:"name"`
	Manufacturer string   `json:"mf"`
	Model        string   `json:"mdl"`
}

type originBlock struct {
	Name string `json:"name"`
	URL  string `json:"url,omitempty"`
}

type availabilityBlock struct {
	Topic               string `json:"topic"`
	PayloadAvailable    string `json:"payload_available,omitempty"`
	PayloadNotAvailable string `json:"payload_not_available,omitempty"`
}

// component is one entity inside the device's cmps map. Fields are
// omitempty so a device_tracker and a sensor share the struct without
// emitting irrelevant keys.
type component struct {
	Platform            string `json:"p"`
	UniqueID            string `json:"unique_id"`
	Name                string `json:"name,omitempty"`
	SourceType          string `json:"source_type,omitempty"`
	DeviceClass         string `json:"device_class,omitempty"`
	UnitOfMeasurement   string `json:"unit_of_measurement,omitempty"`
	StateClass          string `json:"state_class,omitempty"`
	EntityCategory      string `json:"entity_category,omitempty"`
	StateTopic          string `json:"state_topic"`
	ValueTemplate       string `json:"value_template,omitempty"`
	JSONAttributesTopic string `json:"json_attributes_topic,omitempty"`
}

// BuildDeviceDiscovery renders the retained device-based discovery
// payload. The tracker + RSSI components are always present; voltage and
// temperature are included only when withVoltage / withTemp is set
// (lazy growth — see publisher.handle), so a beacon that never reports
// telemetry (e.g. a named device) gets no empty battery/temperature
// sensor.
func BuildDeviceDiscovery(prefix string, ev presence.PresenceEvent, withVoltage, withTemp bool) deviceDiscovery {
	id := ev.Beacon
	entity := EntityID(id)
	st := stateTopic(id)
	tel := telemetryTopic(id)

	cmps := map[string]component{
		// The device_tracker reads the BARE state string (no value_template
		// — HA ignores it for device_tracker under device-based discovery)
		// and surfaces the telemetry JSON as its attributes.
		entity: {
			Platform:            "device_tracker",
			UniqueID:            entity,
			SourceType:          "bluetooth_le",
			StateTopic:          st,
			JSONAttributesTopic: tel,
		},
		entity + "_rssi": {
			Platform:          "sensor",
			UniqueID:          entity + "_rssi",
			Name:              "RSSI",
			DeviceClass:       "signal_strength",
			UnitOfMeasurement: "dBm",
			StateClass:        "measurement",
			EntityCategory:    "diagnostic",
			StateTopic:        tel,
			ValueTemplate:     "{{ value_json.rssi }}",
		},
		// The area sensor carries the AP-derived area label (or "not_home"
		// while away). The tracker state is a bare home/not_home value, so
		// the granular AP-derived location lives here instead. Always
		// present (not lazy) since every tracked beacon resolves an area.
		entity + "_area": {
			Platform:      "sensor",
			UniqueID:      entity + "_area",
			Name:          "Area",
			StateTopic:    tel,
			ValueTemplate: "{{ value_json.area }}",
		},
	}
	if withVoltage {
		cmps[entity+"_voltage"] = component{
			Platform:          "sensor",
			UniqueID:          entity + "_voltage",
			Name:              "Battery voltage",
			DeviceClass:       "voltage",
			UnitOfMeasurement: "mV",
			StateClass:        "measurement",
			EntityCategory:    "diagnostic",
			StateTopic:        tel,
			ValueTemplate:     "{{ value_json.voltage }}",
		}
	}
	if withTemp {
		cmps[entity+"_temperature"] = component{
			Platform:          "sensor",
			UniqueID:          entity + "_temperature",
			Name:              "Temperature",
			DeviceClass:       "temperature",
			UnitOfMeasurement: "°C",
			StateClass:        "measurement",
			StateTopic:        tel,
			ValueTemplate:     "{{ value_json.temperature }}",
		}
	}
	return deviceDiscovery{
		Device: deviceBlock{
			Identifiers:  []string{entity},
			Name:         ev.BeaconName,
			Manufacturer: "briihass",
			Model:        "BLE presence bridge",
		},
		Origin: originBlock{Name: "briihass", URL: "https://github.com/briihass/briihass"},
		Availability: []availabilityBlock{{
			Topic:               bridgeAvailabilityTopic,
			PayloadAvailable:    payloadOnline,
			PayloadNotAvailable: payloadOffline,
		}},
		Components: cmps,
	}
}

// --- single JSON state payload ----------------------------------------

// statePayload is the one JSON document published to the device's
// telemetry topic. Every sensor's value_template (area, rssi, voltage,
// temperature) reads a field from it, and it backs the tracker's
// json_attributes_topic. Absent telemetry fields are omitted so the
// corresponding sensor reads "unknown" rather than a stale or bogus value.
type statePayload struct {
	Area         string   `json:"area"` // AP-derived area label or "not_home"
	RSSI         *int     `json:"rssi,omitempty"`
	Voltage      *int     `json:"voltage,omitempty"`
	Temperature  *float64 `json:"temperature,omitempty"`
	APMac        string   `json:"ap_mac,omitempty"`
	APName       string   `json:"ap_name,omitempty"`
	RSSIEWMA     float64  `json:"rssi_ewma,omitempty"`
	RSSIEffect   float64  `json:"rssi_effective,omitempty"`
	LastSeen     string   `json:"last_seen,omitempty"`
	StickyActive bool     `json:"sticky_active"`
	EmittedAt    string   `json:"emitted_at"`
}

// BuildState renders the JSON state document from a presence event.
// RSSI (the value the signal_strength sensor plots) is the rounded EWMA
// of the current published AP; it is omitted when the beacon is not_home
// so the sensor reads "unknown" while away rather than freezing on the
// last in-range value.
func BuildState(ev presence.PresenceEvent) statePayload {
	s := statePayload{
		Area:         ev.State.String(),
		Voltage:      ev.BatteryMV,
		Temperature:  ev.TemperatureC,
		APMac:        ev.APMac,
		APName:       ev.APName,
		StickyActive: ev.StickyActive,
		EmittedAt:    ev.EmittedAt.UTC().Format(time.RFC3339),
	}
	if ev.State != presence.NotHome && ev.APMac != "" {
		r := int(roundTo(ev.RSSIEWMA, 0))
		s.RSSI = &r
		s.RSSIEWMA = roundTo(ev.RSSIEWMA, 2)
		s.RSSIEffect = roundTo(ev.RSSIEffective, 2)
	}
	if !ev.LastSeen.IsZero() {
		s.LastSeen = ev.LastSeen.UTC().Format(time.RFC3339)
	}
	return s
}

// marshalCompactFn is the JSON-marshal seam. Defaults to json.Marshal;
// tests can swap it to exercise the marshal-failure path.
var marshalCompactFn = func(v any) ([]byte, error) {
	return json.Marshal(v)
}

func marshalCompact(v any) ([]byte, error) {
	return marshalCompactFn(v)
}

// roundTo trims a float to n decimal places.
func roundTo(v float64, n int) float64 {
	p := 1.0
	for i := 0; i < n; i++ {
		p *= 10
	}
	return float64(int64(v*p+sign(v)*0.5))/p + 0
}

func sign(v float64) float64 {
	if v < 0 {
		return -1
	}
	return 1
}
