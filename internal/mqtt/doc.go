// Package mqtt publishes briihass presence events to a Mosquitto broker
// using Home Assistant's MQTT Discovery convention. HA auto-creates
// device_tracker.* entities whose state is the bridge's AP-derived zone
// label. See ADR-0002 for the contract.
//
// The publisher owns a single MQTT connection (paho.mqtt.golang) and runs
// in a dedicated goroutine fed by a bounded channel. On broker disconnect,
// events buffer up to BufferSize and the oldest are dropped on overflow
// (logged + counter incremented).
//
// Topic shape:
//
//	homeassistant/device_tracker/briihass_<id>/config       (retained)
//	homeassistant/device_tracker/briihass_<id>/state        (retained)
//	homeassistant/device_tracker/briihass_<id>/attributes   (retained)
//	briihass/health                                         (non-retained, every 30s)
//
// Where <id> = <uuid_without_dashes>_<major>_<minor>, lowercased.
package mqtt
