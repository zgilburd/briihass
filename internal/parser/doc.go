// Package parser walks a hex-encoded BLE advertisement TLV and returns an
// iBeacon if (and only if) the advertisement contains an Apple-manufacturer
// (0x004C) section with the iBeacon marker bytes 0x02 0x15 followed by 21
// bytes of UUID (16) + Major (2 BE) + Minor (2 BE) + TX power (1 signed).
//
// All other shapes (Apple Continuity, AirDrop, Nearby, AltBeacon, random
// BLE) return ok=false. The caller (internal/ingest) drops those events
// silently while incrementing a metric counter.
//
// Filter-not-parse: this package does not interpret non-iBeacon adverts;
// they are simply rejected.
//
// See ADR-0001 (verified payload schema) and the ruckus-apis skill for the
// canonical parsing recipe.
package parser
