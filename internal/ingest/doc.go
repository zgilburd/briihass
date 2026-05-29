// Package ingest is the HTTP-receiving side of briihass. It serves three
// endpoints to the vRIoT iBeacon plugin (and a local health probe):
//
//	POST /ingest    — gzipped JSON beacon events (~1-2/sec per AP)
//	POST /heartbeat — gateway online/offline list (~1/75s)
//	GET  /health    — k8s liveness/readiness
//
// Auth is a constant-time compare of the inbound Api-Key header against
// INGEST_SHARED_SECRET. A source-IP allowlist (typically the vRIoT VM's
// /32) layers on top. Bodies are always Content-Encoding: gzip; the
// handler gunzips before JSON parsing. Each event's data hex string is
// passed to internal/parser; only iBeacon-shaped manuf-data produces a
// Sighting submitted to internal/presence.
//
// See ADR-0001 for the verified plugin behavior.
package ingest
