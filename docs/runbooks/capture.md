# Runbook — Capturing a real vRIoT iBeacon POST payload

We need a real POST body from the Ruckus vRIoT iBeacon plugin before
locking in the parser. `cmd/captured` is a tiny stdlib-only Go server
that listens on a LAN-reachable port, accepts any HTTP method on any
path, and writes each request to disk as JSON. Keep it inside your
perimeter — **do not** use webhook.site / requestbin / public tunnels for
this; internal IoT data should not leave the network.

## Run it

From the briihass repo root:

```bash
go run ./cmd/captured                          # listens on :8080, writes ./captures/
go run ./cmd/captured -addr :9000 -dir /tmp/caps
```

`captured` has no auth — that's intentional. Its job is to capture
whatever the client sends so we can see it. Configure the vRIoT plugin's
auth fields however you like; the captured JSON files will show exactly
what arrived (headers, query params, body).

At startup it prints the dev machine's LAN IPs:

```
configure the vRIoT iBeacon plugin (or any client) to POST to one of:
  http://192.0.2.20:8080/ingest
  http://172.17.0.1:8080/ingest
```

Pick the one on the same network as the vRIoT VM (the `10.x` one in this
example; `172.17.x` is the docker0 bridge). The path is cosmetic — the
tool accepts any path. Using `/ingest` mirrors the bridge's eventual
endpoint so the captured payload looks just like what production will
receive.

## Point vRIoT at it

In the vRIoT controller UI, configure the iBeacon plugin's HTTP POST
target to the URL printed above. Fill in whatever auth fields the plugin
UI exposes (header value, basic auth, etc.) with placeholder values —
captured will record what the plugin ends up sending. Trigger a beacon
sighting near a gateway (walk near an AP with a beacon).

## Confirm the capture

Each request appears in `./captures/` as
`<timestamp>-<method>-<short-hash>.json`, e.g.

```
captures/20260518T060952.785-POST-d7aa530a.json
```

The file contains the request method, URL, all headers, query params,
and the body. Bodies are stored as UTF-8 text when the content looks
text-like (JSON, XML, form-encoded, valid UTF-8); otherwise they're
base64-encoded. Inspect the headers to identify the plugin's auth scheme
(custom header? basic auth? HMAC signature? nothing?) — this is the
input to the auth decision in [ADR-0001](../adr/0001-http-post-ingest.md).

## What to do with the capture

1. Copy one representative capture into
   `internal/ingest/testdata/sample-post.json` (Phase 2 — that dir
   doesn't exist yet).
2. Update [ADR-0001](../adr/0001-http-post-ingest.md) and the
   `ruckus-apis` skill with the verified field names, content type, and
   auth-header name.
3. **Strip identifying data** before any capture leaves the dev machine.
   Captured files contain real BLE MACs and beacon UUIDs that fingerprint
   physical assets. If you need to share a sample, redact the
   `deviceAddress` / UUID / Major / Minor fields.

## Cleanup

`captures/` is gitignored, but the files contain sensitive identifiers.
After you have the sample you need:

```bash
shred -u captures/*.json && rmdir captures
```

## Why not run captured in-cluster?

You could — a small Deployment + Service + IngressRoute matching the
briihass shape would work. But for a one-shot "what does this payload
actually look like?" capture, running it locally is faster and avoids
a deploy round-trip. If you want a permanent staging endpoint, that's
worth doing in-cluster, but it's a separate decision.
