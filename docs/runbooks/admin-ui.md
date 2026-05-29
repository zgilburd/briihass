# Runbook — admin web UI

briihass ships a small basic-auth web UI for viewing per-beacon state
and editing tunables live (no redeploy). See
[ADR-0006](../adr/0006-presence-model-closest-ap-wins.md) for the
underlying state machine and [`tunables.md`](tunables.md) for the
knobs themselves.

## Endpoints

| Path | What it does |
|---|---|
| `GET /admin` | Redirects to `/admin/status`. |
| `GET /admin/status` | Per-beacon current zone, per-AP effective RSSI table, sticky-window state, heartbeat summary, MQTT publisher health. Auto-refreshes every 5 s via a `<meta http-equiv="refresh">` tag. |
| `GET /admin/tunables` | Form pre-filled with current defaults + per-beacon overrides. |
| `POST /admin/tunables` | Validate → atomic Postgres upsert → hot-reload the presence engine → 303 back to the GET. |
| `GET /admin/devices` | List devices seen in the chosen window (query params `window_n` + `window_unit=m\|h\|d`). Split into Tracked (in the allowlist) and Observed-only. Each row links to the per-device packet view. |
| `POST /admin/devices/promote` | Insert into `beacons` + rebuild engine topology. HA entity appears on the next sighting. |
| `POST /admin/devices/demote` | Delete from `beacons` + rebuild engine topology + publish empty retained payload to the HA Discovery config topic so HA removes the entity idiomatically. |
| `GET /admin/devices/{uuid}_{major}_{minor}/packets` | Per-device packet view: raw advert hex, parsed iBeacon (UUID/Major/Minor/TxPower), full TLV walk, and a link to the originating POST envelope when captured. |
| `GET /admin/zones` | Combined view of zone rows + observed APs. Upsert / delete an AP→zone mapping. |
| `POST /admin/zones` | Upsert or delete (with `action=delete`) the (ap_mac, zone_label) row + rebuild engine topology. |
| `GET /admin/settings` | Retention days (1..30) and the two capture toggles (`capture_per_event_hex`, `capture_full_posts`). |
| `POST /admin/settings` | Save settings + refresh the in-memory snapshot the ingest hot path reads. |
| `GET /admin/posts/{id}` | Raw POST envelope view: gunzipped + pretty-printed JSON body, sha256, endpoint, remote addr. Used by the packet view's "envelope" link. |
| `GET /admin-static/style.css` | The lone CSS asset, embedded into the binary via `embed.FS`. |

## Reaching it

**Prod:** route your ingress / reverse proxy so that
`https://<your-host>/admin` (and everything under `/admin/*`) reaches the
admin listener on port 8082. The same host typically also serves `/ingest`
and `/heartbeat` on port 8080 for vRIoT. Putting an edge SSO layer in front
(and keeping briihass's HTTP Basic-Auth below as a second layer) is
recommended for any internet-exposed deployment.

Keep the `:8081` metrics listener internal-only — only your metrics
scraper should be able to reach it.

```
https://<your-host>/admin           → redirects to /admin/status
https://<your-host>/admin/status    → live state, auto-refreshes every 5 s
https://<your-host>/admin/tunables  → live-editable form
```

**Dev:** the admin binds to `BRIIHASS_ADMIN_ADDR` (default `:8082`).
Browse `http://127.0.0.1:8082/admin` after `make run`.

**Reaching the admin port directly:** if the ingress path is unhealthy and
you need the admin port, port-forward it from your orchestrator (e.g.
`kubectl port-forward` on Kubernetes) to `localhost:8082`.

## Auth

HTTP Basic, constant-time compare against `ADMIN_USER` / `ADMIN_PASS`
env vars (injected from your secret manager in prod, the dev creds file
locally). If either var is unset, the admin UI is **disabled** at
startup — the bridge logs a warning and starts without the admin
listener. This makes it safe to ship a base config that does not
enable the UI by default.

```bash
# dev — fastest way to enable while iterating:
export ADMIN_USER=admin ADMIN_PASS="$(openssl rand -base64 24)"
echo "user=$ADMIN_USER pass=$ADMIN_PASS"
make run
```

The realm string in the WWW-Authenticate header is intentionally
generic (`"restricted"`) — it doesn't telegraph that this is briihass
to passive scanners.

## Editing tunables

The form on `/admin/tunables` mirrors the schema of
[`docs/examples/tunables.yaml`](../examples/tunables.yaml):

- **Defaults** — every field of `defaults:`. Required.
- **Per-beacon overrides** — one collapsible section per tracked
  beacon. Leave a field empty to inherit the default; enter a value
  to pin it for that beacon.

On save, the server:

1. Parses the form (any unparseable number surfaces as a validation
   error on the page; the form re-renders with your edits preserved).
2. Calls `config.Tunables.Validate()` to enforce the same range checks
   that apply at startup.
3. Calls `Store.SaveAll(ctx, newTun)` — a single Postgres
   transaction that upserts the defaults row and rewrites the
   overrides table. Backend is the Postgres database identified by
   `BRIIHASS_POSTGRES_DSN` (see `internal/store/postgres.go`).
4. Calls `engine.ApplyTunables(newTun)` — the presence engine swaps
   the per-beacon `Resolved` values on the next tick.
5. 303 redirects to the GET so a browser refresh doesn't re-POST.

There is **no restart**. The next sighting uses the new values.

## What the UI deliberately does NOT do

- **No filesystem writes.** Every mutation goes to Postgres via the
  connection identified by `BRIIHASS_POSTGRES_DSN`. The writable tables
  are `tunables_defaults`, `tunables_overrides`, `beacons`, `zones`,
  and `settings` — nothing else.
- **No live event feed.** Status auto-refreshes every 5 s; finer
  resolution belongs in Grafana via Prometheus.
- **No cluster mutation.** The UI cannot trigger a pod restart, a
  GitOps PR, or any kubectl-equivalent action. Image / deployment changes
  remain whatever deploy path you use.
- **No sessions, cookies, or CSRF tokens.** Basic-auth on every
  request, cluster-internal binding, and a small mutating surface
  area are the entire access-control model.

## Hardening notes for production

- Expose the admin port only through your ingress / reverse proxy, and
  gate it with two layers of auth:
  1. **Edge SSO** (e.g. an identity-aware proxy) for browser consumers.
  2. **HTTP Basic-Auth** (this server) behind it.
- Rotate `ADMIN_USER` / `ADMIN_PASS` through your secret manager and
  restart the process to pick up new values.
- If you want auditability, scrape the briihass logs — every
  successful tunable save logs `tunables updated via admin UI`.
  Tables in Postgres also carry `updated_at TIMESTAMPTZ` columns so
  you can correlate saves with audit windows.
- The Postgres credentials are supplied via `BRIIHASS_POSTGRES_DSN`;
  source it from your secret manager and rotate it there.
