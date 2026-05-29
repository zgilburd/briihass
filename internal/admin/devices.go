package admin

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"briihass/internal/config"
	"briihass/internal/ids"
	"briihass/internal/parser"
	"briihass/internal/presence"
	"briihass/internal/store"
)

type devicesPage struct {
	Commit        string
	Now           time.Time
	WindowN       int
	WindowUnit    string
	WindowLabel   string
	RetentionDays int
	Tracked       []deviceRow
	Observed      []deviceRow
	Notice        string
	Error         string

	// RebuildError is set when the DB write succeeded but the engine
	// topology refresh failed. The template renders a Retry button
	// (POST to RetryURL) so the operator can re-run ApplyTopology
	// without having to re-issue the promote/demote.
	RebuildError string
	RetryURL     string

	// OrphansReconciled / ReconcileError are surfaced from the
	// /admin/devices/refresh-engine redirect (?orphans=N, ?reconcile_err=…).
	// They keep the success/failure of the manual retry visible after
	// a 303 → GET, so an operator who retried a demote-while-broker-down
	// either sees "Reconciled N orphan HA entit{y|ies}" or the failure
	// banner instead of the page silently clearing.
	OrphansReconciled int
	ReconcileError    string
}

type deviceRow struct {
	Kind        string
	Key         string
	Name        string
	LastAPMac   string
	LastAPName  string
	LastRSSI    int
	LastSeen    time.Time
	AgeSeconds  float64
	SightingCnt int64
	Slug        string // "<kind>.<key>" — form value for promote/demote
	PacketURL   string // URL-safe path to the packet detail view
}

func (s *Server) handleDevices(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.renderDevices(w, r, "", "", "")
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleDevicesRefreshEngine re-runs ApplyTopology against the current
// DB state and, if wired, reconciles MQTT orphans against the current
// allowlist. Linked from the inline-error banner shown on the devices
// page when a promote/demote partially failed (DB write succeeded,
// engine refresh did not, or MQTT remove was dropped under queue
// saturation). Idempotent.
func (s *Server) handleDevicesRefreshEngine(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := s.rebuildEngineTopology(ctx); err != nil {
		s.opts.Logger.Error("rebuild topology (manual retry)", "err", err)
		s.renderDevices(w, r, "", "", err.Error())
		return
	}
	orphans, rerr := s.reconcileOrphans(ctx)
	q := url.Values{"refreshed": {"1"}}
	if rerr != nil {
		s.opts.Logger.Warn("engine topology refreshed but orphan reconcile failed",
			"err", rerr)
		q.Set("reconcile_err", rerr.Error())
	} else {
		s.opts.Logger.Info("engine topology refreshed (manual retry)",
			"orphans_reconciled", orphans)
		if orphans > 0 {
			q.Set("orphans", strconv.Itoa(orphans))
		}
	}
	http.Redirect(w, r, "/admin/devices?"+q.Encode(), http.StatusSeeOther)
}

// handleDevicesResyncHA re-asserts every device's HA discovery config +
// state via the publisher (clears its seen/declared maps and triggers
// RepublishAll). Use after an HA-side change that didn't fire a birth
// message — e.g. an entity manually deleted in HA — so it reappears
// without a demote/promote cycle.
func (s *Server) handleDevicesResyncHA(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.opts.ResyncHA == nil {
		s.renderDevices(w, r, "", "resync: no HA publisher wired", "")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := s.opts.ResyncHA(ctx); err != nil {
		s.opts.Logger.Error("resync HA", "err", err)
		s.renderDevices(w, r, "", "resync HA: "+err.Error(), "")
		return
	}
	s.opts.Logger.Info("HA discovery resync requested via admin")
	http.Redirect(w, r, "/admin/devices?resynced=1", http.StatusSeeOther)
}

// reconcileOrphans converts the current allowlist into a []BeaconKey
// and calls OrphanReconcile. Returns (enqueued, err). A non-nil err
// means the reconcile step did not run (or did not run to completion)
// — distinguishable from "ran successfully and there were zero
// orphans" so the caller can render a banner instead of pretending
// success.
func (s *Server) reconcileOrphans(ctx context.Context) (int, error) {
	if s.opts.OrphanReconcile == nil || s.opts.Devices == nil {
		return 0, nil
	}
	beacons, err := s.opts.Devices.ListBeacons(ctx)
	if err != nil {
		s.opts.Logger.Error("orphan reconcile: list beacons", "err", err)
		return 0, fmt.Errorf("list beacons for reconcile: %w", err)
	}
	known := make([]presence.BeaconKey, 0, len(beacons))
	for _, b := range beacons {
		known = append(known, b.Domain())
	}
	n, oerr := s.opts.OrphanReconcile(ctx, known)
	if oerr != nil {
		s.opts.Logger.Warn("orphan reconcile partial", "enqueued", n, "err", oerr)
		return n, fmt.Errorf("orphan reconcile partial: %w", oerr)
	}
	return n, nil
}

func (s *Server) renderDevices(w http.ResponseWriter, r *http.Request, notice, errMsg, rebuildErr string) {
	n, unit, dur, label := parseWindow(r)
	retention := s.retentionDays()
	if dur > time.Duration(retention)*24*time.Hour {
		dur = time.Duration(retention) * 24 * time.Hour
		label = fmt.Sprintf("%dd (capped to retention)", retention)
	}
	since := time.Now().Add(-dur)
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	devs, err := s.opts.Devices.ListDevicesSince(ctx, since)
	if err != nil {
		// Render error-only: do NOT fall through to a loop over a nil
		// slice that would paint empty Tracked/Observed tables next to
		// a banner the operator might glance past. Empty tables look
		// like "no devices have been seen recently" — a false negative
		// in a UI an operator may act on (e.g. demote the wrong row).
		s.opts.Logger.Error("admin devices list", "err", err)
		s.render(w, "devices.html", devicesPage{
			Commit:        s.opts.BuildCommit,
			Now:           time.Now(),
			WindowN:       n,
			WindowUnit:    unit,
			WindowLabel:   label,
			RetentionDays: retention,
			Notice:        notice,
			Error:         "list devices: " + err.Error(),
		})
		return
	}

	page := devicesPage{
		Commit:        s.opts.BuildCommit,
		Now:           time.Now(),
		WindowN:       n,
		WindowUnit:    unit,
		WindowLabel:   label,
		RetentionDays: retention,
		Notice:        notice,
		Error:         errMsg,
	}
	if rebuildErr != "" {
		page.RebuildError = rebuildErr
		page.RetryURL = "/admin/devices/refresh-engine"
	}
	// Surface the refresh-engine outcome from the redirect query string.
	// handleDevicesRefreshEngine sets ?refreshed=1 plus ?orphans=N (on
	// success with non-zero reconciles) or ?reconcile_err=… (on partial
	// failure). Parsing here keeps the banner state local to the page
	// rather than threading it through renderDevices' positional args.
	if r.URL.Query().Get("refreshed") == "1" {
		if v := r.URL.Query().Get("orphans"); v != "" {
			if n, perr := strconv.Atoi(v); perr == nil && n > 0 {
				page.OrphansReconciled = n
			}
		}
		page.ReconcileError = r.URL.Query().Get("reconcile_err")
	}
	if r.URL.Query().Get("resynced") == "1" && page.Notice == "" {
		page.Notice = "Re-asserted HA discovery for all tracked devices."
	}
	for _, d := range devs {
		key, ok := d.Domain()
		if !ok {
			// Corrupt identity row — skip rather than render a broken link.
			s.opts.Logger.Warn("admin devices: skipping corrupt identity row", "kind", d.Kind, "key", d.Key)
			continue
		}
		row := deviceRow{
			Kind:        d.Kind,
			Key:         d.Key,
			Name:        d.BeaconName,
			LastAPMac:   d.LastAPMac,
			LastAPName:  d.LastAPName,
			LastRSSI:    d.LastRSSI,
			LastSeen:    d.LastSeen,
			AgeSeconds:  time.Since(d.LastSeen).Seconds(),
			SightingCnt: d.SightingCnt,
			Slug:        key.Slug(),
			PacketURL:   packetURL(key),
		}
		if d.Tracked {
			page.Tracked = append(page.Tracked, row)
		} else {
			page.Observed = append(page.Observed, row)
		}
	}
	s.render(w, "devices.html", page)
}

func (s *Server) handleDevicePromote(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	slug := strings.TrimSpace(r.FormValue("slug"))
	name := strings.TrimSpace(r.FormValue("name"))
	notes := strings.TrimSpace(r.FormValue("notes"))
	if slug == "" || name == "" {
		s.renderDevices(w, r, "", "promote: device and name are required", "")
		return
	}
	key, ok := ids.ParseSlug(slug)
	if !ok {
		s.renderDevices(w, r, "", "promote: unrecognized device identity "+slug, "")
		return
	}
	beacon, berr := store.NewBeacon(key, name, notes)
	if berr != nil {
		s.renderDevices(w, r, "", "promote: "+berr.Error(), "")
		return
	}
	// Operator-driven write paths get one ctx for DB+engine derived
	// from r.Context() so a browser cancel terminates the work in
	// flight. MQTT cleanup (none in promote today, but kept symmetric
	// with demote) would use a background ctx so a pre-empted browser
	// can't leave HA staring at a half-removed entity.
	dbCtx, dbCancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer dbCancel()
	if err := s.opts.Devices.PromoteBeacon(dbCtx, beacon); err != nil {
		s.renderDevices(w, r, "", "promote: "+err.Error(), "")
		return
	}
	// DB is the source of truth. If the engine refresh fails, the row
	// stays — we render an inline banner with a Retry button instead
	// of declaring success with a redirect.
	if err := s.rebuildEngineTopology(dbCtx); err != nil {
		s.opts.Logger.Error("rebuild topology after promote", "err", err)
		s.renderDevices(w, r,
			"beacon "+name+" promoted in DB but engine refresh failed",
			"",
			err.Error())
		return
	}
	s.opts.Logger.Info("beacon promoted", "slug", key.Slug(), "name", name)
	http.Redirect(w, r, "/admin/devices?promoted="+url.QueryEscape(name), http.StatusSeeOther)
}

func (s *Server) handleDeviceDemote(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	slug := strings.TrimSpace(r.FormValue("slug"))
	if slug == "" {
		s.renderDevices(w, r, "", "demote: device is required", "")
		return
	}
	key, ok := ids.ParseSlug(slug)
	if !ok {
		s.renderDevices(w, r, "", "demote: unrecognized device identity "+slug, "")
		return
	}
	// dbCtx for DB + engine refresh: tied to r.Context() so a browser
	// cancel terminates the work in flight.
	dbCtx, dbCancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer dbCancel()
	if err := s.opts.Devices.DemoteBeacon(dbCtx, key); err != nil {
		s.renderDevices(w, r, "", "demote: "+err.Error(), "")
		return
	}
	if err := s.rebuildEngineTopology(dbCtx); err != nil {
		s.opts.Logger.Error("rebuild topology after demote", "err", err)
		s.renderDevices(w, r,
			"beacon demoted in DB but engine refresh failed (HA entity not yet removed)",
			"",
			err.Error())
		return
	}
	var notice string
	if s.opts.RemoveMQTTEntity != nil {
		// mqttCtx is decoupled from r.Context(): the MQTT enqueue is
		// non-blocking and a browser cancel here would leave the
		// already-deleted DB row with a lingering HA entity. Use a
		// fresh background ctx with its own 5s budget instead.
		mqttCtx, mqttCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer mqttCancel()
		if err := s.opts.RemoveMQTTEntity(mqttCtx, key); err != nil {
			s.opts.Logger.Error("demote MQTT remove failed", "slug", key.Slug(), "err", err)
			s.renderDevices(w, r,
				"beacon demoted in DB but MQTT entity removal failed (HA entity may linger; retry refresh)",
				"",
				err.Error())
			return
		}
		notice = "demoted"
	} else {
		s.opts.Logger.Warn("demote: no MQTT remover wired; HA entity will linger until manual cleanup")
		notice = "demoted (no MQTT remover wired)"
	}
	s.opts.Logger.Info("beacon demoted", "slug", key.Slug())
	http.Redirect(w, r, "/admin/devices?demoted="+url.QueryEscape(notice), http.StatusSeeOther)
}

// rebuildEngineTopology reads the current allowlist + zones from the
// store and swaps it into the presence engine. Called after every
// promote/demote/zone-change.
func (s *Server) rebuildEngineTopology(ctx context.Context) error {
	if s.opts.Devices == nil || s.opts.Zones == nil {
		return errors.New("rebuildEngineTopology: devices/zones store not wired")
	}
	beacons, err := s.opts.Devices.ListBeacons(ctx)
	if err != nil {
		return fmt.Errorf("list beacons: %w", err)
	}
	zones, err := s.opts.Zones.ListZones(ctx)
	if err != nil {
		return fmt.Errorf("list zones: %w", err)
	}
	zoneMap := make(map[string]string, len(zones))
	for _, z := range zones {
		zoneMap[z.APMac.String()] = z.ZoneLabel.String()
	}
	tracked := make([]config.TrackedBeacon, 0, len(beacons))
	for _, b := range beacons {
		tb, terr := config.NewTrackedBeacon(b.Domain(), b.Name)
		if terr != nil {
			return fmt.Errorf("build tracked beacon: %w", terr)
		}
		tracked = append(tracked, tb)
	}
	top, err := config.NewTopology(zoneMap, tracked)
	if err != nil {
		return fmt.Errorf("validate: %w", err)
	}
	s.opts.Engine.ApplyTopology(top)
	return nil
}

// packetURL builds the URL-safe packet-detail path for a beacon key.
// The slug ("<kind>.<key>") may contain characters that need escaping
// (free-form name/url keys), so the whole slug is path-escaped as a
// single segment; handleDevicePackets reverses this via EscapedPath +
// PathUnescape.
func packetURL(key ids.BeaconKey) string {
	return "/admin/devices/" + url.PathEscape(key.Slug()) + "/packets"
}

// ----- Packet detail view -----

type packetsPage struct {
	Commit       string
	Now          time.Time
	Kind         string
	Key          string
	BeaconName   string
	Tracked      bool
	Observations []packetRow
	Error        string
}

type packetRow struct {
	ID         int64
	ObservedAt time.Time
	APMac      string
	APName     string
	RSSI       int
	TxPower    string
	Battery    string
	Temp       string
	LocalName  string
	Identity   string // kind.key derived from the raw advert, when decodable
	RawHex     string
	TLV        []parser.TLVRecord
	TLVNotice  string
	RawPostID  *int64
}

func (s *Server) handleDevicePackets(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// URL shape: /admin/devices/{escaped-slug}/packets. Use EscapedPath
	// so a %2F inside a free-form key survives net/http path cleaning.
	rest := strings.TrimPrefix(r.URL.EscapedPath(), "/admin/devices/")
	rest = strings.TrimSuffix(rest, "/")
	enc, found := strings.CutSuffix(rest, "/packets")
	if !found || enc == "" {
		http.NotFound(w, r)
		return
	}
	slug, uerr := url.PathUnescape(enc)
	if uerr != nil {
		http.Error(w, "bad device path", http.StatusBadRequest)
		return
	}
	key, ok := ids.ParseSlug(slug)
	if !ok {
		http.Error(w, "unrecognized device identity", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	obs, err := s.opts.Devices.ListObservationsForDevice(ctx, string(key.Kind()), key.Key(), 200)
	page := packetsPage{
		Commit: s.opts.BuildCommit,
		Now:    time.Now(),
		Kind:   string(key.Kind()),
		Key:    key.Key(),
	}
	if err != nil {
		// Render error-only: continuing to ListBeacons + raw-hex
		// parsing on empty obs would either clobber the original error
		// with a secondary "beacon name lookup failed" message, or
		// (worse) render an empty Observations table that looks like
		// "no recent packets" rather than "DB error".
		s.opts.Logger.Error("admin packets list", "err", err)
		page.Error = "list observations: " + err.Error()
		s.render(w, "packets.html", page)
		return
	}
	if len(obs) > 0 {
		page.Tracked = obs[0].Tracked
	}
	if beacons, lerr := s.opts.Devices.ListBeacons(ctx); lerr != nil {
		s.opts.Logger.Error("packets: list beacons", "err", lerr)
		page.Error = "beacon name lookup failed: " + lerr.Error()
	} else {
		for _, b := range beacons {
			if b.Domain() == key {
				page.BeaconName = b.Name
				break
			}
		}
	}
	for _, o := range obs {
		row := packetRow{
			ID:         o.ID,
			ObservedAt: o.ObservedAt,
			APMac:      o.APMac,
			APName:     o.APName,
			RSSI:       o.RSSI,
			LocalName:  o.LocalName,
			RawHex:     o.RawHex,
			RawPostID:  o.RawPostID,
		}
		if o.TxPower != nil {
			row.TxPower = strconv.Itoa(*o.TxPower)
		}
		if o.BatteryMV != nil {
			row.Battery = strconv.Itoa(*o.BatteryMV) + " mV"
		}
		if o.TemperatureC != nil {
			row.Temp = strconv.FormatFloat(*o.TemperatureC, 'f', 1, 64) + " °C"
		}
		if o.RawHex != "" {
			if adv, perr := parser.Parse(o.RawHex); perr == nil {
				if k, idOK := parser.Identify(adv); idOK {
					row.Identity = k.Slug()
				}
			}
			walk, werr := parser.ParseAdvertDebug(o.RawHex)
			if werr != nil {
				row.TLVNotice = werr.Error()
			} else {
				row.TLV = walk.Records
				if walk.Err != "" {
					row.TLVNotice = walk.Err
				}
			}
		} else {
			row.TLVNotice = "raw hex not captured (settings.capture_per_event_hex off when recorded)"
		}
		page.Observations = append(page.Observations, row)
	}
	s.render(w, "packets.html", page)
}

// ----- Raw POST envelope view -----

type rawPostPage struct {
	Commit       string
	ID           int64
	ReceivedAt   time.Time
	Endpoint     string
	RemoteAddr   string
	Encoding     string
	SHA256       string
	PrettyJSON   string
	DecodeNotice string
	SizeBytes    int
}

func (s *Server) handleRawPost(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	idStr := strings.TrimPrefix(r.URL.Path, "/admin/posts/")
	idStr = strings.TrimSuffix(idStr, "/")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.Error(w, "bad post id", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	p, err := s.opts.RawPosts.GetRawPost(ctx, id)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			http.NotFound(w, r)
			return
		}
		s.opts.Logger.Error("admin raw post load", "id", id, "err", err)
		http.Error(w, "raw post load failed", http.StatusInternalServerError)
		return
	}
	page := rawPostPage{
		Commit:     s.opts.BuildCommit,
		ID:         p.ID,
		ReceivedAt: p.ReceivedAt,
		Endpoint:   p.Endpoint,
		RemoteAddr: p.RemoteAddr,
		Encoding:   string(p.ContentEncoding),
		SHA256:     p.BodySHA256,
		SizeBytes:  len(p.Body),
	}
	// Gunzip + pretty-print JSON, best effort. Errors are surfaced as
	// DecodeNotice on the page rather than failing the whole render so
	// the operator can still see the envelope metadata.
	var plain []byte
	if p.ContentEncoding == store.EncodingGzip {
		gr, gerr := gzip.NewReader(strings.NewReader(string(p.Body)))
		if gerr != nil {
			page.DecodeNotice = "gunzip: " + gerr.Error()
		} else {
			data, rerr := io.ReadAll(gr)
			if cerr := gr.Close(); cerr != nil && rerr == nil {
				rerr = cerr
			}
			if rerr != nil {
				// Surface the gunzip failure and refuse to render the
				// parsed pane: handing partial bytes to json.Unmarshal
				// below would clobber DecodeNotice with a JSON error
				// that hides the real cause.
				page.DecodeNotice = "gunzip body: " + rerr.Error()
			} else {
				plain = data
			}
		}
	} else {
		plain = p.Body
	}
	if len(plain) > 0 {
		var v any
		if jerr := json.Unmarshal(plain, &v); jerr == nil {
			pretty, merr := json.MarshalIndent(v, "", "  ")
			if merr != nil {
				// Should be unreachable after a successful Unmarshal, but
				// fall through to the raw body rather than rendering an
				// empty pane.
				page.PrettyJSON = string(plain)
				page.DecodeNotice = "pretty-print failed (showing raw): " + merr.Error()
			} else {
				page.PrettyJSON = string(pretty)
			}
		} else {
			page.PrettyJSON = string(plain)
			page.DecodeNotice = "body did not parse as JSON: " + jerr.Error()
		}
	}
	s.render(w, "post.html", page)
}

// ----- helpers -----

func (s *Server) retentionDays() int {
	if s.opts.SettingsSnap != nil {
		st := s.opts.SettingsSnap.Get()
		if d := st.RetentionDays(); d > 0 {
			return d
		}
	}
	return 7
}

// parseWindow reads window_n + window_unit from the query string,
// applies sensible defaults, and returns the parsed duration + label.
func parseWindow(r *http.Request) (n int, unit string, dur time.Duration, label string) {
	n, _ = strconv.Atoi(r.URL.Query().Get("window_n"))
	unit = strings.ToLower(strings.TrimSpace(r.URL.Query().Get("window_unit")))
	switch unit {
	case "m", "h", "d":
	default:
		unit = "m"
	}
	if n <= 0 {
		n = 15
	}
	switch unit {
	case "m":
		dur = time.Duration(n) * time.Minute
		label = fmt.Sprintf("%dm", n)
	case "h":
		dur = time.Duration(n) * time.Hour
		label = fmt.Sprintf("%dh", n)
	case "d":
		dur = time.Duration(n) * 24 * time.Hour
		label = fmt.Sprintf("%dd", n)
	}
	return n, unit, dur, label
}
