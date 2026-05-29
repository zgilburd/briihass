package admin

import (
	"context"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"briihass/internal/ids"
	"briihass/internal/store"
)

type zonesPage struct {
	Commit string
	Now    time.Time
	Rows   []zoneRow
	Notice string
	Error  string

	// RebuildError is set when a zone insert/delete succeeded but the
	// engine topology refresh failed. The template renders a Retry
	// button (POST to RetryURL) so the operator can re-run ApplyTopology.
	RebuildError string
	RetryURL     string
}

type zoneRow struct {
	APMac     string
	APName    string
	ZoneLabel string
	Known     bool // present in zones table
}

func (s *Server) handleZones(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.renderZones(w, r, "", "", "")
	case http.MethodPost:
		s.handleZonesPost(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleZonesRefreshEngine re-runs ApplyTopology against the current
// DB state and reconciles MQTT orphans. Linked from the inline-error
// banner shown on the zones page when a zone upsert/delete partially
// failed (or when a concurrent demote left an MQTT orphan). Idempotent.
func (s *Server) handleZonesRefreshEngine(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := s.rebuildEngineTopology(ctx); err != nil {
		s.opts.Logger.Error("rebuild topology (zones manual retry)", "err", err)
		s.renderZones(w, r, "", "", err.Error())
		return
	}
	orphans, rerr := s.reconcileOrphans(ctx)
	q := url.Values{"refreshed": {"1"}}
	if rerr != nil {
		s.opts.Logger.Warn("zones engine topology refreshed but orphan reconcile failed",
			"err", rerr)
		q.Set("reconcile_err", rerr.Error())
	} else {
		s.opts.Logger.Info("engine topology refreshed (zones manual retry)",
			"orphans_reconciled", orphans)
	}
	http.Redirect(w, r, "/admin/zones?"+q.Encode(), http.StatusSeeOther)
}

func (s *Server) handleZonesPost(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	action := r.FormValue("action")
	apName := strings.TrimSpace(r.FormValue("ap_name"))
	// MAC validation at the boundary: ids.NewAPMAC rejects malformed
	// input (e.g. "not_a_mac") with a typed error so the bad row can never
	// reach the DB. Without this guard the row would land, then break
	// every subsequent rebuildEngineTopology because config.NewTopology
	// rejects un-parseable MACs — bricking the bridge until the row is
	// manually deleted.
	apMac, macErr := ids.NewAPMAC(r.FormValue("ap_mac"))
	if macErr != nil {
		s.renderZones(w, r, "", "ap_mac: "+macErr.Error(), "")
		return
	}
	dbCtx, dbCancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer dbCancel()
	switch action {
	case "delete":
		if err := s.opts.Zones.DeleteZone(dbCtx, apMac); err != nil {
			s.renderZones(w, r, "", "delete: "+err.Error(), "")
			return
		}
	default:
		label, labelErr := ids.NewZoneLabel(strings.TrimSpace(r.FormValue("zone_label")))
		if labelErr != nil {
			s.renderZones(w, r, "", "zone_label: "+labelErr.Error(), "")
			return
		}
		if err := s.opts.Zones.UpsertZone(dbCtx, store.NewZone(apMac, label, apName)); err != nil {
			s.renderZones(w, r, "", "save: "+err.Error(), "")
			return
		}
	}
	// rebuildCtx is decoupled from r.Context() so a browser cancel
	// after the DB commit doesn't leave the engine map stale. Mirrors
	// the devices.go demote pattern (mqttCtx split) for the same
	// reason: DB success → engine refresh must complete even if the
	// operator closes the tab.
	rebuildCtx, rebuildCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer rebuildCancel()
	if err := s.rebuildEngineTopology(rebuildCtx); err != nil {
		s.opts.Logger.Error("rebuild topology after zone change", "err", err)
		s.renderZones(w, r,
			"zone saved in DB but engine refresh failed",
			"",
			err.Error())
		return
	}
	http.Redirect(w, r, "/admin/zones?saved=1", http.StatusSeeOther)
}

func (s *Server) renderZones(w http.ResponseWriter, r *http.Request, notice, errMsg, rebuildErr string) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	known, kerr := s.opts.Zones.ListZones(ctx)
	if kerr != nil {
		// Render error-only: an empty "known" + populated "observed"
		// would mislead the operator into thinking known zones were
		// deleted. Bail early with the underlying error visible.
		s.opts.Logger.Error("admin zones list", "err", kerr)
		s.render(w, "zones.html", zonesPage{
			Commit: s.opts.BuildCommit,
			Now:    time.Now(),
			Notice: notice,
			Error:  "list zones: " + kerr.Error(),
		})
		return
	}
	retention := s.retentionDays()
	since := time.Now().Add(-time.Duration(retention) * 24 * time.Hour)
	seen, serr := s.opts.Zones.ListAPsSince(ctx, since)
	if serr != nil {
		s.opts.Logger.Error("admin zones observed-aps", "err", serr)
		if errMsg == "" {
			errMsg = "observed APs lookup failed: " + serr.Error()
		}
	}

	byMac := map[string]zoneRow{}
	for _, z := range known {
		mac := z.APMac.String()
		byMac[mac] = zoneRow{APMac: mac, APName: z.APName, ZoneLabel: z.ZoneLabel.String(), Known: true}
	}
	for mac, name := range seen {
		if _, ok := byMac[mac]; ok {
			row := byMac[mac]
			if row.APName == "" {
				row.APName = name
			}
			byMac[mac] = row
			continue
		}
		byMac[mac] = zoneRow{APMac: mac, APName: name, Known: false}
	}
	rows := make([]zoneRow, 0, len(byMac))
	for _, r := range byMac {
		rows = append(rows, r)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Known != rows[j].Known {
			return rows[i].Known
		}
		return rows[i].APMac < rows[j].APMac
	})

	page := zonesPage{
		Commit: s.opts.BuildCommit,
		Now:    time.Now(),
		Rows:   rows,
		Notice: notice,
		Error:  errMsg,
	}
	if rebuildErr != "" {
		page.RebuildError = rebuildErr
		page.RetryURL = "/admin/zones/refresh-engine"
	}
	s.render(w, "zones.html", page)
}
