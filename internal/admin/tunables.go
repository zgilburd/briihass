package admin

import (
	"fmt"
	"net/http"
	"strconv"

	"briihass/internal/config"
)

type tunablesPage struct {
	Commit       string
	Tunables     *config.Tunables
	BeaconNames  []string // names from snapshot, sorted
	Notice       string
	Errors       []string
	FormReposted bool
}

// handleTunables serves GET (form pre-filled with current values) and
// POST (validate -> Store.SaveAll -> ApplyTunables -> redirect).
func (s *Server) handleTunables(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.renderTunables(w, tunablesPage{
			Commit:      s.opts.BuildCommit,
			Tunables:    s.currentTunables(),
			BeaconNames: s.beaconNames(),
		})
	case http.MethodPost:
		s.handleTunablesPost(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) renderTunables(w http.ResponseWriter, page tunablesPage) {
	s.render(w, "tunables.html", page)
}

func (s *Server) beaconNames() []string {
	snap := s.opts.Engine.Snapshot()
	out := make([]string, 0, len(snap.Beacons))
	for _, b := range snap.Beacons {
		out = append(out, b.Name)
	}
	return out
}

func (s *Server) handleTunablesPost(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	cur := s.currentTunables()
	beaconNames := s.beaconNames()

	candidate, errs := parseTunablesForm(r, cur, beaconNames)
	if len(errs) == 0 {
		if vErr := candidate.Validate(); vErr != nil {
			errs = append(errs, vErr.Error())
		}
	}
	if len(errs) > 0 {
		s.renderTunables(w, tunablesPage{
			Commit:       s.opts.BuildCommit,
			Tunables:     candidate, // show the user's edits, not the saved values
			BeaconNames:  beaconNames,
			Errors:       errs,
			FormReposted: true,
		})
		return
	}

	if err := s.opts.Store.SaveAll(r.Context(), candidate); err != nil {
		s.opts.Logger.Error("admin tunables save", "err", err)
		s.renderTunables(w, tunablesPage{
			Commit:       s.opts.BuildCommit,
			Tunables:     candidate,
			BeaconNames:  beaconNames,
			Errors:       []string{"save failed: " + err.Error()},
			FormReposted: true,
		})
		return
	}
	s.opts.Engine.ApplyTunables(candidate)
	s.replaceCurrent(candidate)
	s.opts.Logger.Info("tunables updated via admin UI")

	// PRG: redirect to GET so refresh doesn't re-POST.
	http.Redirect(w, r, "/admin/tunables?saved=1", http.StatusSeeOther)
}

// parseTunablesForm builds a new *config.Tunables from POSTed form
// values. Unknown / unparseable fields produce a list of error
// strings that the caller can render alongside the form.
func parseTunablesForm(r *http.Request, cur *config.Tunables, beaconNames []string) (*config.Tunables, []string) {
	var errs []string

	// Defaults block.
	d := config.DefaultsBlock{}
	d.Alpha = parseFloatField(r, "default_alpha", cur.Defaults.Alpha, &errs)
	d.GracePeriodS = parseIntField(r, "default_grace_period_s", cur.Defaults.GracePeriodS, &errs)
	d.DecayRateDbPerS = parseFloatField(r, "default_decay_rate_db_per_s", cur.Defaults.DecayRateDbPerS, &errs)
	d.PresenceFloorDbm = parseIntField(r, "default_presence_floor_dbm", cur.Defaults.PresenceFloorDbm, &errs)
	d.TAwayMaxS = parseIntField(r, "default_t_away_max_s", cur.Defaults.TAwayMaxS, &errs)
	d.StickyAfterArrivalS = parseIntField(r, "default_sticky_after_arrival_s", cur.Defaults.StickyAfterArrivalS, &errs)
	d.HysteresisDb = parseFloatField(r, "default_hysteresis_db", cur.Defaults.HysteresisDb, &errs)
	d.ConfirmCount = parseIntField(r, "default_confirm_count", cur.Defaults.ConfirmCount, &errs)

	out := &config.Tunables{
		Defaults: d,
		Beacons:  make(map[string]config.Overrides, len(beaconNames)),
	}

	// Per-beacon overrides. For each known beacon, look for prefixed
	// fields; only include the override if the field was submitted
	// with a non-empty value (empty = "use default").
	for _, name := range beaconNames {
		o := config.Overrides{}
		any := false
		if v, ok := optFloat(r, "beacon_"+name+"_alpha", &errs); ok {
			o.Alpha = &v
			any = true
		}
		if v, ok := optInt(r, "beacon_"+name+"_grace_period_s", &errs); ok {
			o.GracePeriodS = &v
			any = true
		}
		if v, ok := optFloat(r, "beacon_"+name+"_decay_rate_db_per_s", &errs); ok {
			o.DecayRateDbPerS = &v
			any = true
		}
		if v, ok := optInt(r, "beacon_"+name+"_presence_floor_dbm", &errs); ok {
			o.PresenceFloorDbm = &v
			any = true
		}
		if v, ok := optInt(r, "beacon_"+name+"_t_away_max_s", &errs); ok {
			o.TAwayMaxS = &v
			any = true
		}
		if v, ok := optInt(r, "beacon_"+name+"_sticky_after_arrival_s", &errs); ok {
			o.StickyAfterArrivalS = &v
			any = true
		}
		if v, ok := optFloat(r, "beacon_"+name+"_hysteresis_db", &errs); ok {
			o.HysteresisDb = &v
			any = true
		}
		if v, ok := optInt(r, "beacon_"+name+"_confirm_count", &errs); ok {
			o.ConfirmCount = &v
			any = true
		}
		if any {
			out.Beacons[name] = o
		}
	}
	return out, errs
}

func parseFloatField(r *http.Request, name string, fallback float64, errs *[]string) float64 {
	raw := r.FormValue(name)
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		*errs = append(*errs, fmt.Sprintf("%s: %v", name, err))
		return fallback
	}
	return v
}

func parseIntField(r *http.Request, name string, fallback int, errs *[]string) int {
	raw := r.FormValue(name)
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		*errs = append(*errs, fmt.Sprintf("%s: %v", name, err))
		return fallback
	}
	return v
}

// optFloat returns (value, true) iff the form had a non-empty entry.
func optFloat(r *http.Request, name string, errs *[]string) (float64, bool) {
	raw := r.FormValue(name)
	if raw == "" {
		return 0, false
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		*errs = append(*errs, fmt.Sprintf("%s: %v", name, err))
		return 0, false
	}
	return v, true
}

func optInt(r *http.Request, name string, errs *[]string) (int, bool) {
	raw := r.FormValue(name)
	if raw == "" {
		return 0, false
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		*errs = append(*errs, fmt.Sprintf("%s: %v", name, err))
		return 0, false
	}
	return v, true
}
