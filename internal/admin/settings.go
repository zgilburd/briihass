package admin

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"briihass/internal/store"
)

type settingsPage struct {
	Commit  string
	Now     time.Time
	Current store.Settings
	Notice  string
	Error   string

	// LoadFailed is true when LoadSettings errored. The template hides
	// the form in that case so the operator can't accidentally save
	// zero-valued settings on top of a transient DB problem.
	LoadFailed bool
}

func (s *Server) handleSettings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		s.renderSettings(w, r, "", "")
	case http.MethodPost:
		s.handleSettingsPost(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) renderSettings(w http.ResponseWriter, r *http.Request, notice, errMsg string) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	cur, err := s.opts.Settings.LoadSettings(ctx)
	loadFailed := false
	if err != nil {
		s.opts.Logger.Error("admin settings load", "err", err)
		errMsg = "load settings: " + err.Error()
		loadFailed = true
		cur = store.Settings{} // explicit zero so the template can't surface stale values
	}
	page := settingsPage{
		Commit:     s.opts.BuildCommit,
		Now:        time.Now(),
		Current:    cur,
		Notice:     notice,
		Error:      errMsg,
		LoadFailed: loadFailed,
	}
	s.render(w, "settings.html", page)
}

func (s *Server) handleSettingsPost(w http.ResponseWriter, r *http.Request) {
	// Defense in depth: if LoadSettings is currently failing the form
	// shouldn't be reachable (the GET render hides it), but if an
	// operator POSTs from a stale tab we still refuse to write.
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if _, lerr := s.opts.Settings.LoadSettings(ctx); lerr != nil {
		s.opts.Logger.Error("admin settings post rejected (load failing)", "err", lerr)
		s.renderSettings(w, r, "", "settings load is failing; refusing to overwrite. retry later.")
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	days, err := strconv.Atoi(r.FormValue("retention_days"))
	if err != nil {
		s.renderSettings(w, r, "", "retention_days must be an integer in 1..30")
		return
	}
	st, verr := store.NewSettings(days,
		r.FormValue("capture_per_event_hex") == "on",
		r.FormValue("capture_full_posts") == "on")
	if verr != nil {
		s.renderSettings(w, r, "", verr.Error())
		return
	}
	if err := s.opts.Settings.SaveSettings(ctx, st); err != nil {
		s.renderSettings(w, r, "", "save: "+err.Error())
		return
	}
	if s.opts.SettingsSnap != nil {
		// SaveSettings already validated; Replace will too. If Replace
		// errors here it's a programmer bug (we just saved the same st),
		// so log loudly and abort the redirect — the operator should see
		// the failure rather than a silent stale snapshot.
		if rerr := s.opts.SettingsSnap.Replace(st); rerr != nil {
			s.opts.Logger.Error("settings snapshot replace rejected after successful save", "err", rerr)
			s.renderSettings(w, r, "", "internal: snapshot validation failed after save — "+rerr.Error())
			return
		}
	}
	s.opts.Logger.Info("settings updated",
		"retention_days", st.RetentionDays(),
		"capture_per_event_hex", st.CapturePerEventHex(),
		"capture_full_posts", st.CaptureFullPosts())
	http.Redirect(w, r, "/admin/settings?saved=1", http.StatusSeeOther)
}
