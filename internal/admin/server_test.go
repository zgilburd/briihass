package admin

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"briihass/internal/config"
	"briihass/internal/ids"
	"briihass/internal/presence"
	"briihass/internal/store"
)

// fakeEngine satisfies the EngineHandle interface for tests.
type fakeEngine struct {
	mu       sync.Mutex
	snap     presence.EngineSnapshot
	tunSet   *config.Tunables
	topSet   *config.Topology
	topCount int
}

func (f *fakeEngine) Snapshot() presence.EngineSnapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.snap
}

func (f *fakeEngine) ApplyTunables(t *config.Tunables) {
	f.mu.Lock()
	f.tunSet = t
	f.mu.Unlock()
}

func (f *fakeEngine) ApplyTopology(top *config.Topology) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.topSet = top
	f.topCount++
}

func (f *fakeEngine) lastApplied() *config.Tunables {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tunSet
}

func (f *fakeEngine) lastAppliedTopology() (*config.Topology, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.topSet, f.topCount
}

// fakeStore satisfies store.Store for tests; in-memory, optionally
// rigged to fail SaveAll.
type fakeStore struct {
	mu       sync.Mutex
	tun      *config.Tunables
	saveErr  error
	saveCnt  int
	lastSave *config.Tunables
}

func (s *fakeStore) LoadAll(ctx context.Context) (*config.Tunables, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tun == nil {
		return nil, store.ErrEmpty
	}
	return s.tun, nil
}

func (s *fakeStore) SaveAll(ctx context.Context, t *config.Tunables) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saveCnt++
	if s.saveErr != nil {
		return s.saveErr
	}
	s.tun = t
	s.lastSave = t
	return nil
}

// mustSettings constructs a store.Settings via the validating
// constructor; tests can no longer build it with struct literals
// because the fields are unexported.
func mustSettings(t *testing.T, retentionDays int, perEventHex, fullPosts bool) store.Settings {
	t.Helper()
	s, err := store.NewSettings(retentionDays, perEventHex, fullPosts)
	if err != nil {
		t.Fatalf("store.NewSettings: %v", err)
	}
	return s
}

func sampleTunables() *config.Tunables {
	return &config.Tunables{
		Defaults: config.DefaultsBlock{
			Alpha:               0.4,
			GracePeriodS:        5,
			DecayRateDbPerS:     2.0,
			PresenceFloorDbm:    -95,
			TAwayMaxS:           30,
			StickyAfterArrivalS: 120,
			HysteresisDb:        4.0,
			ConfirmCount:        2,
		},
		Beacons: map[string]config.Overrides{},
	}
}

func newTestServer(t *testing.T) (*Server, *fakeEngine, *fakeStore) {
	t.Helper()
	st := &fakeStore{tun: sampleTunables()}

	fe := &fakeEngine{
		snap: presence.EngineSnapshot{
			AsOf: time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC),
			Beacons: []presence.BeaconSnapshot{{
				Beacon:       ids.MustNewIBeaconKey("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1", 1, 1),
				Name:         "tag_one",
				CurrentZone:  "zone_a",
				CurrentAP:    "aa:bb:cc:00:00:01",
				LastArrival:  time.Date(2026, 5, 18, 11, 59, 0, 0, time.UTC),
				StickyActive: true,
				APs: []presence.APSnapshot{{
					Mac:            "aa:bb:cc:00:00:01",
					Name:           "AP-A",
					LastRSSI:       -75,
					EWMARSSI:       -75.2,
					EffectiveRSSI:  -75.2,
					LastSightingTs: time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC),
					InPresence:     true,
				}},
			}},
		},
	}

	srv, err := New(Options{
		User:            "admin",
		Pass:            "hunter2",
		Engine:          fe,
		Store:           st,
		CurrentTunables: sampleTunables(),
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		BuildCommit:     "test",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return srv, fe, st
}

func TestAdmin_AuthRequired(t *testing.T) {
	srv, _, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/admin/status", nil)
	rr := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusUnauthorized {
		t.Errorf("unauth status = %d, want 401", rr.Code)
	}
	if !strings.HasPrefix(rr.Header().Get("WWW-Authenticate"), "Basic") {
		t.Errorf("WWW-Authenticate header missing or wrong: %q", rr.Header().Get("WWW-Authenticate"))
	}
}

func TestAdmin_AuthWrongCreds(t *testing.T) {
	srv, _, _ := newTestServer(t)

	cases := []struct {
		name string
		u, p string
	}{
		{"wrong user", "nope", "hunter2"},
		{"wrong pass", "admin", "nope"},
		{"empty", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/admin/status", nil)
			req.SetBasicAuth(tc.u, tc.p)
			rr := httptest.NewRecorder()
			srv.Routes().ServeHTTP(rr, req)
			if rr.Code != http.StatusUnauthorized {
				t.Errorf("status = %d, want 401", rr.Code)
			}
		})
	}
}

// TestAdmin_StatusPageRendersBeaconRows pins behavior of the status page
// against a non-trivial engine snapshot: one beacon in zone (with sticky
// active), one not_home. Regressions that wipe the EWMA column or the
// sticky-state indicator would slip past the bare TestAdmin_StatusPageRenders
// happy-path check.
func TestAdmin_StatusPageRendersBeaconRows(t *testing.T) {
	srv, fe, _ := newTestServer(t)

	// Replace the single-beacon snapshot from newTestServer with one
	// that has a second beacon in not_home and richer EWMA values
	// the template should render.
	fe.snap = presence.EngineSnapshot{
		AsOf: time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC),
		Beacons: []presence.BeaconSnapshot{
			{
				Beacon:       ids.MustNewIBeaconKey("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1", 1, 1),
				Name:         "tag_one",
				CurrentZone:  "zone_a",
				CurrentAP:    "aa:bb:cc:00:00:01",
				LastArrival:  time.Date(2026, 5, 18, 11, 59, 0, 0, time.UTC),
				StickyActive: true,
				APs: []presence.APSnapshot{{
					Mac:            "aa:bb:cc:00:00:01",
					Name:           "AP-A",
					LastRSSI:       -65,
					EWMARSSI:       -67.5,
					EffectiveRSSI:  -67.5,
					LastSightingTs: time.Date(2026, 5, 18, 12, 0, 0, 0, time.UTC),
					InPresence:     true,
				}},
			},
			{
				Beacon:       ids.MustNewIBeaconKey("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb2", 2, 2),
				Name:         "tag_away",
				CurrentZone:  "", // not_home
				StickyActive: false,
				APs:          nil,
			},
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/admin/status", nil)
	req.SetBasicAuth("admin", "hunter2")
	rr := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	body := rr.Body.String()
	for _, want := range []string{
		"tag_one",  // in-zone beacon name
		"tag_away", // not_home beacon name
		"zone_a",   // resolved zone label
		"AP-A",     // AP name from the snapshot
		"-67.5",    // EWMA value — proves the EWMA column rendered
		"sticky",   // sticky-state indicator
	} {
		if !strings.Contains(body, want) {
			t.Errorf("status body missing %q", want)
		}
	}
}

func TestAdmin_StatusPageRenders(t *testing.T) {
	srv, _, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/admin/status", nil)
	req.SetBasicAuth("admin", "hunter2")
	rr := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	body := rr.Body.String()
	for _, want := range []string{"tag_one", "zone_a", "AP-A", "sticky", "briihass"} {
		if !strings.Contains(body, want) {
			t.Errorf("status body missing %q", want)
		}
	}
}

// TestAdmin_NavConsistentAcrossPages locks the shared-nav partial: every
// page must expose all five top-level links and mark exactly one active.
// Before consolidation, status/tunables shipped only 2 of 5 links and
// packets/post highlighted nothing — this pins against that drift.
func TestAdmin_NavConsistentAcrossPages(t *testing.T) {
	srv, _, _ := newTestServer(t)

	wantLinks := []string{
		`href="/admin/status"`,
		`href="/admin/devices"`,
		`href="/admin/zones"`,
		`href="/admin/tunables"`,
		`href="/admin/settings"`,
	}

	cases := []struct {
		path   string
		active string // href expected to carry class="active"
	}{
		{"/admin/status", "/admin/status"},
		{"/admin/tunables", "/admin/tunables"},
	}

	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			req.SetBasicAuth("admin", "hunter2")
			rr := httptest.NewRecorder()
			srv.Routes().ServeHTTP(rr, req)
			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d", rr.Code)
			}
			body := rr.Body.String()
			for _, want := range wantLinks {
				if !strings.Contains(body, want) {
					t.Errorf("nav missing link %q", want)
				}
			}
			if n := strings.Count(body, `class="active"`); n != 1 {
				t.Errorf("active link count = %d, want exactly 1", n)
			}
			wantActive := `<a href="` + tc.active + `" class="active">`
			if !strings.Contains(body, wantActive) {
				t.Errorf("expected active marker on %q", tc.active)
			}
		})
	}
}

func TestAdmin_TunablesGetPreFilled(t *testing.T) {
	srv, _, _ := newTestServer(t)

	req := httptest.NewRequest(http.MethodGet, "/admin/tunables", nil)
	req.SetBasicAuth("admin", "hunter2")
	rr := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	body := rr.Body.String()
	// Defaults are pre-filled from the seed.
	for _, want := range []string{
		`name="default_alpha"`,
		`value="0.4"`,
		`name="default_sticky_after_arrival_s"`,
		`value="120"`,
		`<summary>tag_one</summary>`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("tunables form missing %q", want)
		}
	}
}

func TestAdmin_TunablesPostSavesAndReloads(t *testing.T) {
	srv, fe, st := newTestServer(t)

	form := url.Values{}
	form.Set("default_alpha", "0.5")
	form.Set("default_grace_period_s", "5")
	form.Set("default_decay_rate_db_per_s", "2.0")
	form.Set("default_presence_floor_dbm", "-95")
	form.Set("default_t_away_max_s", "30")
	form.Set("default_sticky_after_arrival_s", "180")
	form.Set("default_hysteresis_db", "4.0")
	form.Set("default_confirm_count", "2")
	form.Set("beacon_tag_one_sticky_after_arrival_s", "240")

	req := httptest.NewRequest(http.MethodPost, "/admin/tunables", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth("admin", "hunter2")
	rr := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status = %d (body=%q), want 303", rr.Code, rr.Body.String())
	}

	// Store saw the save.
	if st.saveCnt != 1 {
		t.Errorf("SaveAll calls = %d, want 1", st.saveCnt)
	}
	if st.lastSave == nil {
		t.Fatal("lastSave nil")
	}
	if st.lastSave.Defaults.Alpha != 0.5 {
		t.Errorf("saved alpha = %v, want 0.5", st.lastSave.Defaults.Alpha)
	}
	if st.lastSave.Defaults.StickyAfterArrivalS != 180 {
		t.Errorf("saved sticky default = %d, want 180", st.lastSave.Defaults.StickyAfterArrivalS)
	}
	if o, ok := st.lastSave.Beacons["tag_one"]; !ok || o.StickyAfterArrivalS == nil || *o.StickyAfterArrivalS != 240 {
		t.Errorf("saved tag_one override = %+v", o)
	}

	// Engine got the hot reload.
	applied := fe.lastApplied()
	if applied == nil {
		t.Fatal("engine.ApplyTunables not called")
	}
	if applied.Defaults.Alpha != 0.5 {
		t.Errorf("engine alpha = %v", applied.Defaults.Alpha)
	}
}

func TestAdmin_TunablesPostValidationError(t *testing.T) {
	srv, fe, st := newTestServer(t)

	form := url.Values{}
	// alpha out of range -> validation error.
	form.Set("default_alpha", "2.5")
	form.Set("default_grace_period_s", "5")
	form.Set("default_decay_rate_db_per_s", "2.0")
	form.Set("default_presence_floor_dbm", "-95")
	form.Set("default_t_away_max_s", "30")
	form.Set("default_sticky_after_arrival_s", "120")
	form.Set("default_hysteresis_db", "4.0")
	form.Set("default_confirm_count", "2")

	req := httptest.NewRequest(http.MethodPost, "/admin/tunables", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth("admin", "hunter2")
	rr := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "Validation errors") {
		t.Errorf("expected validation errors banner")
	}
	if st.saveCnt != 0 {
		t.Errorf("Store.SaveAll calls = %d, want 0 on validation error", st.saveCnt)
	}
	if fe.lastApplied() != nil {
		t.Errorf("engine ApplyTunables should NOT have been called on validation error")
	}
}

func TestAdmin_TunablesPostStoreError(t *testing.T) {
	srv, fe, st := newTestServer(t)
	st.saveErr = errors.New("simulated db failure")

	form := url.Values{}
	form.Set("default_alpha", "0.5")
	form.Set("default_grace_period_s", "5")
	form.Set("default_decay_rate_db_per_s", "2.0")
	form.Set("default_presence_floor_dbm", "-95")
	form.Set("default_t_away_max_s", "30")
	form.Set("default_sticky_after_arrival_s", "120")
	form.Set("default_hysteresis_db", "4.0")
	form.Set("default_confirm_count", "2")

	req := httptest.NewRequest(http.MethodPost, "/admin/tunables", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth("admin", "hunter2")
	rr := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (re-rendered form with error)", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "simulated db failure") {
		t.Errorf("error body missing the underlying message")
	}
	if fe.lastApplied() != nil {
		t.Errorf("engine ApplyTunables should NOT have been called when Store.SaveAll fails")
	}
}

func TestAdmin_RedirectRoot(t *testing.T) {
	srv, _, _ := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/admin", nil)
	req.SetBasicAuth("admin", "hunter2")
	rr := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status = %d, want 303", rr.Code)
	}
	if loc := rr.Header().Get("Location"); loc != "/admin/status" {
		t.Errorf("Location = %q", loc)
	}
}

func TestAdmin_StaticAssets(t *testing.T) {
	srv, _, _ := newTestServer(t)
	req := httptest.NewRequest(http.MethodGet, "/admin-static/style.css", nil)
	rr := httptest.NewRecorder()
	srv.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), ":root") {
		t.Errorf("style.css does not look like our stylesheet (no :root rule)")
	}
}

func TestNew_Validation(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fe := &fakeEngine{}
	st := &fakeStore{tun: sampleTunables()}
	cur := sampleTunables()

	cases := []struct {
		name string
		opts Options
		want string
	}{
		{"no user", Options{Pass: "p", Engine: fe, Store: st, CurrentTunables: cur, Logger: logger}, "User and Pass"},
		{"no pass", Options{User: "u", Engine: fe, Store: st, CurrentTunables: cur, Logger: logger}, "User and Pass"},
		{"no engine", Options{User: "u", Pass: "p", Store: st, CurrentTunables: cur, Logger: logger}, "Engine"},
		{"no store", Options{User: "u", Pass: "p", Engine: fe, CurrentTunables: cur, Logger: logger}, "Store"},
		{"no current", Options{User: "u", Pass: "p", Engine: fe, Store: st, Logger: logger}, "CurrentTunables"},
		{"no logger", Options{User: "u", Pass: "p", Engine: fe, Store: st, CurrentTunables: cur}, "Logger"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(tc.opts)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %v, want substring %q", err, tc.want)
			}
		})
	}
}
