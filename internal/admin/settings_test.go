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

	"briihass/internal/store"
)

// fakeSettingsStore is an in-memory SettingsStore + optional load
// error injection for the load-failure-UX test.
type fakeSettingsStore struct {
	mu       sync.Mutex
	cur      store.Settings
	loadErr  error
	saveErr  error
	saveCnt  int
	lastSave store.Settings
}

func (f *fakeSettingsStore) LoadSettings(_ context.Context) (store.Settings, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.loadErr != nil {
		return store.Settings{}, f.loadErr
	}
	return f.cur, nil
}

func (f *fakeSettingsStore) SaveSettings(_ context.Context, st store.Settings) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.saveErr != nil {
		return f.saveErr
	}
	f.saveCnt++
	f.lastSave = st
	f.cur = st
	return nil
}

func newSettingsServer(t *testing.T, st *fakeSettingsStore, snap *store.SettingsSnapshot) *Server {
	t.Helper()
	s, err := New(Options{
		User:            "u",
		Pass:            "p",
		Engine:          &fakeEngine{},
		Store:           &fakeStore{},
		CurrentTunables: sampleTunables(),
		Settings:        st,
		SettingsSnap:    snap,
		BuildCommit:     "test",
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// TestSettings_SaveUpdatesSnapshot is the load-bearing test: the
// ingest hot path reads from SettingsSnap, so a successful POST must
// propagate to it so retention/capture toggles take effect on the
// very next request.
func TestSettings_SaveUpdatesSnapshot(t *testing.T) {
	fs := &fakeSettingsStore{cur: mustSettings(t, 7, false, false)}
	snap := store.NewSettingsSnapshot(mustSettings(t, 7, false, false))
	s := newSettingsServer(t, fs, snap)

	form := url.Values{
		"retention_days":        {"3"},
		"capture_per_event_hex": {"on"},
		// capture_full_posts intentionally omitted -> should resolve to false
	}
	req := withAuth(httptest.NewRequest(http.MethodPost, "/admin/settings", strings.NewReader(form.Encode())))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status: %d, body=%s", rr.Code, rr.Body.String())
	}
	if fs.saveCnt != 1 {
		t.Fatalf("SaveSettings call count: %d", fs.saveCnt)
	}
	if fs.lastSave.RetentionDays() != 3 || !fs.lastSave.CapturePerEventHex() || fs.lastSave.CaptureFullPosts() {
		t.Errorf("saved settings: %+v", fs.lastSave)
	}
	if got := snap.Get(); got.RetentionDays() != 3 || !got.CapturePerEventHex() {
		t.Errorf("snapshot not refreshed; got %+v", got)
	}
}

// TestSettings_RejectsOutOfRangeRetention enforces the 1..30 bound.
func TestSettings_RejectsOutOfRangeRetention(t *testing.T) {
	for _, days := range []string{"0", "31", "-1", "abc"} {
		t.Run("days="+days, func(t *testing.T) {
			fs := &fakeSettingsStore{cur: mustSettings(t, 7, false, false)}
			snap := store.NewSettingsSnapshot(mustSettings(t, 7, false, false))
			s := newSettingsServer(t, fs, snap)

			form := url.Values{"retention_days": {days}}
			req := withAuth(httptest.NewRequest(http.MethodPost, "/admin/settings", strings.NewReader(form.Encode())))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			rr := httptest.NewRecorder()
			s.Routes().ServeHTTP(rr, req)
			if rr.Code != http.StatusOK {
				t.Fatalf("expected re-rendered page, got %d", rr.Code)
			}
			if fs.saveCnt != 0 {
				t.Errorf("should not have saved on invalid input; saveCnt=%d", fs.saveCnt)
			}
			if got := snap.Get(); got.RetentionDays() != 7 {
				t.Errorf("snapshot mutated on rejected POST: %+v", got)
			}
		})
	}
}

// TestSettings_SaveFailureDoesNotAdvanceSnapshot verifies that an
// error from SaveSettings leaves the in-memory snapshot untouched.
// Otherwise the next ingest request would read a value that's not in
// the DB, and on the next process restart the bridge would silently
// diverge from operator intent.
func TestSettings_SaveFailureDoesNotAdvanceSnapshot(t *testing.T) {
	fs := &fakeSettingsStore{
		cur:     mustSettings(t, 7, false, false),
		saveErr: errors.New("db down"),
	}
	snap := store.NewSettingsSnapshot(mustSettings(t, 7, false, false))
	priorPtr := snap.Get()
	s := newSettingsServer(t, fs, snap)

	form := url.Values{
		"retention_days":        {"3"},
		"capture_per_event_hex": {"on"},
	}
	req := withAuth(httptest.NewRequest(http.MethodPost, "/admin/settings", strings.NewReader(form.Encode())))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected re-rendered page on save failure, got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "save:") {
		t.Errorf("expected save-error banner; body=%s", rr.Body.String())
	}
	got := snap.Get()
	if got.RetentionDays() != 7 || got.CapturePerEventHex() || got.CaptureFullPosts() {
		t.Errorf("snapshot advanced despite save failure: %+v", got)
	}
	// Value equality is the load-bearing assertion; pointer identity
	// across atomic.Pointer Load is not guaranteed. priorPtr is held
	// to ensure the value didn't get mutated in place either.
	if got != priorPtr {
		t.Errorf("snapshot value drifted from prior: prior=%+v got=%+v", priorPtr, got)
	}
}

// TestSettings_LoadErrorHidesForm verifies the load-failure UX:
// the form is hidden and a POST is refused so the operator can't
// overwrite valid settings with form defaults.
func TestSettings_LoadErrorHidesForm(t *testing.T) {
	fs := &fakeSettingsStore{loadErr: errors.New("postgres down")}
	snap := store.NewSettingsSnapshot(mustSettings(t, 7, false, false))
	s := newSettingsServer(t, fs, snap)

	// GET: form hidden.
	req := withAuth(httptest.NewRequest(http.MethodGet, "/admin/settings", nil))
	rr := httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET status: %d", rr.Code)
	}
	body := rr.Body.String()
	if strings.Contains(body, `name="retention_days"`) {
		t.Errorf("form should be hidden when LoadSettings fails")
	}
	if !strings.Contains(body, "load settings:") {
		t.Errorf("expected load-error banner; body=%s", body)
	}

	// POST: rejected.
	form := url.Values{"retention_days": {"5"}}
	req = withAuth(httptest.NewRequest(http.MethodPost, "/admin/settings", strings.NewReader(form.Encode())))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr = httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("POST should re-render, got %d", rr.Code)
	}
	if fs.saveCnt != 0 {
		t.Errorf("POST should not have saved while load is failing; saveCnt=%d", fs.saveCnt)
	}
}
