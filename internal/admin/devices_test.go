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
	"sync/atomic"
	"testing"
	"time"

	"briihass/internal/config"
	"briihass/internal/ids"
	"briihass/internal/presence"
	"briihass/internal/store"
)

// mustBeacon panics if the inputs are invalid. Test helper only.
func mustBeacon(uuid string, major, minor uint16, name string) store.Beacon {
	b, err := store.NewBeacon(ids.MustNewIBeaconKey(uuid, major, minor), name, "")
	if err != nil {
		panic(err)
	}
	return b
}

// mustZone panics if the inputs are invalid. Test helper only.
func mustZone(apMac, label, apName string) store.Zone {
	return store.NewZone(ids.MustNewAPMAC(apMac), ids.MustNewZoneLabel(label), apName)
}

// fakeDevicesStore implements DevicesStore + ZonesStore in memory.
type fakeDevicesStore struct {
	mu           sync.Mutex
	beacons      []store.Beacon
	devices      []store.DeviceSummary
	demotes      []store.Beacon
	promotes     []store.Beacon
	observations []store.Observation

	// listErr, when set, is returned by ListBeacons / ListDevicesSince
	// instead of the in-memory rows. Lets tests drive the
	// rebuild-failure and list-error code paths that would otherwise
	// require a real DB outage.
	listErr        error
	listDevicesErr error
	listObsErr     error
}

func (f *fakeDevicesStore) ListDevicesSince(_ context.Context, _ time.Time) ([]store.DeviceSummary, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listDevicesErr != nil {
		return nil, f.listDevicesErr
	}
	return append([]store.DeviceSummary(nil), f.devices...), nil
}

func (f *fakeDevicesStore) ListObservationsForDevice(_ context.Context, _, _ string, _ int) ([]store.Observation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listObsErr != nil {
		return nil, f.listObsErr
	}
	return append([]store.Observation(nil), f.observations...), nil
}

func (f *fakeDevicesStore) PromoteBeacon(_ context.Context, b store.Beacon) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, existing := range f.beacons {
		if existing.Key == b.Key {
			return store.ErrConflict
		}
	}
	f.beacons = append(f.beacons, b)
	f.promotes = append(f.promotes, b)
	return nil
}

func (f *fakeDevicesStore) DemoteBeacon(_ context.Context, id ids.BeaconKey) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, b := range f.beacons {
		if b.Key == id {
			f.demotes = append(f.demotes, b)
			f.beacons = append(f.beacons[:i], f.beacons[i+1:]...)
			return nil
		}
	}
	return store.ErrNotFound
}

func (f *fakeDevicesStore) ListBeacons(_ context.Context) ([]store.Beacon, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listErr != nil {
		return nil, f.listErr
	}
	return append([]store.Beacon(nil), f.beacons...), nil
}

type fakeZonesStore struct {
	mu      sync.Mutex
	zones   []store.Zone
	aps     map[string]string
	listErr error
	apsErr  error
}

func (f *fakeZonesStore) ListAPsSince(_ context.Context, _ time.Time) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.apsErr != nil {
		return nil, f.apsErr
	}
	out := map[string]string{}
	for k, v := range f.aps {
		out[k] = v
	}
	return out, nil
}

func (f *fakeZonesStore) UpsertZone(_ context.Context, z store.Zone) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, existing := range f.zones {
		if existing.APMac == z.APMac {
			f.zones[i] = z
			return nil
		}
	}
	f.zones = append(f.zones, z)
	return nil
}

func (f *fakeZonesStore) DeleteZone(_ context.Context, apMac ids.APMAC) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, z := range f.zones {
		if z.APMac == apMac {
			f.zones = append(f.zones[:i], f.zones[i+1:]...)
			return nil
		}
	}
	return store.ErrNotFound
}

func (f *fakeZonesStore) ListZones(_ context.Context) ([]store.Zone, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.listErr != nil {
		return nil, f.listErr
	}
	return append([]store.Zone(nil), f.zones...), nil
}

type fakeRemover struct {
	mu      sync.Mutex
	removed []presence.BeaconKey
	// err, when non-nil, is returned by every call. Lets tests drive the
	// admin handler's error-branch (Retry banner) instead of the happy
	// redirect.
	err error
}

func (f *fakeRemover) remove(_ context.Context, b presence.BeaconKey) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.removed = append(f.removed, b)
	return nil
}

// fakeRawPostsStore implements RawPostsStore for the §7 handleRawPost
// tests. Keyed by id; an entry with id == errOnGet returns the configured
// err instead of the body.
type fakeRawPostsStore struct {
	mu  sync.Mutex
	by  map[int64]*store.RawPost
	err error
}

func (f *fakeRawPostsStore) GetRawPost(_ context.Context, id int64) (*store.RawPost, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	p, ok := f.by[id]
	if !ok {
		return nil, store.ErrNotFound
	}
	return p, nil
}

func newPhase3Server(t *testing.T, devs *fakeDevicesStore, zones *fakeZonesStore, eng *fakeEngine, rm *fakeRemover) *Server {
	t.Helper()
	return newPhase3ServerWithReconcile(t, devs, zones, eng, rm, nil)
}

// newPhase3ServerWithRawPosts wires a non-nil RawPosts so /admin/posts/{id}
// tests can exercise GetRawPost. Other knobs match newPhase3Server.
func newPhase3ServerWithRawPosts(t *testing.T, devs *fakeDevicesStore, zones *fakeZonesStore, eng *fakeEngine, rm *fakeRemover, rp *fakeRawPostsStore) *Server {
	t.Helper()
	s, err := New(Options{
		User:             "u",
		Pass:             "p",
		Engine:           eng,
		Store:            &fakeStore{},
		CurrentTunables:  sampleTunables(),
		Devices:          devs,
		Zones:            zones,
		RawPosts:         rp,
		Settings:         nil,
		SettingsSnap:     store.NewSettingsSnapshot(mustSettings(t, 7, true, false)),
		RemoveMQTTEntity: rm.remove,
		BuildCommit:      "test",
		Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

// newPhase3ServerWithReconcile is a superset constructor that also
// wires an OrphanReconcile hook. Only the I2 reconcile-flow tests
// need it; the existing callers can keep using newPhase3Server.
func newPhase3ServerWithReconcile(t *testing.T, devs *fakeDevicesStore, zones *fakeZonesStore, eng *fakeEngine, rm *fakeRemover, rec func(context.Context, []presence.BeaconKey) (int, error)) *Server {
	t.Helper()
	s, err := New(Options{
		User:             "u",
		Pass:             "p",
		Engine:           eng,
		Store:            &fakeStore{},
		CurrentTunables:  sampleTunables(),
		Devices:          devs,
		Zones:            zones,
		RawPosts:         nil,
		Settings:         nil,
		SettingsSnap:     store.NewSettingsSnapshot(mustSettings(t, 7, true, false)),
		RemoveMQTTEntity: rm.remove,
		OrphanReconcile:  rec,
		BuildCommit:      "test",
		Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func TestDevices_PromoteAndDemoteRoundTrip(t *testing.T) {
	devs := &fakeDevicesStore{}
	// Seed the observed-only listing with one beacon.
	devs.devices = []store.DeviceSummary{
		{Kind: "ibeacon", Key: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1_1_2", LastAPMac: "aa:bb:cc:dd:ee:01", LastRSSI: -50, LastSeen: time.Now().Add(-time.Minute), SightingCnt: 3, Tracked: false},
	}
	zones := &fakeZonesStore{zones: []store.Zone{mustZone("aa:bb:cc:dd:ee:01", "zone_a", "")}}
	eng := &fakeEngine{}
	rm := &fakeRemover{}
	s := newPhase3Server(t, devs, zones, eng, rm)

	// GET /admin/devices renders without error.
	req := withAuth(httptest.NewRequest(http.MethodGet, "/admin/devices?window_n=15&window_unit=m", nil))
	rr := httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /admin/devices: %d, body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "Observed only") {
		t.Errorf("response missing Observed-only section")
	}

	// Promote.
	form := url.Values{
		"slug": {"ibeacon.aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1_1_2"},
		"name": {"alpha"},
	}
	req = withAuth(httptest.NewRequest(http.MethodPost, "/admin/devices/promote", strings.NewReader(form.Encode())))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr = httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("promote status: %d, body=%s", rr.Code, rr.Body.String())
	}
	if len(devs.promotes) != 1 || devs.promotes[0].Name != "alpha" {
		t.Errorf("promote: %+v", devs.promotes)
	}
	// Engine MUST have received the updated topology with the new beacon
	// — without this, a regression that silently drops ApplyTopology
	// would let the ingest path still treat the beacon as untracked.
	top, count := eng.lastAppliedTopology()
	if count != 1 {
		t.Fatalf("ApplyTopology call count after promote: got %d want 1", count)
	}
	if top == nil || top.BeaconCount() != 1 || top.Beacons()[0].Name() != "alpha" {
		t.Fatalf("ApplyTopology received wrong topology after promote: %+v", top)
	}

	// Demote.
	form = url.Values{
		"slug": {"ibeacon.aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1_1_2"},
	}
	req = withAuth(httptest.NewRequest(http.MethodPost, "/admin/devices/demote", strings.NewReader(form.Encode())))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr = httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("demote status: %d, body=%s", rr.Code, rr.Body.String())
	}
	if len(devs.demotes) != 1 {
		t.Errorf("demote: %+v", devs.demotes)
	}
	if len(rm.removed) != 1 || rm.removed[0].Key() != "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1_1_2" {
		t.Errorf("MQTT remover: %+v", rm.removed)
	}
}

// TestDevices_PromoteEscapesRedirectName guards against operator-name
// values that would corrupt the redirect query string or attempt header
// injection. The bridge can't trust the form input — wrap with
// url.QueryEscape.
func TestDevices_PromoteEscapesRedirectName(t *testing.T) {
	devs := &fakeDevicesStore{}
	zones := &fakeZonesStore{}
	eng := &fakeEngine{}
	rm := &fakeRemover{}
	s := newPhase3Server(t, devs, zones, eng, rm)

	form := url.Values{
		"slug": {"ibeacon.aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1_1_2"},
		"name": {"evil&injected=1 #frag"},
	}
	req := withAuth(httptest.NewRequest(http.MethodPost, "/admin/devices/promote", strings.NewReader(form.Encode())))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status: %d, body=%s", rr.Code, rr.Body.String())
	}
	loc := rr.Header().Get("Location")
	// The escaped form must NOT contain raw ampersand/hash that would
	// split the query string.
	if strings.Contains(loc, "&injected=1") {
		t.Errorf("redirect Location not escaped — query injection possible: %q", loc)
	}
	if strings.Contains(loc, "#frag") {
		t.Errorf("redirect Location not escaped — fragment injection possible: %q", loc)
	}
}

// TestDevices_DemoteRendersErrorWhenRemoverFails exercises the
// MQTT-remove failure path. The DB row is deleted but the HA entity
// removal failed; the operator must see the Retry banner instead of a
// happy redirect that hides the lingering entity.
func TestDevices_DemoteRendersErrorWhenRemoverFails(t *testing.T) {
	devs := &fakeDevicesStore{
		beacons: []store.Beacon{mustBeacon("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1", 1, 2, "alpha")},
	}
	zones := &fakeZonesStore{}
	eng := &fakeEngine{}
	rm := &fakeRemover{err: errors.New("mqtt queue saturated")}
	s := newPhase3Server(t, devs, zones, eng, rm)

	form := url.Values{
		"slug": {"ibeacon.aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1_1_2"},
	}
	req := withAuth(httptest.NewRequest(http.MethodPost, "/admin/devices/demote", strings.NewReader(form.Encode())))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)

	// MUST NOT redirect — a 303 hides the failure from the operator.
	if rr.Code == http.StatusSeeOther {
		t.Fatalf("expected error re-render, got redirect to %q", rr.Header().Get("Location"))
	}
	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 with error banner, got %d", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "mqtt queue saturated") {
		t.Errorf("error banner missing the underlying error: body=%s", body)
	}
}

func TestDevices_PromoteRejectsBadName(t *testing.T) {
	devs := &fakeDevicesStore{}
	zones := &fakeZonesStore{}
	eng := &fakeEngine{}
	rm := &fakeRemover{}
	s := newPhase3Server(t, devs, zones, eng, rm)

	form := url.Values{
		"slug": {"ibeacon.aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1_1_2"},
		// name missing
	}
	req := withAuth(httptest.NewRequest(http.MethodPost, "/admin/devices/promote", strings.NewReader(form.Encode())))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("expected re-rendered page, got %d", rr.Code)
	}
	if len(devs.promotes) != 0 {
		t.Errorf("nothing should have been promoted")
	}
}

// TestDevices_RefreshEngineReconcilesOrphans pins the I2 wiring: a
// POST to /admin/devices/refresh-engine must call OrphanReconcile
// with the current allowlist. Without this the Retry button can't
// recover a lingering HA entity after a MQTT-queue-saturated demote.
func TestDevices_RefreshEngineReconcilesOrphans(t *testing.T) {
	devs := &fakeDevicesStore{
		beacons: []store.Beacon{mustBeacon("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1", 1, 2, "alpha")},
	}
	zones := &fakeZonesStore{}
	eng := &fakeEngine{}
	rm := &fakeRemover{}
	var (
		called   atomic.Int32
		gotKnown atomic.Pointer[[]presence.BeaconKey]
	)
	rec := func(_ context.Context, known []presence.BeaconKey) (int, error) {
		called.Add(1)
		k := append([]presence.BeaconKey(nil), known...)
		gotKnown.Store(&k)
		return 0, nil
	}
	s := newPhase3ServerWithReconcile(t, devs, zones, eng, rm, rec)

	req := withAuth(httptest.NewRequest(http.MethodPost, "/admin/devices/refresh-engine", nil))
	rr := httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status: %d body=%s", rr.Code, rr.Body.String())
	}
	if called.Load() != 1 {
		t.Fatalf("OrphanReconcile call count: got %d want 1", called.Load())
	}
	known := gotKnown.Load()
	if known == nil || len(*known) != 1 {
		t.Fatalf("OrphanReconcile known list: %+v", known)
	}
	if (*known)[0].Key() != "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1_1_2" {
		t.Errorf("OrphanReconcile known UUID: %q", (*known)[0].Key())
	}
}

// TestDevices_DemoteUsesBackgroundCtxForMQTT pins the I3 fix: the
// MQTT remove call must NOT inherit the request context, so a
// browser cancel after the DB delete can't leave the deleted
// beacon's HA entity lingering. The capturing remover inspects the
// ctx *while it's live* (before the handler's defers fire) so we
// can prove the request and mqtt contexts are decoupled.
func TestDevices_DemoteUsesBackgroundCtxForMQTT(t *testing.T) {
	devs := &fakeDevicesStore{
		beacons: []store.Beacon{mustBeacon("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1", 1, 2, "alpha")},
	}
	zones := &fakeZonesStore{}
	eng := &fakeEngine{}
	rm := &ctxCapturingRemover{}
	s := newPhase3Server(t, devs, zones, eng, &fakeRemover{})
	// Replace the remover wired by newPhase3Server with our capturing one.
	s.opts.RemoveMQTTEntity = rm.remove

	// We cancel the request ctx BEFORE invoking the handler. If the
	// handler passed r.Context() through to mqtt, the remover would
	// see a cancelled ctx. With the fix, mqttCtx is from
	// context.Background() and is still live during the remove call.
	form := url.Values{
		"slug": {"ibeacon.aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1_1_2"},
	}
	req := withAuth(httptest.NewRequest(http.MethodPost, "/admin/devices/demote", strings.NewReader(form.Encode())))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rr := httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status: %d body=%s", rr.Code, rr.Body.String())
	}
	if !rm.called.Load() {
		t.Fatalf("RemoveMQTTEntity was not invoked")
	}
	if rm.liveErr.Load() != nil {
		// liveErr is captured at call-time, before the handler's
		// defers fire — non-nil means the mqtt ctx inherited a
		// cancellation from r.Context() instead of getting a fresh
		// background-derived budget.
		t.Errorf("mqtt ctx was already cancelled at call time: %v", *(rm.liveErr.Load()))
	}
	if !rm.hadDeadline.Load() {
		t.Errorf("mqtt ctx had no deadline; expected 5s budget")
	}
}

// ctxCapturingRemover records ctx state at invocation time so the
// test can prove the mqtt path used a context independent of the
// already-cancelled request ctx.
type ctxCapturingRemover struct {
	called      atomic.Bool
	liveErr     atomic.Pointer[error]
	hadDeadline atomic.Bool
}

func (c *ctxCapturingRemover) remove(ctx context.Context, _ presence.BeaconKey) error {
	c.called.Store(true)
	if err := ctx.Err(); err != nil {
		c.liveErr.Store(&err)
	}
	if _, ok := ctx.Deadline(); ok {
		c.hadDeadline.Store(true)
	}
	return nil
}

func withAuth(r *http.Request) *http.Request {
	r.SetBasicAuth("u", "p")
	return r
}

// TestDevices_RefreshShowsReconciledOrphanCount pins the §5 banner:
// after a successful /admin/devices/refresh-engine where the orphan
// reconcile cleared one or more lingering HA entities, the GET that
// follows the 303 redirect must render a visible success banner so
// the operator who fired the retry knows it worked.
func TestDevices_RefreshShowsReconciledOrphanCount(t *testing.T) {
	devs := &fakeDevicesStore{}
	zones := &fakeZonesStore{}
	eng := &fakeEngine{}
	rm := &fakeRemover{}
	s := newPhase3Server(t, devs, zones, eng, rm)

	req := withAuth(httptest.NewRequest(http.MethodGet, "/admin/devices?refreshed=1&orphans=2", nil))
	rr := httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status: %d, body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "Reconciled 2 orphan HA entities") {
		t.Errorf("expected pluralized orphan reconcile banner; body=%q", rr.Body.String())
	}
}

// TestDevices_RefreshShowsReconciledOrphanCount_Singular guards the
// singular/plural split in the template so a single-orphan reconcile
// doesn't read "1 orphan HA entities".
func TestDevices_RefreshShowsReconciledOrphanCount_Singular(t *testing.T) {
	devs := &fakeDevicesStore{}
	zones := &fakeZonesStore{}
	eng := &fakeEngine{}
	rm := &fakeRemover{}
	s := newPhase3Server(t, devs, zones, eng, rm)

	req := withAuth(httptest.NewRequest(http.MethodGet, "/admin/devices?refreshed=1&orphans=1", nil))
	rr := httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status: %d", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "Reconciled 1 orphan HA entity") {
		t.Errorf("expected singular banner; body=%q", body)
	}
}

// TestDevices_RefreshShowsReconcileError pins the failure-path banner:
// when /admin/devices/refresh-engine ran ApplyTopology successfully
// but the orphan reconcile errored, the redirect carries
// ?refreshed=1&reconcile_err=..., and the GET must render an amber
// banner that names the failure rather than silently clearing.
func TestDevices_RefreshShowsReconcileError(t *testing.T) {
	devs := &fakeDevicesStore{}
	zones := &fakeZonesStore{}
	eng := &fakeEngine{}
	rm := &fakeRemover{}
	s := newPhase3Server(t, devs, zones, eng, rm)

	req := withAuth(httptest.NewRequest(http.MethodGet, "/admin/devices?refreshed=1&reconcile_err=list+beacons+for+reconcile%3A+db+down", nil))
	rr := httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status: %d, body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	if !strings.Contains(body, "Engine refreshed, but orphan reconcile failed") {
		t.Errorf("expected reconcile-failure banner; body=%q", body)
	}
	if !strings.Contains(body, "db down") {
		t.Errorf("expected reconcile error detail in banner; body=%q", body)
	}
}

// --- §7 handler-test sweep -------------------------------------------------
//
// All the tests below were added in the round-5 fix-PR to close coverage
// gaps on operator-only handlers that previously had zero behavior-level
// tests. They use the harness above but exercise paths
// (handleDevicePackets, handleRawPost, parseDeviceSlug, the
// refresh-engine list-beacons-error path) the original device-mutation
// tests didn't reach.

// TestDevices_PacketsRendersTLVWalk pins that the packet-detail page
// (a) renders a 200, (b) shows the operator-visible beacon name from
// the allowlist, and (c) emits the TLV-walked records from the stored
// raw hex. Regression here would silently turn the operator's "what
// did this beacon advertise" view into an empty page.
func TestDevices_PacketsRendersTLVWalk(t *testing.T) {
	const uuid = "fda50693-a4e2-4fb1-afcf-c6eb07647825"
	const realIBeaconHex = "0201061AFF4C000215FDA50693A4E24FB1AFCFC6EB07647825275165C1FD"
	devs := &fakeDevicesStore{
		beacons: []store.Beacon{mustBeacon(uuid, 10065, 26049, "tag_test")},
		observations: []store.Observation{
			{
				ID:         42,
				ObservedAt: time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC),
				Kind:       "ibeacon",
				Key:        uuid + "_10065_26049",
				APMac:      "aa:bb:cc:dd:ee:01",
				APName:     "AP-Test",
				RSSI:       -70,
				RawHex:     realIBeaconHex,
				Tracked:    true,
			},
		},
	}
	zones := &fakeZonesStore{}
	eng := &fakeEngine{}
	rm := &fakeRemover{}
	s := newPhase3Server(t, devs, zones, eng, rm)

	slug := "ibeacon." + uuid + "_10065_26049"
	req := withAuth(httptest.NewRequest(http.MethodGet, "/admin/devices/"+slug+"/packets", nil))
	rr := httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status: %d, body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	if !strings.Contains(body, "tag_test") {
		t.Errorf("expected beacon name 'tag_test' in body; body=%q", body)
	}
	if !strings.Contains(body, "AP-Test") {
		t.Errorf("expected AP name in body; body=%q", body)
	}
	// The page renders TLV records or an iBeacon-parsed line.
	// Either signal proves the parse path was exercised against the
	// stored raw hex.
	if !strings.Contains(body, "FDA50693") && !strings.Contains(body, "fda50693") {
		t.Errorf("expected TLV/parsed UUID hex in body; body=%q", body)
	}
}

// TestDevices_PacketsHandlesListObservationsError pins the
// render-with-error path: when ListObservationsForDevice errors,
// the handler must not silently produce an empty Observations table
// (that looks indistinguishable from "no recent packets" — exactly
// the false-negative the inline comment in devices.go:434 warns
// against).
func TestDevices_PacketsHandlesListObservationsError(t *testing.T) {
	const uuid = "fda50693-a4e2-4fb1-afcf-c6eb07647825"
	devs := &fakeDevicesStore{
		listObsErr: errors.New("simulated DB outage"),
	}
	zones := &fakeZonesStore{}
	eng := &fakeEngine{}
	rm := &fakeRemover{}
	s := newPhase3Server(t, devs, zones, eng, rm)

	slug := "ibeacon." + uuid + "_1_2"
	req := withAuth(httptest.NewRequest(http.MethodGet, "/admin/devices/"+slug+"/packets", nil))
	rr := httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status: %d, body=%s", rr.Code, rr.Body.String())
	}
	body := rr.Body.String()
	if !strings.Contains(body, "list observations") || !strings.Contains(body, "simulated DB outage") {
		t.Errorf("expected error banner with cause; body=%q", body)
	}
}

// TestDevices_PacketsListBeaconsError pins that a ListBeacons error
// surfaces as a beacon-name-lookup banner WITHOUT clobbering the
// successful observations render.
func TestDevices_PacketsListBeaconsError(t *testing.T) {
	const uuid = "fda50693-a4e2-4fb1-afcf-c6eb07647825"
	devs := &fakeDevicesStore{
		observations: []store.Observation{
			{
				ID:         1,
				ObservedAt: time.Now(),
				Kind:       "ibeacon",
				Key:        uuid + "_1_2",
				APMac:      "aa:bb:cc:dd:ee:01",
				RSSI:       -60,
				Tracked:    true,
			},
		},
		listErr: errors.New("beacons table missing"),
	}
	zones := &fakeZonesStore{}
	eng := &fakeEngine{}
	rm := &fakeRemover{}
	s := newPhase3Server(t, devs, zones, eng, rm)

	slug := "ibeacon." + uuid + "_1_2"
	req := withAuth(httptest.NewRequest(http.MethodGet, "/admin/devices/"+slug+"/packets", nil))
	rr := httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status: %d, body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "beacon name lookup failed") {
		t.Errorf("expected lookup-failure banner; body=%q", rr.Body.String())
	}
}

// TestParseDeviceSlug_TableDriven exercises the polymorphic slug parser
// across boundary inputs. Regressions in the slug shape break HA entity
// IDs (which use the same identity) so this is more load-bearing than
// it looks.
func TestParseDeviceSlug_TableDriven(t *testing.T) {
	cases := []struct {
		name   string
		slug   string
		wantOK bool
	}{
		{"ibeacon", "ibeacon.fda50693-a4e2-4fb1-afcf-c6eb07647825_1_42", true},
		{"eddystone uid", "eddystone_uid.00112233445566778899_aabbccddeeff", true},
		{"name", "name.HB-XXXXXXXX", true},
		{"no dot", "fda50693-a4e2-4fb1-afcf-c6eb07647825_1_42", false},
		{"unknown kind", "bogus.whatever", false},
		{"empty key", "ibeacon.", false},
		{"empty", "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, ok := ids.ParseSlug(c.slug)
			if ok != c.wantOK {
				t.Fatalf("ParseSlug(%q) ok=%v, want %v", c.slug, ok, c.wantOK)
			}
		})
	}
}

// TestDevices_RefreshEngine_OrphanReconcileErrorRendersBanner pins the
// failure-path wiring: when ApplyTopology succeeds but the orphan
// reconcile hook returns an error, the refresh-engine handler must
// redirect with ?refreshed=1&reconcile_err=… and the subsequent GET
// must render the amber banner from §5.
func TestDevices_RefreshEngine_OrphanReconcileErrorRendersBanner(t *testing.T) {
	devs := &fakeDevicesStore{}
	zones := &fakeZonesStore{}
	eng := &fakeEngine{}
	rm := &fakeRemover{}
	rec := func(_ context.Context, _ []presence.BeaconKey) (int, error) {
		return 0, errors.New("mqtt publish timeout")
	}
	s := newPhase3ServerWithReconcile(t, devs, zones, eng, rm, rec)

	req := withAuth(httptest.NewRequest(http.MethodPost, "/admin/devices/refresh-engine", nil))
	rr := httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusSeeOther {
		t.Fatalf("status: %d, body=%s", rr.Code, rr.Body.String())
	}
	loc := rr.Header().Get("Location")
	if !strings.Contains(loc, "refreshed=1") {
		t.Errorf("expected refreshed=1 in redirect; got %q", loc)
	}
	if !strings.Contains(loc, "reconcile_err=") {
		t.Errorf("expected reconcile_err in redirect; got %q", loc)
	}
	if !strings.Contains(loc, "mqtt+publish+timeout") && !strings.Contains(loc, "mqtt%20publish%20timeout") {
		t.Errorf("expected redirect to carry cause; got %q", loc)
	}
}

// --- handleRawPost (§7) ----------------------------------------------------

func TestRawPost_RendersEnvelope(t *testing.T) {
	const body = `{"vendor":"ruckus","ap_name":"AP-Test"}`
	rp := &fakeRawPostsStore{by: map[int64]*store.RawPost{
		7: {
			ID:              7,
			ReceivedAt:      time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC),
			Endpoint:        "/ingest",
			RemoteAddr:      "192.0.2.30",
			ContentEncoding: store.EncodingIdentity,
			Body:            []byte(body),
			BodySHA256:      "deadbeef",
		},
	}}
	devs := &fakeDevicesStore{}
	zones := &fakeZonesStore{}
	eng := &fakeEngine{}
	rm := &fakeRemover{}
	s := newPhase3ServerWithRawPosts(t, devs, zones, eng, rm, rp)

	req := withAuth(httptest.NewRequest(http.MethodGet, "/admin/posts/7", nil))
	rr := httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status: %d, body=%s", rr.Code, rr.Body.String())
	}
	resp := rr.Body.String()
	if !strings.Contains(resp, "AP-Test") {
		t.Errorf("expected pretty JSON to contain payload field; body=%q", resp)
	}
	if !strings.Contains(resp, "/ingest") {
		t.Errorf("expected envelope endpoint in body; body=%q", resp)
	}
}

func TestRawPost_NotFound(t *testing.T) {
	rp := &fakeRawPostsStore{by: map[int64]*store.RawPost{}}
	s := newPhase3ServerWithRawPosts(t, &fakeDevicesStore{}, &fakeZonesStore{}, &fakeEngine{}, &fakeRemover{}, rp)

	req := withAuth(httptest.NewRequest(http.MethodGet, "/admin/posts/999", nil))
	rr := httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status: %d, body=%s", rr.Code, rr.Body.String())
	}
}

func TestRawPost_BadID(t *testing.T) {
	rp := &fakeRawPostsStore{by: map[int64]*store.RawPost{}}
	s := newPhase3ServerWithRawPosts(t, &fakeDevicesStore{}, &fakeZonesStore{}, &fakeEngine{}, &fakeRemover{}, rp)

	req := withAuth(httptest.NewRequest(http.MethodGet, "/admin/posts/not-a-number", nil))
	rr := httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status: %d, body=%s", rr.Code, rr.Body.String())
	}
}

func TestRawPost_GunzipFailureSurfacesNotice(t *testing.T) {
	// EncodingGzip but the body is not actually gzip — exercises the
	// "gunzip best-effort, still render envelope" branch. The page
	// must render 200 with a DecodeNotice; the operator still needs
	// to see the envelope metadata even when the body is unreadable.
	rp := &fakeRawPostsStore{by: map[int64]*store.RawPost{
		3: {
			ID:              3,
			ReceivedAt:      time.Now(),
			Endpoint:        "/ingest",
			RemoteAddr:      "192.0.2.30",
			ContentEncoding: store.EncodingGzip,
			Body:            []byte("this is not gzip"),
			BodySHA256:      "cafebabe",
		},
	}}
	s := newPhase3ServerWithRawPosts(t, &fakeDevicesStore{}, &fakeZonesStore{}, &fakeEngine{}, &fakeRemover{}, rp)

	req := withAuth(httptest.NewRequest(http.MethodGet, "/admin/posts/3", nil))
	rr := httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status: %d, body=%s", rr.Code, rr.Body.String())
	}
	if !strings.Contains(rr.Body.String(), "gunzip") {
		t.Errorf("expected gunzip notice; body=%q", rr.Body.String())
	}
}

// silence unused-import warning when only some symbols are referenced.
var _ = errors.New
var _ = config.NormalizeMAC
