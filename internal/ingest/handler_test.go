package ingest

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"briihass/internal/ids"
	"briihass/internal/metrics"
	"briihass/internal/presence"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// captureSubmit collects every Sighting handed to PresenceSubmit so
// tests can assert on what crossed the parser/filter boundary.
type captureSubmit struct {
	mu sync.Mutex
	s  []presence.Sighting
}

func (c *captureSubmit) submit(s presence.Sighting) {
	c.mu.Lock()
	c.s = append(c.s, s)
	c.mu.Unlock()
}

func (c *captureSubmit) snapshot() []presence.Sighting {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]presence.Sighting, len(c.s))
	copy(out, c.s)
	return out
}

type captureHeartbeat struct {
	mu      sync.Mutex
	online  [][]string
	offline [][]string
}

func (c *captureHeartbeat) cb(on, off []string) {
	c.mu.Lock()
	c.online = append(c.online, on)
	c.offline = append(c.offline, off)
	c.mu.Unlock()
}

func newTestServer(t *testing.T, opts func(*Options)) (*Server, *captureSubmit, *captureHeartbeat) {
	s, sub, hb, _ := newTestServerWithMetrics(t, opts)
	return s, sub, hb
}

func newTestServerWithMetrics(t *testing.T, opts func(*Options)) (*Server, *captureSubmit, *captureHeartbeat, *metrics.Registry) {
	t.Helper()
	sub := &captureSubmit{}
	hb := &captureHeartbeat{}
	reg := metrics.New()
	o := Options{
		APIKey:         "secret-key",
		PresenceSubmit: sub.submit,
		OnHeartbeat:    hb.cb,
		BeaconLookup: func(id presence.BeaconKey) (string, bool) {
			// Track exactly one beacon.
			if id == ids.MustNewIBeaconKey("fda50693-a4e2-4fb1-afcf-c6eb07647825", 10065, 26049) {
				return "tag_test", true
			}
			return "", false
		},
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		Metrics: reg,
	}
	if opts != nil {
		opts(&o)
	}
	s, err := New(o)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s, sub, hb, reg
}

func gzipBody(t *testing.T, raw []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	if _, err := gw.Write(raw); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

func postJSON(t *testing.T, s *Server, url string, payload any, apiKey string, gzipped bool) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var body []byte
	var ce string
	if gzipped {
		body = gzipBody(t, raw)
		ce = "gzip"
	} else {
		body = raw
	}
	req := httptest.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if ce != "" {
		req.Header.Set("Content-Encoding", ce)
	}
	if apiKey != "" {
		req.Header.Set("Api-Key", apiKey)
	}
	rr := httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)
	return rr
}

// Sample iBeacon hex matching the test BeaconLookup. UUID
// fda50693-a4e2-4fb1-afcf-c6eb07647825, Major=0x2751=10065,
// Minor=0x65C1=26049, TX=-3. Public well-known SDK UUID.
const realIBeaconHex = "0201061AFF4C000215FDA50693A4E24FB1AFCFC6EB07647825275165C1FD"

// Apple Continuity (non-iBeacon noise).
const nonIBeaconHex = "02011A0EFF4C000F05900088D05C10022904020A00"

func TestIngest_HappyPath(t *testing.T) {
	s, sub, _ := newTestServer(t, nil)

	payload := ingestPayload{
		GatewayEUID: "AA:BB:CC:00:00:01",
		APName:      "AP-Test",
		Events: []ingestEvent{
			{Data: realIBeaconHex, DeviceEUID: "00:00:00:00:00:00:00:01", RSSI: -75, Timestamp: 1779085843216},
			{Data: nonIBeaconHex, DeviceEUID: "00:00:00:00:00:00:00:02", RSSI: -50, Timestamp: 1779085843300},
		},
	}
	rr := postJSON(t, s, "/ingest", payload, "secret-key", true)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%q", rr.Code, rr.Body.String())
	}
	got := sub.snapshot()
	if len(got) != 1 {
		t.Fatalf("want 1 submitted sighting (iBeacon only; noise dropped), got %d", len(got))
	}
	if got[0].BeaconName != "tag_test" {
		t.Errorf("BeaconName = %q", got[0].BeaconName)
	}
	if got[0].APMac != "aa:bb:cc:00:00:01" {
		t.Errorf("APMac lowercased: %q", got[0].APMac)
	}
	if got[0].APName != "AP-Test" {
		t.Errorf("APName threaded: %q", got[0].APName)
	}
	if got[0].RSSI != -75 {
		t.Errorf("RSSI: %d", got[0].RSSI)
	}
}

func TestIngest_Unauthorized(t *testing.T) {
	s, sub, _ := newTestServer(t, nil)
	payload := ingestPayload{GatewayEUID: "aa:bb:cc:00:00:01", Events: []ingestEvent{{Data: realIBeaconHex, RSSI: -70}}}

	cases := []struct {
		name string
		key  string
		want int
	}{
		{"missing", "", http.StatusUnauthorized},
		{"wrong", "nope", http.StatusUnauthorized},
		{"matches", "secret-key", http.StatusOK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rr := postJSON(t, s, "/ingest", payload, tc.key, true)
			if rr.Code != tc.want {
				t.Errorf("status = %d, want %d", rr.Code, tc.want)
			}
		})
	}
	// Submissions only happened for the authorized request.
	if got := len(sub.snapshot()); got != 1 {
		t.Errorf("submissions = %d, want 1", got)
	}
}

func TestIngest_SourceIPAllowlist(t *testing.T) {
	s, sub, _ := newTestServer(t, func(o *Options) {
		o.AllowedSourceCIDRs = []string{"192.0.2.0/24"}
	})
	payload := ingestPayload{GatewayEUID: "aa:bb:cc:00:00:01", Events: []ingestEvent{{Data: realIBeaconHex, RSSI: -70}}}

	// Direct invocation via httptest sets RemoteAddr to 192.0.2.1:1234
	// in the helper below.
	raw, _ := json.Marshal(payload)
	body := gzipBody(t, raw)
	for _, tc := range []struct {
		name   string
		remote string
		want   int
	}{
		{"allowed", "192.0.2.30:51740", http.StatusOK},
		{"denied", "10.0.0.1:51740", http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/ingest", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Content-Encoding", "gzip")
			req.Header.Set("Api-Key", "secret-key")
			req.RemoteAddr = tc.remote
			rr := httptest.NewRecorder()
			s.Routes().ServeHTTP(rr, req)
			if rr.Code != tc.want {
				t.Errorf("status = %d (body=%q), want %d", rr.Code, rr.Body.String(), tc.want)
			}
		})
	}
	if got := len(sub.snapshot()); got != 1 {
		t.Errorf("submissions = %d (allowed branch only)", got)
	}
}

func TestIngest_UnknownBeacon_Drops(t *testing.T) {
	s, sub, _ := newTestServer(t, nil)

	// Use the parser package's other test hex — different UUID, not
	// in the BeaconLookup allowlist. Should be dropped silently.
	const otherIBeaconHex = "0201061AFF4C000215AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA1EC052241C5"

	payload := ingestPayload{
		GatewayEUID: "aa:bb:cc:00:00:01",
		Events:      []ingestEvent{{Data: otherIBeaconHex, RSSI: -70, Timestamp: 1779085843216}},
	}
	rr := postJSON(t, s, "/ingest", payload, "secret-key", true)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	if len(sub.snapshot()) != 0 {
		t.Errorf("unknown beacon should not be submitted: %v", sub.snapshot())
	}
}

func TestIngest_NonGzipBody_Accepted(t *testing.T) {
	// In dev / synthetic traffic, we might not gzip. The handler
	// should still accept it (vRIoT always gzips, but we don't want
	// to make that a hard requirement for clients).
	s, sub, _ := newTestServer(t, nil)
	payload := ingestPayload{
		GatewayEUID: "aa:bb:cc:00:00:01",
		Events:      []ingestEvent{{Data: realIBeaconHex, RSSI: -75, Timestamp: 1779085843216}},
	}
	rr := postJSON(t, s, "/ingest", payload, "secret-key", false)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%q", rr.Code, rr.Body.String())
	}
	if len(sub.snapshot()) != 1 {
		t.Errorf("submissions = %d", len(sub.snapshot()))
	}
}

func TestIngest_MalformedGzip(t *testing.T) {
	s, sub, _ := newTestServer(t, nil)
	req := httptest.NewRequest(http.MethodPost, "/ingest", strings.NewReader("not gzip"))
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("Api-Key", "secret-key")
	rr := httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
	if len(sub.snapshot()) != 0 {
		t.Errorf("nothing should have been submitted")
	}
}

func TestIngest_MalformedJSON(t *testing.T) {
	s, _, _ := newTestServer(t, nil)
	body := gzipBody(t, []byte("not json"))
	req := httptest.NewRequest(http.MethodPost, "/ingest", bytes.NewReader(body))
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set("Api-Key", "secret-key")
	rr := httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rr.Code)
	}
}

func TestIngest_WrongMethod(t *testing.T) {
	s, _, _ := newTestServer(t, nil)
	req := httptest.NewRequest(http.MethodGet, "/ingest", nil)
	rr := httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", rr.Code)
	}
}

func TestHeartbeat_HappyPath(t *testing.T) {
	s, _, hb := newTestServer(t, nil)
	payload := heartbeatPayload{
		Vendor:  "ibeacon",
		Online:  []string{"AA:BB:CC:00:00:01", "AA:BB:CC:00:00:02"},
		Offline: []string{"AA:BB:CC:00:00:03"},
	}
	rr := postJSON(t, s, "/heartbeat", payload, "secret-key", true)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	if len(hb.online) != 1 {
		t.Fatalf("heartbeat callback fired %d times, want 1", len(hb.online))
	}
	if len(hb.online[0]) != 2 || len(hb.offline[0]) != 1 {
		t.Errorf("online=%v offline=%v", hb.online[0], hb.offline[0])
	}
}

func TestHealth_AlwaysOK(t *testing.T) {
	s, _, _ := newTestServer(t, nil)
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rr := httptest.NewRecorder()
	s.Routes().ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rr.Code)
	}
	if rr.Body.String() != "ok" {
		t.Errorf("body = %q", rr.Body.String())
	}
}

func TestNew_Validation(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	noop := func(presence.Sighting) {}
	noopHB := func([]string, []string) {}
	noopLookup := func(presence.BeaconKey) (string, bool) { return "", false }

	cases := []struct {
		name string
		opts Options
		want string
	}{
		{"no api key", Options{PresenceSubmit: noop, OnHeartbeat: noopHB, BeaconLookup: noopLookup, Logger: logger, Metrics: metrics.New()}, "APIKey"},
		{"no submit", Options{APIKey: "k", OnHeartbeat: noopHB, BeaconLookup: noopLookup, Logger: logger, Metrics: metrics.New()}, "PresenceSubmit"},
		{"no heartbeat cb", Options{APIKey: "k", PresenceSubmit: noop, BeaconLookup: noopLookup, Logger: logger, Metrics: metrics.New()}, "OnHeartbeat"},
		{"no lookup", Options{APIKey: "k", PresenceSubmit: noop, OnHeartbeat: noopHB, Logger: logger, Metrics: metrics.New()}, "BeaconLookup"},
		{"no logger", Options{APIKey: "k", PresenceSubmit: noop, OnHeartbeat: noopHB, BeaconLookup: noopLookup, Metrics: metrics.New()}, "Logger"},
		{"no metrics", Options{APIKey: "k", PresenceSubmit: noop, OnHeartbeat: noopHB, BeaconLookup: noopLookup, Logger: logger}, "Metrics"},
		{"bad cidr", Options{APIKey: "k", PresenceSubmit: noop, OnHeartbeat: noopHB, BeaconLookup: noopLookup, Logger: logger, Metrics: metrics.New(), AllowedSourceCIDRs: []string{"not-a-cidr"}}, "bad CIDR"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := New(tc.opts)
			if err == nil {
				t.Fatal("want error, got nil")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("err = %q, want substring %q", err.Error(), tc.want)
			}
		})
	}
}

// Quick sanity check that the unused import of context (kept around
// in case the integration test in this package needs it later) does
// not get pruned.
var _ = context.Background

// captureObservations collects every ObservationRecord routed through
// RecordObservation. Used to verify the Phase 3 record-then-drop
// branch fires for both tracked and untracked beacons.
type captureObservations struct {
	mu sync.Mutex
	o  []ObservationRecord
}

func (c *captureObservations) record(o ObservationRecord) {
	c.mu.Lock()
	c.o = append(c.o, o)
	c.mu.Unlock()
}

func (c *captureObservations) snapshot() []ObservationRecord {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]ObservationRecord, len(c.o))
	copy(out, c.o)
	return out
}

func TestIngest_RecordsObservationForTrackedAndUntracked(t *testing.T) {
	obs := &captureObservations{}
	s, sub, _ := newTestServer(t, func(o *Options) {
		o.CaptureSettings = func() CaptureSettings {
			return CaptureSettings{CapturePerEventHex: true}
		}
		o.RecordObservation = obs.record
	})

	const otherIBeaconHex = "0201061AFF4C000215AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA1EC052241C5"
	payload := ingestPayload{
		GatewayEUID: "aa:bb:cc:00:00:01",
		APName:      "AP-Test",
		Events: []ingestEvent{
			{Data: realIBeaconHex, RSSI: -75, Timestamp: 1779085843216},  // tracked
			{Data: otherIBeaconHex, RSSI: -80, Timestamp: 1779085843300}, // observed-only
			{Data: nonIBeaconHex, RSSI: -50, Timestamp: 1779085843400},   // noise (not iBeacon)
		},
	}
	rr := postJSON(t, s, "/ingest", payload, "secret-key", true)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d", rr.Code)
	}
	got := obs.snapshot()
	if len(got) != 2 {
		t.Fatalf("observations = %d, want 2 (tracked + untracked iBeacons)", len(got))
	}
	var tracked, untracked *ObservationRecord
	for i := range got {
		if got[i].Tracked {
			r := got[i]
			tracked = &r
		} else {
			r := got[i]
			untracked = &r
		}
	}
	if tracked == nil || tracked.RawHex != realIBeaconHex || tracked.RSSI != -75 {
		t.Errorf("tracked observation: %+v", tracked)
	}
	if untracked == nil || untracked.RawHex != otherIBeaconHex {
		t.Errorf("untracked observation: %+v", untracked)
	}
	if len(sub.snapshot()) != 1 {
		t.Errorf("only the tracked beacon should reach PresenceSubmit, got %d", len(sub.snapshot()))
	}
}

func TestIngest_RecordsRawPostWhenCaptureFullPostsOn(t *testing.T) {
	rawPosts := struct {
		mu  sync.Mutex
		all []RawPostRecord
	}{}
	s, _, _ := newTestServer(t, func(o *Options) {
		o.CaptureSettings = func() CaptureSettings {
			return CaptureSettings{CaptureFullPosts: true}
		}
		o.RecordRawPost = func(_ context.Context, p RawPostRecord) (int64, error) {
			rawPosts.mu.Lock()
			rawPosts.all = append(rawPosts.all, p)
			id := int64(len(rawPosts.all))
			rawPosts.mu.Unlock()
			return id, nil
		}
		o.RecordObservation = func(ObservationRecord) {}
	})
	payload := ingestPayload{
		GatewayEUID: "aa:bb:cc:00:00:01",
		Events:      []ingestEvent{{Data: realIBeaconHex, RSSI: -75, Timestamp: 1779085843216}},
	}
	postJSON(t, s, "/ingest", payload, "secret-key", true)
	rawPosts.mu.Lock()
	defer rawPosts.mu.Unlock()
	if len(rawPosts.all) != 1 {
		t.Fatalf("raw_posts = %d, want 1", len(rawPosts.all))
	}
	if rawPosts.all[0].Endpoint != "/ingest" || rawPosts.all[0].ContentEncoding != "gzip" {
		t.Errorf("raw_post: %+v", rawPosts.all[0])
	}
	if len(rawPosts.all[0].Body) == 0 || rawPosts.all[0].BodySHA256 == "" {
		t.Errorf("body / sha256 not populated: %+v", rawPosts.all[0])
	}
}

// TestIngest_TLVWalkErrorIncrementsParserErrors pins the I8 split:
// a TLV walk failure must increment ParserErrors (not the catch-all
// NonIBeaconEvents counter). A sustained per-AP rate of this counter
// is the early signal for an AP firmware bug.
func TestIngest_TLVWalkErrorIncrementsParserErrors(t *testing.T) {
	s, _, _, reg := newTestServerWithMetrics(t, nil)

	// First byte 0xFF declares a 255-byte TLV record, but the buffer
	// is only 2 bytes — parser.ParseAdvert returns (zero, false, err).
	const truncatedHex = "FF00"
	payload := ingestPayload{
		GatewayEUID: "aa:bb:cc:00:00:01",
		Events:      []ingestEvent{{Data: truncatedHex, RSSI: -80}},
	}
	rr := postJSON(t, s, "/ingest", payload, "secret-key", true)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%q", rr.Code, rr.Body.String())
	}

	got := readCounterVec(t, reg.ParserErrors, "aa:bb:cc:00:00:01")
	if got != 1 {
		t.Errorf("ParserErrors[aa:bb:cc:00:00:01] = %v, want 1", got)
	}
	// NonIBeaconEvents MUST be untouched — the whole point of the
	// split is that "TLV walk failed" doesn't get conflated with
	// "valid non-iBeacon advert".
	non := readCounterVec(t, reg.AnonymousAdverts, "aa:bb:cc:00:00:01")
	if non != 0 {
		t.Errorf("NonIBeaconEvents[aa:bb:cc:00:00:01] = %v, want 0 (TLV-walk failures must go to ParserErrors)", non)
	}
}

// readCounterVec reads a single label combination's value from a
// CounterVec. Returns 0 if the labels aren't registered yet (Prometheus
// counters are created lazily on first Inc).
func readCounterVec(t *testing.T, cv *prometheus.CounterVec, labels ...string) float64 {
	t.Helper()
	m, err := cv.GetMetricWithLabelValues(labels...)
	if err != nil {
		t.Fatalf("GetMetricWithLabelValues: %v", err)
	}
	var pb dto.Metric
	if err := m.(prometheus.Metric).Write(&pb); err != nil {
		t.Fatalf("Metric.Write: %v", err)
	}
	return pb.Counter.GetValue()
}

// TestMsToTime_TableDriven pins the zero-substitution contract for the
// pure helper. Production callers must observe IsZero() and replace
// with server time; this test guards the helper's behavior in isolation.
func TestMsToTime_TableDriven(t *testing.T) {
	cases := []struct {
		name    string
		ms      int64
		wantZ   bool
		wantSec int64
	}{
		{"zero", 0, true, 0},
		{"negative", -1, true, 0},
		{"positive epoch ms", 1779085843216, false, 1779085843},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := msToTime(c.ms)
			if got.IsZero() != c.wantZ {
				t.Fatalf("IsZero = %v, want %v (got=%v)", got.IsZero(), c.wantZ, got)
			}
			if !c.wantZ && got.Unix() != c.wantSec {
				t.Errorf("Unix() = %d, want %d", got.Unix(), c.wantSec)
			}
		})
	}
}

// TestIngest_ZeroTimestampSubstitutesNow exercises the §1 fix: an
// ev.Timestamp of 0 must NOT poison the observation row with year 0001
// (which the retention prune would sweep on the next tick) — the
// handler substitutes NowFn().UTC() and increments IngestBadTimestamp.
func TestIngest_ZeroTimestampSubstitutesNow(t *testing.T) {
	obs := &captureObservations{}
	frozen := time.Date(2026, 5, 26, 12, 34, 56, 0, time.UTC)
	s, sub, _, reg := newTestServerWithMetrics(t, func(o *Options) {
		o.NowFn = func() time.Time { return frozen }
		o.CaptureSettings = func() CaptureSettings { return CaptureSettings{} }
		o.RecordObservation = obs.record
	})
	payload := ingestPayload{
		GatewayEUID: "aa:bb:cc:00:00:01",
		APName:      "AP-Test",
		Events: []ingestEvent{
			{Data: realIBeaconHex, RSSI: -75, Timestamp: 0}, // missing
		},
	}
	rr := postJSON(t, s, "/ingest", payload, "secret-key", true)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%q", rr.Code, rr.Body.String())
	}
	got := obs.snapshot()
	if len(got) != 1 {
		t.Fatalf("observations = %d, want 1", len(got))
	}
	if !got[0].ObservedAt.Equal(frozen) {
		t.Errorf("ObservedAt = %v, want substituted %v", got[0].ObservedAt, frozen)
	}
	// Sighting must use the same substituted value so EWMA decay math
	// doesn't see year 0001 → ~2k years elapsed → instant floor.
	sights := sub.snapshot()
	if len(sights) != 1 {
		t.Fatalf("sightings = %d, want 1 (tracked)", len(sights))
	}
	if !sights[0].At.Equal(frozen) {
		t.Errorf("Sighting.At = %v, want substituted %v", sights[0].At, frozen)
	}
	if got := readCounterVec(t, reg.IngestBadTimestamp, "aa:bb:cc:00:00:01"); got != 1 {
		t.Errorf("IngestBadTimestamp[aa:bb:cc:00:00:01] = %v, want 1", got)
	}
}

// TestIngest_PositiveTimestampUsedAsIs pins that the substitution
// path is gated — a real positive ev.Timestamp must reach the
// observation row unchanged, and the counter must NOT tick.
func TestIngest_PositiveTimestampUsedAsIs(t *testing.T) {
	obs := &captureObservations{}
	s, sub, _, reg := newTestServerWithMetrics(t, func(o *Options) {
		o.NowFn = func() time.Time { return time.Date(2099, 1, 1, 0, 0, 0, 0, time.UTC) }
		o.CaptureSettings = func() CaptureSettings { return CaptureSettings{} }
		o.RecordObservation = obs.record
	})
	payload := ingestPayload{
		GatewayEUID: "aa:bb:cc:00:00:01",
		Events:      []ingestEvent{{Data: realIBeaconHex, RSSI: -75, Timestamp: 1779085843216}},
	}
	postJSON(t, s, "/ingest", payload, "secret-key", true)
	got := obs.snapshot()
	if len(got) != 1 || got[0].ObservedAt.Unix() != 1779085843 {
		t.Fatalf("ObservedAt = %v, want unix=1779085843 (vRIoT-provided)", got[0].ObservedAt)
	}
	if got := sub.snapshot()[0].At.Unix(); got != 1779085843 {
		t.Errorf("Sighting.At Unix = %d, want 1779085843", got)
	}
	if v := readCounterVec(t, reg.IngestBadTimestamp, "aa:bb:cc:00:00:01"); v != 0 {
		t.Errorf("IngestBadTimestamp should not tick on valid timestamp; got %v", v)
	}
}

func TestIngest_NoRawPostWhenCaptureOff(t *testing.T) {
	called := false
	s, _, _ := newTestServer(t, func(o *Options) {
		o.CaptureSettings = func() CaptureSettings { return CaptureSettings{} }
		o.RecordRawPost = func(_ context.Context, _ RawPostRecord) (int64, error) {
			called = true
			return 1, nil
		}
	})
	payload := ingestPayload{
		GatewayEUID: "aa:bb:cc:00:00:01",
		Events:      []ingestEvent{{Data: realIBeaconHex, RSSI: -75, Timestamp: 1779085843216}},
	}
	postJSON(t, s, "/ingest", payload, "secret-key", true)
	if called {
		t.Error("RecordRawPost called despite CaptureFullPosts=false")
	}
}
