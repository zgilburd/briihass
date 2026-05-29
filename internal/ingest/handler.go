// Package ingest is the HTTP-receiving side of briihass. See ADR-0001.
package ingest

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"briihass/internal/metrics"
	"briihass/internal/parser"
	"briihass/internal/presence"
)

// CaptureSettings is the hot-read view of the two retention dimensions
// the operator can toggle from /admin/settings. The ingest path
// reaches into this via Options.CaptureSettings on every POST.
type CaptureSettings struct {
	CapturePerEventHex bool
	CaptureFullPosts   bool
}

// RawPostRecord is the envelope offered to RecordRawPost when
// CaptureFullPosts is on. Body is the raw wire bytes (gzip or
// identity, per ContentEncoding) so the admin viewer can replay
// exactly what vRIoT sent.
type RawPostRecord struct {
	Endpoint        string
	RemoteAddr      string
	ContentEncoding string
	Body            []byte
	BodySHA256      string
}

// ObservationRecord is one per-event row emitted by the ingest path,
// for an advert that resolved to a stable packet identity. RawPostID is
// non-nil only when the request's envelope was persisted. Enrichment
// fields (BatteryMV, TemperatureC, LocalName) are populated when
// telemetry could be attributed to this identity within the POST.
type ObservationRecord struct {
	ObservedAt   time.Time
	Kind         string
	Key          string
	APMac        string
	APName       string
	RSSI         int
	TxPower      *int
	BatteryMV    *int
	TemperatureC *float64
	LocalName    string
	RawHex       string
	RawPostID    *int64
	Tracked      bool
}

// Options configures the ingest server.
type Options struct {
	// APIKey is the value vRIoT must present in the Api-Key header.
	APIKey string

	// AllowedSourceCIDRs restricts inbound POSTs by client IP. Empty
	// slice means "allow any" — useful for dev. In prod set this to
	// the vRIoT VM's IP as a /32 (the placeholder 192.0.2.30/32 is
	// from RFC 5737 documentation space — substitute the operator's
	// real value via configuration).
	AllowedSourceCIDRs []string

	// PresenceSubmit is called once per parsed iBeacon sighting.
	PresenceSubmit func(presence.Sighting)

	// OnHeartbeat is called once per /heartbeat POST with the parsed
	// gateway-online and gateway-offline lists.
	OnHeartbeat func(online, offline []string)

	// BeaconLookup reports whether a packet-derived identity is on the
	// current allowlist and returns its display name. Unknown beacons
	// are dropped from the engine path (but still observed when
	// capture is on); UnknownBeacons counter increments per drop.
	BeaconLookup func(presence.BeaconKey) (name string, tracked bool)

	// MaxBodyBytes caps the size of any inbound request body
	// (post-gunzip). Default 1 MiB.
	MaxBodyBytes int64

	// CaptureSettings returns the live capture toggles. If nil,
	// capture is disabled regardless of RecordRawPost / RecordObservation.
	CaptureSettings func() CaptureSettings

	// RecordRawPost persists the request envelope and returns its id.
	// Called once per POST when CaptureSettings.CaptureFullPosts is on.
	// Nil disables envelope persistence.
	RecordRawPost func(ctx context.Context, p RawPostRecord) (int64, error)

	// RecordObservation persists one observation row. Called for both
	// tracked and untracked beacons. Should be non-blocking (the
	// implementation typically forwards into a buffered channel). Nil
	// disables observation persistence.
	RecordObservation func(o ObservationRecord)

	Logger  *slog.Logger
	Metrics *metrics.Registry

	// NowFn returns the current wall-clock time. Injected so tests can
	// freeze the substitution clock used when ev.Timestamp is missing
	// or non-positive. Defaults to time.Now.
	NowFn func() time.Time
}

func (o *Options) applyDefaults() {
	if o.MaxBodyBytes <= 0 {
		o.MaxBodyBytes = 1 << 20 // 1 MiB
	}
	if o.NowFn == nil {
		o.NowFn = time.Now
	}
}

// Server wraps an http.Handler that serves /ingest, /heartbeat, and
// /health. Hold-once construction; the returned handler is stateless
// (only side-effects are PresenceSubmit + metrics).
type Server struct {
	opts        Options
	allowedNets []*net.IPNet

	// lastParserErrLog is the timestamp (UnixNano) of the most recent
	// rate-limited Warn for a TLV walk failure. CAS gates the log to
	// at most one per second per process so one stuck AP can't swamp
	// the log; the counter still ticks on every event.
	lastParserErrLog atomic.Int64

	// lastBadTimestampLog gates the Warn for the ev.Timestamp <= 0
	// substitution path the same way (the counter still ticks on
	// every event so a sustained per-AP rate is visible in metrics).
	lastBadTimestampLog atomic.Int64
}

// New validates the options and returns a Server.
func New(opts Options) (*Server, error) {
	opts.applyDefaults()
	if opts.APIKey == "" {
		return nil, errors.New("ingest.Options: APIKey required")
	}
	if opts.PresenceSubmit == nil {
		return nil, errors.New("ingest.Options: PresenceSubmit required")
	}
	if opts.OnHeartbeat == nil {
		return nil, errors.New("ingest.Options: OnHeartbeat required")
	}
	if opts.BeaconLookup == nil {
		return nil, errors.New("ingest.Options: BeaconLookup required")
	}
	if opts.Logger == nil {
		return nil, errors.New("ingest.Options: Logger required")
	}
	if opts.Metrics == nil {
		return nil, errors.New("ingest.Options: Metrics required")
	}

	s := &Server{opts: opts}
	for _, cidr := range opts.AllowedSourceCIDRs {
		_, n, err := net.ParseCIDR(cidr)
		if err != nil {
			return nil, fmt.Errorf("ingest.Options: bad CIDR %q: %w", cidr, err)
		}
		s.allowedNets = append(s.allowedNets, n)
	}
	return s, nil
}

// Routes returns the http.Handler. Endpoints:
//
//	POST /ingest    — vRIoT beacon events (gzipped JSON)
//	POST /heartbeat — vRIoT gateway online/offline list (gzipped JSON)
//	GET  /health    — liveness/readiness
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ingest", s.requireAuth("/ingest", s.handleIngest))
	mux.HandleFunc("/heartbeat", s.requireAuth("/heartbeat", s.handleHeartbeat))
	mux.HandleFunc("/health", s.handleHealth)
	return mux
}

// requireAuth wraps a handler with Api-Key + source-IP check.
func (s *Server) requireAuth(endpoint string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			s.opts.Metrics.IngestRequests.WithLabelValues(endpoint, "405").Inc()
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		// Source IP allowlist (if configured).
		if len(s.allowedNets) > 0 {
			ip := clientIP(r)
			if !s.ipAllowed(ip) {
				s.opts.Metrics.AuthFailures.WithLabelValues(endpoint, "source_ip").Inc()
				s.opts.Metrics.IngestRequests.WithLabelValues(endpoint, "403").Inc()
				s.opts.Logger.Warn("ingest rejected", "endpoint", endpoint, "reason", "source_ip", "ip", ip)
				http.Error(w, "forbidden", http.StatusForbidden)
				return
			}
		}
		// Constant-time API-key compare.
		got := r.Header.Get("Api-Key")
		if subtle.ConstantTimeCompare([]byte(got), []byte(s.opts.APIKey)) != 1 {
			s.opts.Metrics.AuthFailures.WithLabelValues(endpoint, "api_key").Inc()
			s.opts.Metrics.IngestRequests.WithLabelValues(endpoint, "401").Inc()
			s.opts.Logger.Warn("ingest rejected", "endpoint", endpoint, "reason", "api_key", "ip", clientIP(r))
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		h(w, r)
	}
}

func (s *Server) ipAllowed(ip string) bool {
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return false
	}
	for _, n := range s.allowedNets {
		if n.Contains(parsed) {
			return true
		}
	}
	return false
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// readBody applies gunzip if the request is Content-Encoding: gzip
// (vRIoT always sets this; the conditional is for testability).
// Caps at MaxBodyBytes post-decode. Returns the plaintext along with
// the raw wire bytes (gzipped if so encoded) — the raw bytes are what
// gets stored in raw_posts when capture is enabled.
func (s *Server) readBody(r *http.Request, endpoint string) (plain, raw []byte, err error) {
	limited := http.MaxBytesReader(nil, r.Body, s.opts.MaxBodyBytes*4)
	defer limited.Close()
	raw, err = io.ReadAll(limited)
	if err != nil {
		return nil, nil, fmt.Errorf("read body: %w", err)
	}
	if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		gr, gerr := gzip.NewReader(bytes.NewReader(raw))
		if gerr != nil {
			s.opts.Metrics.GunzipFailures.Inc()
			return nil, raw, fmt.Errorf("gzip new reader: %w", gerr)
		}
		plain, err = io.ReadAll(io.LimitReader(gr, s.opts.MaxBodyBytes))
		// gzip.Reader.Close returns the trailing-CRC validation error;
		// not checking it lets a truncated upload read as success.
		if cerr := gr.Close(); cerr != nil && err == nil {
			err = cerr
		}
		if err != nil {
			s.opts.Metrics.GunzipFailures.Inc()
			return nil, raw, fmt.Errorf("gunzip read: %w", err)
		}
		return plain, raw, nil
	}
	// Body was not gzip-encoded; plaintext == raw.
	return raw, raw, nil
}

// ingestPayload mirrors the verified /ingest schema (ADR-0001 + the
// vriot_plugin_schema project memory). Only the fields the bridge
// consumes are typed; everything else is ignored.
type ingestPayload struct {
	GatewayEUID string        `json:"gateway_euid"`
	APName      string        `json:"ap_name"`
	APLocation  string        `json:"ap_location"`
	APModel     string        `json:"ap_model"`
	APIP        string        `json:"ap_ip_address"`
	Timestamp   int64         `json:"timestamp"`
	Events      []ingestEvent `json:"events"`
}

type ingestEvent struct {
	Data       string `json:"data"`        // hex BLE advert
	DeviceEUID string `json:"device_euid"` // BLE source address
	RSSI       int    `json:"rssi"`        // dBm
	Timestamp  int64  `json:"timestamp"`   // ms epoch
}

func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	plain, raw, err := s.readBody(r, "/ingest")
	if err != nil {
		s.opts.Metrics.IngestRequests.WithLabelValues("/ingest", "400").Inc()
		s.opts.Logger.Warn("/ingest body", "err", err)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	var p ingestPayload
	if err := json.Unmarshal(plain, &p); err != nil {
		s.opts.Metrics.IngestRequests.WithLabelValues("/ingest", "400").Inc()
		s.opts.Logger.Warn("/ingest json", "err", err)
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}

	ap := strings.ToLower(p.GatewayEUID)

	captureCfg := s.capture()
	rawPostID := s.maybeRecordRawPost(r.Context(), "/ingest", r, raw, captureCfg)

	// Two-pass over the POST's events (ADR-0008 spec C). Pass 1 decodes
	// every advert, derives a packet identity, and — using device_euid as
	// a WITHIN-POST correlation hint only — collects telemetry frames
	// (Eddystone-TLM) keyed by euid so an interleaved UID/TLM device can
	// have its battery/temp attributed to its stable identity. euid is
	// never persisted nor used as a durable key.
	type parsedEvent struct {
		ev  ingestEvent
		adv parser.Advert
		key presence.BeaconKey
		ok  bool
	}
	items := make([]parsedEvent, 0, len(p.Events))
	euidTelemetry := make(map[string]advertEnrichment)
	for _, ev := range p.Events {
		adv, perr := parser.Parse(ev.Data)
		if perr != nil {
			// TLV-walk failure (truncated frame, bad length octet, etc.).
			// A sustained per-AP rate suggests a firmware bug rather than
			// ordinary BLE noise.
			s.opts.Metrics.ParserErrors.WithLabelValues(ap).Inc()
			now := time.Now().UnixNano()
			prev := s.lastParserErrLog.Load()
			if now-prev >= int64(time.Second) && s.lastParserErrLog.CompareAndSwap(prev, now) {
				s.opts.Logger.Warn("/ingest TLV walk failed", "ap", ap, "err", perr)
			}
			continue
		}
		key, ok := parser.Identify(adv)
		items = append(items, parsedEvent{ev: ev, adv: adv, key: key, ok: ok})
		if enr, has := telemetryFromAdvert(adv); has && ev.DeviceEUID != "" {
			euidTelemetry[ev.DeviceEUID] = enr // last-wins within the POST
		}
	}

	for _, it := range items {
		if !it.ok {
			// Anonymous/ephemeral advert (rotating Apple Continuity,
			// Eddystone-TLM/URL, EID). Count, don't store (ADR-0008).
			s.opts.Metrics.AnonymousAdverts.WithLabelValues(ap).Inc()
			continue
		}
		kind := string(it.key.Kind())
		s.opts.Metrics.AdvertsByKind.WithLabelValues(kind, ap).Inc()

		name, tracked := s.opts.BeaconLookup(it.key)
		if !tracked {
			s.opts.Metrics.UnknownBeacons.WithLabelValues(kind, it.key.Key(), ap).Inc()
		}

		// Resolve the event clock once and reuse it for both the
		// observation row and the presence Sighting. A missing or
		// non-positive timestamp would otherwise become time.Time{}
		// (year 0001), which the retention prune would sweep on the next
		// tick AND which would blow up the EWMA decay math in the engine
		// (now.Sub(year0001) ≈ 2k years → instant floor → arrival never
		// publishes — arrival SLO hazard).
		obsAt := msToTime(it.ev.Timestamp)
		if obsAt.IsZero() {
			obsAt = s.opts.NowFn().UTC()
			s.opts.Metrics.IngestBadTimestamp.WithLabelValues(ap).Inc()
			s.maybeLogBadTimestamp(ap, it.ev.Timestamp)
		}

		// Enrichment: this advert's own fields, with any telemetry
		// correlated by euid within this POST overlaid (the common
		// Eddystone UID-frame-plus-TLM-frame case).
		enr := enrichmentFor(it.adv, euidTelemetry[it.ev.DeviceEUID])

		s.recordObservation(ObservationRecord{
			ObservedAt:   obsAt,
			Kind:         kind,
			Key:          it.key.Key(),
			APMac:        ap,
			APName:       p.APName,
			RSSI:         it.ev.RSSI,
			TxPower:      enr.txPower,
			BatteryMV:    enr.batteryMV,
			TemperatureC: enr.temperatureC,
			LocalName:    it.adv.LocalName,
			RawHex:       conditionalHex(it.ev.Data, captureCfg.CapturePerEventHex),
			RawPostID:    rawPostID,
			Tracked:      tracked,
		})

		if !tracked {
			continue
		}
		s.opts.PresenceSubmit(presence.Sighting{
			Beacon:       it.key,
			BeaconName:   name,
			APMac:        ap,
			APName:       p.APName,
			RSSI:         it.ev.RSSI,
			At:           obsAt,
			BatteryMV:    enr.batteryMV,
			TemperatureC: enr.temperatureC,
		})
	}

	s.opts.Metrics.IngestRequests.WithLabelValues("/ingest", "200").Inc()
	w.WriteHeader(http.StatusOK)
}

// advertEnrichment carries the telemetry/enrichment fields salvaged
// from an advert (or correlated from a telemetry-only frame). All
// fields are optional; nil means absent.
type advertEnrichment struct {
	txPower      *int
	batteryMV    *int
	temperatureC *float64
}

// telemetryFromAdvert extracts telemetry that has no identity of its
// own (Eddystone-TLM battery/temperature). has=false when the advert
// carries no such telemetry.
func telemetryFromAdvert(adv parser.Advert) (advertEnrichment, bool) {
	if adv.Eddystone == nil || adv.Eddystone.FrameType != 0x20 {
		return advertEnrichment{}, false
	}
	var enr advertEnrichment
	if adv.Eddystone.BatteryMV != nil {
		enr.batteryMV = intPtr(int(*adv.Eddystone.BatteryMV))
	}
	if adv.Eddystone.TemperatureC != nil {
		t := *adv.Eddystone.TemperatureC
		enr.temperatureC = &t
	}
	return enr, enr.batteryMV != nil || enr.temperatureC != nil
}

// enrichmentFor builds the enrichment for an identity-bearing advert:
// its own TX power (iBeacon calibrated power preferred, else the AD
// 0x0A advertised power), plus any battery/temperature correlated from
// a telemetry-only frame sharing this advert's euid within the POST.
func enrichmentFor(adv parser.Advert, correlated advertEnrichment) advertEnrichment {
	out := correlated
	switch {
	case adv.IBeacon != nil:
		out.txPower = intPtr(int(adv.IBeacon.TxPower))
	case adv.TxPower != nil:
		out.txPower = intPtr(int(*adv.TxPower))
	}
	return out
}

// capture is the hot-path accessor that protects callers from a nil
// settings hook. When CaptureSettings is nil, both toggles are off.
func (s *Server) capture() CaptureSettings {
	if s.opts.CaptureSettings == nil {
		return CaptureSettings{}
	}
	return s.opts.CaptureSettings()
}

// maybeRecordRawPost persists the request envelope (gzipped bytes +
// metadata) when capture is on. Returns the assigned id pointer (or
// nil when capture is off / the recorder errors).
func (s *Server) maybeRecordRawPost(ctx context.Context, endpoint string, r *http.Request, raw []byte, captureCfg CaptureSettings) *int64 {
	if !captureCfg.CaptureFullPosts || s.opts.RecordRawPost == nil || len(raw) == 0 {
		return nil
	}
	sum := sha256.Sum256(raw)
	id, err := s.opts.RecordRawPost(ctx, RawPostRecord{
		Endpoint:        endpoint,
		RemoteAddr:      clientIP(r),
		ContentEncoding: r.Header.Get("Content-Encoding"),
		Body:            raw,
		BodySHA256:      hex.EncodeToString(sum[:]),
	})
	if err != nil {
		s.opts.Metrics.RawPostInsertErrors.Inc()
		s.opts.Logger.Error("record raw_post failed", "endpoint", endpoint, "err", err)
		return nil
	}
	return &id
}

func (s *Server) recordObservation(o ObservationRecord) {
	if s.opts.RecordObservation == nil {
		return
	}
	s.opts.RecordObservation(o)
}

// maybeLogBadTimestamp emits at most one Warn per second per process so
// a regressed vRIoT firmware sending zero timestamps at the full event
// rate can't swamp the log. The counter still ticks on every event.
func (s *Server) maybeLogBadTimestamp(ap string, raw int64) {
	now := time.Now().UnixNano()
	prev := s.lastBadTimestampLog.Load()
	if now-prev < int64(time.Second) {
		return
	}
	if !s.lastBadTimestampLog.CompareAndSwap(prev, now) {
		return
	}
	s.opts.Logger.Warn("/ingest event has bad timestamp; substituting server time",
		"ap", ap, "raw_timestamp_ms", raw)
}

func conditionalHex(h string, on bool) string {
	if !on {
		return ""
	}
	return h
}

func intPtr(v int) *int { return &v }

// heartbeatPayload mirrors the verified /heartbeat schema.
// CapitalCase keys are the vRIoT plugin's choice.
type heartbeatPayload struct {
	Vendor  string   `json:"vendor"`
	Online  []string `json:"Online"`
	Offline []string `json:"Offline"`
}

func (s *Server) handleHeartbeat(w http.ResponseWriter, r *http.Request) {
	plain, raw, err := s.readBody(r, "/heartbeat")
	if err != nil {
		s.opts.Metrics.IngestRequests.WithLabelValues("/heartbeat", "400").Inc()
		s.opts.Logger.Warn("/heartbeat body", "err", err)
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	var p heartbeatPayload
	if err := json.Unmarshal(plain, &p); err != nil {
		s.opts.Metrics.IngestRequests.WithLabelValues("/heartbeat", "400").Inc()
		s.opts.Logger.Warn("/heartbeat json", "err", err)
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	_ = s.maybeRecordRawPost(r.Context(), "/heartbeat", r, raw, s.capture())

	s.opts.OnHeartbeat(p.Online, p.Offline)
	s.opts.Metrics.HeartbeatGateways.Set(float64(len(p.Online)))
	s.opts.Metrics.IngestRequests.WithLabelValues("/heartbeat", "200").Inc()
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	// Liveness only: if the HTTP server is up, we're alive. Deeper
	// checks (MQTT connected, presence engine pumping) belong on the
	// metrics listener's /ready endpoint and /admin/status.
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func msToTime(ms int64) time.Time {
	if ms <= 0 {
		return time.Time{}
	}
	sec := ms / 1000
	nsec := (ms % 1000) * int64(time.Millisecond)
	return time.Unix(sec, nsec).UTC()
}
