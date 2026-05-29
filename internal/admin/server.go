// Package admin serves a small basic-auth web UI for viewing bridge
// status and editing the runtime configuration that lives in Postgres.
//
// Routes:
//
//	GET  /admin                              redirect to /admin/status
//	GET  /admin/status                       per-beacon engine state
//	GET  /admin/tunables                     tunables form pre-filled from store
//	POST /admin/tunables                     validate + SaveAll to store + hot reload
//	GET  /admin/devices                      observed + tracked beacons table
//	POST /admin/devices/promote              insert beacons row + ApplyTopology
//	POST /admin/devices/demote               delete beacons row + ApplyTopology + RemoveMQTTEntity
//	POST /admin/devices/refresh-engine       retry ApplyTopology + reconcile MQTT orphans
//	GET  /admin/devices/{slug}/packets       observation detail with TLV walk
//	GET  /admin/zones                        AP MAC -> zone label table
//	POST /admin/zones                        upsert/delete zone + ApplyTopology
//	POST /admin/zones/refresh-engine         retry ApplyTopology + reconcile MQTT orphans
//	GET  /admin/settings                     retention/capture form
//	POST /admin/settings                     SaveSettings + snapshot Replace
//	GET  /admin/posts/{id}                   captured raw POST envelope viewer
//
// No SPA, no JS framework. html/template renders pages; one small CSS
// embedded via embed.FS. Auth: HTTP Basic, constant-time compare
// against the credentials in Options.
package admin

import (
	"context"
	"crypto/subtle"
	"embed"
	"errors"
	"fmt"
	"html/template"
	"io/fs"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"briihass/internal/config"
	"briihass/internal/ids"
	"briihass/internal/presence"
	"briihass/internal/store"
)

//go:embed templates/*.html static/*.css
var assets embed.FS

// EngineHandle is the small set of operations the admin UI needs from
// the presence engine.
type EngineHandle interface {
	Snapshot() presence.EngineSnapshot
	ApplyTunables(*config.Tunables)
	ApplyTopology(*config.Topology)
}

// DevicesStore is the admin's view of the observation / allowlist
// surface. *store.Postgres satisfies it; tests use a fake.
type DevicesStore interface {
	ListDevicesSince(ctx context.Context, since time.Time) ([]store.DeviceSummary, error)
	ListObservationsForDevice(ctx context.Context, kind, key string, limit int) ([]store.Observation, error)
	PromoteBeacon(ctx context.Context, b store.Beacon) error
	DemoteBeacon(ctx context.Context, key ids.BeaconKey) error
	ListBeacons(ctx context.Context) ([]store.Beacon, error)
}

// ZonesStore is the admin's view of the (ap_mac -> zone) table.
type ZonesStore interface {
	ListAPsSince(ctx context.Context, since time.Time) (map[string]string, error)
	UpsertZone(ctx context.Context, z store.Zone) error
	DeleteZone(ctx context.Context, apMac ids.APMAC) error
	ListZones(ctx context.Context) ([]store.Zone, error)
}

// RawPostsStore is the admin's view of the captured POST envelopes.
type RawPostsStore interface {
	GetRawPost(ctx context.Context, id int64) (*store.RawPost, error)
}

// SettingsStore is the admin's view of the retention/capture settings.
type SettingsStore interface {
	LoadSettings(ctx context.Context) (store.Settings, error)
	SaveSettings(ctx context.Context, st store.Settings) error
}

// HeartbeatGetter returns the most recent (online, offline) lists
// from /heartbeat. Wired in by main.go.
type HeartbeatGetter func() (online, offline []string)

// MQTTStatus is the subset of publisher counters the status page
// renders. Returned by MQTTStatusGetter as a named struct so callers
// (and the template) cannot transpose Dropped / OK / Errors at the
// call boundary.
type MQTTStatus struct {
	QueueDepth    int
	QueueCapacity int
	Dropped       uint64
	PublishOK     uint64
	PublishErr    uint64
	Connected     bool
}

// MQTTStatusGetter exposes a few publisher counters for the status
// page. Wired in by main.go.
type MQTTStatusGetter func() MQTTStatus

// Options configures the admin server.
type Options struct {
	// Basic-auth credentials. Constant-time compared.
	User string
	Pass string

	// Engine handle (presence.Engine satisfies this).
	Engine EngineHandle

	// Store is the tunables persistence backend (Postgres in prod;
	// see internal/store). SaveAll runs on every successful
	// admin UI POST.
	Store store.Store

	// CurrentTunables is the in-memory tunables document the admin
	// UI renders + updates. Held as a pointer that the admin server
	// swaps after each save.
	CurrentTunables *config.Tunables

	// Heartbeat surface for the status page (optional; may be nil).
	Heartbeat HeartbeatGetter

	// MQTT publisher status for the status page (optional; may be nil).
	MQTT MQTTStatusGetter

	// Devices, Zones, RawPosts, Settings are the persistence surfaces.
	// All satisfied by *store.Postgres in prod.
	Devices  DevicesStore
	Zones    ZonesStore
	RawPosts RawPostsStore
	Settings SettingsStore

	// SettingsSnap is the in-memory mirror of the settings row
	// that the ingest hot path reads. Updated on every successful
	// /admin/settings POST.
	SettingsSnap *store.SettingsSnapshot

	// RemoveMQTTEntity wipes the HA Discovery entity for a beacon
	// being demoted by enqueuing an empty retained publish onto the
	// publisher queue. Returns an error if the queue is saturated so
	// the operator can retry. Nil means "skip MQTT cleanup" — admin
	// will warn when it can't reach the publisher.
	RemoveMQTTEntity func(context.Context, presence.BeaconKey) error

	// OrphanReconcile, when set, diffs the publisher's in-memory
	// "seen" set against the supplied known allowlist and enqueues
	// removes for any entity not in known. The refresh-engine
	// handlers call this after rebuildEngineTopology so a previously
	// failed demote-step (DB ok, MQTT saturated) self-heals on the
	// next retry click. Returns (orphans-enqueued, first-error).
	// Nil-safe: a missing wiring just means refresh only rebuilds
	// engine topology (the prior behavior).
	OrphanReconcile func(context.Context, []presence.BeaconKey) (int, error)

	// ResyncHA re-asserts all HA discovery config + state on demand (the
	// "Resync HA" button). Used after an HA-side change that didn't fire a
	// birth message — e.g. a single entity manually deleted in HA. Nil-safe:
	// a missing wiring just hides the button's effect.
	ResyncHA func(context.Context) error

	// BuildCommit is the binary's main.commit value; rendered in the
	// page footer so operators know what they're looking at.
	BuildCommit string

	Logger *slog.Logger
}

// Server is the admin HTTP handler.
type Server struct {
	opts    Options
	tmpl    *template.Template
	static  fs.FS
	mu      sync.RWMutex // protects current tunables pointer
	current *config.Tunables
}

// New constructs the admin server with the embedded templates parsed.
func New(opts Options) (*Server, error) {
	if opts.User == "" || opts.Pass == "" {
		return nil, errors.New("admin.Options: User and Pass required")
	}
	if opts.Engine == nil {
		return nil, errors.New("admin.Options: Engine required")
	}
	if opts.Store == nil {
		return nil, errors.New("admin.Options: Store required")
	}
	if opts.CurrentTunables == nil {
		return nil, errors.New("admin.Options: CurrentTunables required")
	}
	if opts.Logger == nil {
		return nil, errors.New("admin.Options: Logger required")
	}

	tmpl, err := template.New("").Funcs(templateFuncs).ParseFS(assets, "templates/*.html")
	if err != nil {
		return nil, fmt.Errorf("parse templates: %w", err)
	}
	staticFS, err := fs.Sub(assets, "static")
	if err != nil {
		return nil, fmt.Errorf("sub static: %w", err)
	}

	return &Server{
		opts:    opts,
		tmpl:    tmpl,
		static:  staticFS,
		current: opts.CurrentTunables,
	}, nil
}

// Routes returns the http.Handler. Mount under any prefix; this
// handler only handles its own /admin/* paths and /admin-static/*.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/admin", s.requireAuth(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/admin/status", http.StatusSeeOther)
	}))
	mux.HandleFunc("/admin/status", s.requireAuth(s.handleStatus))
	mux.HandleFunc("/admin/tunables", s.requireAuth(s.handleTunables))
	if s.opts.Devices != nil {
		mux.HandleFunc("/admin/devices", s.requireAuth(s.handleDevices))
		mux.HandleFunc("/admin/devices/promote", s.requireAuth(s.handleDevicePromote))
		mux.HandleFunc("/admin/devices/demote", s.requireAuth(s.handleDeviceDemote))
		mux.HandleFunc("/admin/devices/refresh-engine", s.requireAuth(s.handleDevicesRefreshEngine))
		mux.HandleFunc("/admin/devices/resync-ha", s.requireAuth(s.handleDevicesResyncHA))
		mux.HandleFunc("/admin/devices/", s.requireAuth(s.handleDevicePackets))
	}
	if s.opts.Zones != nil {
		mux.HandleFunc("/admin/zones", s.requireAuth(s.handleZones))
		mux.HandleFunc("/admin/zones/refresh-engine", s.requireAuth(s.handleZonesRefreshEngine))
	}
	if s.opts.Settings != nil {
		mux.HandleFunc("/admin/settings", s.requireAuth(s.handleSettings))
	}
	if s.opts.RawPosts != nil {
		mux.HandleFunc("/admin/posts/", s.requireAuth(s.handleRawPost))
	}
	mux.Handle("/admin-static/", http.StripPrefix("/admin-static/", http.FileServer(http.FS(s.static))))
	return mux
}

// requireAuth wraps a handler with HTTP Basic and a constant-time
// compare. The realm string is generic so it doesn't telegraph that
// this server runs briihass to passive scanners.
func (s *Server) requireAuth(h http.HandlerFunc) http.HandlerFunc {
	wantU := []byte(s.opts.User)
	wantP := []byte(s.opts.Pass)
	return func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok ||
			subtle.ConstantTimeCompare([]byte(u), wantU) != 1 ||
			subtle.ConstantTimeCompare([]byte(p), wantP) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="restricted", charset="UTF-8"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		h(w, r)
	}
}

// currentTunables returns the latest in-memory tunables, copied so
// the caller can read it without holding the lock.
func (s *Server) currentTunables() *config.Tunables {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.current
}

// replaceCurrent swaps the in-memory tunables after a successful save.
func (s *Server) replaceCurrent(t *config.Tunables) {
	s.mu.Lock()
	s.current = t
	s.mu.Unlock()
}
