// briihass is the bridge daemon. See ARCHITECTURE.md and the
// docs/adr/* series for the design.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"briihass/internal/admin"
	"briihass/internal/clock"
	"briihass/internal/config"
	"briihass/internal/ids"
	"briihass/internal/ingest"
	"briihass/internal/metrics"
	"briihass/internal/mqtt"
	"briihass/internal/presence"
	"briihass/internal/store"
)

// commit is set at build time via -ldflags="-X main.commit=...".
var commit = "unknown"

func main() {
	var (
		tunablesSeed = flag.String("tunables-seed", env("BRIIHASS_TUNABLES_SEED", "/etc/briihass/tunables-seed.yaml"), "path to tunables seed YAML, used only when the postgres store is empty (first start)")
		ingestAddr   = flag.String("ingest-addr", env("BRIIHASS_INGEST_ADDR", ":8080"), "ingest/heartbeat/health listener address")
		metricsAddr  = flag.String("metrics-addr", env("BRIIHASS_METRICS_ADDR", ":8081"), "Prometheus /metrics listener address (cluster-internal)")
		adminAddr    = flag.String("admin-addr", env("BRIIHASS_ADMIN_ADDR", ":8082"), "admin UI listener address (cluster-internal; basic-auth)")
	)
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	logger.Info("briihass starting", "commit", commit, "tunables_seed", *tunablesSeed)

	if err := run(logger, *tunablesSeed, *ingestAddr, *metricsAddr, *adminAddr); err != nil {
		logger.Error("briihass exiting on error", "err", err)
		os.Exit(1)
	}
	logger.Info("briihass stopped cleanly")
}

func env(key, dflt string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return dflt
}

func run(logger *slog.Logger, tunablesSeedPath, ingestAddr, metricsAddr, adminAddr string) error {
	// --- Credentials from env -------------------------------------
	apiKey := os.Getenv("INGEST_SHARED_SECRET")
	if apiKey == "" {
		return errors.New("INGEST_SHARED_SECRET env var required")
	}
	mqttUser := os.Getenv("MQTT_USER")
	mqttPass := os.Getenv("MQTT_PASS")
	if mqttUser == "" || mqttPass == "" {
		return errors.New("MQTT_USER and MQTT_PASS env vars required")
	}
	postgresDSN := os.Getenv("BRIIHASS_POSTGRES_DSN")
	if postgresDSN == "" {
		return errors.New("BRIIHASS_POSTGRES_DSN env var required")
	}
	mqttBroker := envOr("MQTT_BROKER_URL", "tcp://localhost:1883")

	// --- Tunables store + seed-if-empty ---------------------------
	pgCtx, pgCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer pgCancel()
	tunStore, err := store.NewPostgres(pgCtx, postgresDSN, logger.With("subsys", "store"), 10)
	if err != nil {
		return fmt.Errorf("postgres tunables store: %w", err)
	}
	defer tunStore.Close()
	seedYAML, rerr := os.ReadFile(tunablesSeedPath)
	if rerr != nil {
		return fmt.Errorf("read tunables seed: %w", rerr)
	}
	tun, serr := store.SeedFromYAMLIfEmpty(pgCtx, tunStore, seedYAML)
	if serr != nil {
		return fmt.Errorf("seed tunables: %w", serr)
	}
	logger.Info("tunables loaded", "overrides", len(tun.Beacons))

	// --- Topology from store -------------------------------------
	top, err := loadTopologyFromStore(pgCtx, tunStore)
	if err != nil {
		return fmt.Errorf("load topology: %w", err)
	}
	orphans, err := config.CrossValidate(top, tun)
	if err != nil {
		return fmt.Errorf("cross-validate config: %w", err)
	}
	logger.Info("topology loaded from store",
		"zones", top.ZoneCount(),
		"tracked_beacons", top.BeaconCount(),
		"orphan_overrides", len(orphans))
	for _, name := range orphans {
		logger.Warn("tunables override has no matching beacon; override is inert until /admin/devices promote, or remove via /admin/tunables save",
			"beacon", name)
	}

	// --- Settings snapshot (in-memory mirror of settings row) ----
	initialSettings, err := tunStore.LoadSettings(pgCtx)
	if err != nil {
		return fmt.Errorf("load settings: %w", err)
	}
	settingsSnap := store.NewSettingsSnapshot(initialSettings)
	logger.Info("settings loaded",
		"retention_days", initialSettings.RetentionDays(),
		"capture_per_event_hex", initialSettings.CapturePerEventHex(),
		"capture_full_posts", initialSettings.CaptureFullPosts())

	allowedSourceCIDRs := splitCSV(os.Getenv("INGEST_ALLOWED_CIDRS"))

	// --- Metrics --------------------------------------------------
	reg := metrics.New()
	reg.TunablesOrphanOverrides.Set(float64(len(orphans)))

	// --- MQTT publisher (started first so engine has somewhere to send) ---
	pub, err := mqtt.New(mqtt.Options{
		BrokerURL: mqttBroker,
		Username:  mqttUser,
		Password:  mqttPass,
		ClientID:  envOr("MQTT_CLIENT_ID", "briihass"),
		Logger:    logger.With("subsys", "mqtt"),
		OnMarshalFailed: func(kind string) {
			reg.MQTTMarshalFailed.WithLabelValues(kind).Inc()
		},
	})
	if err != nil {
		return fmt.Errorf("mqtt publisher: %w", err)
	}
	if err := pub.Connect(); err != nil {
		// Don't hard-fail on initial connect; paho's auto-reconnect
		// will keep trying. Counter + Warn so the cold-start failure is
		// visible on /metrics, not just in pod logs.
		reg.MQTTInitialConnectFailed.Inc()
		logger.Warn("initial MQTT connect failed; will keep retrying", "err", err)
	}

	// --- Presence engine ------------------------------------------
	events := make(chan presence.PresenceEvent, 1024)
	engine := presence.NewEngine(top, tun, clock.Real{}, events)

	// --- Rehydrate presence state (warm boot) ---------------------
	// Re-assert the last persisted per-beacon zone so a fresh pod does
	// not cold-start every beacon at not_home and then republish
	// not_home via the telemetry pump before it re-observes the beacon
	// (the mid-deploy away-flap; see engine.RestoreState). Done before
	// the ingest listener binds (lines below), so the pod is never
	// Ready-but-cold. A load failure degrades to a cold start — the
	// pre-rehydration behavior — rather than holding ingest hostage to
	// the presence_state table.
	if prows, lerr := tunStore.LoadPresenceState(pgCtx); lerr != nil {
		logger.Warn("load presence state failed; cold start", "err", lerr)
	} else {
		snaps := make([]presence.BeaconSnapshot, 0, len(prows))
		for _, r := range prows {
			bk, ok := ids.FromStoreKey(r.Kind, r.Key)
			if !ok {
				continue
			}
			snaps = append(snaps, presence.BeaconSnapshot{
				Beacon:      bk,
				CurrentZone: r.CurrentZone,
				CurrentAP:   r.CurrentAP,
				LastArrival: r.LastArrival,
			})
		}
		logger.Info("presence state rehydrated", "restored", engine.RestoreState(snaps), "rows", len(prows))
	}

	// On an HA birth ("online") message — or a manual admin resync — the
	// publisher clears its seen/declared maps; RepublishAll then re-emits
	// current state for every tracked beacon so discovery config + state
	// are re-asserted. This is what self-heals HA after a broker re-add /
	// HA restart / reload without a demote/promote cycle.
	pub.OnHAOnline = engine.RepublishAll

	// Wire engine metric hooks.
	engine.OnEventEmitted = func(ev presence.PresenceEvent) {
		// Arrival-edge latency: from sighting LastSeen to event emit.
		if !ev.LastSeen.IsZero() && ev.State != presence.NotHome {
			lat := ev.EmittedAt.Sub(ev.LastSeen).Seconds()
			if lat >= 0 && lat < 60 { // sanity bound; the clocks should agree
				reg.ArrivalLatencySeconds.Observe(lat)
			}
		}
		// Sticky-window gauge.
		val := 0.0
		if ev.StickyActive {
			val = 1.0
		}
		reg.BeaconInSticky.WithLabelValues(ev.BeaconName).Set(val)
		// Per-AP effective RSSI snapshot (only for non-departure
		// events; departures don't have meaningful AP/RSSI).
		if ev.State != presence.NotHome && ev.APMac != "" {
			reg.PerAPEffectiveRSSI.WithLabelValues(ev.BeaconName, ev.APMac).Set(ev.RSSIEffective)
		}
	}
	engine.OnUnknownBeacon = func(id presence.BeaconKey, ap string) {
		reg.UnknownBeacons.WithLabelValues(string(id.Kind()), id.Key(), ap).Inc()
	}
	// Closest AP is in presence but absent from the zones map. Without a
	// signal here a misconfigured zones table silently swallows every
	// sighting for the affected AP — exactly the case CLAUDE.md calls
	// out as "no operator-visible signal" risk.
	var lastUnknownAPLog atomic.Int64
	engine.OnUnknownAP = func(id presence.BeaconKey, beaconName, ap string) {
		reg.UnknownAP.WithLabelValues(beaconName, ap).Inc()
		now := time.Now().UnixNano()
		prev := lastUnknownAPLog.Load()
		if now-prev >= int64(time.Second) && lastUnknownAPLog.CompareAndSwap(prev, now) {
			logger.Warn("engine: closest AP in presence but missing from zones map",
				"beacon", beaconName, "ap", ap)
		}
	}
	// Drops on the engine -> MQTT channel mean an arrival/departure edge
	// vanished — the arrival SLO breaker. Surface to /metrics and
	// log at most once a second so the log isn't swamped under a stuck
	// publisher.
	var lastEventDropLog atomic.Int64
	engine.OnEventDropped = func(ev presence.PresenceEvent) {
		reg.EngineEventsDropped.Inc()
		now := time.Now().UnixNano()
		prev := lastEventDropLog.Load()
		if now-prev >= int64(time.Second) && lastEventDropLog.CompareAndSwap(prev, now) {
			logger.Warn("engine event dropped (downstream channel full)",
				"beacon", ev.BeaconName, "state", ev.State, "ap", ev.APMac,
				"queued", len(events), "cap", cap(events))
		}
	}
	// Publisher queue drops (saturated MQTT path). Counter + rate-limited
	// log mirroring the engine drop hook above. The counter itself is
	// already incremented inside enqueue (p.dropped → reg.MQTTDropped via
	// the periodic Stats snapshot below); the log lets an oncall correlate
	// "HA stopped seeing events at 14:32" with the broker outage.
	var lastPubDropLog atomic.Int64
	pub.OnDropped = func(info mqtt.DropInfo) {
		now := time.Now().UnixNano()
		prev := lastPubDropLog.Load()
		if now-prev >= int64(time.Second) && lastPubDropLog.CompareAndSwap(prev, now) {
			reg.MQTTPublisherDropLogs.Inc()
			logger.Warn("mqtt publisher dropped item (queue saturated)",
				"kind", info.Kind, "beacon", info.Beacon.Slug(),
				"state", info.State, "ap", info.APMac)
		}
	}

	// --- Observations writer (buffered, batched) ------------------
	var lastSubmitDropLog atomic.Int64
	obsWriter := store.NewObservationsWriter(tunStore, store.ObservationsWriterOptions{
		BufferSize:    2048,
		MaxBatch:      256,
		FlushInterval: time.Second,
		OnBatchDropped: func(rows int) {
			reg.ObservationsRowsDropped.Add(float64(rows))
		},
		// Submit-time drops happen when Postgres is too slow and the
		// in-memory channel fills — every increment is a sighting row
		// (often the arrival edge for the arrival SLO) that never
		// made it to the DB. Rate-limited Warn mirrors the engine and
		// publisher drop hooks above.
		OnSubmitDropped: func(o store.Observation) {
			reg.ObservationsSubmitDropped.Inc()
			now := time.Now().UnixNano()
			prev := lastSubmitDropLog.Load()
			if now-prev >= int64(time.Second) && lastSubmitDropLog.CompareAndSwap(prev, now) {
				logger.Warn("observations writer dropped submit (queue saturated)",
					"kind", o.Kind, "key", o.Key,
					"ap", o.APMac, "tracked", o.Tracked)
			}
		},
	}, logger.With("subsys", "obs-writer"))

	// --- Raw-posts writer (async, drop-oldest) --------------------
	// Mirrors obsWriter so a Postgres stall on raw_post inserts does
	// not block the ingest goroutine. AllocateID is the only synchronous
	// step (a sub-millisecond nextval) so observations can carry an
	// immediate RawPostID pointer; the envelope INSERT happens later
	// from the writer goroutine. See raw_posts_writer.go for the
	// FK-dropping rationale.
	var lastRawPostDropLog atomic.Int64
	rawPostsWriter := store.NewRawPostsWriter(tunStore, store.RawPostsWriterOptions{
		BufferSize:    512,
		MaxBatch:      32,
		FlushInterval: time.Second,
		OnBatchDropped: func(rows int) {
			reg.RawPostInsertErrors.Add(float64(rows))
		},
		OnSubmitDropped: func(p store.RawPost) {
			now := time.Now().UnixNano()
			prev := lastRawPostDropLog.Load()
			if now-prev >= int64(time.Second) && lastRawPostDropLog.CompareAndSwap(prev, now) {
				logger.Warn("raw_posts writer dropped submit (queue saturated)",
					"endpoint", p.Endpoint, "sha256", p.BodySHA256)
			}
		},
	}, logger.With("subsys", "raw-posts-writer"))

	// --- Heartbeat state holder -----------------------------------
	hbState := newHeartbeatState()

	// --- Ingest server --------------------------------------------
	ingestSrv, err := ingest.New(ingest.Options{
		APIKey:             apiKey,
		AllowedSourceCIDRs: allowedSourceCIDRs,
		PresenceSubmit:     engine.Submit,
		OnHeartbeat: func(online, offline []string) {
			hbState.set(online, offline)
			logger.Info("heartbeat", "online", len(online), "offline", len(offline))
		},
		// BeaconLookup goes through the engine so promote/demote take
		// effect immediately without restarting the bridge.
		BeaconLookup: engine.IsTracked,
		CaptureSettings: func() ingest.CaptureSettings {
			st := settingsSnap.Get()
			return ingest.CaptureSettings{
				CapturePerEventHex: st.CapturePerEventHex(),
				CaptureFullPosts:   st.CaptureFullPosts(),
			}
		},
		RecordRawPost: func(ctx context.Context, p ingest.RawPostRecord) (int64, error) {
			ce := store.EncodingIdentity
			if strings.EqualFold(p.ContentEncoding, "gzip") {
				ce = store.EncodingGzip
			}
			// AllocateID uses a short timeout so a sick DB sheds capture
			// for this request rather than hanging the ingest goroutine
			// (the I2 fix). Failure → the ingest handler treats capture
			// as off for this request (no RawPostID stamped on
			// observations); the row is not persisted.
			allocCtx, allocCancel := context.WithTimeout(ctx, 200*time.Millisecond)
			defer allocCancel()
			id, aerr := rawPostsWriter.AllocateID(allocCtx)
			if aerr != nil {
				return 0, aerr
			}
			rawPostsWriter.Submit(id, store.RawPost{
				Endpoint:        p.Endpoint,
				RemoteAddr:      p.RemoteAddr,
				ContentEncoding: ce,
				Body:            p.Body,
				BodySHA256:      p.BodySHA256,
			})
			return id, nil
		},
		RecordObservation: func(o ingest.ObservationRecord) {
			obsWriter.Submit(store.Observation{
				ObservedAt:   o.ObservedAt,
				Kind:         o.Kind,
				Key:          o.Key,
				APMac:        o.APMac,
				APName:       o.APName,
				RSSI:         o.RSSI,
				TxPower:      o.TxPower,
				BatteryMV:    o.BatteryMV,
				TemperatureC: o.TemperatureC,
				LocalName:    o.LocalName,
				RawHex:       o.RawHex,
				RawPostID:    o.RawPostID,
				Tracked:      o.Tracked,
			})
		},
		Logger:  logger.With("subsys", "ingest"),
		Metrics: reg,
	})
	if err != nil {
		return fmt.Errorf("ingest: %w", err)
	}

	// --- Admin UI -------------------------------------------------
	adminUser := os.Getenv("ADMIN_USER")
	adminPass := os.Getenv("ADMIN_PASS")
	var adminHTTP *http.Server
	if adminUser != "" && adminPass != "" {
		adminSrv, err := admin.New(admin.Options{
			User:            adminUser,
			Pass:            adminPass,
			Engine:          engine,
			Store:           tunStore,
			CurrentTunables: tun,
			Heartbeat:       hbState.get,
			MQTT: func() admin.MQTTStatus {
				st := pub.Stats()
				return admin.MQTTStatus{
					QueueDepth:    st.QueueDepth,
					QueueCapacity: st.QueueCapacity,
					Dropped:       st.Dropped,
					PublishOK:     st.PublishOK,
					PublishErr:    st.PublishErr,
					Connected:     st.Connected,
				}
			},
			Devices:      tunStore,
			Zones:        tunStore,
			RawPosts:     tunStore,
			Settings:     tunStore,
			SettingsSnap: settingsSnap,
			RemoveMQTTEntity: func(ctx context.Context, b presence.BeaconKey) error {
				return pub.RemoveEntity(ctx, b)
			},
			OrphanReconcile: func(ctx context.Context, known []presence.BeaconKey) (int, error) {
				return pub.RepublishOrphans(ctx, known)
			},
			ResyncHA: func(context.Context) error {
				pub.ResyncDiscovery()
				return nil
			},
			BuildCommit: commit,
			Logger:      logger.With("subsys", "admin"),
		})
		if err != nil {
			return fmt.Errorf("admin: %w", err)
		}
		adminHTTP = &http.Server{
			Addr:              adminAddr,
			Handler:           adminSrv.Routes(),
			ReadHeaderTimeout: 10 * time.Second,
		}
	} else {
		logger.Warn("admin UI disabled (ADMIN_USER/ADMIN_PASS not set)")
	}

	ingestHTTP := &http.Server{
		Addr:              ingestAddr,
		Handler:           ingestSrv.Routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	metricsHTTP := &http.Server{
		Addr:              metricsAddr,
		Handler:           metricsHandler(reg, pub),
		ReadHeaderTimeout: 10 * time.Second,
	}

	// --- Lifecycle / orchestration --------------------------------
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		engine.Run(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		pub.Run(ctx)
	}()

	// Observations writer: batched inserts so ingest never blocks on Postgres.
	wg.Add(1)
	go func() {
		defer wg.Done()
		obsWriter.Run(ctx)
	}()

	// Raw-posts writer: same async pattern so envelope persistence
	// can't backpressure the ingest goroutine either.
	wg.Add(1)
	go func() {
		defer wg.Done()
		rawPostsWriter.Run(ctx)
	}()

	retentionRunner := store.NewRetentionRunner(tunStore, settingsSnap,
		logger.With("subsys", "retention"),
		store.RetentionRunnerOptions{
			Interval:  time.Hour,
			OnSkipped: func() { reg.RetentionSkipped.Inc() },
			OnFailed:  func(error) { reg.RetentionPruneFailed.Inc() },
		})
	wg.Add(1)
	go func() {
		defer wg.Done()
		retentionRunner.Run(ctx)
	}()

	// Pump presence events -> mqtt publisher.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-events:
				pub.Publish(ev)
			}
		}
	}()

	// Periodically snapshot publisher stats into metrics.
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				st := pub.Stats()
				reg.MQTTQueueDepth.Set(float64(st.QueueDepth))
				if st.Connected {
					reg.MQTTConnected.Set(1)
				} else {
					reg.MQTTConnected.Set(0)
				}
			}
		}
	}()

	// Telemetry pump: re-emit current state for every tracked beacon on a
	// slow cadence so the RSSI / voltage / temperature sensors update
	// continuously even when there are no zone transitions (presence
	// events alone are too sparse to plot). RepublishAll is read-only
	// w.r.t. the state machine; config is only re-published when a
	// component is newly declared, so steady-state traffic is one state
	// JSON per beacon per tick.
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(20 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				engine.RepublishAll()
			}
		}
	}()

	// Persist the presence snapshot so a restart boots warm (rehydrated
	// above via engine.RestoreState). Full-replace each tick — cheap at
	// our beacon cardinality, and it drops rows for demoted beacons. The
	// drain below does one final flush so the last pre-SIGTERM zones are
	// captured (the periodic flush may be up to 5s stale at shutdown).
	persistPresence := func(ctx context.Context) {
		snap := engine.Snapshot()
		rows := make([]store.PresenceStateRow, 0, len(snap.Beacons))
		for _, bs := range snap.Beacons {
			rows = append(rows, store.PresenceStateRow{
				Kind:        string(bs.Beacon.Kind()),
				Key:         bs.Beacon.Key(),
				CurrentZone: bs.CurrentZone,
				CurrentAP:   bs.CurrentAP,
				LastArrival: bs.LastArrival,
			})
		}
		if err := tunStore.SavePresenceState(ctx, rows); err != nil {
			logger.Warn("persist presence state", "err", err)
		}
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				persistPresence(ctx)
			}
		}
	}()

	// Postcondition for the three listener goroutines below:
	// a non-ErrServerClosed exit from ListenAndServe must cancel the
	// root context (via stop()). Without this, the goroutine returns
	// silently and the pod stays Ready while accepting zero traffic —
	// K8s never restarts because nothing fails health checks. With
	// it, <-ctx.Done() below unblocks, the orderly drain runs, and
	// the pod exits cleanly so the restartPolicy can take over.
	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Info("ingest listener", "addr", ingestAddr)
		if err := ingestHTTP.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("ingest server", "err", err)
			stop()
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Info("metrics listener", "addr", metricsAddr)
		if err := metricsHTTP.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("metrics server", "err", err)
			stop()
		}
	}()

	if adminHTTP != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			logger.Info("admin listener", "addr", adminAddr)
			if err := adminHTTP.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("admin server", "err", err)
				stop()
			}
		}()
	}

	<-ctx.Done()
	logger.Info("shutdown signal received; draining...")

	// Stop HTTP listeners first so no new sightings enter.
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := ingestHTTP.Shutdown(shutdownCtx); err != nil {
		logger.Warn("ingest server shutdown", "err", err)
	}
	if err := metricsHTTP.Shutdown(shutdownCtx); err != nil {
		logger.Warn("metrics server shutdown", "err", err)
	}
	if adminHTTP != nil {
		if err := adminHTTP.Shutdown(shutdownCtx); err != nil {
			logger.Warn("admin server shutdown", "err", err)
		}
	}

	// Give engine + publisher a moment to drain queued events.
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer drainCancel()
	go func() {
		wg.Wait()
		drainCancel()
	}()
	<-drainCtx.Done()

	// Final presence flush so the next pod boots with the freshest zones.
	// Uses a fresh context since the root ctx is already cancelled.
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 3*time.Second)
	persistPresence(flushCtx)
	flushCancel()

	_ = pub.Close()
	return nil
}

// readyAfter is the startup grace period before /ready starts gating
// on MQTT connectivity. Lets paho's auto-reconnect get a fair chance
// before K8s rolls the pod.
const readyAfter = 60 * time.Second

func metricsHandler(reg *metrics.Registry, pub *mqtt.Publisher) http.Handler {
	mux := http.NewServeMux()
	startedAt := time.Now()
	mux.Handle("/metrics", reg.Handler())
	// /health is liveness: the process is up and the HTTP listener is
	// answering. Used by K8s livenessProbe.
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	// /ready is readiness: the bridge is functionally able to publish
	// to HA. During the startup grace window we return OK so paho's
	// auto-reconnect can establish the first session; afterwards a
	// disconnected publisher means HA isn't receiving events and K8s
	// should stop sending traffic (or alert on the gauge).
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if time.Since(startedAt) < readyAfter {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("starting"))
			return
		}
		if pub != nil && !pub.Stats().Connected {
			w.Header().Set("Retry-After", "10")
			http.Error(w, "mqtt not connected", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready"))
	})
	return mux
}

// heartbeatState is the small holder that bridges the ingest
// /heartbeat callback to the admin UI's status page. Goroutine-safe.
type heartbeatState struct {
	mu      sync.Mutex
	online  []string
	offline []string
}

func newHeartbeatState() *heartbeatState { return &heartbeatState{} }

func (h *heartbeatState) set(online, offline []string) {
	h.mu.Lock()
	h.online = append([]string(nil), online...)
	h.offline = append([]string(nil), offline...)
	h.mu.Unlock()
}

func (h *heartbeatState) get() (online, offline []string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.online...), append([]string(nil), h.offline...)
}

// loadTopologyFromStore composes a config.Topology from the beacons +
// zones tables. An empty store returns an empty topology — bootstrap
// state — and the engine just won't emit events until the operator
// promotes from /admin/devices.
func loadTopologyFromStore(ctx context.Context, s *store.Postgres) (*config.Topology, error) {
	beacons, err := s.ListBeacons(ctx)
	if err != nil {
		return nil, fmt.Errorf("list beacons: %w", err)
	}
	zones, err := s.ListZones(ctx)
	if err != nil {
		return nil, fmt.Errorf("list zones: %w", err)
	}
	zoneMap := make(map[string]string, len(zones))
	for _, z := range zones {
		zoneMap[z.APMac.String()] = z.ZoneLabel.String()
	}
	tracked := make([]config.TrackedBeacon, 0, len(beacons))
	for _, b := range beacons {
		tb, terr := config.NewTrackedBeacon(b.Domain(), b.Name)
		if terr != nil {
			return nil, fmt.Errorf("topology from store: %w", terr)
		}
		tracked = append(tracked, tb)
	}
	top, err := config.NewTopology(zoneMap, tracked)
	if err != nil {
		return nil, fmt.Errorf("topology from store invalid: %w", err)
	}
	return top, nil
}

func envOr(key, dflt string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return dflt
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func fmtUint(n uint16) string {
	return fmtInt(int64(n))
}

// fmtInt is a tiny helper to avoid pulling in strconv just for one
// site; main is otherwise stdlib-clean.
func fmtInt(n int64) string {
	if n == 0 {
		return "0"
	}
	const digits = "0123456789"
	var buf [20]byte
	pos := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		pos--
		buf[pos] = digits[n%10]
		n /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
