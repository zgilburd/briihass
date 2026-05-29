package store

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"
)

// RetentionRunner periodically prunes observations + raw_posts older
// than settings.retention_days. The settings value is read from the
// live SettingsSnapshot on every tick so a /admin/settings POST takes
// effect on the next prune without requiring a restart.
type RetentionRunner struct {
	store     retentionStore
	snap      *SettingsSnapshot
	logger    *slog.Logger
	interval  time.Duration
	now       func() time.Time
	onSkipped func()
	onFailed  func(error)

	// pruneObservations and pruneRawPosts counters surface what the
	// runner has reaped over the process lifetime. Concurrent-safe so
	// Totals() can be called from any goroutine.
	pruneObservations atomic.Int64
	pruneRawPosts     atomic.Int64
}

// retentionStore is the small interface RetentionRunner needs.
// *Postgres satisfies it; tests use a fake.
type retentionStore interface {
	PruneOlderThan(ctx context.Context, before time.Time) (observations, rawPosts int64, err error)
}

// RetentionRunnerOptions configures a RetentionRunner.
type RetentionRunnerOptions struct {
	Interval time.Duration // default time.Hour
	Now      func() time.Time

	// OnSkipped fires when a prune cycle was skipped because the
	// current settings snapshot has a non-positive retention_days.
	// Nil-safe. Wire to a Prometheus counter so the disabled-retention
	// state is visible on /metrics (otherwise the only signal is a
	// Warn log at most once per Interval).
	OnSkipped func()

	// OnFailed fires when a prune cycle errored out before deleting
	// anything (e.g. Postgres unreachable, statement timeout). Nil-safe.
	// Wire to a Prometheus counter so silent disk-bloat — the exact
	// failure mode this subsystem exists to prevent — surfaces on /metrics.
	OnFailed func(error)
}

// NewRetentionRunner wires a runner. Run blocks; call from a goroutine.
func NewRetentionRunner(s retentionStore, snap *SettingsSnapshot, logger *slog.Logger, opts RetentionRunnerOptions) *RetentionRunner {
	if opts.Interval <= 0 {
		opts.Interval = time.Hour
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &RetentionRunner{
		store:     s,
		snap:      snap,
		logger:    logger,
		interval:  opts.Interval,
		now:       opts.Now,
		onSkipped: opts.OnSkipped,
		onFailed:  opts.OnFailed,
	}
}

// RunOnce performs one prune cycle. Exposed (not just internal) so
// tests can drive it deterministically without spinning the ticker.
func (r *RetentionRunner) RunOnce(ctx context.Context) {
	st := r.snap.Get()
	days := st.RetentionDays()
	if days <= 0 {
		r.logger.Warn("retention: snapshot has non-positive retention_days; skipping prune")
		if r.onSkipped != nil {
			r.onSkipped()
		}
		return
	}
	before := r.now().Add(-time.Duration(days) * 24 * time.Hour)
	pruneCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	obs, rp, err := r.store.PruneOlderThan(pruneCtx, before)
	cancel()
	if err != nil {
		r.logger.Error("retention prune failed", "err", err)
		if r.onFailed != nil {
			r.onFailed(err)
		}
		return
	}
	r.pruneObservations.Add(obs)
	r.pruneRawPosts.Add(rp)
	if obs > 0 || rp > 0 {
		r.logger.Info("retention prune",
			"observations", obs, "raw_posts", rp,
			"older_than", before.UTC().Format(time.RFC3339))
	}
}

// Run executes the retention loop until ctx is cancelled. Calls
// RunOnce immediately at start so retention is honored on rollouts
// after the operator shortened the window.
func (r *RetentionRunner) Run(ctx context.Context) {
	t := time.NewTicker(r.interval)
	defer t.Stop()
	r.RunOnce(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			r.RunOnce(ctx)
		}
	}
}

// Totals returns the cumulative count of pruned rows since process
// start. Safe to call from any goroutine.
func (r *RetentionRunner) Totals() (observations, rawPosts int64) {
	return r.pruneObservations.Load(), r.pruneRawPosts.Load()
}
