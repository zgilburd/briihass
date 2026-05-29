package store

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeRetentionStore records the cutoff each PruneOlderThan call
// receives so tests can verify the runner reads the live snapshot.
type fakeRetentionStore struct {
	mu     sync.Mutex
	calls  []time.Time // before-times in call order
	retObs int64
	retRP  int64
	retErr error
}

func (f *fakeRetentionStore) PruneOlderThan(_ context.Context, before time.Time) (int64, int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, before)
	return f.retObs, f.retRP, f.retErr
}

// TestRetentionRunner_UsesLiveSnapshot is the load-bearing test: an
// operator changing retention_days via /admin/settings must affect
// the very next prune, not require a restart.
func TestRetentionRunner_UsesLiveSnapshot(t *testing.T) {
	frozen := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	now := func() time.Time { return frozen }

	fs := &fakeRetentionStore{}
	snap := NewSettingsSnapshot(mustSettings(t, 7, false, false))
	r := NewRetentionRunner(fs, snap, discardLogger(t), RetentionRunnerOptions{Now: now})

	r.RunOnce(context.Background())
	// /admin/settings POST flips retention 7 -> 2.
	if err := snap.Replace(mustSettings(t, 2, false, false)); err != nil {
		t.Fatalf("Replace: %v", err)
	}
	r.RunOnce(context.Background())

	fs.mu.Lock()
	defer fs.mu.Unlock()
	if len(fs.calls) != 2 {
		t.Fatalf("PruneOlderThan calls: got %d want 2", len(fs.calls))
	}
	want1 := frozen.Add(-7 * 24 * time.Hour)
	want2 := frozen.Add(-2 * 24 * time.Hour)
	if !fs.calls[0].Equal(want1) {
		t.Errorf("first cutoff: got %v want %v", fs.calls[0], want1)
	}
	if !fs.calls[1].Equal(want2) {
		t.Errorf("second cutoff after Replace: got %v want %v", fs.calls[1], want2)
	}
}

// TestRetentionRunner_SkipsOnZeroRetention guards against a corrupt
// snapshot returning RetentionDays=0; pruning with cutoff=now would
// delete everything in the system.
func TestRetentionRunner_SkipsOnZeroRetention(t *testing.T) {
	fs := &fakeRetentionStore{}
	// Zero-value Settings is the only way to land RetentionDays() == 0
	// from outside the constructor; the runner must refuse to prune.
	snap := NewSettingsSnapshot(Settings{})
	r := NewRetentionRunner(fs, snap, discardLogger(t), RetentionRunnerOptions{})

	r.RunOnce(context.Background())

	fs.mu.Lock()
	if len(fs.calls) != 0 {
		t.Errorf("RunOnce should refuse to prune with RetentionDays<=0; got %d calls", len(fs.calls))
	}
	fs.mu.Unlock()
}

// TestRetentionRunner_OnFailedFires pins the C3 fix: when
// PruneOlderThan returns an error, the OnFailed hook MUST fire so
// the silent-disk-bloat failure mode surfaces on /metrics. Without
// this, an unreachable Postgres would only show up as an Error log.
func TestRetentionRunner_OnFailedFires(t *testing.T) {
	wantErr := errors.New("postgres down")
	fs := &fakeRetentionStore{retErr: wantErr}
	snap := NewSettingsSnapshot(mustSettings(t, 1, false, false))
	var (
		failedCount atomic.Int32
		lastErr     atomic.Pointer[error]
	)
	r := NewRetentionRunner(fs, snap, discardLogger(t), RetentionRunnerOptions{
		OnFailed: func(err error) {
			failedCount.Add(1)
			e := err
			lastErr.Store(&e)
		},
	})

	r.RunOnce(context.Background())
	if failedCount.Load() != 1 {
		t.Fatalf("OnFailed fire count: got %d want 1", failedCount.Load())
	}
	if got := lastErr.Load(); got == nil || !errors.Is(*got, wantErr) {
		t.Errorf("OnFailed received wrong err: %v", got)
	}
}

// TestRetentionRunner_RunCallsInitiallyAndOnTick pins T4 from the
// round-4 review: the Run ticker loop MUST call RunOnce immediately
// at start (so a freshly-rolled-out bridge honors a shortened
// retention window without waiting an hour) and then periodically.
// A regression that delayed the first prune until +Interval would
// otherwise land green — RunOnce is fully tested but the ticker
// loop was not.
func TestRetentionRunner_RunCallsInitiallyAndOnTick(t *testing.T) {
	fs := &fakeRetentionStore{}
	snap := NewSettingsSnapshot(mustSettings(t, 1, false, false))
	// Use a small interval so the test runs fast but is comfortably
	// above the cost of one PruneOlderThan call.
	r := NewRetentionRunner(fs, snap, discardLogger(t), RetentionRunnerOptions{
		Interval: 10 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.Run(ctx)
		close(done)
	}()

	// Wait for at least 3 calls (1 initial + 2 ticks). Bounded wait
	// so a Run that never calls PruneOlderThan fails loudly rather
	// than hanging.
	deadline := time.Now().Add(2 * time.Second)
	for {
		fs.mu.Lock()
		n := len(fs.calls)
		fs.mu.Unlock()
		if n >= 3 {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatalf("Run did not call PruneOlderThan at least 3 times within 2s; got %d", n)
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return within 1s of ctx cancel")
	}
}

// TestRetentionRunner_Totals accumulates pruned rows across calls.
func TestRetentionRunner_Totals(t *testing.T) {
	fs := &fakeRetentionStore{retObs: 5, retRP: 3}
	snap := NewSettingsSnapshot(mustSettings(t, 1, false, false))
	r := NewRetentionRunner(fs, snap, discardLogger(t), RetentionRunnerOptions{})

	r.RunOnce(context.Background())
	r.RunOnce(context.Background())

	obs, rp := r.Totals()
	if obs != 10 || rp != 6 {
		t.Errorf("Totals: got obs=%d rp=%d want 10/6", obs, rp)
	}
}

// mustSettings constructs a Settings via NewSettings or fails the
// test. Used because Settings fields are unexported and the only way
// to build a non-zero value is through the constructor.
func mustSettings(t *testing.T, retentionDays int, perEventHex, fullPosts bool) Settings {
	t.Helper()
	s, err := NewSettings(retentionDays, perEventHex, fullPosts)
	if err != nil {
		t.Fatalf("NewSettings: %v", err)
	}
	return s
}

// silence unused-import warning if slog/io are pulled by the helper only.
var _ = slog.Default
var _ io.Writer = io.Discard
