package store

import (
	"context"
	"fmt"
	"sync/atomic"
)

// Settings is the operator-tunable knobs that live next to (but not
// inside) the engine tunables document. retentionDays bounds the
// observation/raw_post windows; the two capture booleans gate ingest
// recording.
//
// Fields are unexported so the only way to construct a non-zero value
// is through NewSettings — the 1..30 retention bound is enforced at
// the type level, not just at the snapshot boundary. The zero value
// is intentionally invalid (RetentionDays()==0); the retention runner
// already handles it by skipping the prune.
type Settings struct {
	retentionDays      int
	capturePerEventHex bool
	captureFullPosts   bool
}

// RetentionDays returns the retention window in days. Zero only for the
// zero-value Settings; NewSettings guarantees [1, 30].
func (s Settings) RetentionDays() int { return s.retentionDays }

// CapturePerEventHex reports whether ingest should persist each
// event's raw advert hex on the observations row.
func (s Settings) CapturePerEventHex() bool { return s.capturePerEventHex }

// CaptureFullPosts reports whether ingest should persist the full
// per-request envelope in raw_posts.
func (s Settings) CaptureFullPosts() bool { return s.captureFullPosts }

// retentionBounds are the inclusive bounds on Settings.RetentionDays.
const (
	retentionMin = 1
	retentionMax = 30
)

// NewSettings validates the operator-supplied tuple. Returns a
// non-nil error if retentionDays is outside [1, 30]; the booleans
// are accepted as-is.
func NewSettings(retentionDays int, perEventHex, fullPosts bool) (Settings, error) {
	if retentionDays < retentionMin || retentionDays > retentionMax {
		return Settings{}, fmt.Errorf("retention_days must be %d..%d (got %d)",
			retentionMin, retentionMax, retentionDays)
	}
	return Settings{
		retentionDays:      retentionDays,
		capturePerEventHex: perEventHex,
		captureFullPosts:   fullPosts,
	}, nil
}

// LoadSettings returns the single settings row, validated. A row
// with retention_days outside the canonical bound is returned as an
// error so the admin/ingest paths can render an explicit failure
// instead of operating on a corrupt value.
func (s *Postgres) LoadSettings(ctx context.Context) (Settings, error) {
	var (
		days        int
		perEventHex bool
		fullPosts   bool
	)
	err := s.pool.QueryRow(ctx, `
		SELECT retention_days, capture_per_event_hex, capture_full_posts
		  FROM settings WHERE id = 1
	`).Scan(&days, &perEventHex, &fullPosts)
	if err != nil {
		return Settings{}, fmt.Errorf("load settings: %w", err)
	}
	return NewSettings(days, perEventHex, fullPosts)
}

// SaveSettings upserts the single settings row after validating the
// shape via NewSettings.
func (s *Postgres) SaveSettings(ctx context.Context, st Settings) error {
	v, err := NewSettings(st.retentionDays, st.capturePerEventHex, st.captureFullPosts)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO settings (id, retention_days, capture_per_event_hex, capture_full_posts, updated_at)
		VALUES (1, $1, $2, $3, now())
		ON CONFLICT (id) DO UPDATE SET
			retention_days        = EXCLUDED.retention_days,
			capture_per_event_hex = EXCLUDED.capture_per_event_hex,
			capture_full_posts    = EXCLUDED.capture_full_posts,
			updated_at            = now()
	`, v.retentionDays, v.capturePerEventHex, v.captureFullPosts)
	if err != nil {
		return fmt.Errorf("save settings: %w", err)
	}
	return nil
}

// SettingsSnapshot is a lock-free read-mostly cache of the settings
// row. The ingest hot path reads from it; the admin POST handler
// calls Replace after every successful save.
type SettingsSnapshot struct {
	v atomic.Pointer[Settings]
}

// NewSettingsSnapshot seeds the snapshot with an initial value. Caller
// must supply a validated Settings (e.g. via NewSettings); the
// snapshot does not re-validate on every Get for hot-path speed.
func NewSettingsSnapshot(initial Settings) *SettingsSnapshot {
	s := &SettingsSnapshot{}
	s.v.Store(&initial)
	return s
}

// Get returns the current value (cheap; one atomic load). Calling
// Get on a nil receiver is a programmer error and panics — the prior
// silent-default fallback was a frequent source of bugs because a
// "RetentionDays=7" surfacing where the snapshot was forgotten to be
// initialized looked like real configuration.
func (s *SettingsSnapshot) Get() Settings {
	return *s.v.Load()
}

// Replace swaps the snapshot atomically. The supplied Settings is
// re-validated via NewSettings so the snapshot can never hold a value
// outside the canonical bound — defends against callers that built a
// zero-value Settings literal rather than going through NewSettings.
func (s *SettingsSnapshot) Replace(st Settings) error {
	v, err := NewSettings(st.retentionDays, st.capturePerEventHex, st.captureFullPosts)
	if err != nil {
		return err
	}
	s.v.Store(&v)
	return nil
}
