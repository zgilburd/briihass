package store

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"briihass/internal/config"
)

//go:embed schema.sql
var schemaSQL string

// Postgres is a Store backed by a Postgres database via pgxpool.
type Postgres struct {
	pool *pgxpool.Pool
	log  *slog.Logger
}

// NewPostgres opens a pool against dsn, applies the schema, and
// returns a ready Store. The initial connect is retried up to
// maxAttempts times with a fixed 2s backoff — first start often
// races the briihass-postgres StatefulSet readiness.
func NewPostgres(ctx context.Context, dsn string, logger *slog.Logger, maxAttempts int) (*Postgres, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	var pool *pgxpool.Pool
	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		p, err := pgxpool.New(ctx, dsn)
		if err == nil {
			pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			err = p.Ping(pingCtx)
			cancel()
			if err == nil {
				pool = p
				break
			}
			p.Close()
		}
		lastErr = err
		logger.Warn("postgres connect attempt failed",
			"attempt", attempt, "max", maxAttempts, "err", err)
		if attempt == maxAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	if pool == nil {
		return nil, fmt.Errorf("postgres connect after %d attempt(s): %w", maxAttempts, lastErr)
	}

	s := &Postgres{pool: pool, log: logger}
	if err := s.applySchema(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return s, nil
}

// Close releases the connection pool. Safe to call multiple times.
func (s *Postgres) Close() {
	s.pool.Close()
}

func (s *Postgres) applySchema(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, schemaSQL)
	return err
}

// LoadAll returns the current tunables. Returns ErrEmpty when the
// defaults table is empty (cold start).
func (s *Postgres) LoadAll(ctx context.Context) (*config.Tunables, error) {
	d, err := s.loadDefaults(ctx)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrEmpty
		}
		return nil, err
	}
	overrides, err := s.loadOverrides(ctx)
	if err != nil {
		return nil, err
	}
	return &config.Tunables{Defaults: d, Beacons: overrides}, nil
}

func (s *Postgres) loadDefaults(ctx context.Context) (config.DefaultsBlock, error) {
	var d config.DefaultsBlock
	err := s.pool.QueryRow(ctx, `
		SELECT alpha, grace_period_s, decay_rate_db_per_s, presence_floor_dbm,
		       t_away_max_s, sticky_after_arrival_s, hysteresis_db, confirm_count
		  FROM tunables_defaults WHERE id = 1
	`).Scan(&d.Alpha, &d.GracePeriodS, &d.DecayRateDbPerS, &d.PresenceFloorDbm,
		&d.TAwayMaxS, &d.StickyAfterArrivalS, &d.HysteresisDb, &d.ConfirmCount)
	if err != nil {
		return d, err
	}
	return d, nil
}

func (s *Postgres) loadOverrides(ctx context.Context) (map[string]config.Overrides, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT beacon_name, alpha, grace_period_s, decay_rate_db_per_s, presence_floor_dbm,
		       t_away_max_s, sticky_after_arrival_s, hysteresis_db, confirm_count
		  FROM tunables_overrides
	`)
	if err != nil {
		return nil, fmt.Errorf("query overrides: %w", err)
	}
	defer rows.Close()
	out := make(map[string]config.Overrides)
	for rows.Next() {
		var name string
		var o config.Overrides
		if err := rows.Scan(&name, &o.Alpha, &o.GracePeriodS, &o.DecayRateDbPerS,
			&o.PresenceFloorDbm, &o.TAwayMaxS, &o.StickyAfterArrivalS,
			&o.HysteresisDb, &o.ConfirmCount); err != nil {
			return nil, fmt.Errorf("scan override: %w", err)
		}
		out[name] = o
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows: %w", err)
	}
	return out, nil
}

// SaveAll atomically replaces the persisted tunables with t. Defaults
// upserts in place; overrides are wiped and rewritten so a removed
// beacon override disappears.
func (s *Postgres) SaveAll(ctx context.Context, t *config.Tunables) error {
	if t == nil {
		return errors.New("SaveAll: nil tunables")
	}
	// Validate at the persistence boundary so a caller that built the
	// document via direct struct literals (bypassing ParseTunables)
	// cannot land a malformed row. The admin handler validates
	// upstream too; this is defense-in-depth, matching the
	// settings.NewSettings pattern.
	if err := t.Validate(); err != nil {
		return fmt.Errorf("SaveAll: %w", err)
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		INSERT INTO tunables_defaults (id, alpha, grace_period_s, decay_rate_db_per_s,
			presence_floor_dbm, t_away_max_s, sticky_after_arrival_s, hysteresis_db,
			confirm_count, updated_at)
		VALUES (1, $1, $2, $3, $4, $5, $6, $7, $8, now())
		ON CONFLICT (id) DO UPDATE SET
			alpha = EXCLUDED.alpha,
			grace_period_s = EXCLUDED.grace_period_s,
			decay_rate_db_per_s = EXCLUDED.decay_rate_db_per_s,
			presence_floor_dbm = EXCLUDED.presence_floor_dbm,
			t_away_max_s = EXCLUDED.t_away_max_s,
			sticky_after_arrival_s = EXCLUDED.sticky_after_arrival_s,
			hysteresis_db = EXCLUDED.hysteresis_db,
			confirm_count = EXCLUDED.confirm_count,
			updated_at = now()
	`, t.Defaults.Alpha, t.Defaults.GracePeriodS, t.Defaults.DecayRateDbPerS,
		t.Defaults.PresenceFloorDbm, t.Defaults.TAwayMaxS, t.Defaults.StickyAfterArrivalS,
		t.Defaults.HysteresisDb, t.Defaults.ConfirmCount); err != nil {
		return fmt.Errorf("upsert defaults: %w", err)
	}

	if _, err := tx.Exec(ctx, `DELETE FROM tunables_overrides`); err != nil {
		return fmt.Errorf("clear overrides: %w", err)
	}
	for name, o := range t.Beacons {
		if _, err := tx.Exec(ctx, `
			INSERT INTO tunables_overrides (beacon_name, alpha, grace_period_s,
				decay_rate_db_per_s, presence_floor_dbm, t_away_max_s,
				sticky_after_arrival_s, hysteresis_db, confirm_count, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now())
		`, name, o.Alpha, o.GracePeriodS, o.DecayRateDbPerS, o.PresenceFloorDbm,
			o.TAwayMaxS, o.StickyAfterArrivalS, o.HysteresisDb, o.ConfirmCount); err != nil {
			return fmt.Errorf("insert override %q: %w", name, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}
