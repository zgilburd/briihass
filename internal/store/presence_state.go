package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// PresenceStateRow is one persisted per-beacon presence snapshot: the
// last published zone, the AP backing it, and the sticky-arrival
// timestamp. CurrentZone == "" means not_home. LastArrival is the zero
// time when the beacon has never arrived (stored as NULL).
type PresenceStateRow struct {
	Kind        string
	Key         string
	CurrentZone string
	CurrentAP   string
	LastArrival time.Time
}

// SavePresenceState replaces the persisted presence snapshot with rows
// in a single transaction. Full replace (not upsert) so a demoted
// beacon's row disappears on the next flush; an empty slice clears the
// table. kind/key are validated up front so a malformed row can't open
// a transaction we'd only have to roll back.
func (s *Postgres) SavePresenceState(ctx context.Context, rows []PresenceStateRow) error {
	for i, r := range rows {
		if r.Kind == "" || r.Key == "" {
			return fmt.Errorf("PresenceStateRow[%d]: kind and key required", i)
		}
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `DELETE FROM presence_state`); err != nil {
		return fmt.Errorf("clear presence_state: %w", err)
	}
	for _, r := range rows {
		var lastArrival any
		if !r.LastArrival.IsZero() {
			lastArrival = r.LastArrival
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO presence_state (kind, key, current_zone, current_ap, last_arrival_ts, updated_at)
			VALUES ($1, $2, $3, $4, $5, now())
		`, r.Kind, r.Key, r.CurrentZone, r.CurrentAP, lastArrival); err != nil {
			return fmt.Errorf("insert presence_state %s/%s: %w", r.Kind, r.Key, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// LoadPresenceState returns every persisted presence row. Called once at
// boot to rehydrate the engine before the ingest listener starts.
func (s *Postgres) LoadPresenceState(ctx context.Context) ([]PresenceStateRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT kind, key, current_zone, current_ap, last_arrival_ts
		  FROM presence_state
	`)
	if err != nil {
		return nil, fmt.Errorf("query presence_state: %w", err)
	}
	defer rows.Close()
	var out []PresenceStateRow
	for rows.Next() {
		var r PresenceStateRow
		var lastArrival *time.Time
		if err := rows.Scan(&r.Kind, &r.Key, &r.CurrentZone, &r.CurrentAP, &lastArrival); err != nil {
			return nil, fmt.Errorf("scan presence_state: %w", err)
		}
		if lastArrival != nil {
			r.LastArrival = *lastArrival
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
