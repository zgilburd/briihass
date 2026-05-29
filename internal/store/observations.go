package store

import (
	"context"
	"fmt"
	"time"

	"briihass/internal/ids"

	"github.com/jackc/pgx/v5"
)

// Observation is one BLE iBeacon advert as recorded by the bridge.
//
// Optional-field convention (asymmetric and intentional):
//   - TxPower and RawPostID are *T because their absence is
//     informational at the consumer (e.g. tx_power=nil means vRIoT
//     omitted the field on this advert; nil-vs-zero matters for
//     analytics).
//   - APName and RawHex are zero-value-as-absent (empty string). The
//     admin packets renderer paints an explicit "raw hex not captured
//     (capture_per_event_hex off when recorded)" notice when RawHex
//     is empty, so the distinction between "absent" and "empty
//     payload" is collapsed at the renderer; no downstream consumer
//     differentiates them. ap_name is similarly cosmetic.
//
// If a future consumer needs the absent/empty distinction for either
// string field, promote it to *string here AND update InsertObservations
// and ListObservationsForDevice (the COALESCE in the SQL preserves the
// distinction at the row level).
type Observation struct {
	ID         int64
	ObservedAt time.Time
	Kind       string // ids.Kind string (ibeacon, eddystone_uid, name, …)
	Key        string // canonical, kind-specific identity key
	APMac      string
	APName     string // zero-value-as-absent; see type doc
	RSSI       int
	TxPower    *int
	// Enrichment (Phase 4): populated when attributable to this identity
	// within the POST (e.g. an Eddystone-TLM frame from the same euid).
	// nil/empty means absent.
	BatteryMV    *int
	TemperatureC *float64
	LocalName    string
	RawHex       string // zero-value-as-absent; see type doc
	RawPostID    *int64 // may be nil when capture_full_posts is off
	Tracked      bool
}

// InsertObservations writes a batch via pgx.CopyFrom — one network
// round trip per batch regardless of len(obs). Empty slices are no-ops.
// Returns an error if any row has an empty uuid or ap_mac; validation
// runs up front so a single bad row can't partially commit.
func (s *Postgres) InsertObservations(ctx context.Context, obs []Observation) error {
	if len(obs) == 0 {
		return nil
	}
	// Validate the whole batch before touching the DB so a malformed row
	// doesn't open a transaction we'd need to roll back. kind+key must be
	// non-empty so observations join against `beacons` by string
	// equality; an empty identity would never associate with an
	// allowlist entry.
	for i, o := range obs {
		if o.Kind == "" || o.Key == "" || o.APMac == "" {
			return fmt.Errorf("Observation[%d]: kind, key and ap_mac required", i)
		}
	}
	rows := make([][]any, len(obs))
	for i, o := range obs {
		var apName any
		if o.APName != "" {
			apName = o.APName
		}
		var localName any
		if o.LocalName != "" {
			localName = o.LocalName
		}
		var rawHex any
		if o.RawHex != "" {
			rawHex = o.RawHex
		}
		var rawPostID any
		if o.RawPostID != nil {
			rawPostID = *o.RawPostID
		}
		var txPower any
		if o.TxPower != nil {
			txPower = *o.TxPower
		}
		var battery any
		if o.BatteryMV != nil {
			battery = *o.BatteryMV
		}
		var temp any
		if o.TemperatureC != nil {
			temp = *o.TemperatureC
		}
		rows[i] = []any{
			o.ObservedAt, o.Kind, o.Key,
			o.APMac, apName, o.RSSI, txPower, battery, temp, localName,
			rawHex, rawPostID, o.Tracked,
		}
	}
	cols := []string{"observed_at", "kind", "key", "ap_mac", "ap_name",
		"rssi", "tx_power", "battery_mv", "temperature_c", "local_name",
		"raw_hex", "raw_post_id", "tracked"}
	n, err := s.pool.CopyFrom(ctx,
		pgx.Identifier{"observations"}, cols, pgx.CopyFromRows(rows))
	if err != nil {
		return fmt.Errorf("copy observations: %w", err)
	}
	if int(n) != len(rows) {
		return fmt.Errorf("copy observations: wrote %d of %d rows", n, len(rows))
	}
	return nil
}

// DeviceSummary is one row in the /admin/devices listing.
type DeviceSummary struct {
	Kind        string
	Key         string
	LastAPMac   string
	LastAPName  string
	LastRSSI    int
	LastSeen    time.Time
	SightingCnt int64
	Tracked     bool   // true if currently in the allowlist
	BeaconName  string // populated when Tracked
}

// Domain projects a DeviceSummary into the canonical ids.BeaconKey. Rows
// come from observations whose (kind,key) was validated at write time;
// ok is false only for a corrupt row.
func (d DeviceSummary) Domain() (ids.BeaconKey, bool) {
	return ids.FromStoreKey(d.Kind, d.Key)
}

// ListDevicesSince returns one summary row per (kind,key) seen at or
// after `since`. The Tracked flag and BeaconName are joined from the
// beacons table.
func (s *Postgres) ListDevicesSince(ctx context.Context, since time.Time) ([]DeviceSummary, error) {
	rows, err := s.pool.Query(ctx, `
		WITH agg AS (
		  SELECT kind, key,
		         count(*)         AS sighting_cnt,
		         max(observed_at) AS last_seen
		    FROM observations
		   WHERE observed_at >= $1
		   GROUP BY kind, key
		),
		latest AS (
		  SELECT DISTINCT ON (o.kind, o.key)
		         o.kind, o.key, o.ap_mac, COALESCE(o.ap_name, '') AS ap_name, o.rssi, o.observed_at
		    FROM observations o
		   WHERE o.observed_at >= $1
		   ORDER BY o.kind, o.key, o.observed_at DESC
		)
		SELECT a.kind, a.key, l.ap_mac, l.ap_name, l.rssi, a.last_seen, a.sighting_cnt,
		       b.name IS NOT NULL AS tracked, COALESCE(b.name, '') AS beacon_name
		  FROM agg a
		  JOIN latest l USING (kind, key)
		  LEFT JOIN beacons b
		         ON b.kind = a.kind AND b.key = a.key
		 ORDER BY a.last_seen DESC
	`, since)
	if err != nil {
		return nil, fmt.Errorf("query devices: %w", err)
	}
	defer rows.Close()
	var out []DeviceSummary
	for rows.Next() {
		var d DeviceSummary
		if err := rows.Scan(&d.Kind, &d.Key, &d.LastAPMac, &d.LastAPName, &d.LastRSSI,
			&d.LastSeen, &d.SightingCnt, &d.Tracked, &d.BeaconName); err != nil {
			return nil, fmt.Errorf("scan device: %w", err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// ListObservationsForDevice returns up to limit recent observations
// for one (kind,key), newest first.
func (s *Postgres) ListObservationsForDevice(ctx context.Context, kind, key string, limit int) ([]Observation, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, observed_at, kind, key, ap_mac, COALESCE(ap_name, ''),
		       rssi, tx_power, battery_mv, temperature_c, COALESCE(local_name, ''),
		       COALESCE(raw_hex, ''), raw_post_id, tracked
		  FROM observations
		 WHERE kind = $1 AND key = $2
		 ORDER BY observed_at DESC
		 LIMIT $3
	`, kind, key, limit)
	if err != nil {
		return nil, fmt.Errorf("query observations: %w", err)
	}
	defer rows.Close()
	var out []Observation
	for rows.Next() {
		var o Observation
		var txp *int
		var battery *int
		var temp *float64
		var rawPostID *int64
		if err := rows.Scan(&o.ID, &o.ObservedAt, &o.Kind, &o.Key, &o.APMac, &o.APName,
			&o.RSSI, &txp, &battery, &temp, &o.LocalName, &o.RawHex, &rawPostID, &o.Tracked); err != nil {
			return nil, fmt.Errorf("scan observation: %w", err)
		}
		o.TxPower = txp
		o.BatteryMV = battery
		o.TemperatureC = temp
		o.RawPostID = rawPostID
		out = append(out, o)
	}
	return out, rows.Err()
}

// ListAPsSince returns AP MACs seen since `since`, paired with the
// most recent ap_name. Used by /admin/zones to surface "APs you can
// label" without operator typing.
func (s *Postgres) ListAPsSince(ctx context.Context, since time.Time) (map[string]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT ON (ap_mac) ap_mac, COALESCE(ap_name, '')
		  FROM observations
		 WHERE observed_at >= $1
		 ORDER BY ap_mac, observed_at DESC
	`, since)
	if err != nil {
		return nil, fmt.Errorf("query aps: %w", err)
	}
	defer rows.Close()
	out := map[string]string{}
	for rows.Next() {
		var mac, name string
		if err := rows.Scan(&mac, &name); err != nil {
			return nil, fmt.Errorf("scan ap: %w", err)
		}
		out[mac] = name
	}
	return out, rows.Err()
}

// PruneOlderThan deletes observations + raw_posts older than `before`.
// Run by the retention worker.
func (s *Postgres) PruneOlderThan(ctx context.Context, before time.Time) (observations, rawPosts int64, err error) {
	tag1, err := s.pool.Exec(ctx, `DELETE FROM observations WHERE observed_at < $1`, before)
	if err != nil {
		return 0, 0, fmt.Errorf("prune observations: %w", err)
	}
	tag2, err := s.pool.Exec(ctx, `DELETE FROM raw_posts WHERE received_at < $1`, before)
	if err != nil {
		return tag1.RowsAffected(), 0, fmt.Errorf("prune raw_posts: %w", err)
	}
	return tag1.RowsAffected(), tag2.RowsAffected(), nil
}
