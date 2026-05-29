-- Schema for the briihass persistence backend.
-- Applied idempotently via CREATE TABLE IF NOT EXISTS on every boot.
-- New columns can be added by appending ALTER TABLE ... ADD COLUMN
-- IF NOT EXISTS statements at the bottom; no separate migration
-- tool is used.

-- Tunables (Phase 2): presence-engine knobs.
CREATE TABLE IF NOT EXISTS tunables_defaults (
  id                       INTEGER PRIMARY KEY CHECK (id = 1),
  alpha                    DOUBLE PRECISION NOT NULL CHECK (alpha > 0 AND alpha <= 1),
  grace_period_s           INTEGER NOT NULL CHECK (grace_period_s >= 0),
  decay_rate_db_per_s      DOUBLE PRECISION NOT NULL CHECK (decay_rate_db_per_s >= 0),
  presence_floor_dbm       INTEGER NOT NULL CHECK (presence_floor_dbm BETWEEN -127 AND 0),
  t_away_max_s             INTEGER NOT NULL CHECK (t_away_max_s >= 1),
  sticky_after_arrival_s   INTEGER NOT NULL CHECK (sticky_after_arrival_s >= 0),
  hysteresis_db            DOUBLE PRECISION NOT NULL CHECK (hysteresis_db >= 0),
  confirm_count            INTEGER NOT NULL CHECK (confirm_count >= 1),
  updated_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS tunables_overrides (
  beacon_name              TEXT PRIMARY KEY,
  alpha                    DOUBLE PRECISION,
  grace_period_s           INTEGER,
  decay_rate_db_per_s      DOUBLE PRECISION,
  presence_floor_dbm       INTEGER,
  t_away_max_s             INTEGER,
  sticky_after_arrival_s   INTEGER,
  hysteresis_db            DOUBLE PRECISION,
  confirm_count            INTEGER,
  updated_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Phase 4: the allowlist. Authoritative source of "what to track".
-- Operator-mutated via /admin/devices promote/demote.
-- Identity is polymorphic + packet-derived (ADR-0008): (kind, key)
-- replaces the iBeacon-only (uuid, major, minor) tuple. kind is an
-- ids.Kind discriminator; key is the canonical, kind-specific string.
CREATE TABLE IF NOT EXISTS beacons (
  kind           TEXT NOT NULL CHECK (kind ~ '^[a-z][a-z0-9_]{0,31}$'),
  key            TEXT NOT NULL CHECK (length(key) BETWEEN 1 AND 255),
  name           TEXT NOT NULL UNIQUE,
  promoted_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  notes          TEXT,
  PRIMARY KEY (kind, key)
);

-- Phase 4 migration: fold a pre-existing iBeacon-only beacons table
-- (uuid, major, minor PK) into the polymorphic (kind, key) shape.
-- Tolerant of a fresh DB (columns already absent → no-ops). Backfills
-- kind='ibeacon', key='<uuid>_<major>_<minor>' from the legacy columns,
-- then drops them and re-keys. Remove this block once every deployed
-- environment has rolled past Phase 4.
ALTER TABLE beacons ADD COLUMN IF NOT EXISTS kind TEXT;
ALTER TABLE beacons ADD COLUMN IF NOT EXISTS key  TEXT;
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns
              WHERE table_name='beacons' AND column_name='uuid') THEN
    UPDATE beacons
       SET kind = 'ibeacon',
           key  = uuid || '_' || major::text || '_' || minor::text
     WHERE kind IS NULL OR key IS NULL;
    ALTER TABLE beacons DROP CONSTRAINT IF EXISTS beacons_pkey;
    ALTER TABLE beacons ALTER COLUMN kind SET NOT NULL;
    ALTER TABLE beacons ALTER COLUMN key  SET NOT NULL;
    ALTER TABLE beacons ADD PRIMARY KEY (kind, key);
    ALTER TABLE beacons DROP COLUMN uuid;
    ALTER TABLE beacons DROP COLUMN major;
    ALTER TABLE beacons DROP COLUMN minor;
  END IF;
END $$;

-- AP MAC -> zone label. Operator-mutated via /admin/zones.
-- CHECK constraints mirror ids.NewAPMAC and ids.NewZoneLabel so a row
-- that somehow bypassed the application layer (e.g. direct psql
-- write, restored backup) cannot persist a value that would later
-- crash rebuildEngineTopology with "not a valid MAC address" or
-- corrupt MQTT topic templates.
CREATE TABLE IF NOT EXISTS zones (
  ap_mac         TEXT PRIMARY KEY CHECK (ap_mac ~ '^[0-9a-f]{2}(:[0-9a-f]{2}){5}$'),
  zone_label     TEXT NOT NULL CHECK (zone_label ~ '^[a-z][a-z0-9_]{0,63}$'),
  ap_name        TEXT,
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Full vRIoT POST envelopes (gzipped body + select headers).
-- Optional; gated by settings.capture_full_posts. Pruned by the
-- retention worker in lockstep with observations.
CREATE TABLE IF NOT EXISTS raw_posts (
  id               BIGSERIAL PRIMARY KEY,
  received_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  endpoint         TEXT NOT NULL,
  remote_addr      TEXT,
  content_encoding TEXT,
  body_gzip        BYTEA NOT NULL,
  body_sha256      TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS raw_posts_received_at_idx ON raw_posts (received_at DESC);

-- Every BLE advert with a stable packet-derived identity, tracked or
-- not. Short retention. Anonymous/ephemeral adverts are counted in
-- metrics, not stored here (ADR-0008). Identity is (kind, key).
-- Enrichment columns (battery_mv, temperature_c, local_name) carry
-- telemetry attributed to this identity within a POST.
-- raw_hex carries the per-event BLE advert (NULL if
-- settings.capture_per_event_hex is off). raw_post_id links back to
-- the request envelope (NULL if settings.capture_full_posts is off).
-- tracked records whether the beacon was on the allowlist at
-- observation time (used to surface "observed-only" in the UI).
--
-- raw_post_id is a SOFT POINTER (no FK constraint). The async
-- RawPostsWriter pre-allocates raw_posts ids via nextval before
-- handing them to ingest, then INSERTs the envelope rows out-of-band.
-- Observations may reach the DB before the envelope row exists; a
-- hard FK would reject those inserts. The retention worker prunes
-- both tables on the same wall-clock window so dangling raw_post_id
-- pointers self-heal as part of normal expiry.
CREATE TABLE IF NOT EXISTS observations (
  id             BIGSERIAL PRIMARY KEY,
  observed_at    TIMESTAMPTZ NOT NULL,
  kind           TEXT NOT NULL,
  key            TEXT NOT NULL,
  ap_mac         TEXT NOT NULL,
  ap_name        TEXT,
  rssi           INTEGER NOT NULL,
  tx_power       INTEGER,
  battery_mv     INTEGER,
  temperature_c  DOUBLE PRECISION,
  local_name     TEXT,
  raw_hex        TEXT,
  raw_post_id    BIGINT, -- soft pointer; see comment above
  tracked        BOOLEAN NOT NULL
);

-- One-shot Phase 3 migration: drop the FK constraint that earlier
-- schema versions created on raw_post_id. Tolerant of either (a) fresh
-- DB with no constraint or (b) prior schema that wrote the FK.
ALTER TABLE observations DROP CONSTRAINT IF EXISTS observations_raw_post_id_fkey;

-- Phase 4 migration: re-key observations from (uuid,major,minor) to
-- (kind,key) and add enrichment columns. Tolerant of a fresh DB.
-- Backfills kind='ibeacon', key='<uuid>_<major>_<minor>' for legacy
-- rows, then drops the old columns. Retention (<=30d) ages out anything
-- not backfilled. Remove this block once every environment has rolled
-- past Phase 4.
ALTER TABLE observations ADD COLUMN IF NOT EXISTS kind          TEXT;
ALTER TABLE observations ADD COLUMN IF NOT EXISTS key           TEXT;
ALTER TABLE observations ADD COLUMN IF NOT EXISTS battery_mv    INTEGER;
ALTER TABLE observations ADD COLUMN IF NOT EXISTS temperature_c DOUBLE PRECISION;
ALTER TABLE observations ADD COLUMN IF NOT EXISTS local_name    TEXT;
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM information_schema.columns
              WHERE table_name='observations' AND column_name='uuid') THEN
    UPDATE observations
       SET kind = 'ibeacon',
           key  = uuid || '_' || major::text || '_' || minor::text
     WHERE kind IS NULL OR key IS NULL;
    ALTER TABLE observations DROP COLUMN uuid;
    ALTER TABLE observations DROP COLUMN major;
    ALTER TABLE observations DROP COLUMN minor;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS observations_observed_at_idx ON observations (observed_at DESC);
CREATE INDEX IF NOT EXISTS observations_beacon_idx      ON observations (kind, key, observed_at DESC);
CREATE INDEX IF NOT EXISTS observations_ap_idx          ON observations (ap_mac, observed_at DESC);
DROP INDEX IF EXISTS observations_beacon_idx_legacy;

-- Single-row settings the admin UI can edit. Hot-read by the ingest
-- path via an in-memory snapshot refreshed on save.
CREATE TABLE IF NOT EXISTS settings (
  id                       INTEGER PRIMARY KEY CHECK (id = 1),
  retention_days           INTEGER NOT NULL CHECK (retention_days BETWEEN 1 AND 30) DEFAULT 7,
  capture_per_event_hex    BOOLEAN NOT NULL DEFAULT TRUE,
  capture_full_posts       BOOLEAN NOT NULL DEFAULT FALSE,
  updated_at               TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO settings (id, retention_days, capture_per_event_hex, capture_full_posts)
VALUES (1, 7, TRUE, FALSE)
ON CONFLICT (id) DO NOTHING;

-- Persisted presence-engine state so a fresh pod boots warm instead of
-- cold. Without this a restart (deploy, evict, crash) starts every
-- tracked beacon at not_home and the 20s telemetry pump republishes
-- not_home before the new pod re-observes the beacon — flapping a
-- present beacon to away mid-deploy (the sticky-arrival regression). One
-- row per tracked (kind, key): the last published zone, the AP backing
-- it, and the sticky-arrival timestamp so the not_home-suppression
-- window (ADR-0006) survives the restart too. current_zone='' means
-- not_home; last_arrival_ts NULL means never arrived. Rewritten in full
-- on each flush, so rows for demoted beacons disappear.
CREATE TABLE IF NOT EXISTS presence_state (
  kind            TEXT NOT NULL CHECK (kind ~ '^[a-z][a-z0-9_]{0,31}$'),
  key             TEXT NOT NULL CHECK (length(key) BETWEEN 1 AND 255),
  current_zone    TEXT NOT NULL DEFAULT '',
  current_ap      TEXT NOT NULL DEFAULT '',
  last_arrival_ts TIMESTAMPTZ,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (kind, key)
);
