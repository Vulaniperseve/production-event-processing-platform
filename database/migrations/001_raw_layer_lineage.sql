-- ======================================================================
-- Migration 001: complete raw layer, quarantine table and lineage
--
-- Safe to run more than once (idempotent). Upgrades databases created
-- before the raw layer had status tracking / lineage columns.
--
-- Run:
--   psql -h <host> -U <user> -d <db> -f database/migrations/001_raw_layer_lineage.sql
-- ======================================================================

BEGIN;

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS quarantine;

-- ---------------------------------------------------------------------
-- raw.raw_api_events
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.raw_api_events (
    id             SERIAL PRIMARY KEY,
    source_system  VARCHAR(50) NOT NULL,
    symbol         VARCHAR(20),
    raw_json       JSONB NOT NULL,
    extracted_at   TIMESTAMP DEFAULT now()
);

-- TEXT -> JSONB (no-op if already JSONB)
DO $$
BEGIN
    IF (SELECT data_type FROM information_schema.columns
        WHERE table_schema = 'raw'
          AND table_name   = 'raw_api_events'
          AND column_name  = 'raw_json') = 'text' THEN
        ALTER TABLE raw.raw_api_events
            ALTER COLUMN raw_json TYPE JSONB USING raw_json::jsonb;
    END IF;
END $$;

ALTER TABLE raw.raw_api_events
    ADD COLUMN IF NOT EXISTS payload_hash      VARCHAR(64),
    ADD COLUMN IF NOT EXISTS api_status        VARCHAR(20),
    ADD COLUMN IF NOT EXISTS record_count      INTEGER,
    ADD COLUMN IF NOT EXISTS processing_status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    ADD COLUMN IF NOT EXISTS error_message     TEXT,
    ADD COLUMN IF NOT EXISTS processed_at      TIMESTAMP;

-- Backfill metadata for rows saved before this migration
UPDATE raw.raw_api_events
SET api_status   = raw_json->>'status',
    record_count = jsonb_array_length(COALESCE(raw_json->'values', '[]'::jsonb)),
    payload_hash = encode(sha256(convert_to(raw_json::text, 'UTF8')), 'hex')
WHERE payload_hash IS NULL;

CREATE INDEX IF NOT EXISTS ix_raw_api_events_payload_hash
    ON raw.raw_api_events (payload_hash);
CREATE INDEX IF NOT EXISTS ix_raw_api_events_status
    ON raw.raw_api_events (processing_status);

-- ---------------------------------------------------------------------
-- quarantine.quarantined_records
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS quarantine.quarantined_records (
    id              SERIAL PRIMARY KEY,
    raw_event_id    INTEGER NOT NULL REFERENCES raw.raw_api_events (id),
    symbol          VARCHAR(20),
    record          JSONB NOT NULL,
    reason          TEXT NOT NULL,
    quarantined_at  TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_quarantine_quarantined_records_raw_event_id
    ON quarantine.quarantined_records (raw_event_id);

-- ---------------------------------------------------------------------
-- staging.market_events: lineage back to raw
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.market_events (
    event_id       VARCHAR(36) PRIMARY KEY,
    entity_id      VARCHAR(20) NOT NULL,
    event_time     TIMESTAMP,
    event_type     VARCHAR(50),
    value          DOUBLE PRECISION,
    source_system  VARCHAR(50),
    loaded_at      TIMESTAMP DEFAULT now()
);

ALTER TABLE staging.market_events
    ADD COLUMN IF NOT EXISTS raw_event_id INTEGER REFERENCES raw.raw_api_events (id);

CREATE INDEX IF NOT EXISTS ix_staging_market_events_raw_event_id
    ON staging.market_events (raw_event_id);

COMMIT;
