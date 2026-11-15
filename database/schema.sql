-- ======================================================================
-- Production Event Processing Platform - database schema (reference DDL)
--
-- Layers:
--   raw         API responses exactly as received (append-only)
--   quarantine  records that failed validation, linked to raw
--   staging     cleaned, deduplicated events, linked to raw
--
-- Apply all migrations: python -m database.migrate  (analytics views: 002)
-- Source of truth: database/migrations/ (this file mirrors the tables only)
-- ======================================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS quarantine;

CREATE TABLE IF NOT EXISTS raw.raw_api_events (
    id                 SERIAL PRIMARY KEY,
    source_system      VARCHAR(50) NOT NULL,
    symbol             VARCHAR(20),
    raw_json           JSONB NOT NULL,
    payload_hash       VARCHAR(64),
    api_status         VARCHAR(20),
    record_count       INTEGER,
    processing_status  VARCHAR(20) NOT NULL DEFAULT 'PENDING',
        -- PENDING | VALIDATED | LOADED | NO_VALID_RECORDS | FAILED
    error_message      TEXT,
    extracted_at       TIMESTAMP DEFAULT now(),
    processed_at       TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_raw_api_events_payload_hash
    ON raw.raw_api_events (payload_hash);
CREATE INDEX IF NOT EXISTS ix_raw_api_events_status
    ON raw.raw_api_events (processing_status);

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

CREATE TABLE IF NOT EXISTS staging.market_events (
    event_id       VARCHAR(36) PRIMARY KEY,   -- deterministic hash: idempotency key
    entity_id      VARCHAR(20) NOT NULL,
    event_time     TIMESTAMP,
    event_type     VARCHAR(50),
    value          DOUBLE PRECISION,
    source_system  VARCHAR(50),
    raw_event_id   INTEGER REFERENCES raw.raw_api_events (id),
    loaded_at      TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_staging_market_events_raw_event_id
    ON staging.market_events (raw_event_id);
