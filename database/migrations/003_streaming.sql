-- ======================================================================
-- Migration 003: streaming layer
--
-- Kafka topic market.price_ticks
--     -> Spark Structured Streaming
--         -> streaming.price_ticks     every valid tick, once (event_id PK)
--         -> streaming.rejected_ticks  invalid messages + reason (stream quarantine)
--         -> streaming.price_bars_5m   5-minute OHLC bars per symbol, by event time
--         -> streaming.batch_log       one row per micro-batch (throughput, dupes)
--
-- Exactly-once results from at-least-once delivery: Spark replays a
-- micro-batch after a failure, so every write here is idempotent
-- (ON CONFLICT on natural keys, batch_log keyed by query + batch id).
--
-- Idempotent: safe to run more than once.
-- ======================================================================

BEGIN;

CREATE SCHEMA IF NOT EXISTS streaming;

CREATE TABLE IF NOT EXISTS streaming.price_ticks (
    event_id        VARCHAR(36) PRIMARY KEY,
    symbol          VARCHAR(16)      NOT NULL,
    event_ts        TIMESTAMPTZ      NOT NULL,   -- when the price happened
    price           DOUBLE PRECISION NOT NULL,
    volume          BIGINT,
    source          VARCHAR(16)      NOT NULL,   -- replay | live
    produced_at     TIMESTAMPTZ,                 -- when the producer sent it
    kafka_partition INTEGER,
    kafka_offset    BIGINT,
    processed_at    TIMESTAMPTZ      NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_price_ticks_symbol_ts
    ON streaming.price_ticks (symbol, event_ts DESC);
CREATE INDEX IF NOT EXISTS ix_price_ticks_processed
    ON streaming.price_ticks (processed_at DESC);

CREATE TABLE IF NOT EXISTS streaming.rejected_ticks (
    kafka_partition INTEGER     NOT NULL,
    kafka_offset    BIGINT      NOT NULL,
    raw_message     TEXT,
    reason          TEXT        NOT NULL,
    rejected_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (kafka_partition, kafka_offset)      -- a replayed batch can't double-count
);

CREATE TABLE IF NOT EXISTS streaming.price_bars_5m (
    symbol       VARCHAR(16)      NOT NULL,
    window_start TIMESTAMPTZ      NOT NULL,
    window_end   TIMESTAMPTZ      NOT NULL,
    open         DOUBLE PRECISION NOT NULL,
    high         DOUBLE PRECISION NOT NULL,
    low          DOUBLE PRECISION NOT NULL,
    close        DOUBLE PRECISION NOT NULL,
    ticks        BIGINT           NOT NULL,
    volume       BIGINT,
    updated_at   TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, window_start)
);

CREATE TABLE IF NOT EXISTS streaming.batch_log (
    query_name   VARCHAR(32) NOT NULL,
    batch_id     BIGINT      NOT NULL,
    rows_in      INTEGER     NOT NULL,
    inserted     INTEGER     NOT NULL,
    duplicates   INTEGER     NOT NULL,
    rejected     INTEGER     NOT NULL,
    duration_ms  INTEGER,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (query_name, batch_id)
);

CREATE INDEX IF NOT EXISTS ix_batch_log_processed
    ON streaming.batch_log (processed_at DESC);

COMMIT;
