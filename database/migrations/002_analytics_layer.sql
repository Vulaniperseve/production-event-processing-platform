-- ======================================================================
-- Migration 002: analytics layer
--
-- staging.market_events (clean, deduplicated events)
--     -> analytics.daily_prices        one close price per symbol per day
--     -> analytics.daily_returns       day-over-day simple and log returns
--     -> analytics.price_indicators    moving averages, volatility, drawdown
--     -> analytics.symbol_summary      one row per symbol (MATERIALIZED)
--
-- raw / quarantine / staging
--     -> analytics.pipeline_health     ingestion and data-quality metrics
--     -> analytics.data_freshness      how current each symbol's data is
--
-- Plain views are always up to date, and cheap at this data size.
-- symbol_summary aggregates the full history of every symbol, so it is
-- materialized and refreshed by the pipeline after each load.
--
-- Rolling windows use ROWS (trading days), not calendar days: markets are
-- closed on weekends and holidays, so "20-day" means 20 trading sessions.
--
-- Idempotent: safe to run more than once.
-- ======================================================================

BEGIN;

CREATE SCHEMA IF NOT EXISTS analytics;

-- ---------------------------------------------------------------------
-- daily_prices: the last observed price per symbol per calendar day.
-- (Works for intraday intervals too: 1h data collapses to the day's close.)
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics.daily_prices AS
SELECT DISTINCT ON (entity_id, event_time::date)
    entity_id           AS symbol,
    event_time::date    AS trade_date,
    value               AS close_price,
    event_time          AS observed_at,
    raw_event_id
FROM staging.market_events
WHERE event_type = 'MARKET_PRICE'
  AND event_time IS NOT NULL
  AND value IS NOT NULL
ORDER BY entity_id, event_time::date, event_time DESC;

-- ---------------------------------------------------------------------
-- daily_returns: LAG() compares each day with the previous trading day.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics.daily_returns AS
WITH priced AS (
    SELECT
        symbol,
        trade_date,
        close_price,
        LAG(close_price) OVER w AS prev_close,
        LAG(trade_date)  OVER w AS prev_trade_date
    FROM analytics.daily_prices
    WINDOW w AS (PARTITION BY symbol ORDER BY trade_date)
)
SELECT
    symbol,
    trade_date,
    close_price,
    prev_close,
    prev_trade_date,
    CASE WHEN prev_close > 0
         THEN close_price / prev_close - 1 END            AS daily_return,
    CASE WHEN prev_close > 0 AND close_price > 0
         THEN LN(close_price / prev_close) END            AS log_return
FROM priced;

-- ---------------------------------------------------------------------
-- price_indicators: rolling windows over trading days.
-- A window statistic is NULL until the window is full, so a "20-day"
-- average is never silently computed from 3 days of data.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics.price_indicators AS
SELECT
    symbol,
    trade_date,
    close_price,
    daily_return,

    CASE WHEN COUNT(*) OVER w5 = 5
         THEN AVG(close_price) OVER w5 END                        AS sma_5,

    CASE WHEN COUNT(*) OVER w20 = 20
         THEN AVG(close_price) OVER w20 END                       AS sma_20,

    -- 20-day volatility of daily returns (needs 20 returns = 21 prices)
    CASE WHEN COUNT(daily_return) OVER w20 = 20
         THEN STDDEV_SAMP(daily_return) OVER w20 END              AS volatility_20d,

    CASE WHEN COUNT(daily_return) OVER w20 = 20
         THEN STDDEV_SAMP(daily_return) OVER w20 * SQRT(252) END  AS volatility_20d_annualized,

    MAX(close_price) OVER running                                 AS running_peak,
    close_price / NULLIF(MAX(close_price) OVER running, 0) - 1    AS drawdown,

    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date)   AS trading_day_number

FROM analytics.daily_returns
WINDOW
    w5      AS (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW),
    w20     AS (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
    running AS (PARTITION BY symbol ORDER BY trade_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);

-- ---------------------------------------------------------------------
-- symbol_summary: one row per symbol (materialized).
-- ---------------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.symbol_summary AS
WITH ordered AS (
    SELECT
        *,
        FIRST_VALUE(close_price) OVER (
            PARTITION BY symbol ORDER BY trade_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS first_close,
        LAST_VALUE(close_price) OVER (
            PARTITION BY symbol ORDER BY trade_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS last_close
    FROM analytics.price_indicators
)
SELECT
    symbol,
    MIN(trade_date)                                              AS first_date,
    MAX(trade_date)                                              AS last_date,
    COUNT(*)                                                     AS trading_days,
    MAX(first_close)                                             AS first_close,
    MAX(last_close)                                              AS last_close,
    MAX(last_close) / NULLIF(MAX(first_close), 0) - 1            AS period_return,
    AVG(daily_return)                                            AS avg_daily_return,
    STDDEV_SAMP(daily_return)                                    AS daily_volatility,
    STDDEV_SAMP(daily_return) * SQRT(252)                        AS annualized_volatility,
    MIN(drawdown)                                                AS max_drawdown,
    MAX(daily_return)                                            AS best_day_return,
    (ARRAY_AGG(trade_date ORDER BY daily_return DESC NULLS LAST))[1] AS best_day,
    MIN(daily_return)                                            AS worst_day_return,
    (ARRAY_AGG(trade_date ORDER BY daily_return ASC NULLS LAST))[1]  AS worst_day,
    now()                                                        AS refreshed_at
FROM ordered
GROUP BY symbol;

-- Required for REFRESH MATERIALIZED VIEW CONCURRENTLY (readers never blocked)
CREATE UNIQUE INDEX IF NOT EXISTS ux_symbol_summary_symbol
    ON analytics.symbol_summary (symbol);

-- ---------------------------------------------------------------------
-- pipeline_health: what happened to every API response, per day/symbol.
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics.pipeline_health AS
SELECT
    r.extracted_at::date                                                  AS run_date,
    r.symbol,
    COUNT(*)                                                              AS api_responses,
    COUNT(*) FILTER (WHERE r.processing_status = 'LOADED')                AS loaded,
    COUNT(*) FILTER (WHERE r.processing_status = 'NO_VALID_RECORDS')      AS no_valid_records,
    COUNT(*) FILTER (WHERE r.processing_status = 'FAILED')                AS failed,
    COUNT(*) FILTER (WHERE r.processing_status IN ('PENDING', 'VALIDATED')) AS unfinished,
    COALESCE(SUM(r.record_count), 0)                                      AS records_received,
    COALESCE(SUM(q.quarantined), 0)                                       AS records_quarantined,
    COALESCE(SUM(s.loaded), 0)                                            AS new_events_loaded,
    ROUND(
        COALESCE(SUM(q.quarantined), 0)::numeric
        / NULLIF(SUM(r.record_count), 0), 4
    )                                                                     AS quarantine_rate
FROM raw.raw_api_events r
LEFT JOIN (
    SELECT raw_event_id, COUNT(*) AS quarantined
    FROM quarantine.quarantined_records GROUP BY raw_event_id
) q ON q.raw_event_id = r.id
LEFT JOIN (
    SELECT raw_event_id, COUNT(*) AS loaded
    FROM staging.market_events GROUP BY raw_event_id
) s ON s.raw_event_id = r.id
GROUP BY r.extracted_at::date, r.symbol;

-- ---------------------------------------------------------------------
-- data_freshness: is each symbol's data current?
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW analytics.data_freshness AS
SELECT
    entity_id                                                    AS symbol,
    MAX(event_time)::date                                        AS latest_trade_date,
    MAX(loaded_at)                                               AS last_loaded_at,
    ROUND(EXTRACT(EPOCH FROM (now() - MAX(loaded_at))) / 3600, 1) AS hours_since_last_load,
    CURRENT_DATE - MAX(event_time)::date                         AS days_behind
FROM staging.market_events
GROUP BY entity_id;

COMMIT;
