"""
Read-only data access for the dashboard: the PostgreSQL warehouse and the
Parquet data lake.

* Every database session is opened with default_transaction_read_only=on,
  so the dashboard cannot change data even if a query were wrong.
* No Streamlit imports here: pages add caching on top, tests call these
  functions directly.
"""

import os
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text


# ----------------------------------------------------------------- engine
def make_engine(url=None):
    if url is None:
        url = (
            f"postgresql+psycopg2://{os.getenv('DB_USER', 'postgres')}:"
            f"{os.getenv('DB_PASSWORD', 'postgres')}@{os.getenv('DB_HOST', 'localhost')}:"
            f"{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'postgres')}"
        )
    return create_engine(
        url,
        pool_pre_ping=True,
        pool_size=2,
        max_overflow=2,
        connect_args={"options": "-c default_transaction_read_only=on",
                      "connect_timeout": 5},
    )


def query(engine, sql, **params):
    with engine.connect() as conn:
        result = conn.execute(text(sql), params)
        return pd.DataFrame(result.mappings().all(), columns=list(result.keys()))


# -------------------------------------------------------------- warehouse
def symbol_summary(engine):
    return query(engine, """
        SELECT symbol, first_date, last_date, trading_days, first_close, last_close,
               period_return, annualized_volatility, max_drawdown,
               best_day, best_day_return, worst_day, worst_day_return, refreshed_at
        FROM analytics.symbol_summary
        ORDER BY symbol
    """)


def price_indicators(engine, symbol):
    return query(engine, """
        SELECT trade_date, close_price, daily_return, sma_5, sma_20,
               volatility_20d_annualized, running_peak, drawdown
        FROM analytics.price_indicators
        WHERE symbol = :symbol
        ORDER BY trade_date
    """, symbol=symbol)


def pipeline_health(engine, days=30):
    return query(engine, """
        SELECT run_date, symbol, api_responses, loaded, no_valid_records, failed,
               unfinished, records_received, records_quarantined, new_events_loaded,
               quarantine_rate
        FROM analytics.pipeline_health
        WHERE run_date >= CURRENT_DATE - :days
        ORDER BY run_date DESC, symbol
    """, days=days)


def data_freshness(engine):
    return query(engine, """
        SELECT symbol, latest_trade_date, last_loaded_at, hours_since_last_load, days_behind
        FROM analytics.data_freshness
        ORDER BY symbol
    """)


def raw_status_counts(engine):
    return query(engine, """
        SELECT processing_status AS status, COUNT(*) AS responses
        FROM raw.raw_api_events
        GROUP BY processing_status
        ORDER BY responses DESC
    """)


def quarantine_reasons(engine):
    return query(engine, """
        SELECT reason, symbol, COUNT(*) AS records, MAX(quarantined_at) AS last_seen
        FROM quarantine.quarantined_records
        GROUP BY reason, symbol
        ORDER BY records DESC
    """)


def recent_quarantine(engine, limit=20):
    return query(engine, """
        SELECT quarantined_at, symbol, reason, raw_event_id, record::text AS record
        FROM quarantine.quarantined_records
        ORDER BY quarantined_at DESC
        LIMIT :limit
    """, limit=limit)


def warehouse_totals(engine):
    return query(engine, """
        SELECT
            (SELECT COUNT(*) FROM raw.raw_api_events)              AS raw_responses,
            (SELECT COUNT(*) FROM quarantine.quarantined_records)  AS quarantined,
            (SELECT COUNT(*) FROM staging.market_events)           AS staging_events,
            (SELECT COUNT(DISTINCT entity_id) FROM staging.market_events) AS symbols
    """).iloc[0].to_dict()


def _naive_utc(ts):
    """raw.extracted_at is a UTC timestamp without time zone."""
    if ts is None:
        return None
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    if ts.tzinfo is not None:
        ts = ts.astimezone(UTC).replace(tzinfo=None)
    return ts


def run_numbers(engine, since, until=None):
    """
    What one ingestion run did, from the raw responses it stored
    (extracted between the run's start and end).
    """
    since, until = _naive_utc(since), _naive_utc(until)
    df = query(engine, """
        WITH run AS (
            SELECT id, symbol, record_count, processing_status
            FROM raw.raw_api_events
            WHERE extracted_at >= :since
              AND (CAST(:until AS timestamp) IS NULL OR extracted_at <= :until)
        )
        SELECT
            COUNT(*)                                              AS api_responses,
            COALESCE(SUM(record_count), 0)                        AS records_received,
            COUNT(*) FILTER (WHERE processing_status = 'LOADED')  AS responses_loaded,
            COUNT(*) FILTER (WHERE processing_status = 'FAILED')  AS responses_failed,
            (SELECT COUNT(*) FROM quarantine.quarantined_records q
              WHERE q.raw_event_id IN (SELECT id FROM run))       AS quarantined,
            (SELECT COUNT(*) FROM staging.market_events s
              WHERE s.raw_event_id IN (SELECT id FROM run))       AS new_events
        FROM run
    """, since=since, until=until)
    numbers = {k: int(v or 0) for k, v in df.iloc[0].to_dict().items()}
    numbers["valid"] = max(numbers["records_received"] - numbers["quarantined"], 0)
    numbers["already_present"] = max(numbers["valid"] - numbers["new_events"], 0)
    return numbers


def staging_event_ids(engine):
    df = query(engine, "SELECT entity_id AS symbol, event_id FROM staging.market_events")
    return df


# ------------------------------------------------------------------- lake
def lake_root():
    return Path(os.getenv("LAKE_ROOT", "data/lake"))


def _data_files(base, suffix):
    if not base.is_dir():
        return []
    return [
        p for p in base.rglob(f"*{suffix}")
        if p.is_file() and not any(part.startswith(("_", ".")) for part in p.relative_to(base).parts)
    ]


def lake_layers(root=None):
    """Files, bytes, rows and partitions per lake layer (Parquet footers only, no data read)."""
    import pyarrow.parquet as pq

    root = Path(root) if root else lake_root()
    layers = {}
    specs = {
        "bronze": (root / "bronze", ".json"),
        "silver": (root / "silver" / "market_prices", ".parquet"),
        "gold": (root / "gold", ".parquet"),
    }
    for name, (base, suffix) in specs.items():
        files = _data_files(base, suffix)
        rows = None
        if suffix == ".parquet":
            rows = sum(pq.ParquetFile(f).metadata.num_rows for f in files)
        partitions = len({f.parent for f in files})
        layers[name] = {
            "files": len(files),
            "bytes": sum(f.stat().st_size for f in files),
            "rows": rows,
            "partitions": partitions,
            "updated": max((f.stat().st_mtime for f in files), default=None),
        }
    return layers


def read_gold(table, root=None):
    root = Path(root) if root else lake_root()
    path = root / "gold" / table
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def silver_event_ids(root=None):
    root = Path(root) if root else lake_root()
    path = root / "silver" / "market_prices"
    if not path.exists():
        return pd.DataFrame(columns=["symbol", "event_id"])
    df = pd.read_parquet(path, columns=["event_id", "symbol"])
    df["symbol"] = df["symbol"].astype(str)
    return df[["symbol", "event_id"]]


# ------------------------------------------------------------ comparisons
def compare_event_ids(warehouse_ids, lake_ids):
    """Per-symbol overlap of event ids between PostgreSQL staging and silver."""
    w = warehouse_ids.groupby("symbol")["event_id"].apply(set)
    lake = lake_ids.groupby("symbol")["event_id"].apply(set)
    rows = []
    for symbol in sorted(set(w.index) | set(lake.index)):
        a, b = w.get(symbol, set()), lake.get(symbol, set())
        rows.append({
            "symbol": symbol,
            "warehouse": len(a),
            "lake": len(b),
            "matching": len(a & b),
            "only_in_warehouse": len(a - b),
            "only_in_lake": len(b - a),
        })
    return pd.DataFrame(rows, columns=["symbol", "warehouse", "lake", "matching",
                                       "only_in_warehouse", "only_in_lake"])


def compare_returns(summary, gold_daily):
    """Period return per symbol: SQL analytics vs Spark gold."""
    if summary.empty or gold_daily.empty:
        return pd.DataFrame(columns=["symbol", "warehouse_return", "lake_return", "difference"])
    gold = gold_daily.copy()
    gold["symbol"] = gold["symbol"].astype(str)
    gold = gold.sort_values(["symbol", "trade_date"])
    lake = gold.groupby("symbol")["close_price"].agg(["first", "last"])
    lake_ret = (lake["last"] / lake["first"] - 1).rename("lake_return")
    wh = summary.set_index("symbol")["period_return"].astype(float).rename("warehouse_return")
    out = pd.concat([wh, lake_ret], axis=1).reset_index(names="symbol")
    out["difference"] = (out["warehouse_return"] - out["lake_return"]).abs()
    return out


# -------------------------------------------------------------- streaming
def stream_available(engine):
    """True once migration 003 has created the streaming tables."""
    df = query(engine, "SELECT to_regclass('streaming.price_ticks') IS NOT NULL AS ok")
    return bool(df.iloc[0]["ok"])


def stream_overview(engine):
    df = query(engine, """
        WITH recent AS (
            SELECT EXTRACT(EPOCH FROM (processed_at - produced_at)) AS latency_s
            FROM streaming.price_ticks
            WHERE produced_at IS NOT NULL
            ORDER BY processed_at DESC
            LIMIT 1000
        )
        SELECT
            (SELECT COUNT(*) FROM streaming.price_ticks)                         AS ticks,
            (SELECT COUNT(*) FROM streaming.price_ticks
              WHERE processed_at > now() - interval '5 minutes')                 AS ticks_5m,
            (SELECT COUNT(DISTINCT symbol) FROM streaming.price_ticks)           AS symbols,
            -- Last batch with data, including batches of only duplicates
            -- (those add no tick rows but show the stream is alive).
            (SELECT MAX(processed_at) FROM streaming.batch_log
              WHERE query_name = 'ticks' AND rows_in > 0)                        AS last_processed,
            (SELECT COUNT(*) FROM streaming.rejected_ticks)                      AS rejected,
            (SELECT COALESCE(SUM(duplicates), 0) FROM streaming.batch_log
              WHERE query_name = 'ticks')                                        AS duplicates,
            (SELECT COUNT(*) FROM streaming.batch_log WHERE query_name = 'ticks') AS batches,
            (SELECT COUNT(*) FROM streaming.price_bars_5m)                       AS bars,
            (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY latency_s) FROM recent)  AS latency_p50,
            (SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_s) FROM recent) AS latency_p95
    """)
    return df.iloc[0].to_dict()


def stream_bars(engine, symbol, limit=200):
    return query(engine, """
        SELECT window_start, open, high, low, close, ticks, volume
        FROM (SELECT * FROM streaming.price_bars_5m WHERE symbol = :symbol
              ORDER BY window_start DESC LIMIT :limit) b
        ORDER BY window_start
    """, symbol=symbol, limit=limit)


def stream_symbols(engine):
    return list(query(engine, "SELECT DISTINCT symbol FROM streaming.price_bars_5m ORDER BY 1")["symbol"])


def stream_throughput(engine, minutes=60):
    return query(engine, """
        SELECT date_trunc('minute', processed_at) AS minute,
               SUM(inserted)   AS new_ticks,
               SUM(duplicates) AS duplicates,
               SUM(rejected)   AS rejected
        FROM streaming.batch_log
        WHERE query_name = 'ticks' AND processed_at > now() - make_interval(mins => :minutes)
        GROUP BY 1 ORDER BY 1
    """, minutes=minutes)


def stream_latest_ticks(engine, limit=12):
    return query(engine, """
        SELECT symbol, event_ts, price, source, kafka_partition, kafka_offset,
               ROUND(EXTRACT(EPOCH FROM (processed_at - produced_at))::numeric, 2) AS latency_s
        FROM streaming.price_ticks
        ORDER BY processed_at DESC, event_ts DESC
        LIMIT :limit
    """, limit=limit)


def stream_rejects(engine, limit=20):
    return query(engine, """
        SELECT rejected_at, reason, kafka_partition, kafka_offset, LEFT(raw_message, 120) AS message
        FROM streaming.rejected_ticks
        ORDER BY rejected_at DESC
        LIMIT :limit
    """, limit=limit)
