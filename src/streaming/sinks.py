"""
PostgreSQL sinks for the streaming job.

Every function is idempotent, so a micro-batch Spark replays after a crash
can't create duplicates (exactly-once results on top of at-least-once
delivery). Each micro-batch is written in one transaction together with its
batch_log row: either all of it is stored, or none of it.
"""

import os

from psycopg2.extras import execute_values


def connect():
    import psycopg2

    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        connect_timeout=10,
        application_name="stream-processor",
    )


def write_ticks(cur, ticks):
    """Insert valid ticks; returns how many were new (the rest were duplicates)."""
    if not ticks:
        return 0
    rows = execute_values(cur, """
        INSERT INTO streaming.price_ticks
            (event_id, symbol, event_ts, price, volume, source, produced_at,
             kafka_partition, kafka_offset)
        VALUES %s
        ON CONFLICT (event_id) DO NOTHING
        RETURNING 1
    """, [(t["event_id"], t["symbol"], t["event_ts"], t["price"], t["volume"], t["source"],
           t["produced_at"], t["kafka_partition"], t["kafka_offset"]) for t in ticks],
        fetch=True)
    return len(rows)


def write_rejects(cur, rejects):
    if not rejects:
        return 0
    rows = execute_values(cur, """
        INSERT INTO streaming.rejected_ticks (kafka_partition, kafka_offset, raw_message, reason)
        VALUES %s
        ON CONFLICT (kafka_partition, kafka_offset) DO NOTHING
        RETURNING 1
    """, [(r["kafka_partition"], r["kafka_offset"], r["raw_message"], r["reason"]) for r in rejects],
        fetch=True)
    return len(rows)


def upsert_bars(cur, bars):
    """
    Spark's update output mode re-emits the full, current aggregate of any
    window that changed, so overwriting the stored bar is correct.
    """
    if not bars:
        return 0
    execute_values(cur, """
        INSERT INTO streaming.price_bars_5m
            (symbol, window_start, window_end, open, high, low, close, ticks, volume)
        VALUES %s
        ON CONFLICT (symbol, window_start) DO UPDATE SET
            window_end = EXCLUDED.window_end, open = EXCLUDED.open, high = EXCLUDED.high,
            low = EXCLUDED.low, close = EXCLUDED.close, ticks = EXCLUDED.ticks,
            volume = EXCLUDED.volume, updated_at = now()
    """, [(b["symbol"], b["window_start"], b["window_end"], b["open"], b["high"], b["low"],
           b["close"], b["ticks"], b["volume"]) for b in bars])
    return len(bars)


def log_batch(cur, query_name, batch_id, rows_in, inserted, duplicates, rejected, duration_ms):
    cur.execute("""
        INSERT INTO streaming.batch_log
            (query_name, batch_id, rows_in, inserted, duplicates, rejected, duration_ms)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (query_name, batch_id) DO NOTHING
    """, (query_name, batch_id, rows_in, inserted, duplicates, rejected, duration_ms))
