"""The streaming sinks against a real PostgreSQL: idempotent under replays."""

from src.streaming import sinks
from tests.integration.helpers import scalar


def tick(event_id, price=100.0, ts="2026-11-17T14:30:00.000Z", offset=0):
    return {"event_id": event_id, "symbol": "AAPL", "event_ts": ts, "price": price, "volume": 10,
            "source": "replay", "produced_at": "2026-11-18T09:00:00.000Z",
            "kafka_partition": 0, "kafka_offset": offset}


def bar(close, ticks):
    return {"symbol": "AAPL", "window_start": "2026-11-17T14:30:00.000Z",
            "window_end": "2026-11-17T14:35:00.000Z", "open": 100.0, "high": 110.0,
            "low": 90.0, "close": close, "ticks": ticks, "volume": 100}


def with_cursor(engine, fn):
    raw = engine.raw_connection()
    try:
        cur = raw.cursor()
        result = fn(cur)
        raw.commit()
        return result
    finally:
        raw.close()


def test_ticks_are_stored_once(engine, clean_db):
    first = with_cursor(engine, lambda c: sinks.write_ticks(c, [tick("a"), tick("b", offset=1)]))
    again = with_cursor(engine, lambda c: sinks.write_ticks(c, [tick("b", offset=1), tick("c", offset=2)]))

    assert (first, again) == (2, 1)
    assert scalar(engine, "SELECT COUNT(*) FROM streaming.price_ticks") == 3
    assert str(scalar(engine, "SELECT event_ts AT TIME ZONE 'UTC' FROM streaming.price_ticks "
                              "WHERE event_id = 'a'")) == "2026-11-17 14:30:00"


def test_rejects_are_keyed_by_kafka_position(engine, clean_db):
    reject = {"kafka_partition": 0, "kafka_offset": 7, "raw_message": "garbage", "reason": "Malformed JSON"}
    assert with_cursor(engine, lambda c: sinks.write_rejects(c, [reject])) == 1
    assert with_cursor(engine, lambda c: sinks.write_rejects(c, [reject])) == 0


def test_bars_are_overwritten_with_the_latest_aggregate(engine, clean_db):
    with_cursor(engine, lambda c: sinks.upsert_bars(c, [bar(close=101.0, ticks=2)]))
    with_cursor(engine, lambda c: sinks.upsert_bars(c, [bar(close=99.0, ticks=3)]))

    assert scalar(engine, "SELECT COUNT(*) FROM streaming.price_bars_5m") == 1
    assert scalar(engine, "SELECT close FROM streaming.price_bars_5m") == 99.0
    assert scalar(engine, "SELECT ticks FROM streaming.price_bars_5m") == 3


def test_batch_log_ignores_a_replayed_batch(engine, clean_db):
    for _ in range(2):
        with_cursor(engine, lambda c: sinks.log_batch(c, "ticks", 5, 10, 8, 2, 0, 42))
    assert scalar(engine, "SELECT COUNT(*) FROM streaming.batch_log") == 1
    assert scalar(engine, "SELECT duplicates FROM streaming.batch_log") == 2
