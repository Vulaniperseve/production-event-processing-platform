"""
Spark Structured Streaming tests, without Kafka: the same parse/aggregate
code runs on a batch DataFrame, and the full streaming queries run on a
file source with trigger(availableNow=True).
"""

import json
import math
from datetime import UTC, datetime

from src.streaming.messages import make_tick, tick_problem
from src.streaming.processor import parse, price_bars, start_queries, valid_ticks

SOURCE_SCHEMA = "value STRING, partition INT, offset LONG"


def tick(symbol="AAPL", minute=30, price=100.0, second=0, **overrides):
    t = make_tick(symbol, datetime(2026, 11, 17, 14, minute, second, tzinfo=UTC), price, 10, "replay",
                  produced_at=datetime(2026, 11, 18, 9, 0, tzinfo=UTC))
    return {**t, **overrides}


def kafka_rows(messages):
    """Kafka-like rows: value as text, partition, offset."""
    return [(m if isinstance(m, str) else json.dumps(m), 0, i) for i, m in enumerate(messages)]


def frame(spark, messages):
    return spark.createDataFrame(kafka_rows(messages), SOURCE_SCHEMA)


CASES = [
    tick(),
    tick(symbol=""),
    tick(event_ts="yesterday"),
    tick(price=0),
    tick(price=-3.0),
    tick(price=math.inf),
    tick(price=math.nan),
    tick(price="231.5"),
    "not json at all",
    '"just a string"',
]


def python_reason(message):
    if isinstance(message, str):
        try:
            message = json.loads(message)
        except ValueError:
            return "Malformed JSON"
    return tick_problem(message)


def test_parse_applies_the_same_rules_as_the_python_validator(spark):
    rows = parse(frame(spark, CASES)).orderBy("kafka_offset").collect()
    assert [r["reason"] for r in rows] == [python_reason(m) for m in CASES]


def test_parse_recomputes_the_producer_event_id(spark):
    good = tick()
    wrong_id = tick(minute=31, event_id="tampered")
    rows = parse(frame(spark, [good, wrong_id])).orderBy("kafka_offset").collect()

    assert rows[0]["event_id"] == good["event_id"]
    assert rows[1]["event_id"] == tick(minute=31)["event_id"]      # not "tampered"
    assert rows[0]["symbol"] == "AAPL" and rows[0]["price"] == 100.0
    assert rows[0]["event_ts"] == datetime(2026, 11, 17, 14, 30)    # UTC session time zone
    assert (rows[0]["kafka_partition"], rows[0]["kafka_offset"]) == (0, 0)


def test_price_bars_build_event_time_ohlc(spark):
    # Sent out of order: open/close must follow event time, not arrival.
    messages = [tick(minute=33, price=104), tick(minute=30, price=100), tick(minute=31, price=107),
                tick(minute=32, price=95), tick(minute=36, price=110), tick("MSFT", 30, 500)]
    bars = {(r["symbol"], r["window_start"].minute): r.asDict()
            for r in price_bars(valid_ticks(parse(frame(spark, messages)))).collect()}

    aapl = bars[("AAPL", 30)]
    assert (aapl["open"], aapl["high"], aapl["low"], aapl["close"]) == (100, 107, 95, 104)
    assert aapl["ticks"] == 4 and aapl["volume"] == 40
    assert bars[("AAPL", 35)]["ticks"] == 1
    assert bars[("MSFT", 30)]["close"] == 500


class FakeConn:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self):
        return self

    def close(self):
        pass


class RecordingSinks:
    """Stands in for src.streaming.sinks and dedupes like the real tables."""

    def __init__(self):
        self.ticks, self.rejects, self.bars, self.log = {}, {}, {}, []

    def write_ticks(self, cur, ticks):
        new = [t for t in ticks if t["event_id"] not in self.ticks]
        self.ticks.update({t["event_id"]: t for t in new})
        return len(new)

    def write_rejects(self, cur, rejects):
        new = [r for r in rejects if (r["kafka_partition"], r["kafka_offset"]) not in self.rejects]
        self.rejects.update({(r["kafka_partition"], r["kafka_offset"]): r for r in new})
        return len(new)

    def upsert_bars(self, cur, bars):
        self.bars.update({(b["symbol"], b["window_start"]): b for b in bars})
        return len(bars)

    def log_batch(self, cur, query, batch_id, rows_in, inserted, duplicates, rejected, ms):
        self.log.append((query, batch_id, rows_in, inserted, duplicates, rejected))


def run_stream(spark, source_dir, checkpoints, sinks):
    source = spark.readStream.schema(SOURCE_SCHEMA).json(str(source_dir))
    queries = start_queries(source, FakeConn, str(checkpoints),
                            trigger={"availableNow": True}, sinks=sinks)
    for q in queries:
        q.awaitTermination(120)
        assert q.exception() is None


def write_messages(directory, name, messages, first_offset=0):
    directory.mkdir(parents=True, exist_ok=True)
    with open(directory / name, "w") as f:
        for value, partition, offset in kafka_rows(messages):
            f.write(json.dumps({"value": value, "partition": partition,
                                "offset": offset + first_offset}) + "\n")


def test_streaming_queries_end_to_end_with_checkpoint_resume(spark, tmp_path):
    source, checkpoints, sinks = tmp_path / "source", tmp_path / "checkpoints", RecordingSinks()

    write_messages(source, "a.json", [tick(minute=30, price=100), tick(minute=31, price=102),
                                      tick(price=-1, minute=32), "garbage"])
    run_stream(spark, source, checkpoints, sinks)

    assert len(sinks.ticks) == 2
    assert {r["reason"] for r in sinks.rejects.values()} == {"Non-positive or non-finite price",
                                                             "Malformed JSON"}
    bar = sinks.bars[("AAPL", "2026-11-17T14:30:00.000Z")]
    assert (bar["open"], bar["close"], bar["ticks"]) == (100, 102, 2)
    assert next(t for t in sinks.ticks.values())["event_ts"].endswith("Z")    # explicit UTC

    # Restart: the checkpoint makes the job read only the new file. One
    # message is a re-sent duplicate and is dropped by the event_id key.
    write_messages(source, "b.json", [tick(minute=31, price=102), tick(minute=33, price=99)],
                   first_offset=4)
    run_stream(spark, source, checkpoints, sinks)

    second_ticks_batch = [entry for entry in sinks.log if entry[0] == "ticks"][-1]
    assert second_ticks_batch[2:] == (2, 1, 1, 0)       # rows in, new, duplicate, rejected
    assert len(sinks.ticks) == 3
    bar = sinks.bars[("AAPL", "2026-11-17T14:30:00.000Z")]
    assert (bar["low"], bar["close"], bar["ticks"]) == (99, 99, 3)    # window updated in place
