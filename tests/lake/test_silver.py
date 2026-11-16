"""Silver layer: validation, lineage, deduplication and incremental upserts."""

from pyspark.sql import functions as F

from src.lake.config import silver_path, silver_rejected_path
from src.lake.silver import build_silver
from src.transformers.transformer import DataTransformer
from src.validators.validator import DataValidator
from tests.lake.conftest import land

MIXED = [
    ("2026-11-12", "228.40"),
    ("2026-11-13", "230.10"),
    ("2026-11-14 15:30:00", "231.00"),   # intraday timestamp format
    (None, "231.20"),                    # missing datetime
    ("", ""),                            # both missing
    ("not-a-date", "230.00"),            # invalid datetime
    ("2026-11-10", "ERROR"),             # invalid price
    ("2026-11-09", "0"),                 # non-positive
    ("2026-11-08", "-5.2"),              # negative
    ("2026-11-07", "nan"),               # not finite
    ("2026-11-06", "inf"),               # not finite
]


def silver_rows(spark, lake):
    return {
        r["trade_ts"].strftime("%Y-%m-%d"): r
        for r in spark.read.parquet(str(silver_path(lake))).collect()
    }


def test_valid_rows_reach_silver_and_bad_rows_are_rejected(spark, lake):
    land(lake, 1, "2026-11-15T06:00:00", MIXED)
    land(lake, 2, "2026-11-15T06:00:05", [], status="error")   # API error response

    m = build_silver(spark, root=lake)

    assert m["bronze_responses"] == 2
    assert (m["records"], m["valid"], m["rejected"]) == (11, 3, 8)

    reasons = sorted(r.reason for r in spark.read.parquet(str(silver_rejected_path(lake))).collect())
    assert reasons == sorted([
        "Missing fields: ['datetime']",
        "Missing fields: ['datetime', 'close']",
        "Invalid datetime",
        "Invalid close price",
        "Non-positive or non-finite close price",
        "Non-positive or non-finite close price",
        "Non-positive or non-finite close price",
        "Non-positive or non-finite close price",
    ])


def test_silver_and_postgres_pipeline_accept_the_same_records(spark, lake):
    """The Spark rules must match validator.py, or the two systems diverge."""
    land(lake, 1, "2026-11-15T06:00:00", MIXED)
    build_silver(spark, root=lake)

    payload = {"meta": {"symbol": "AAPL"}, "status": "ok",
               "values": [{"datetime": d, "close": c} for d, c in MIXED]}
    python_valid, _ = DataValidator.validate_records(payload)

    spark_valid = {r.trade_ts.strftime("%Y-%m-%d") for r in spark.read.parquet(str(silver_path(lake))).collect()}

    assert spark_valid == {r["datetime"][:10] for r in python_valid}


def test_event_ids_match_the_postgres_pipeline(spark, lake):
    prices = [("2026-11-12", "228.40"), ("2026-11-13", "230.10")]
    land(lake, 1, "2026-11-15T06:00:00", prices)
    build_silver(spark, root=lake)

    expected = DataTransformer.transform({
        "meta": {"symbol": "AAPL"}, "status": "ok",
        "values": [{"datetime": d, "close": c} for d, c in prices],
    })

    spark_ids = {r.event_id for r in spark.read.parquet(str(silver_path(lake))).collect()}
    assert spark_ids == set(expected["event_id"])


def test_partition_layout_and_lineage(spark, lake):
    land(lake, 7, "2026-11-15T06:00:00", [("2026-10-31", "220"), ("2026-11-03", "225")])
    build_silver(spark, root=lake)

    dirs = sorted(
        p.relative_to(silver_path(lake)).as_posix()
        for p in silver_path(lake).glob("symbol=*/trade_month=*")
    )
    assert dirs == ["symbol=AAPL/trade_month=2026-10", "symbol=AAPL/trade_month=2026-11"]

    rows = spark.read.parquet(str(silver_path(lake))).collect()
    assert {r.raw_event_id for r in rows} == {7}
    assert {r.ingest_date for r in rows} == {"2026-11-15"}


def test_overlapping_extractions_latest_wins(spark, lake):
    land(lake, 1, "2026-11-14T06:00:00", [("2026-11-12", "100"), ("2026-11-13", "101")])
    build_silver(spark, ingest_dates=["2026-11-14"], root=lake)

    # next day: 11-13 is restated by the API, 11-14 is new
    land(lake, 2, "2026-11-15T06:00:00", [("2026-11-13", "105"), ("2026-11-14", "102")])
    m = build_silver(spark, ingest_dates=["2026-11-15"], root=lake)

    rows = silver_rows(spark, lake)
    assert sorted(rows) == ["2026-11-12", "2026-11-13", "2026-11-14"]
    assert (rows["2026-11-13"].close_price, rows["2026-11-13"].raw_event_id) == (105.0, 2)
    assert rows["2026-11-12"].raw_event_id == 1   # untouched row kept its lineage
    assert m["silver_rows_written"] == 3


def test_rerun_is_idempotent(spark, lake):
    land(lake, 1, "2026-11-15T06:00:00", [("2026-11-12", "100"), ("2026-11-13", "101")])

    build_silver(spark, root=lake)
    first = sorted(tuple(r) for r in spark.read.parquet(str(silver_path(lake))).collect())

    build_silver(spark, root=lake)
    build_silver(spark, root=lake)
    again = sorted(tuple(r) for r in spark.read.parquet(str(silver_path(lake))).collect())

    assert first == again


def test_incremental_run_only_rewrites_affected_partitions(spark, lake):
    land(lake, 1, "2026-11-14T06:00:00", [("2026-10-30", "90"), ("2026-11-12", "100")])
    build_silver(spark, ingest_dates=["2026-11-14"], root=lake)

    october = silver_path(lake) / "symbol=AAPL" / "trade_month=2026-10"
    november = silver_path(lake) / "symbol=AAPL" / "trade_month=2026-11"
    files_before = {"oct": sorted(p.name for p in october.glob("*.parquet")),
                    "nov": sorted(p.name for p in november.glob("*.parquet"))}

    land(lake, 2, "2026-11-15T06:00:00", [("2026-11-13", "101")])   # November only
    m = build_silver(spark, ingest_dates=["2026-11-15"], root=lake)

    assert m["partitions_written"] == 1
    assert sorted(p.name for p in october.glob("*.parquet")) == files_before["oct"]  # untouched
    assert sorted(p.name for p in november.glob("*.parquet")) != files_before["nov"]  # rewritten
    assert len(silver_rows(spark, lake)) == 3


def test_full_refresh_equals_incremental(spark, lake, tmp_path):
    batches = [
        (1, "2026-11-14T06:00:00", [("2026-10-30", "90"), ("2026-11-12", "100")]),
        (2, "2026-11-15T06:00:00", [("2026-11-12", "99"), ("2026-11-13", "101")]),
    ]
    incremental, full = lake, tmp_path / "full"

    for raw_id, ts, prices in batches:
        land(incremental, raw_id, ts, prices)
        build_silver(spark, ingest_dates=[ts[:10]], root=incremental)
        land(full, raw_id, ts, prices)
    build_silver(spark, root=full)

    def snapshot(root):
        df = spark.read.parquet(str(silver_path(root)))
        return sorted(tuple(r) for r in df.select("event_id", "close_price", "raw_event_id").collect())

    assert snapshot(incremental) == snapshot(full)


def test_empty_lake_is_a_no_op(spark, lake):
    assert build_silver(spark, root=lake)["valid"] == 0
    assert build_silver(spark, ingest_dates=["2026-01-01"], root=lake)["valid"] == 0
    assert not silver_path(lake).exists()


def test_silver_schema(spark, lake):
    land(lake, 1, "2026-11-15T06:00:00", [("2026-11-12", "100")])
    build_silver(spark, root=lake)

    types = dict(spark.read.parquet(str(silver_path(lake))).dtypes)
    assert types["close_price"] == "double"
    assert types["trade_ts"] == "timestamp"
    assert types["trade_date"] == "date"
    assert types["raw_event_id"] == "bigint"
    assert spark.read.parquet(str(silver_path(lake))).where(F.col("event_id").isNull()).count() == 0
