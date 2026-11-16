"""
Silver layer (Spark): bronze JSON -> validated, deduplicated Parquet.

    silver/market_prices/symbol=AAPL/trade_month=2026-11/part-*.parquet
    silver/market_prices_rejected/ingest_date=2026-11-15/part-*.parquet

* Explicit schema: bronze is read with a declared schema, never inferred,
  so a malformed file cannot silently change column types.
* Same data-quality rules as the PostgreSQL pipeline (validator.py), with
  rejected rows kept, with a reason, in a separate dataset.
* Same deterministic event_id as staging.market_events, so an event has
  the same identity in the warehouse and the lake.
* Incremental partition upsert: only (symbol, trade_month) partitions that
  received new data are read, merged and rewritten. Overlapping extractions
  are deduplicated, with the most recent extraction winning.
"""

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, LongType, StringType, StructField, StructType

from src.lake.config import bronze_path, silver_path, silver_rejected_path
from src.logger import logger

EVENT_TYPE = "MARKET_PRICE"

BRONZE_SCHEMA = StructType([
    StructField("raw_event_id", LongType()),
    StructField("source_system", StringType()),
    StructField("symbol", StringType()),
    StructField("extracted_at", StringType()),
    StructField("payload_hash", StringType()),
    StructField("payload", StructType([
        StructField("status", StringType()),
        StructField("meta", StructType([
            StructField("symbol", StringType()),
            StructField("interval", StringType()),
        ])),
        StructField("values", ArrayType(StructType([
            StructField("datetime", StringType()),
            StructField("close", StringType()),
        ]))),
    ])),
])

SILVER_COLUMNS = [
    "event_id", "symbol", "trade_ts", "trade_date", "close_price",
    "raw_event_id", "extracted_at", "ingest_date", "trade_month",
]


def read_bronze(spark, ingest_dates=None, root=None):
    """Read bronze JSON (all of it, or only the given ingest_date partitions)."""

    base = bronze_path(root)

    if ingest_dates is None:
        paths = [str(base)] if base.exists() else []
    else:
        paths = [
            str(base / f"ingest_date={d}")
            for d in ingest_dates
            if (base / f"ingest_date={d}").exists()
        ]

    if not paths:
        return None

    return (
        spark.read
        .schema(BRONZE_SCHEMA)
        .option("basePath", str(base))
        .json(paths)
    )


def explode_records(bronze_df):
    """One row per price record, from responses whose API status is 'ok'."""

    return (
        bronze_df
        .where(F.col("payload.status") == "ok")
        .select(
            "raw_event_id",
            F.col("extracted_at").cast("timestamp").alias("extracted_at"),
            F.col("ingest_date").cast("string").alias("ingest_date"),
            F.coalesce(F.col("payload.meta.symbol"), F.col("symbol")).alias("symbol"),
            F.explode("payload.values").alias("v"),
        )
        .select(
            "raw_event_id", "extracted_at", "ingest_date", "symbol",
            F.col("v.datetime").alias("datetime_raw"),
            F.col("v.close").alias("close_raw"),
        )
    )


def with_quality_checks(records):
    """Add parsed columns and a rejection `reason` (NULL means valid)."""

    dt_missing = F.col("datetime_raw").isNull() | (F.trim("datetime_raw") == "")
    close_missing = F.col("close_raw").isNull() | (F.trim("close_raw") == "")

    trade_ts = F.try_to_timestamp(F.col("datetime_raw"))
    close = F.col("close_raw").try_cast("double")

    reason = (
        F.when(dt_missing & close_missing, F.lit("Missing fields: ['datetime', 'close']"))
        .when(dt_missing, F.lit("Missing fields: ['datetime']"))
        .when(close_missing, F.lit("Missing fields: ['close']"))
        .when(trade_ts.isNull(), F.lit("Invalid datetime"))
        .when(close.isNull(), F.lit("Invalid close price"))
        .when(
            F.isnan(close) | (F.abs(close) == F.lit(float("inf"))) | (close <= 0),
            F.lit("Non-positive or non-finite close price"),
        )
    )

    return (
        records
        .withColumn("trade_ts", trade_ts)
        .withColumn("close_price", close)
        .withColumn("reason", reason)
    )


def to_silver_rows(valid):
    """Shape valid records into the silver schema."""

    return (
        valid
        .withColumn(
            "event_id",
            # Same formula as DataTransformer: sha256("SYMBOL|datetime|TYPE")[:36]
            F.substring(
                F.sha2(F.concat_ws("|", "symbol", "datetime_raw", F.lit(EVENT_TYPE)), 256),
                1, 36,
            ),
        )
        .withColumn("trade_date", F.to_date("trade_ts"))
        .withColumn("trade_month", F.date_format("trade_ts", "yyyy-MM"))
        .select(*SILVER_COLUMNS)
    )


def deduplicate(df):
    """Keep one row per event_id: the most recent extraction wins."""

    latest_first = Window.partitionBy("event_id").orderBy(
        F.col("extracted_at").desc(), F.col("raw_event_id").desc()
    )

    return (
        df.withColumn("_rn", F.row_number().over(latest_first))
        .where(F.col("_rn") == 1)
        .drop("_rn")
    )


def build_silver(spark, ingest_dates=None, root=None):
    """
    Process bronze into silver. `ingest_dates=None` reprocesses all of bronze.
    Returns a metrics dict.
    """

    metrics = {
        "bronze_responses": 0, "records": 0, "valid": 0,
        "rejected": 0, "partitions_written": 0, "silver_rows_written": 0,
    }

    bronze = read_bronze(spark, ingest_dates, root)
    if bronze is None:
        logger.info("Silver: no bronze data to process.")
        return metrics

    metrics["bronze_responses"] = bronze.count()

    checked = with_quality_checks(explode_records(bronze)).cache()
    metrics["records"] = checked.count()

    # --- rejected records --------------------------------------------------
    rejected = checked.where(F.col("reason").isNotNull()).select(
        "raw_event_id", "symbol", "datetime_raw", "close_raw", "reason", "ingest_date",
    )
    metrics["rejected"] = rejected.count()

    if metrics["rejected"]:
        (
            rejected.coalesce(1)
            .write.mode("overwrite")
            .partitionBy("ingest_date")
            .parquet(str(silver_rejected_path(root)))
        )

    # --- valid records: incremental partition upsert -------------------------
    new_rows = deduplicate(to_silver_rows(checked.where(F.col("reason").isNull())))
    metrics["valid"] = new_rows.count()

    if metrics["valid"]:
        target = silver_path(root)
        affected = new_rows.select("symbol", "trade_month").distinct()

        if target.exists():
            existing = spark.read.parquet(str(target)).join(
                F.broadcast(affected), ["symbol", "trade_month"], "inner"
            )
            merged = deduplicate(existing.select(*SILVER_COLUMNS).unionByName(new_rows))
        else:
            merged = new_rows

        # Materialize before overwriting: `merged` reads the very partitions
        # this write replaces.
        merged = merged.localCheckpoint()

        metrics["partitions_written"] = affected.count()
        metrics["silver_rows_written"] = merged.count()

        (
            merged
            .repartition("symbol", "trade_month")  # one file per partition
            .write.mode("overwrite")
            .partitionBy("symbol", "trade_month")
            .parquet(str(target))
        )

    checked.unpersist()

    logger.info(f"Silver build complete: {metrics}")
    return metrics
