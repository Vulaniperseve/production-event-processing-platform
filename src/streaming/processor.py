"""
Spark Structured Streaming job: Kafka -> validated ticks + 5-minute bars in
PostgreSQL.

    Kafka market.price_ticks
        └─ parse + validate (same rules as the batch validator)
             ├─ ticks query: valid ticks -> streaming.price_ticks (dedup by event_id)
             │                invalid    -> streaming.rejected_ticks (with reason)
             └─ bars query:  event-time 5-minute OHLC windows, 10-minute
                             watermark -> streaming.price_bars_5m (upsert)

Fault tolerance: Kafka offsets are tracked in the checkpoint directory, so
after a restart the job resumes exactly where it stopped, and replays the
last unfinished micro-batch. The sinks are idempotent, so that replay never
duplicates data.

    python -m src.streaming.processor
"""

import logging
import os
import time

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, StructField, StructType,
)

log = logging.getLogger("stream-processor")

TOPIC = os.getenv("STREAM_TOPIC", "market.price_ticks")
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.4"

TICK_SCHEMA = StructType([
    StructField("schema_version", IntegerType()),
    StructField("event_id", StringType()),
    StructField("symbol", StringType()),
    StructField("event_ts", StringType()),
    StructField("price", DoubleType()),
    StructField("volume", LongType()),
    StructField("source", StringType()),
    StructField("produced_at", StringType()),
])

BAR_WINDOW = os.getenv("STREAM_BAR_WINDOW", "5 minutes")
WATERMARK = os.getenv("STREAM_WATERMARK", "10 minutes")


# ------------------------------------------------------------ transforms
def parse(source):
    """
    Kafka rows (value, partition, offset) -> one row per message with typed
    fields and a rejection `reason` (null when valid). Works on batch and
    streaming DataFrames alike, so it is unit-tested without Kafka.
    """
    raw = F.col("value").cast("string")
    t = F.from_json(raw, TICK_SCHEMA)
    parsed = source.select(
        raw.alias("raw_message"),
        t.alias("t"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
    )
    event_ts = F.try_to_timestamp(F.col("t.event_ts"))
    price = F.col("t.price")
    reason = (
        F.when(F.col("t").isNull() | F.col("t.schema_version").isNull(), F.lit("Malformed JSON"))
        .when(F.col("t.symbol").isNull() | (F.trim(F.col("t.symbol")) == ""), F.lit("Missing symbol"))
        .when(event_ts.isNull(), F.lit("Invalid timestamp"))
        .when(price.isNull() | F.isnan(price) | (price <= 0)
              | (F.abs(price) == F.lit(float("inf"))), F.lit("Non-positive or non-finite price"))
    )
    return parsed.select(
        # Recomputed from the message itself, the same way the producer
        # does: a wrong or missing event_id can't break deduplication.
        F.substring(F.sha2(F.concat_ws("|", F.col("t.symbol"), F.col("t.event_ts"),
                                       F.lit("PRICE_TICK")), 256), 1, 36).alias("event_id"),
        F.upper(F.trim(F.col("t.symbol"))).alias("symbol"),
        event_ts.alias("event_ts"),
        price.alias("price"),
        F.col("t.volume").alias("volume"),
        F.coalesce(F.col("t.source"), F.lit("unknown")).alias("source"),
        F.try_to_timestamp(F.col("t.produced_at")).alias("produced_at"),
        "kafka_partition", "kafka_offset", "raw_message",
        reason.alias("reason"),
    )


def valid_ticks(parsed):
    return parsed.where(F.col("reason").isNull())


def price_bars(ticks, window=BAR_WINDOW, watermark=WATERMARK):
    """
    Event-time OHLC bars. The watermark lets Spark drop the state of windows
    older than (latest event time - watermark): memory stays bounded, and
    ticks that arrive later than that are ignored.
    """
    ticks = ticks.withWatermark("event_ts", watermark)
    # A re-sent tick must not be counted twice in a bar. On a stream,
    # deduplicating "within the watermark" keeps only recent event ids in
    # state, so memory stays bounded on an endless stream.
    ticks = (ticks.dropDuplicatesWithinWatermark(["event_id"]) if ticks.isStreaming
             else ticks.dropDuplicates(["event_id"]))
    return (
        ticks
        .groupBy(F.window("event_ts", window).alias("w"), "symbol")
        .agg(
            F.min_by("price", "event_ts").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.max_by("price", "event_ts").alias("close"),
            F.count("*").alias("ticks"),
            F.sum("volume").alias("volume"),
        )
        .select("symbol", F.col("w.start").alias("window_start"),
                F.col("w.end").alias("window_end"),
                "open", "high", "low", "close", "ticks", "volume")
    )


# --------------------------------------------------------- batch writers
def _utc_strings(df, *columns):
    """
    Collect timestamps as ISO strings with an explicit UTC offset. Python
    datetimes from collect() are naive and in the *Python process's* local
    time zone, which would silently shift times on a non-UTC machine.
    """
    for c in columns:
        df = df.withColumn(c, F.date_format(F.col(c), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"))
    return df


def tick_writer(connect, sinks=None):
    """foreachBatch function for the ticks query."""
    if sinks is None:
        from src.streaming import sinks

    def write(batch_df, batch_id):
        started = time.monotonic()
        rows = [r.asDict() for r in _utc_strings(batch_df, "event_ts", "produced_at").collect()]
        good = [r for r in rows if r["reason"] is None]
        bad = [r for r in rows if r["reason"] is not None]
        conn = connect()
        try:
            with conn, conn.cursor() as cur:          # one transaction per micro-batch
                inserted = sinks.write_ticks(cur, good)
                rejected = sinks.write_rejects(cur, bad)
                sinks.log_batch(cur, "ticks", batch_id, len(rows), inserted, len(good) - inserted,
                                rejected, int((time.monotonic() - started) * 1000))
        finally:
            conn.close()
        if rows:
            log.info("ticks batch %s: %d in, %d new, %d duplicate, %d rejected",
                     batch_id, len(rows), inserted, len(good) - inserted, len(bad))

    return write


def bar_writer(connect, sinks=None):
    """foreachBatch function for the 5-minute bars query."""
    if sinks is None:
        from src.streaming import sinks

    def write(batch_df, batch_id):
        started = time.monotonic()
        bars = [r.asDict() for r in _utc_strings(batch_df, "window_start", "window_end").collect()]
        conn = connect()
        try:
            with conn, conn.cursor() as cur:
                written = sinks.upsert_bars(cur, bars)
                sinks.log_batch(cur, "bars_5m", batch_id, len(bars), written, 0, 0,
                                int((time.monotonic() - started) * 1000))
        finally:
            conn.close()

    return write


# --------------------------------------------------------------- queries
def start_queries(source, connect, checkpoint_root, trigger=None, sinks=None):
    """Start both streaming queries on one parsed source."""
    trigger = trigger or {"processingTime": "5 seconds"}
    parsed = parse(source)
    ticks_query = (
        parsed.writeStream.queryName("ticks")
        .foreachBatch(tick_writer(connect, sinks))
        .option("checkpointLocation", f"{checkpoint_root}/ticks")
        .trigger(**trigger)
        .start()
    )
    bars_query = (
        price_bars(valid_ticks(parsed)).writeStream.queryName("bars_5m")
        .outputMode("update")
        .foreachBatch(bar_writer(connect, sinks))
        .option("checkpointLocation", f"{checkpoint_root}/bars_5m")
        .trigger(**trigger)
        .start()
    )
    return ticks_query, bars_query


def kafka_source(spark, bootstrap, topic=TOPIC):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        # A pattern instead of a fixed topic: if the processor starts before
        # the producer has created the topic, Spark waits for it instead of
        # failing, and picks it up within ~10 s of it appearing.
        .option("subscribePattern", topic.replace(".", r"\."))
        .option("kafka.metadata.max.age.ms", "10000")
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", 5000)       # back-pressure: bounded batches
        # The broker keeps 24 h of data: if it expired before being read,
        # continue with what's left instead of stopping the stream.
        .option("failOnDataLoss", "false")
        .load()
    )


def main():
    from src.lake.config import lake_root
    from src.lake.spark import build_spark
    from src.streaming.sinks import connect

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    # Make sure the streaming tables exist (versioned, idempotent migrations).
    from database.database import engine
    from database.migrate import apply_migrations
    apply_migrations(engine, log=log.info)

    spark = build_spark("price-stream", extra_conf={
        "spark.jars.packages": KAFKA_PACKAGE,
        "spark.jars.ivy": os.getenv("SPARK_IVY_DIR", "/opt/airflow/ivy"),
        "spark.sql.shuffle.partitions": "2",
        # State store for the window aggregation, sized for a small machine.
        "spark.sql.streaming.stateStore.providerClass":
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
    })
    spark.sparkContext.setLogLevel("WARN")
    checkpoints = os.getenv("STREAM_CHECKPOINT_DIR", str(lake_root() / "_checkpoints" / "price_stream"))
    queries = start_queries(kafka_source(spark, os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")),
                            connect, checkpoints)
    log.info("streaming: %s (checkpoints in %s)", ", ".join(q.name for q in queries), checkpoints)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
