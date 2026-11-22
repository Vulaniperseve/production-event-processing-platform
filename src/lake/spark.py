"""SparkSession configuration for the data lake jobs."""

import os
from contextlib import contextmanager

# "Small data mode" (on by default, SPARK_SMALL_DATA_MODE=false turns it off).
# This project's batches are a few thousand rows, so a job's time is mostly the
# JVM starting and warming up, not data processing. Benchmarked on this laptop
# (benchmarks/bench_lake.py, medians of 3 alternating runs): silver 27.5 s ->
# 17.3 s, gold 12.9 s -> 7.7 s, a warm silver rebuild 14.9 s -> 5.2 s.
#   -XX:TieredStopAtLevel=1   only the quick JIT compiler (C1): code gets fast
#                             sooner; the slower C2 optimiser only pays off in
#                             jobs that run for minutes
#   -XX:+UseSerialGC          simplest garbage collector, fine for a 1 GB heap
#   codegen.wholeStage=false  skip generating and compiling Java per query plan,
#                             which costs more than it saves on tiny data
# On a cluster with real data volumes, turn it off: these trade peak
# throughput for fast startup.
SMALL_DATA_CONF = {
    "spark.driver.extraJavaOptions": "-XX:TieredStopAtLevel=1 -XX:+UseSerialGC",
    "spark.sql.codegen.wholeStage": "false",
}


def small_data_mode():
    return os.getenv("SPARK_SMALL_DATA_MODE", "true").lower() not in ("false", "0", "no")


def build_spark(app_name="market-data-lake", extra_conf=None):
    from pyspark.sql import SparkSession

    conf = {**(SMALL_DATA_CONF if small_data_mode() else {}), **(extra_conf or {})}
    builder = SparkSession.builder
    for key, value in conf.items():
        builder = builder.config(key, value)
    return (
        builder
        .appName(app_name)
        # Local mode: driver and executor in one JVM. On a cluster (EMR,
        # Databricks) only SPARK_MASTER changes, not the job code.
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1g"))
        # The default of 200 shuffle partitions is for clusters; with a few
        # thousand rows it just creates 200 tiny tasks and files.
        .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        # Overwrite only the partitions a job writes (incremental upserts).
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # Keep partition values (e.g. trade_month=2026-11) as strings.
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


@contextmanager
def spark_session(app_name="market-data-lake"):
    spark = build_spark(app_name)
    try:
        yield spark
    finally:
        spark.stop()
