"""SparkSession configuration for the data lake jobs."""

import os
from contextlib import contextmanager


def build_spark(app_name="market-data-lake", extra_conf=None):
    from pyspark.sql import SparkSession

    builder = SparkSession.builder
    for key, value in (extra_conf or {}).items():
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
