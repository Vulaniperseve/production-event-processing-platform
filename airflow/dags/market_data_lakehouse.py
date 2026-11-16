"""
Market data lakehouse: raw PostgreSQL responses -> Bronze -> Silver -> Gold.

    land_bronze ─> build_silver ─> build_gold ─> publish_to_s3

* Data-aware scheduling: runs whenever production_event_pipeline updates the
  `market_events_staging` Asset, instead of guessing a time with cron. The two
  DAGs stay decoupled: a Spark failure never blocks ingestion.
* land_bronze copies only raw responses not yet in the lake (plain Python).
* build_silver (Spark) processes only the newly landed ingest dates, and
  upserts just the affected Parquet partitions. Trigger with
  {"full_refresh": true} to rebuild silver from all of bronze.
* build_gold (Spark) rebuilds the gold datasets from silver.
* publish_to_s3 makes the S3 copy of the lake match the local one (only
  changed files are uploaded, replaced files are deleted). It runs even when
  silver/gold were skipped, so a publish that failed earlier catches up on
  the next run. It skips itself when LAKE_S3_BUCKET is not set.
* The Spark tasks run one at a time (max_active_runs=1, sequential tasks), so
  only one Spark JVM is ever alive on this machine.
"""

import os
from datetime import datetime, timedelta

from airflow.exceptions import AirflowSkipException
from airflow.sdk import Asset, Param, dag, task

STAGING_ASSET = Asset("market_events_staging")
GOLD_ASSET = Asset("market_data_lake_gold")


@dag(
    dag_id="market_data_lakehouse",
    description="Bronze -> Silver -> Gold data lake (Spark, Parquet)",
    start_date=datetime(2026, 1, 1),
    schedule=[STAGING_ASSET],
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(minutes=30),
    },
    params={"full_refresh": Param(False, type="boolean")},
    tags=["lakehouse", "spark", "parquet"],
)
def market_data_lakehouse():

    @task(task_id="land_bronze")
    def land_bronze():
        from src.lake.bronze import land_raw_to_bronze

        return land_raw_to_bronze()

    @task(task_id="build_silver")
    def build_silver(landed: dict, **context):
        from src.lake.silver import build_silver as silver_job
        from src.lake.spark import spark_session

        full_refresh = context["params"]["full_refresh"]

        if not full_refresh and not landed["ingest_dates"]:
            raise AirflowSkipException("No new bronze data; silver and gold are current.")

        with spark_session("silver") as spark:
            return silver_job(spark, None if full_refresh else landed["ingest_dates"])

    @task(task_id="build_gold", outlets=[GOLD_ASSET])
    def build_gold(silver_metrics: dict):
        from src.lake.gold import build_gold as gold_job
        from src.lake.spark import spark_session

        with spark_session("gold") as spark:
            return gold_job(spark)

    @task(task_id="publish_to_s3", trigger_rule="none_failed")
    def publish_to_s3():
        if not os.getenv("LAKE_S3_BUCKET"):
            raise AirflowSkipException("LAKE_S3_BUCKET is not set; the lake stays local only.")

        from src.lake.s3_sync import sync_lake_to_s3

        return sync_lake_to_s3()

    build_gold(build_silver(land_bronze())) >> publish_to_s3()


market_data_lakehouse()
