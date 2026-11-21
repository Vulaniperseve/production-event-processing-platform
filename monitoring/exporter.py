"""
Platform metrics exporter for Prometheus (http://<host>:9200/metrics).

Data observability, not just "is the server up": every 30 seconds it turns
the platform's state into metrics that alert rules can watch:

* pipeline runs   last state, last success time and duration of each DAG (Airflow REST API)
* freshness       hours since each symbol was last loaded, trading days behind
* data quality    raw response statuses, quarantined records, quarantine rate
* the lake        files, rows and bytes per layer
* reconciliation  events only in the lake or only in the warehouse, and the
                  largest difference in returns between SQL and Spark (every 5 min)
* streaming       ticks, latency p95, duplicates, rejects, last batch age,
                  and consumer lag (Kafka end offsets vs Spark's checkpoint)

Each collector is independent: if one source is down it reports
platform_collector_up{collector="..."} 0 and the others keep working.

    python -m monitoring.exporter
"""

import logging
import os
import time
from datetime import datetime

from prometheus_client import CollectorRegistry, Counter, Gauge, start_http_server

from dashboard.lib import data
from dashboard.lib.airflow_client import AirflowClient
from dashboard.lib.pipeline_status import INGEST_DAG, LAKE_DAG
from monitoring import kafka_lag

log = logging.getLogger("exporter")

REGISTRY = CollectorRegistry()


def _g(name, doc, labels=()):
    return Gauge(name, doc, labels, registry=REGISTRY)


M = {
    "collector_up": _g("platform_collector_up", "1 if the last collection from this source worked", ["collector"]),
    "collect_seconds": _g("platform_collect_duration_seconds", "Time the last collection took", ["collector"]),
    "dag_last_success": _g("platform_dag_last_run_success", "1 if the latest finished run succeeded", ["dag"]),
    "dag_running": _g("platform_dag_running", "1 while a run is queued or running", ["dag"]),
    "dag_last_success_ts": _g("platform_dag_last_success_timestamp_seconds", "End time of the last successful run", ["dag"]),
    "dag_last_duration": _g("platform_dag_last_run_duration_seconds", "Duration of the latest finished run", ["dag"]),
    "data_age_hours": _g("platform_data_age_hours", "Hours since the symbol was last loaded", ["symbol"]),
    "days_behind": _g("platform_data_days_behind", "Days between today and the latest trade date", ["symbol"]),
    "staging_events": _g("platform_staging_events", "Events in staging.market_events"),
    "raw_responses": _g("platform_raw_responses", "Raw API responses by processing status", ["status"]),
    "quarantined": _g("platform_quarantined_records", "Records in quarantine"),
    "quarantine_rate": _g("platform_quarantine_rate", "Quarantined / received records, last 90 days"),
    "lake_files": _g("platform_lake_files", "Data files per lake layer", ["layer"]),
    "lake_rows": _g("platform_lake_rows", "Rows per Parquet lake layer", ["layer"]),
    "lake_bytes": _g("platform_lake_bytes", "Bytes per lake layer", ["layer"]),
    "lake_only": _g("platform_lake_only_events", "Events in Spark silver that are missing in PostgreSQL"),
    "warehouse_only": _g("platform_warehouse_only_events", "Events in PostgreSQL that are missing in the lake"),
    "return_diff": _g("platform_return_max_difference", "Largest |SQL - Spark| period return difference"),
    "stream_ticks": _g("platform_stream_ticks", "Ticks stored by the streaming job"),
    "stream_duplicates": _g("platform_stream_duplicates", "Duplicate ticks dropped"),
    "stream_rejected": _g("platform_stream_rejected", "Invalid messages rejected"),
    "stream_latency_p95": _g("platform_stream_latency_p95_seconds", "p95 producer-to-PostgreSQL latency"),
    "stream_batch_age": _g("platform_stream_last_batch_age_seconds", "Seconds since the last micro-batch with data"),
    "stream_lag": _g("platform_stream_lag_messages", "Kafka messages not yet processed by Spark"),
    "kafka_up": _g("platform_kafka_up", "1 if the Kafka broker answered"),
    "offset_resets": _g("platform_stream_offset_resets", "Partitions whose offsets went backwards (topic recreated)"),
}
ERRORS = Counter("platform_collector_errors", "Failed collections", ["collector"], registry=REGISTRY)

METRIC_NAMES = sorted({g._name for g in M.values()} | {"platform_collector_errors_total"})


def _ts(iso):
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() if iso else None


def _num(value, default=0.0):
    try:
        v = float(value)
        return default if v != v else v      # NaN -> default
    except (TypeError, ValueError):
        return default


# ------------------------------------------------------------ collectors
def collect_airflow(client):
    for dag in (INGEST_DAG, LAKE_DAG):
        runs = client.recent_runs(dag, limit=25)
        M["dag_running"].labels(dag).set(int(any(r.get("state") in ("queued", "running") for r in runs[:2])))
        finished = [r for r in runs if r.get("state") in ("success", "failed")]
        if finished:
            latest = finished[0]
            M["dag_last_success"].labels(dag).set(int(latest["state"] == "success"))
            start, end = _ts(latest.get("start_date")), _ts(latest.get("end_date"))
            if start and end:
                M["dag_last_duration"].labels(dag).set(end - start)
        success = next((r for r in finished if r["state"] == "success"), None)
        if success and _ts(success.get("end_date")):
            M["dag_last_success_ts"].labels(dag).set(_ts(success["end_date"]))


def collect_warehouse(engine):
    fresh = data.data_freshness(engine)
    M["data_age_hours"].clear()
    M["days_behind"].clear()
    for _, row in fresh.iterrows():
        M["data_age_hours"].labels(row["symbol"]).set(_num(row["hours_since_last_load"]))
        M["days_behind"].labels(row["symbol"]).set(_num(row["days_behind"]))

    totals = data.warehouse_totals(engine)
    M["staging_events"].set(_num(totals["staging_events"]))
    M["quarantined"].set(_num(totals["quarantined"]))

    M["raw_responses"].clear()
    for _, row in data.raw_status_counts(engine).iterrows():
        M["raw_responses"].labels(row["status"]).set(_num(row["responses"]))

    health = data.pipeline_health(engine, 90)
    received = _num(health["records_received"].sum()) if not health.empty else 0
    quarantined = _num(health["records_quarantined"].sum()) if not health.empty else 0
    M["quarantine_rate"].set(quarantined / received if received else 0)


def collect_lake(root=None):
    for layer, stats in data.lake_layers(root).items():
        M["lake_files"].labels(layer).set(stats["files"])
        M["lake_bytes"].labels(layer).set(stats["bytes"])
        if stats["rows"] is not None:
            M["lake_rows"].labels(layer).set(stats["rows"])


def collect_reconciliation(engine, root=None):
    ids = data.compare_event_ids(data.staging_event_ids(engine), data.silver_event_ids(root))
    M["lake_only"].set(int(ids["only_in_lake"].sum()) if not ids.empty else 0)
    M["warehouse_only"].set(int(ids["only_in_warehouse"].sum()) if not ids.empty else 0)
    returns = data.compare_returns(data.symbol_summary(engine), data.read_gold("daily_metrics", root))
    M["return_diff"].set(_num(returns["difference"].max()) if not returns.empty else 0)


def collect_streaming(engine):
    if not data.stream_available(engine):
        return
    o = data.stream_overview(engine)
    M["stream_ticks"].set(_num(o["ticks"]))
    M["stream_duplicates"].set(_num(o["duplicates"]))
    M["stream_rejected"].set(_num(o["rejected"]))
    M["stream_latency_p95"].set(_num(o["latency_p95"]))
    last = o["last_processed"]
    if last is not None and last == last:
        M["stream_batch_age"].set(max(time.time() - last.timestamp(), 0))


def collect_kafka(bootstrap, topic, checkpoint_dir):
    try:
        ends = kafka_lag.kafka_end_offsets(bootstrap, topic)
    except Exception:
        M["kafka_up"].set(0)       # streaming is optional: Kafka being off isn't an error
        return
    M["kafka_up"].set(1)
    processed = kafka_lag.committed_offsets(checkpoint_dir, topic)
    M["stream_lag"].set(kafka_lag.consumer_lag(ends, processed))
    M["offset_resets"].set(len(kafka_lag.offset_resets(ends, processed)))


def run_collector(name, fn, *args):
    started = time.monotonic()
    try:
        fn(*args)
        M["collector_up"].labels(name).set(1)
    except Exception as exc:
        M["collector_up"].labels(name).set(0)
        ERRORS.labels(name).inc()
        log.warning("collector %s failed: %s", name, exc)
    finally:
        M["collect_seconds"].labels(name).set(time.monotonic() - started)


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    port = int(os.getenv("EXPORTER_PORT", "9200"))
    interval = int(os.getenv("COLLECT_INTERVAL", "30"))
    slow_interval = int(os.getenv("RECONCILE_INTERVAL", "300"))
    topic = os.getenv("STREAM_TOPIC", "market.price_ticks")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    checkpoint = os.getenv("STREAM_CHECKPOINT_DIR",
                           str(data.lake_root() / "_checkpoints" / "price_stream" / "ticks"))

    engine = data.make_engine()
    client = AirflowClient()
    start_http_server(port, registry=REGISTRY)
    log.info("serving metrics on :%d/metrics", port)

    last_slow = 0.0
    while True:
        run_collector("airflow", collect_airflow, client)
        run_collector("warehouse", collect_warehouse, engine)
        run_collector("lake", collect_lake)
        run_collector("streaming", collect_streaming, engine)
        run_collector("kafka", collect_kafka, bootstrap, topic, checkpoint)
        if time.monotonic() - last_slow >= slow_interval:
            # Reading every event id from Parquet is the expensive part: less often.
            run_collector("reconciliation", collect_reconciliation, engine)
            last_slow = time.monotonic()
        time.sleep(interval)


if __name__ == "__main__":
    main()
