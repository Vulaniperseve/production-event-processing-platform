from datetime import datetime, timezone

import pandas as pd
import pytest

pytest.importorskip("prometheus_client")

from monitoring import exporter  # noqa: E402
from monitoring.exporter import M, REGISTRY  # noqa: E402


def value(name, **labels):
    return REGISTRY.get_sample_value(name, labels or None)


class FakeAirflow:
    def __init__(self, runs):
        self.runs = runs

    def recent_runs(self, dag, limit=25):
        return self.runs.get(dag, [])


def run(state, start="2026-11-20T06:00:00Z", end="2026-11-20T06:04:00Z"):
    return {"state": state, "start_date": start, "end_date": end}


def test_airflow_collector_reports_last_result_and_last_success():
    client = FakeAirflow({
        "production_event_pipeline": [
            run("failed", "2026-11-21T06:00:00Z", "2026-11-21T06:01:30Z"),
            run("success"),
        ],
        "market_data_lakehouse": [run("running", end=None), run("success")],
    })
    exporter.collect_airflow(client)

    ingest = {"dag": "production_event_pipeline"}
    assert value("platform_dag_last_run_success", **ingest) == 0
    assert value("platform_dag_last_run_duration_seconds", **ingest) == 90
    assert value("platform_dag_last_success_timestamp_seconds", **ingest) == \
        datetime(2026, 11, 20, 6, 4, tzinfo=timezone.utc).timestamp()
    assert value("platform_dag_running", **ingest) == 0
    assert value("platform_dag_running", dag="market_data_lakehouse") == 1
    assert value("platform_dag_last_run_success", dag="market_data_lakehouse") == 1


def test_warehouse_collector(monkeypatch):
    d = exporter.data
    monkeypatch.setattr(d, "data_freshness", lambda e: pd.DataFrame(
        {"symbol": ["AAPL", "MSFT"], "hours_since_last_load": [2.5, None], "days_behind": [1, 6]}))
    monkeypatch.setattr(d, "warehouse_totals", lambda e: {"staging_events": 306, "quarantined": 3})
    monkeypatch.setattr(d, "raw_status_counts", lambda e: pd.DataFrame(
        {"status": ["LOADED", "FAILED"], "responses": [19, 1]}))
    monkeypatch.setattr(d, "pipeline_health", lambda e, days: pd.DataFrame(
        {"records_received": [100, 100], "records_quarantined": [3, 0]}))

    exporter.collect_warehouse(engine=None)

    assert value("platform_data_age_hours", symbol="AAPL") == 2.5
    assert value("platform_data_age_hours", symbol="MSFT") == 0       # missing -> 0, not NaN
    assert value("platform_data_days_behind", symbol="MSFT") == 6
    assert value("platform_staging_events") == 306
    assert value("platform_raw_responses", status="FAILED") == 1
    assert value("platform_quarantine_rate") == pytest.approx(0.015)


def test_removed_symbols_disappear_from_metrics(monkeypatch):
    d = exporter.data
    for symbols in (["AAPL", "TSLA"], ["AAPL"]):
        monkeypatch.setattr(d, "data_freshness", lambda e, s=symbols: pd.DataFrame(
            {"symbol": s, "hours_since_last_load": [1.0] * len(s), "days_behind": [0] * len(s)}))
        monkeypatch.setattr(d, "warehouse_totals", lambda e: {"staging_events": 1, "quarantined": 0})
        monkeypatch.setattr(d, "raw_status_counts", lambda e: pd.DataFrame({"status": [], "responses": []}))
        monkeypatch.setattr(d, "pipeline_health", lambda e, days: pd.DataFrame())
        exporter.collect_warehouse(engine=None)
    assert value("platform_data_age_hours", symbol="TSLA") is None


def test_reconciliation_collector(monkeypatch):
    d = exporter.data
    monkeypatch.setattr(d, "staging_event_ids", lambda e: pd.DataFrame({"symbol": ["A", "A"], "event_id": ["1", "2"]}))
    monkeypatch.setattr(d, "silver_event_ids", lambda root=None: pd.DataFrame({"symbol": ["A", "A"], "event_id": ["1", "3"]}))
    monkeypatch.setattr(d, "symbol_summary", lambda e: pd.DataFrame({"symbol": ["A"], "period_return": [0.1]}))
    monkeypatch.setattr(d, "read_gold", lambda t, root=None: pd.DataFrame(
        {"symbol": ["A", "A"], "trade_date": [1, 2], "close_price": [100.0, 110.0]}))

    exporter.collect_reconciliation(engine=None)

    assert value("platform_lake_only_events") == 1
    assert value("platform_warehouse_only_events") == 1
    assert value("platform_return_max_difference") == pytest.approx(0, abs=1e-12)


def test_kafka_down_is_not_an_error(monkeypatch):
    def unreachable(*a, **k):
        raise RuntimeError("no broker")

    monkeypatch.setattr(exporter.kafka_lag, "kafka_end_offsets", unreachable)
    exporter.run_collector("kafka", exporter.collect_kafka, "kafka:9092", "t", "/nowhere")

    assert value("platform_kafka_up") == 0
    assert value("platform_collector_up", collector="kafka") == 1


def test_a_failing_source_is_reported_not_raised():
    before = value("platform_collector_errors_total", collector="warehouse") or 0

    def broken(engine):
        raise ConnectionError("db down")

    exporter.run_collector("warehouse", broken, None)

    assert value("platform_collector_up", collector="warehouse") == 0
    assert value("platform_collector_errors_total", collector="warehouse") == before + 1
    assert value("platform_collect_duration_seconds", collector="warehouse") >= 0


def test_metric_names_are_namespaced():
    assert all(name.startswith("platform_") for name in exporter.METRIC_NAMES)
    assert len(M) == len({g._name for g in M.values()})
