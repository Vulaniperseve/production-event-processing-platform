"""
Smoke tests: every dashboard page renders without errors, using Streamlit's
headless AppTest and fake data (no database, Airflow or lake needed).
"""

from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("streamlit")
pytest.importorskip("plotly")
from streamlit.testing.v1 import AppTest  # noqa: E402

from dashboard.lib import resources  # noqa: E402
from dashboard.lib.airflow_client import AirflowUnavailable  # noqa: E402
from tests.dashboard.test_pipeline_status import (  # noqa: E402
    INGEST_RUN,
    LAKE_RUN,
    full_ingest,
    full_lake,
)

SYMBOLS = ["AAPL", "MSFT"]
DAYS = [date(2026, 8, 1) + timedelta(days=i) for i in range(40)]


def prices(symbol):
    close = 100 * np.cumprod(1 + np.linspace(-0.01, 0.012, len(DAYS)))
    df = pd.DataFrame({"trade_date": DAYS, "close_price": close})
    df["daily_return"] = df["close_price"].pct_change()
    df["sma_5"] = df["close_price"].rolling(5).mean()
    df["sma_20"] = df["close_price"].rolling(20).mean()
    df["volatility_20d_annualized"] = 0.2
    df["running_peak"] = df["close_price"].cummax()
    df["drawdown"] = df["close_price"] / df["running_peak"] - 1
    return df


SUMMARY = pd.DataFrame([{
    "symbol": s, "first_date": DAYS[0], "last_date": DAYS[-1], "trading_days": 40,
    "first_close": prices(s)["close_price"].iloc[0], "last_close": prices(s)["close_price"].iloc[-1],
    "period_return": prices(s)["close_price"].iloc[-1] / prices(s)["close_price"].iloc[0] - 1,
    "annualized_volatility": 0.2, "max_drawdown": -0.05, "best_day": DAYS[3],
    "best_day_return": 0.03, "worst_day": DAYS[5], "worst_day_return": -0.02,
    "refreshed_at": None,
} for s in SYMBOLS])

MONTHLY = pd.DataFrame({"symbol": ["AAPL", "AAPL", "MSFT", "MSFT"],
                        "month": ["2026-08", "2026-09"] * 2,
                        "monthly_return": [None, 0.02, None, -0.01]})
CORR = pd.DataFrame({"symbol_a": ["AAPL"], "symbol_b": ["MSFT"],
                     "return_correlation": [0.15], "overlapping_days": [39]})
DAILY = pd.concat([prices(s).assign(symbol=s) for s in SYMBOLS])
LAKE = {"bronze": {"files": 2, "bytes": 2048, "rows": None, "partitions": 1, "updated": None},
        "silver": {"files": 4, "bytes": 4096, "rows": 80, "partitions": 4, "updated": None},
        "gold": {"files": 4, "bytes": 4096, "rows": 85, "partitions": 3, "updated": None}}
HEALTH = pd.DataFrame({"run_date": [DAYS[-1]] * 2, "symbol": SYMBOLS, "api_responses": [1, 1],
                       "loaded": [1, 1], "no_valid_records": [0, 0], "failed": [0, 0],
                       "unfinished": [0, 0], "records_received": [40, 40],
                       "records_quarantined": [1, 0], "new_events_loaded": [39, 40],
                       "quarantine_rate": [0.025, 0.0]})


class FakeAirflow:
    def __init__(self, fail=False):
        self.fail = fail
        self.triggered = []

    def _check(self):
        if self.fail:
            raise AirflowUnavailable("Airflow is not reachable at http://x")

    def latest_run(self, dag_id):
        self._check()
        return INGEST_RUN if dag_id == "production_event_pipeline" else LAKE_RUN

    def task_instances(self, dag_id, run_id):
        return full_ingest() if dag_id == "production_event_pipeline" else full_lake()

    def recent_runs(self, dag_id, limit=10):
        return [self.latest_run(dag_id)]

    def is_paused(self, dag_id):
        return False

    def unpause(self, dag_id):
        pass

    def trigger(self, dag_id, conf=None):
        self.triggered.append((dag_id, conf))
        return {"dag_run_id": "manual__new"}


@pytest.fixture
def fakes(monkeypatch):
    airflow = FakeAirflow()
    patches = {
        "airflow": lambda: airflow,
        "symbol_summary": lambda: SUMMARY,
        "price_indicators": prices,
        "gold": lambda t: {"monthly_summary": MONTHLY, "return_correlation": CORR,
                           "daily_metrics": DAILY}[t],
        "warehouse_totals": lambda: {"raw_responses": 2, "quarantined": 1,
                                     "staging_events": 80, "symbols": 2},
        "pipeline_health": lambda days=30: HEALTH,
        "raw_status_counts": lambda: pd.DataFrame({"status": ["LOADED"], "responses": [2]}),
        "data_freshness": lambda: pd.DataFrame({"symbol": SYMBOLS, "latest_trade_date": [DAYS[-1]] * 2,
                                                "last_loaded_at": [None] * 2,
                                                "hours_since_last_load": [1.0] * 2,
                                                "days_behind": [1, 1]}),
        "quarantine_reasons": lambda: pd.DataFrame({"reason": ["Missing close price"], "symbol": ["AAPL"],
                                                    "records": [1], "last_seen": [None]}),
        "recent_quarantine": lambda limit=20: pd.DataFrame(),
        "event_id_comparison": lambda: pd.DataFrame([{
            "symbol": s, "warehouse": 40, "lake": 40, "matching": 40,
            "only_in_warehouse": 0, "only_in_lake": 0} for s in SYMBOLS]),
        "lake_layers": lambda: LAKE,
        "run_numbers": lambda since, until: {"api_responses": 2, "records_received": 80,
                                             "quarantined": 1, "valid": 79, "new_events": 79,
                                             "already_present": 0, "responses_loaded": 2,
                                             "responses_failed": 0},
    }
    stream_bars = pd.DataFrame({"window_start": pd.date_range("2026-11-17 14:30", periods=4, freq="5min", tz="UTC"),
                                "open": [1.0, 2, 3, 4], "high": [2.0, 3, 4, 5], "low": [0.5, 1, 2, 3],
                                "close": [2.0, 3, 4, 5], "ticks": [5, 5, 5, 5], "volume": [10, 10, 10, 10]})
    patches.update({
        "stream_available": lambda: True,
        "stream_overview": lambda: {"ticks": 20, "ticks_5m": 20, "symbols": 2,
                                    "last_processed": pd.Timestamp.now(tz="UTC"), "rejected": 1,
                                    "duplicates": 20, "batches": 4, "bars": 4,
                                    "latency_p50": 5.4, "latency_p95": 7.9},
        "stream_symbols": lambda: SYMBOLS,
        "stream_bars": lambda symbol: stream_bars,
        "stream_throughput": lambda: pd.DataFrame({"minute": pd.date_range("2026-11-18", periods=2, freq="1min"),
                                                   "new_ticks": [10, 10], "duplicates": [0, 20],
                                                   "rejected": [1, 0]}),
        "stream_latest_ticks": lambda: pd.DataFrame({"symbol": ["AAPL"], "price": [1.0]}),
        "stream_rejects": lambda: pd.DataFrame({"reason": ["Malformed JSON"]}),
    })
    for name, fn in patches.items():
        monkeypatch.setattr(resources, name, fn)
    return airflow


def run_page(name):
    at = AppTest.from_file(f"../../dashboard/views/{name}.py", default_timeout=30)
    at.run()
    return at


def test_pipeline_page_renders_live_view(fakes):
    at = run_page("pipeline")
    assert not at.exception
    assert any("Last run complete" in s.value for s in at.success)


def test_pipeline_button_triggers_a_run(fakes):
    at = run_page("pipeline")
    at.button[0].click().run()
    assert not at.exception
    assert fakes.triggered == [("production_event_pipeline", {"symbols": ["AAPL", "GOOGL", "MSFT"]})]


def test_pipeline_page_survives_airflow_being_down(fakes):
    fakes.fail = True
    at = run_page("pipeline")
    assert not at.exception
    assert any("isn't reachable" in e.value for e in at.error)


def test_results_page_renders(fakes):
    at = run_page("results")
    assert not at.exception
    assert len(at.metric) >= 6


def test_quality_page_shows_proof(fakes):
    at = run_page("quality")
    assert not at.exception
    assert any("identical event id" in s.value for s in at.success)


def test_quality_page_flags_a_mismatch(fakes, monkeypatch):
    wrong = SUMMARY.assign(period_return=SUMMARY["period_return"] + 0.01)
    monkeypatch.setattr(resources, "symbol_summary", lambda: wrong)
    at = run_page("quality")
    assert not at.exception
    assert any("differ" in w.value for w in at.warning)


def test_stream_page_renders_live_data(fakes):
    at = run_page("stream")
    assert not at.exception
    assert any("Receiving ticks" in s.value for s in at.success)
    assert [m.value for m in at.metric][:2] == ["20", "20"]


def test_stream_page_explains_how_to_start_when_not_set_up(fakes, monkeypatch):
    monkeypatch.setattr(resources, "stream_available", lambda: False)
    at = run_page("stream")
    assert not at.exception
    assert any("stream.bat" in i.value for i in at.info)
