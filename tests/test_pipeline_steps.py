"""
Tests for the orchestrator-independent pipeline steps.

The raw layer, quarantine and staging loader are replaced with in-memory
fakes, so these tests check the flow (extract once, read from raw, status
transitions, lineage) without needing PostgreSQL or the Twelve Data API.
"""

import copy

import pytest

from database.models import RawStatus
from src.pipeline import steps


class FakeRawStore:
    def __init__(self):
        self.rows = {}
        self.next_id = 1

    def save(self, data, source_system="TwelveData"):
        raw_event_id = self.next_id
        self.next_id += 1
        self.rows[raw_event_id] = {
            "payload": copy.deepcopy(data),
            "status": RawStatus.PENDING,
            "error": None,
        }
        return raw_event_id

    def get_payload(self, raw_event_id):
        return copy.deepcopy(self.rows[raw_event_id]["payload"])

    def set_status(self, raw_event_id, status, error_message=None):
        self.rows[raw_event_id]["status"] = status
        self.rows[raw_event_id]["error"] = error_message


class FakeStaging:
    """Mimics INSERT ... ON CONFLICT (event_id) DO NOTHING."""

    def __init__(self):
        self.events = {}

    def load(self, df, raw_event_id=None):
        inserted = 0
        for row in df.to_dict(orient="records"):
            if row["event_id"] not in self.events:
                self.events[row["event_id"]] = {**row, "raw_event_id": raw_event_id}
                inserted += 1
        return inserted, len(df) - inserted


class FakeQuarantine:
    def __init__(self):
        self.by_raw_id = {}

    def save_to_db(self, invalid_records, raw_event_id, symbol=None):
        self.by_raw_id[raw_event_id] = list(invalid_records)  # replace = idempotent
        return len(invalid_records)


class CountingExtractor:
    calls = 0
    payload = None

    def extract(self, **kwargs):
        CountingExtractor.calls += 1
        return copy.deepcopy(CountingExtractor.payload)


@pytest.fixture
def fakes(monkeypatch, api_payload):
    raw, staging, quarantine = FakeRawStore(), FakeStaging(), FakeQuarantine()

    CountingExtractor.calls = 0
    CountingExtractor.payload = api_payload

    monkeypatch.setattr(steps, "RawStore", raw)
    monkeypatch.setattr(steps, "PostgresLoader", staging)
    monkeypatch.setattr(steps, "QuarantineHandler", quarantine)
    monkeypatch.setattr(steps, "TwelveDataExtractor", CountingExtractor)

    return raw, staging, quarantine


def test_full_run_calls_api_exactly_once(fakes):
    raw, staging, quarantine = fakes

    result = steps.run_pipeline("AAPL")

    assert CountingExtractor.calls == 1
    assert result["validation"]["valid_count"] == 2
    assert result["validation"]["invalid_count"] == 3
    assert result["load"] == {"raw_event_id": 1, "inserted": 2, "skipped": 0}


def test_status_lifecycle_and_lineage(fakes):
    raw, staging, quarantine = fakes

    raw_event_id = steps.extract_to_raw()
    assert raw.rows[raw_event_id]["status"] == RawStatus.PENDING

    steps.validate_raw(raw_event_id)
    assert raw.rows[raw_event_id]["status"] == RawStatus.VALIDATED
    assert len(quarantine.by_raw_id[raw_event_id]) == 3

    steps.transform_and_load(raw_event_id)
    assert raw.rows[raw_event_id]["status"] == RawStatus.LOADED

    # every staged event points back to the raw response it came from
    assert {e["raw_event_id"] for e in staging.events.values()} == {raw_event_id}


def test_retrying_downstream_steps_is_idempotent(fakes):
    """An Airflow retry re-runs a step: no new API call, no duplicates."""
    raw, staging, quarantine = fakes

    raw_event_id = steps.extract_to_raw()

    steps.validate_raw(raw_event_id)
    steps.validate_raw(raw_event_id)  # retry

    first = steps.transform_and_load(raw_event_id)
    second = steps.transform_and_load(raw_event_id)  # retry

    assert CountingExtractor.calls == 1
    assert len(quarantine.by_raw_id[raw_event_id]) == 3
    assert first["inserted"] == 2
    assert second == {"raw_event_id": raw_event_id, "inserted": 0, "skipped": 2}
    assert len(staging.events) == 2


def test_new_extraction_with_overlapping_data_does_not_duplicate(fakes):
    raw, staging, _ = fakes

    steps.run_pipeline()
    result = steps.run_pipeline()  # next day: API returns overlapping history

    assert CountingExtractor.calls == 2
    assert len(raw.rows) == 2  # raw is append-only: both responses kept
    assert result["load"]["inserted"] == 0
    assert len(staging.events) == 2


def test_invalid_structure_marks_raw_failed(fakes, api_payload):
    raw, _, _ = fakes

    CountingExtractor.payload = {**api_payload, "status": "error"}
    raw_event_id = steps.extract_to_raw()

    with pytest.raises(steps.StructureValidationError):
        steps.validate_raw(raw_event_id)

    assert raw.rows[raw_event_id]["status"] == RawStatus.FAILED
    assert raw.rows[raw_event_id]["error"]


def test_all_records_invalid_skips_load(fakes):
    raw, staging, quarantine = fakes

    CountingExtractor.payload = {
        "meta": {"symbol": "AAPL"},
        "status": "ok",
        "values": [{"datetime": None, "close": "1"}, {"datetime": "2026-01-01", "close": "x"}],
    }

    result = steps.run_pipeline()

    assert result["load"] is None
    assert raw.rows[1]["status"] == RawStatus.NO_VALID_RECORDS
    assert len(quarantine.by_raw_id[1]) == 2
    assert staging.events == {}


def test_load_failure_marks_raw_failed(fakes, monkeypatch):
    raw, _, _ = fakes

    class BrokenLoader:
        @staticmethod
        def load(df, raw_event_id=None):
            raise RuntimeError("database unavailable")

    monkeypatch.setattr(steps, "PostgresLoader", BrokenLoader)

    raw_event_id = steps.extract_to_raw()
    steps.validate_raw(raw_event_id)

    with pytest.raises(RuntimeError):
        steps.transform_and_load(raw_event_id)

    assert raw.rows[raw_event_id]["status"] == RawStatus.FAILED
    assert "database unavailable" in raw.rows[raw_event_id]["error"]
