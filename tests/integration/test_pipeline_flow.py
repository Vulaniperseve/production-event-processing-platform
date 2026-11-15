"""
End-to-end pipeline steps against real PostgreSQL.

Only the HTTP call is replaced (a fake extractor that counts calls); the
raw layer, validation, quarantine, transformation and staging load are real.
"""

import copy

import pytest

from database.models import RawStatus
from src.pipeline import steps
from tests.integration.helpers import query, scalar

GOOD_AND_BAD = {
    "meta": {"symbol": "AAPL", "interval": "1day"},
    "status": "ok",
    "values": [
        {"datetime": "2026-11-13", "close": "230.10"},
        {"datetime": "2026-11-12", "close": "228.40"},
        {"datetime": "2026-11-11", "close": "ERROR"},
    ],
}


class FakeExtractor:
    calls = 0
    payload = GOOD_AND_BAD

    def extract(self, **kwargs):
        FakeExtractor.calls += 1
        return copy.deepcopy(FakeExtractor.payload)


@pytest.fixture
def fake_api(monkeypatch, clean_db):
    FakeExtractor.calls = 0
    FakeExtractor.payload = GOOD_AND_BAD
    monkeypatch.setattr(steps, "TwelveDataExtractor", FakeExtractor)
    return FakeExtractor


def _status(engine, raw_event_id):
    return scalar(
        engine,
        "SELECT processing_status FROM raw.raw_api_events WHERE id = :id",
        id=raw_event_id,
    )


def test_full_run(clean_db, fake_api):
    result = steps.run_pipeline("AAPL")
    raw_event_id = result["validation"]["raw_event_id"]

    assert fake_api.calls == 1
    assert result["validation"]["valid_count"] == 2
    assert result["validation"]["invalid_count"] == 1
    assert result["load"]["inserted"] == 2

    assert _status(clean_db, raw_event_id) == RawStatus.LOADED

    # lineage: staging and quarantine both point back at the raw response
    assert scalar(
        clean_db,
        "SELECT count(*) FROM staging.market_events WHERE raw_event_id = :id",
        id=raw_event_id,
    ) == 2
    assert query(
        clean_db,
        "SELECT reason FROM quarantine.quarantined_records WHERE raw_event_id = :id",
        id=raw_event_id,
    )[0][0] == "Invalid close price"


def test_replaying_steps_never_calls_api_or_duplicates(clean_db, fake_api):
    """What an Airflow retry/clear does: re-run steps for the same raw_event_id."""
    raw_event_id = steps.extract_to_raw()
    steps.validate_raw(raw_event_id)
    steps.transform_and_load(raw_event_id)

    steps.validate_raw(raw_event_id)
    replay = steps.transform_and_load(raw_event_id)

    assert fake_api.calls == 1
    assert replay == {"raw_event_id": raw_event_id, "inserted": 0, "skipped": 2}
    assert scalar(clean_db, "SELECT count(*) FROM staging.market_events") == 2
    assert scalar(clean_db, "SELECT count(*) FROM quarantine.quarantined_records") == 1


def test_daily_runs_with_overlapping_history(clean_db, fake_api):
    steps.run_pipeline()

    fake_api.payload = {
        **GOOD_AND_BAD,
        "values": [
            {"datetime": "2026-11-14", "close": "231.00"},  # new day
            {"datetime": "2026-11-13", "close": "230.10"},  # already loaded
        ],
    }
    result = steps.run_pipeline()

    assert result["load"] == {
        "raw_event_id": result["validation"]["raw_event_id"],
        "inserted": 1,
        "skipped": 1,
    }
    assert scalar(clean_db, "SELECT count(*) FROM raw.raw_api_events") == 2
    assert scalar(clean_db, "SELECT count(*) FROM staging.market_events") == 3


def test_api_error_response_is_recorded_as_failed(clean_db, fake_api):
    fake_api.payload = {"status": "error", "code": 429, "message": "API credits exhausted"}

    raw_event_id = steps.extract_to_raw()

    with pytest.raises(steps.StructureValidationError):
        steps.validate_raw(raw_event_id)

    status, error = query(
        clean_db,
        "SELECT processing_status, error_message FROM raw.raw_api_events WHERE id = :id",
        id=raw_event_id,
    )[0]
    assert status == RawStatus.FAILED
    assert "meta" in error
    # the failed response is still preserved for debugging
    assert scalar(
        clean_db,
        "SELECT raw_json->>'message' FROM raw.raw_api_events WHERE id = :id",
        id=raw_event_id,
    ) == "API credits exhausted"
