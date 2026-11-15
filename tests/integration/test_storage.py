"""Raw layer, staging loader and quarantine against real PostgreSQL."""

import pandas as pd
import pytest
from sqlalchemy.exc import IntegrityError

from database.models import RawStatus
from src.loaders.postgres_loader import PostgresLoader
from src.loaders.raw_store import RawStore, payload_hash
from src.validators.quarantine import QuarantineHandler
from tests.integration.helpers import query, scalar

PAYLOAD = {
    "meta": {"symbol": "MSFT", "interval": "1day"},
    "status": "ok",
    "values": [
        {"datetime": "2026-11-12", "close": "410.25"},
        {"datetime": "2026-11-13", "close": "412.80"},
    ],
}


def _events(raw_event_id=None, days=("2026-11-12", "2026-11-13")):
    return pd.DataFrame([
        {
            "event_id": f"MSFT-{day}",
            "entity_id": "MSFT",
            "event_time": day,
            "event_type": "MARKET_PRICE",
            "value": "410.0",
            "source_system": "TwelveData",
        }
        for day in days
    ])


def test_raw_save_and_read_round_trip(clean_db):
    raw_event_id = RawStore.save(PAYLOAD)

    assert RawStore.get_payload(raw_event_id) == PAYLOAD

    row = query(
        clean_db,
        "SELECT symbol, api_status, record_count, processing_status, payload_hash "
        "FROM raw.raw_api_events WHERE id = :id",
        id=raw_event_id,
    )[0]
    assert tuple(row) == ("MSFT", "ok", 2, RawStatus.PENDING, payload_hash(PAYLOAD))


def test_raw_is_append_only(clean_db):
    """The same payload ingested twice is kept twice (audit trail), same hash."""
    first, second = RawStore.save(PAYLOAD), RawStore.save(PAYLOAD)

    assert first != second
    assert scalar(clean_db, "SELECT count(DISTINCT payload_hash) FROM raw.raw_api_events") == 1


def test_raw_status_update(clean_db):
    raw_event_id = RawStore.save(PAYLOAD)

    RawStore.set_status(raw_event_id, RawStatus.FAILED, "boom")

    status, error, processed_at = query(
        clean_db,
        "SELECT processing_status, error_message, processed_at "
        "FROM raw.raw_api_events WHERE id = :id",
        id=raw_event_id,
    )[0]
    assert (status, error) == (RawStatus.FAILED, "boom")
    assert processed_at is not None


def test_missing_raw_event_raises(clean_db):
    with pytest.raises(LookupError):
        RawStore.get_payload(999)


def test_staging_load_is_idempotent_with_lineage(clean_db):
    raw_event_id = RawStore.save(PAYLOAD)

    assert PostgresLoader.load(_events(), raw_event_id) == (2, 0)
    assert PostgresLoader.load(_events(), raw_event_id) == (0, 2)

    rows = query(clean_db, "SELECT raw_event_id, value FROM staging.market_events")
    assert len(rows) == 2
    assert {r[0] for r in rows} == {raw_event_id}
    assert all(isinstance(r[1], float) for r in rows)


def test_staging_load_partial_overlap(clean_db):
    """Next day's extraction overlaps: only the new day is inserted."""
    raw_event_id = RawStore.save(PAYLOAD)
    PostgresLoader.load(_events(), raw_event_id)

    inserted, skipped = PostgresLoader.load(
        _events(days=("2026-11-13", "2026-11-14")), raw_event_id
    )

    assert (inserted, skipped) == (1, 1)
    assert scalar(clean_db, "SELECT count(*) FROM staging.market_events") == 3


def test_staging_rejects_unknown_raw_event(clean_db):
    """The foreign key protects lineage: staging rows must point at real raw rows."""
    with pytest.raises(IntegrityError):
        PostgresLoader.load(_events(), raw_event_id=999)

    assert scalar(clean_db, "SELECT count(*) FROM staging.market_events") == 0


def test_quarantine_is_replaced_on_retry(clean_db):
    raw_event_id = RawStore.save(PAYLOAD)
    bad = [
        {"record": {"datetime": None, "close": "1"}, "reason": "Missing fields: ['datetime']"},
        {"record": {"datetime": "2026-11-12", "close": "ERROR"}, "reason": "Invalid close price"},
    ]

    QuarantineHandler.save_to_db(bad, raw_event_id, "MSFT")
    QuarantineHandler.save_to_db(bad, raw_event_id, "MSFT")  # retry

    rows = query(
        clean_db,
        "SELECT record->>'close', reason FROM quarantine.quarantined_records "
        "WHERE raw_event_id = :id ORDER BY id",
        id=raw_event_id,
    )
    assert [tuple(r) for r in rows] == [
        ("1", "Missing fields: ['datetime']"),
        ("ERROR", "Invalid close price"),
    ]
