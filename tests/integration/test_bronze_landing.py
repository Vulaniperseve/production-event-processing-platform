"""Landing raw responses from PostgreSQL into the bronze layer."""

import json

from src.lake.bronze import land_raw_to_bronze
from src.lake.config import bronze_path
from src.loaders.raw_store import RawStore

PAYLOAD = {"meta": {"symbol": "AAPL"}, "status": "ok", "values": [{"datetime": "2026-11-13", "close": "230.1"}]}


def test_land_raw_to_bronze_is_incremental(clean_db, tmp_path):
    first = RawStore.save(PAYLOAD)
    error = RawStore.save({"status": "error", "code": 429, "message": "credits"})

    result = land_raw_to_bronze(clean_db, root=tmp_path)

    assert result["landed"] == 2
    files = sorted(bronze_path(tmp_path).rglob("*.json"))
    assert len(files) == 2

    doc = json.loads(files[0].read_text(encoding="utf-8"))
    assert doc["raw_event_id"] == first
    assert doc["payload"] == PAYLOAD                 # payload unchanged
    assert doc["payload_hash"] and doc["extracted_at"]

    # error responses are raw data too: they are landed, silver filters them
    assert json.loads(files[1].read_text(encoding="utf-8"))["raw_event_id"] == error

    # second run: nothing new; third run: only the new response
    assert land_raw_to_bronze(clean_db, root=tmp_path) == {"landed": 0, "ingest_dates": []}
    RawStore.save(PAYLOAD)
    assert land_raw_to_bronze(clean_db, root=tmp_path)["landed"] == 1
