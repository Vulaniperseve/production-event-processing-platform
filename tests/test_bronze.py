import json

from src.lake.bronze import landed_raw_event_ids, write_bronze_record

RECORD = {
    "raw_event_id": 42,
    "source_system": "TwelveData",
    "symbol": "AAPL",
    "extracted_at": "2026-11-15T21:30:16.193886",
    "payload_hash": "abc",
    "payload": {"status": "ok", "meta": {"symbol": "AAPL"}, "values": []},
}


def test_bronze_file_layout_and_content(tmp_path):
    path = write_bronze_record(RECORD, root=tmp_path)

    assert path.relative_to(tmp_path).as_posix() == (
        "bronze/twelvedata_time_series/ingest_date=2026-11-15/raw_event_id=000000042.json"
    )
    assert json.loads(path.read_text(encoding="utf-8")) == RECORD
    # single-line JSON document (JSON Lines), so Spark can split and read it
    assert path.read_text(encoding="utf-8").count("\n") == 1


def test_rewrite_is_idempotent_and_leaves_no_temp_files(tmp_path):
    write_bronze_record(RECORD, root=tmp_path)
    write_bronze_record(RECORD, root=tmp_path)

    files = [p.name for p in tmp_path.rglob("*") if p.is_file()]
    assert files == ["raw_event_id=000000042.json"]


def test_landed_ids_are_read_from_file_names(tmp_path):
    for raw_id, day in [(1, "2026-11-14"), (2, "2026-11-15"), (10, "2026-11-15")]:
        write_bronze_record({**RECORD, "raw_event_id": raw_id, "extracted_at": f"{day}T06:00:00"}, root=tmp_path)

    assert landed_raw_event_ids(tmp_path) == {1, 2, 10}
    assert landed_raw_event_ids(tmp_path / "empty") == set()
