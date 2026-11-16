"""
Bronze layer: land raw API responses in the lake, unchanged.

    bronze/twelvedata_time_series/ingest_date=YYYY-MM-DD/raw_event_id=000000042.json

One JSON document per raw response (an envelope with lineage metadata
around the untouched payload). This is plain Python, not Spark: it is a
copy job, and Spark's strength is the transformation that follows.
"""

import json
import os
from pathlib import Path

from src.lake.config import bronze_path
from src.logger import logger


def landed_raw_event_ids(root=None):
    """raw_event_ids already present in bronze (derived from file names)."""
    return {
        int(path.stem.split("=", 1)[1])
        for path in bronze_path(root).glob("ingest_date=*/raw_event_id=*.json")
    }


def write_bronze_record(record, root=None):
    """
    Atomically write one raw response to bronze. Returns the file path.

    The file is written under a hidden temporary name (Spark ignores files
    starting with "." or "_") and then renamed, so a reader never sees a
    half-written file. Re-writing the same raw_event_id is idempotent.
    """

    ingest_date = str(record["extracted_at"])[:10]
    target_dir = bronze_path(root) / f"ingest_date={ingest_date}"
    target_dir.mkdir(parents=True, exist_ok=True)

    target = target_dir / f"raw_event_id={int(record['raw_event_id']):09d}.json"
    tmp = target_dir / f".{target.name}.tmp"

    tmp.write_text(json.dumps(record, sort_keys=True, default=str) + "\n", encoding="utf-8")
    os.replace(tmp, target)

    return target


def land_raw_to_bronze(engine=None, root=None):
    """
    Copy every raw response not yet in bronze.

    Uses a set difference of ids rather than a "max id" watermark: SERIAL ids
    are allocated before commit, so a lower id can become visible after a
    higher one, and a watermark would skip it forever.

    Returns {"landed": n, "ingest_dates": [...]}.
    """

    from sqlalchemy import text

    if engine is None:
        from database.database import engine

    with engine.connect() as conn:
        all_ids = {row[0] for row in conn.execute(text("SELECT id FROM raw.raw_api_events"))}

    missing = sorted(all_ids - landed_raw_event_ids(root))

    if not missing:
        logger.info("Bronze is up to date: nothing to land.")
        return {"landed": 0, "ingest_dates": []}

    ingest_dates = set()

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT id, source_system, symbol, extracted_at, payload_hash, raw_json "
                "FROM raw.raw_api_events WHERE id = ANY(:ids) ORDER BY id"
            ),
            {"ids": missing},
        ).mappings().all()

    for row in rows:
        path = write_bronze_record(
            {
                "raw_event_id": row["id"],
                "source_system": row["source_system"],
                "symbol": row["symbol"],
                "extracted_at": row["extracted_at"].isoformat(),
                "payload_hash": row["payload_hash"],
                "payload": row["raw_json"],
            },
            root,
        )
        ingest_dates.add(Path(path).parent.name.split("=", 1)[1])

    logger.info(f"Landed {len(rows)} raw responses in bronze: dates={sorted(ingest_dates)}")

    return {"landed": len(rows), "ingest_dates": sorted(ingest_dates)}
