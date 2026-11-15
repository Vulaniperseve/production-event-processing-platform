"""
Pipeline steps, independent of the orchestrator.

Each step takes / returns small, JSON-serialisable values (a raw_event_id or
a summary dict). The data itself is never passed between steps: it lives in
the raw layer, and every step reads it from there. This means:

  * the API is called exactly once per run (in extract_to_raw)
  * every downstream step works on the *same* payload
  * any step can be retried or replayed without re-calling the API
  * the same functions run from Airflow (DAG) or locally (main.py)
"""

from database.models import RawStatus

from src.extractors.twelve_data_extractor import TwelveDataExtractor
from src.loaders.postgres_loader import PostgresLoader
from src.loaders.raw_store import RawStore
from src.logger import logger
from src.transformers.transformer import DataTransformer
from src.validators.quarantine import QuarantineHandler
from src.validators.validator import DataValidator


class StructureValidationError(ValueError):
    """
    The API response is structurally invalid. Retrying will not fix it,
    so orchestrators should fail fast instead of retrying.
    """


def extract_to_raw(symbol="AAPL", interval="1day", outputsize=10):
    """Call the API once and persist the response. Returns raw_event_id."""

    data = TwelveDataExtractor().extract(
        symbol=symbol,
        interval=interval,
        outputsize=outputsize,
    )

    if data is None:
        raise ValueError("Extraction returned no data.")

    return RawStore.save(data)


def validate_raw(raw_event_id):
    """
    Structure + record-level validation of a raw response.
    Invalid records are quarantined (linked to raw_event_id).
    """

    data = RawStore.get_payload(raw_event_id)

    try:
        DataValidator.validate_structure(data)
    except ValueError as e:
        RawStore.set_status(raw_event_id, RawStatus.FAILED, str(e))
        raise StructureValidationError(str(e)) from e

    valid_records, invalid_records = DataValidator.validate_records(data)

    symbol = data["meta"].get("symbol")

    QuarantineHandler.save_to_db(invalid_records, raw_event_id, symbol)

    status = RawStatus.VALIDATED if valid_records else RawStatus.NO_VALID_RECORDS
    RawStore.set_status(raw_event_id, status)

    summary = {
        "raw_event_id": raw_event_id,
        "symbol": symbol,
        "valid_count": len(valid_records),
        "invalid_count": len(invalid_records),
    }

    logger.info(f"Validation summary: {summary}")

    return summary


def transform_and_load(raw_event_id):
    """
    Transform the valid records of a raw response and load them into staging.

    Valid records are re-derived from raw with the same deterministic
    validator, so this step depends only on raw_event_id. Loading is
    idempotent (ON CONFLICT DO NOTHING on event_id), so retries are safe.
    """

    data = RawStore.get_payload(raw_event_id)

    valid_records, _ = DataValidator.validate_records(data)

    if not valid_records:
        logger.warning(f"raw_event_id={raw_event_id}: no valid records to load.")
        RawStore.set_status(raw_event_id, RawStatus.NO_VALID_RECORDS)
        return {"raw_event_id": raw_event_id, "inserted": 0, "skipped": 0}

    try:
        df = DataTransformer.transform({
            "meta": data["meta"],
            "status": data["status"],
            "values": valid_records,
        })

        inserted, skipped = PostgresLoader.load(df, raw_event_id=raw_event_id)

    except Exception as e:
        RawStore.set_status(raw_event_id, RawStatus.FAILED, str(e))
        raise

    RawStore.set_status(raw_event_id, RawStatus.LOADED)

    summary = {
        "raw_event_id": raw_event_id,
        "inserted": inserted,
        "skipped": skipped,
    }

    logger.info(f"Load summary: {summary}")

    return summary


def run_pipeline(symbol="AAPL", interval="1day", outputsize=10):
    """Run all steps in sequence (local execution without Airflow)."""

    raw_event_id = extract_to_raw(symbol, interval, outputsize)

    validation = validate_raw(raw_event_id)

    if validation["valid_count"] == 0:
        return {"validation": validation, "load": None}

    load = transform_and_load(raw_event_id)

    return {"validation": validation, "load": load}
