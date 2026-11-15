import hashlib
import json
from datetime import datetime, timezone

from database.database import SessionLocal
from database.models import RawAPIEvent, RawStatus

from src.logger import logger


SOURCE_SYSTEM = "TwelveData"


def payload_hash(data):
    """SHA-256 of the canonical (sorted-key) JSON payload."""
    canonical = json.dumps(data, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class RawStore:
    """
    Read/write access to the raw layer (raw.raw_api_events).

    The raw layer is append-only: every API response is stored once, exactly
    as received, and every downstream step reads from here by raw_event_id.
    """

    @staticmethod
    def save(data, source_system=SOURCE_SYSTEM):
        """Persist one API response. Returns the new raw_event_id."""

        if data is None:
            raise ValueError("Cannot save an empty API response to the raw layer.")

        meta = data.get("meta") or {}
        values = data.get("values")

        raw_event = RawAPIEvent(
            source_system=source_system,
            symbol=meta.get("symbol"),
            raw_json=data,
            payload_hash=payload_hash(data),
            api_status=data.get("status"),
            record_count=len(values) if isinstance(values, list) else 0,
            processing_status=RawStatus.PENDING,
        )

        session = SessionLocal()

        try:
            session.add(raw_event)
            session.commit()

            raw_event_id = raw_event.id

            logger.info(
                f"Raw API response saved: raw_event_id={raw_event_id}, "
                f"symbol={raw_event.symbol}, records={raw_event.record_count}"
            )

            return raw_event_id

        except Exception as e:
            session.rollback()
            logger.error(f"Failed to save raw API response: {e}")
            raise

        finally:
            session.close()

    @staticmethod
    def get_payload(raw_event_id):
        """Return the stored API payload (dict) for a raw_event_id."""

        session = SessionLocal()

        try:
            raw_event = session.get(RawAPIEvent, raw_event_id)

            if raw_event is None:
                raise LookupError(f"raw_event_id={raw_event_id} not found.")

            return raw_event.raw_json

        finally:
            session.close()

    @staticmethod
    def set_status(raw_event_id, status, error_message=None):
        """Record where this raw response is in its processing lifecycle."""

        session = SessionLocal()

        try:
            raw_event = session.get(RawAPIEvent, raw_event_id)

            if raw_event is None:
                raise LookupError(f"raw_event_id={raw_event_id} not found.")

            raw_event.processing_status = status
            raw_event.error_message = error_message
            # naive UTC, matching the TIMESTAMP (without time zone) column
            raw_event.processed_at = datetime.now(timezone.utc).replace(tzinfo=None)

            session.commit()

            logger.info(f"raw_event_id={raw_event_id} status -> {status}")

        except Exception:
            session.rollback()
            raise

        finally:
            session.close()
