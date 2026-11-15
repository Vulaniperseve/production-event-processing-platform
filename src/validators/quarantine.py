import json
from datetime import datetime
from pathlib import Path

from src.logger import logger


class QuarantineHandler:

    @staticmethod
    def save(invalid_records):

        if not invalid_records:
            logger.info("No invalid records to quarantine.")
            return None

        quarantine_dir = Path("data/quarantine")

        quarantine_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        filename = datetime.now().strftime(
            "invalid_records_%Y%m%d_%H%M%S.json"
        )

        filepath = quarantine_dir / filename

        with open(filepath, "w") as file:

            json.dump(
                invalid_records,
                file,
                indent=4,
                default=str
            )

        logger.warning(
            f"Quarantined {len(invalid_records)} invalid records."
        )

        logger.warning(
            f"Quarantine file saved to: {filepath}"
        )

        return filepath

    @staticmethod
    def save_to_db(invalid_records, raw_event_id, symbol=None):
        """
        Persist invalid records to quarantine.quarantined_records, linked to
        the raw response they came from.

        Idempotent per raw_event_id: existing quarantine rows for this raw
        event are replaced, so a retried validation task never duplicates them.
        """

        from database.database import SessionLocal
        from database.models import QuarantinedRecord

        session = SessionLocal()

        try:
            session.query(QuarantinedRecord).filter(
                QuarantinedRecord.raw_event_id == raw_event_id
            ).delete(synchronize_session=False)

            session.add_all([
                QuarantinedRecord(
                    raw_event_id=raw_event_id,
                    symbol=symbol,
                    record=json.loads(json.dumps(item["record"], default=str)),
                    reason=item["reason"],
                )
                for item in invalid_records
            ])

            session.commit()

            if invalid_records:
                logger.warning(
                    f"Quarantined {len(invalid_records)} invalid records "
                    f"to database for raw_event_id={raw_event_id}."
                )

            return len(invalid_records)

        except Exception:
            session.rollback()
            raise

        finally:
            session.close()