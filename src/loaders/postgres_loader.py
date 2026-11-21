from sqlalchemy.dialects.postgresql import insert

from database.database import SessionLocal
from database.models import MarketEvent
from src.logger import logger


class PostgresLoader:

    @staticmethod
    def build_rows(df, raw_event_id=None):
        """Convert the transformed DataFrame into staging rows."""

        return [
            {
                "event_id": row["event_id"],
                "entity_id": row["entity_id"],
                "event_time": row["event_time"],
                "event_type": row["event_type"],
                "value": float(row["value"]),
                "source_system": row["source_system"],
                "raw_event_id": raw_event_id,
            }
            for row in df.to_dict(orient="records")
        ]

    @staticmethod
    def load(df, raw_event_id=None):
        """
        Idempotently load events into staging.market_events.

        Uses a single INSERT ... ON CONFLICT (event_id) DO NOTHING, so
        re-running the same batch (Airflow retry, backfill, manual rerun)
        never creates duplicates. The database enforces it, instead of a
        check-then-insert loop that costs one query per row and can race.

        Returns (inserted_count, skipped_count).
        """

        rows = PostgresLoader.build_rows(df, raw_event_id)

        logger.info(f"Starting PostgreSQL load. Records received: {len(rows)}")

        if not rows:
            return 0, 0

        session = SessionLocal()

        try:
            stmt = (
                insert(MarketEvent.__table__)
                .values(rows)
                .on_conflict_do_nothing(index_elements=["event_id"])
            )

            result = session.execute(stmt)
            session.commit()

            inserted_count = result.rowcount
            skipped_count = len(rows) - inserted_count

            logger.info(
                f"PostgreSQL load successful. "
                f"Inserted: {inserted_count}, "
                f"Skipped duplicates: {skipped_count}"
            )

            return inserted_count, skipped_count

        except Exception as e:
            session.rollback()
            logger.error(f"PostgreSQL load failed. Transaction rolled back: {e}")
            raise

        finally:
            session.close()
