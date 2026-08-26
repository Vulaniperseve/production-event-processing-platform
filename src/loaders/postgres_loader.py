from database.database import SessionLocal
from database.models import MarketEvent

from src.logger import logger


class PostgresLoader:

    @staticmethod
    def load(df):

        session = SessionLocal()

        inserted_count = 0
        skipped_count = 0

        try:

            logger.info(
                f"Starting PostgreSQL load. Records received: {len(df)}"
            )

            for _, row in df.iterrows():

                # Check whether this event already exists
                existing_event = session.get(
                    MarketEvent,
                    row["event_id"]
                )

                if existing_event:
                    skipped_count += 1

                    logger.info(
                        f"Duplicate event skipped: {row['event_id']}"
                    )

                    continue

                event = MarketEvent(
                    event_id=row["event_id"],
                    entity_id=row["entity_id"],
                    event_time=row["event_time"],
                    event_type=row["event_type"],
                    value=float(row["value"]),
                    source_system=row["source_system"]
                )

                session.add(event)

                inserted_count += 1

            session.commit()

            logger.info(
                f"PostgreSQL load successful. "
                f"Inserted: {inserted_count}, "
                f"Skipped duplicates: {skipped_count}"
            )

            return inserted_count, skipped_count

        except Exception as e:

            session.rollback()

            logger.error(
                f"PostgreSQL load failed. "
                f"Transaction rolled back: {e}"
            )

            raise

        finally:

            session.close()