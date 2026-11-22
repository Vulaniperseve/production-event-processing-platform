import csv
import io

from sqlalchemy import text

from database.database import SessionLocal
from src.logger import logger

COLUMNS = ("event_id", "entity_id", "event_time", "event_type", "value",
           "source_system", "raw_event_id")
_COLUMN_LIST = ", ".join(COLUMNS)


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
    def _to_csv(rows):
        """Rows as CSV text for COPY. None becomes an empty field, which COPY reads as NULL."""
        buffer = io.StringIO()
        writer = csv.writer(buffer, lineterminator="\n")
        for row in rows:
            writer.writerow(["" if row[c] is None else row[c] for c in COLUMNS])
        buffer.seek(0)
        return buffer

    @staticmethod
    def load(df, raw_event_id=None):
        """
        Idempotently load events into staging.market_events.

        Two steps in one transaction:

        1. COPY the batch into a temporary table. COPY is PostgreSQL's bulk
           loader: one streamed command instead of one INSERT per row (or per
           page of rows), so it scales to large batches.
        2. INSERT ... SELECT from it with ON CONFLICT (event_id) DO NOTHING.
           Re-running the same batch (Airflow retry, backfill, manual rerun)
           never creates duplicates, the database enforces it, and the
           foreign key to raw.raw_api_events (lineage) is still checked.
           The statement's row count is exactly the number inserted.

        Returns (inserted_count, skipped_count).
        """

        rows = PostgresLoader.build_rows(df, raw_event_id)

        logger.info(f"Starting PostgreSQL load. Records received: {len(rows)}")

        if not rows:
            return 0, 0

        session = SessionLocal()

        try:
            conn = session.connection()
            conn.exec_driver_sql(
                "CREATE TEMP TABLE _staging_load "
                "(LIKE staging.market_events INCLUDING DEFAULTS) ON COMMIT DROP"
            )

            # COPY needs the driver's own cursor (psycopg2), inside the same transaction.
            with conn.connection.dbapi_connection.cursor() as cursor:
                cursor.copy_expert(
                    f"COPY _staging_load ({_COLUMN_LIST}) FROM STDIN WITH (FORMAT csv)",
                    PostgresLoader._to_csv(rows),
                )

            # Through SQLAlchemy, so a lineage violation raises IntegrityError as before.
            result = conn.execute(text(
                f"INSERT INTO staging.market_events ({_COLUMN_LIST}) "  # noqa: S608
                f"SELECT {_COLUMN_LIST} FROM _staging_load "
                "ON CONFLICT (event_id) DO NOTHING"
            ))
            inserted_count = result.rowcount
            session.commit()

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
