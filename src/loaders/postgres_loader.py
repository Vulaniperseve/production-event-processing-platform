from database.database import SessionLocal
from database.models import MarketEvent


class PostgresLoader:

    @staticmethod
    def load(df):

        session = SessionLocal()

        try:

            for _, row in df.iterrows():

                event = MarketEvent(

                    event_id=row["event_id"],
                    entity_id=row["entity_id"],
                    event_time=row["event_time"],
                    event_type=row["event_type"],
                    value=float(row["value"]),
                    source_system=row["source_system"]

                )

                session.merge(event)

            session.commit()

            print(f"✓ Loaded {len(df)} records into PostgreSQL.")

        except Exception as e:

            session.rollback()

            print(e)

        finally:

            session.close()