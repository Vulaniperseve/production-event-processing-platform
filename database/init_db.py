from database.database import engine
from database.models import Base


SCHEMAS = ("raw", "staging", "quarantine")


def init_database():
    with engine.begin() as connection:
        for schema in SCHEMAS:
            connection.exec_driver_sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    Base.metadata.create_all(bind=engine)


if __name__ == "__main__":
    init_database()
    print("Database schemas and tables initialized successfully.")
