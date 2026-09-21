from database.database import engine
from database.models import Base


def init_database():
    with engine.begin() as connection:
        connection.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS raw")
        connection.exec_driver_sql("CREATE SCHEMA IF NOT EXISTS staging")

    Base.metadata.create_all(bind=engine)


if __name__ == "__main__":
    init_database()
    print("Database schemas and tables initialized successfully.")