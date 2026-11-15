"""
Create or upgrade the database schema.

The SQL migrations in database/migrations/ are the single source of truth.
(The SQLAlchemy models must match them; tests/integration checks for drift.)
"""

from database.database import engine
from database.migrate import apply_migrations


def init_database():
    return apply_migrations(engine)


if __name__ == "__main__":
    init_database()
    print("Database schema is up to date.")
