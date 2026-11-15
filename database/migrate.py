"""
Minimal versioned migration runner (the idea behind Flyway / Alembic).

Applies database/migrations/NNN_*.sql in filename order and records each
applied version in public.schema_migrations, so every environment (local,
CI, Docker) reaches the same schema by running one command:

    python -m database.migrate

Each migration script is also written to be idempotent, so re-applying one
(e.g. on a database migrated by hand before this runner existed) is safe.
"""

import hashlib
from pathlib import Path

MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"

TRACKING_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS public.schema_migrations (
    version     VARCHAR(255) PRIMARY KEY,
    checksum    VARCHAR(64)  NOT NULL,
    applied_at  TIMESTAMP    NOT NULL DEFAULT now()
)
"""


def checksum_of(sql):
    """
    SHA-256 of the script with line endings normalised, so a Windows
    checkout (CRLF) and a Linux one (LF) of the same file agree.
    """
    normalised = sql.replace("\r\n", "\n")
    return hashlib.sha256(normalised.encode("utf-8")).hexdigest()


def migration_files():
    return sorted(MIGRATIONS_DIR.glob("[0-9][0-9][0-9]_*.sql"))


def _execute_script(engine, sql):
    # Commit explicitly: setting autocommit on SQLAlchemy's pooled wrapper
    # does not reach the underlying psycopg2 connection.
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            cur.execute(sql)
        raw.commit()
    except Exception:
        raw.rollback()
        raise
    finally:
        raw.close()


def applied_versions(engine):
    _execute_script(engine, TRACKING_TABLE_DDL)
    with engine.connect() as conn:
        rows = conn.exec_driver_sql(
            "SELECT version, checksum FROM public.schema_migrations"
        ).fetchall()
    return {version: checksum for version, checksum in rows}


def apply_migrations(engine, log=print):
    """Apply pending migrations. Returns the list of versions applied."""

    done = applied_versions(engine)
    newly_applied = []

    for path in migration_files():
        version = path.stem
        sql = path.read_text(encoding="utf-8")
        checksum = checksum_of(sql)

        if version in done:
            if done[version] != checksum:
                log(
                    f"WARNING: {version} was modified after it was applied. "
                    f"Add a new migration instead of editing an applied one."
                )
            continue

        log(f"Applying {version} ...")
        _execute_script(engine, sql)

        with engine.begin() as conn:
            conn.exec_driver_sql(
                "INSERT INTO public.schema_migrations (version, checksum) "
                "VALUES (%(version)s, %(checksum)s)",
                {"version": version, "checksum": checksum},
            )

        newly_applied.append(version)

    log(
        f"Migrations applied: {newly_applied}" if newly_applied
        else "Database is up to date."
    )
    return newly_applied


if __name__ == "__main__":
    from database.database import engine

    apply_migrations(engine)
