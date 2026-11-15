"""Shared helpers for integration tests (real PostgreSQL)."""

from pathlib import Path

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MIGRATION = PROJECT_ROOT / "database" / "migrations" / "001_raw_layer_lineage.sql"
SCHEMAS = ("raw", "staging", "quarantine")


def drop_schemas(engine):
    with engine.begin() as conn:
        for schema in SCHEMAS:
            conn.exec_driver_sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def run_sql_script(engine, sql):
    """Execute a multi-statement SQL script the way psql would."""
    # Note: setting `autocommit` on the pooled wrapper would NOT reach the
    # underlying psycopg2 connection, and the pool rolls back on close.
    # Commit explicitly instead.
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            cur.execute(sql)
        raw.commit()
    finally:
        raw.close()


def run_migration(engine):
    run_sql_script(engine, MIGRATION.read_text(encoding="utf-8"))


def query(engine, sql, **params):
    with engine.connect() as conn:
        return conn.execute(text(sql), params).fetchall()


def scalar(engine, sql, **params):
    return query(engine, sql, **params)[0][0]


def table_columns(engine):
    """{(schema, table, column): (data_type, max_length, is_nullable)}"""
    rows = query(
        engine,
        """
        SELECT table_schema, table_name, column_name,
               data_type, character_maximum_length, is_nullable
        FROM information_schema.columns
        WHERE table_schema IN ('raw', 'staging', 'quarantine')
        """,
    )
    return {(r[0], r[1], r[2]): (r[3], r[4], r[5]) for r in rows}
