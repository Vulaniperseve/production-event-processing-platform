"""Shared helpers for integration tests (real PostgreSQL)."""

from sqlalchemy import text

from database.migrate import MIGRATIONS_DIR, apply_migrations, migration_files

SCHEMAS = ("raw", "staging", "quarantine", "analytics", "streaming")
TABLE_SCHEMAS = ("raw", "staging", "quarantine")


def drop_schemas(engine):
    with engine.begin() as conn:
        for schema in SCHEMAS:
            conn.exec_driver_sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn.exec_driver_sql("DROP TABLE IF EXISTS public.schema_migrations")


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
    """Bring the database up to date with the project's migration runner."""
    return apply_migrations(engine, log=lambda message: None)


def query(engine, sql, **params):
    with engine.connect() as conn:
        return conn.execute(text(sql), params).fetchall()


def scalar(engine, sql, **params):
    return query(engine, sql, **params)[0][0]


def table_columns(engine):
    """{(schema, table, column): (data_type, max_length, is_nullable)} for base tables."""
    rows = query(
        engine,
        """
        SELECT c.table_schema, c.table_name, c.column_name,
               c.data_type, c.character_maximum_length, c.is_nullable
        FROM information_schema.columns c
        JOIN information_schema.tables t
          ON t.table_schema = c.table_schema AND t.table_name = c.table_name
        WHERE c.table_schema IN ('raw', 'staging', 'quarantine')
          AND t.table_type = 'BASE TABLE'
        """,
    )
    return {(r[0], r[1], r[2]): (r[3], r[4], r[5]) for r in rows}


def insert_prices(engine, symbol, prices, start_day=1, event_type="MARKET_PRICE"):
    """Insert daily close prices for 2026-01-<start_day..> straight into staging."""
    with engine.begin() as conn:
        for offset, price in enumerate(prices):
            day = start_day + offset
            conn.execute(
                text(
                    "INSERT INTO staging.market_events "
                    "(event_id, entity_id, event_time, event_type, value, source_system) "
                    "VALUES (:id, :symbol, :ts, :type, :value, 'test')"
                ),
                {
                    "id": f"{symbol}-{event_type}-{day}",
                    "symbol": symbol,
                    "ts": f"2026-01-{day:02d}",
                    "type": event_type,
                    "value": price,
                },
            )


__all__ = [
    "MIGRATIONS_DIR",
    "migration_files",
    "SCHEMAS",
    "TABLE_SCHEMAS",
    "drop_schemas",
    "run_sql_script",
    "run_migration",
    "query",
    "scalar",
    "table_columns",
    "insert_prices",
]
