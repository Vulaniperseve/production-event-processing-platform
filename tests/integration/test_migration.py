"""The migration script and the SQLAlchemy models must agree with the database."""

import json

from database.migrate import apply_migrations
from database.models import Base
from tests.integration.helpers import (
    SCHEMAS,
    drop_schemas,
    migration_files,
    query,
    run_migration,
    run_sql_script,
    scalar,
    table_columns,
)


def test_runner_records_versions_and_skips_applied(clean_db):
    versions = [row[0] for row in query(
        clean_db, "SELECT version FROM public.schema_migrations ORDER BY version"
    )]
    assert versions == [path.stem for path in migration_files()]
    assert versions[:2] == ["001_raw_layer_lineage", "002_analytics_layer"]

    assert run_migration(clean_db) == []  # nothing pending


def test_runner_warns_when_applied_migration_was_edited(clean_db):
    run_sql_script(clean_db, "UPDATE public.schema_migrations SET checksum = 'x'")

    messages = []
    apply_migrations(clean_db, log=messages.append)

    assert any("modified after it was applied" in m for m in messages)


def test_every_migration_script_is_idempotent(clean_db):
    """Re-executing each script directly (bypassing the runner) changes nothing."""
    before = table_columns(clean_db)

    for path in migration_files():
        run_sql_script(clean_db, path.read_text(encoding="utf-8"))

    assert table_columns(clean_db) == before
    assert scalar(
        clean_db,
        "SELECT count(*) FROM information_schema.views WHERE table_schema = 'analytics'",
    ) == 5


def test_migration_upgrades_legacy_schema_and_keeps_data(engine):
    """Databases created before the raw layer was completed are upgraded in place."""
    drop_schemas(engine)

    legacy_payload = {
        "meta": {"symbol": "AAPL"},
        "status": "ok",
        "values": [{"datetime": "2026-09-01", "close": "200.0"}] * 3,
    }

    run_sql_script(engine, f"""
        CREATE SCHEMA raw;
        CREATE SCHEMA staging;
        CREATE TABLE raw.raw_api_events (
            id SERIAL PRIMARY KEY,
            source_system VARCHAR(50) NOT NULL,
            symbol VARCHAR(20),
            raw_json TEXT NOT NULL,
            extracted_at TIMESTAMP DEFAULT now()
        );
        CREATE TABLE staging.market_events (
            event_id VARCHAR(36) PRIMARY KEY,
            entity_id VARCHAR(20) NOT NULL,
            event_time TIMESTAMP,
            event_type VARCHAR(50),
            value DOUBLE PRECISION,
            source_system VARCHAR(50),
            loaded_at TIMESTAMP DEFAULT now()
        );
        INSERT INTO raw.raw_api_events (source_system, symbol, raw_json)
        VALUES ('TwelveData', 'AAPL', '{json.dumps(legacy_payload)}');
        INSERT INTO staging.market_events (event_id, entity_id, value)
        VALUES ('legacy-1', 'AAPL', 200.0);
    """)

    run_migration(engine)

    # data survived
    assert scalar(engine, "SELECT count(*) FROM raw.raw_api_events") == 1
    assert scalar(engine, "SELECT count(*) FROM staging.market_events") == 1

    # TEXT -> JSONB, and the JSON is queryable
    assert scalar(engine, "SELECT jsonb_typeof(raw_json) FROM raw.raw_api_events") == "object"
    assert scalar(engine, "SELECT raw_json->'meta'->>'symbol' FROM raw.raw_api_events") == "AAPL"

    # new columns backfilled from the payload
    row = query(
        engine,
        "SELECT api_status, record_count, processing_status, length(payload_hash) "
        "FROM raw.raw_api_events",
    )[0]
    assert tuple(row) == ("ok", 3, "PENDING", 64)

    # lineage column added; legacy rows simply have no lineage
    assert scalar(engine, "SELECT raw_event_id FROM staging.market_events") is None


def test_models_match_migration(engine):
    """
    Schema drift check: a database built by Base.metadata.create_all
    (database/init_db.py) must have exactly the same columns as one built
    by the migration script.
    """
    drop_schemas(engine)
    with engine.begin() as conn:
        for schema in SCHEMAS:
            conn.exec_driver_sql(f"CREATE SCHEMA {schema}")
    Base.metadata.create_all(bind=engine)
    from_models = table_columns(engine)

    drop_schemas(engine)
    run_migration(engine)
    from_migration = table_columns(engine)

    assert from_models == from_migration
