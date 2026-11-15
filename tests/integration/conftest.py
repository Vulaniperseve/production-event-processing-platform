"""
Integration test fixtures: these tests talk to a REAL PostgreSQL database.

They are skipped unless RUN_INTEGRATION_TESTS=1 is set, and they refuse to
run unless DB_NAME contains "test", because every test DROPS and recreates
the raw, staging and quarantine schemas.

Local run (throwaway container, never your Airflow database):

    docker run -d --name pg-test -e POSTGRES_PASSWORD=postgres \
        -e POSTGRES_DB=pipeline_test -p 5433:5432 postgres:16

    RUN_INTEGRATION_TESTS=1 DB_HOST=localhost DB_PORT=5433 \
    DB_NAME=pipeline_test DB_USER=postgres DB_PASSWORD=postgres \
        python -m pytest -m integration -v
"""

import os
from pathlib import Path

import pytest

from tests.integration.helpers import drop_schemas, run_migration


def pytest_collection_modifyitems(config, items):
    here = Path(__file__).parent
    for item in items:
        if Path(str(item.fspath)).is_relative_to(here):
            item.add_marker(pytest.mark.integration)


@pytest.fixture(scope="session")
def engine():
    if os.getenv("RUN_INTEGRATION_TESTS") != "1":
        pytest.skip("Set RUN_INTEGRATION_TESTS=1 to run integration tests.")

    db_name = os.getenv("DB_NAME", "")
    if "test" not in db_name.lower():
        pytest.exit(
            f"Refusing to run destructive integration tests against "
            f"DB_NAME={db_name!r}: the name must contain 'test'.",
            returncode=2,
        )

    from database.database import engine as app_engine

    return app_engine


@pytest.fixture
def clean_db(engine):
    """A fresh database built by the migration script, for every test."""
    drop_schemas(engine)
    run_migration(engine)
    yield engine
    engine.dispose()
