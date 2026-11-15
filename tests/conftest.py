import os
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

# Unit tests never connect to a database, but importing database.database
# builds an engine, which needs a well-formed URL.
for key, value in {
    "DB_HOST": "localhost",
    "DB_PORT": "5432",
    "DB_NAME": "test_db",
    "DB_USER": "test_user",
    "DB_PASSWORD": "test_password",
}.items():
    os.environ.setdefault(key, value)


@pytest.fixture
def api_payload():
    return {
        "meta": {"symbol": "AAPL", "interval": "1day"},
        "status": "ok",
        "values": [
            {"datetime": "2026-08-26", "close": "230.50"},
            {"datetime": "2026-08-25", "close": "229.10"},
            {"datetime": None, "close": "231.20"},
            {"datetime": "2026-08-24", "close": "ERROR"},
            {"close": "232.10"},
        ],
    }


@pytest.fixture(autouse=True)
def isolated_cwd(tmp_path, monkeypatch):
    """Keep files written by the pipeline (CSV, quarantine) out of the repo."""
    monkeypatch.chdir(tmp_path)
