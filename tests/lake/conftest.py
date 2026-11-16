"""
Spark tests. Skipped automatically when PySpark or Java is unavailable
(e.g. a Windows venv); they run in the Airflow image and in CI.
"""

import importlib.util
import os
import shutil

import pytest

SPARK_AVAILABLE = (
    importlib.util.find_spec("pyspark") is not None
    and (shutil.which("java") is not None or bool(os.getenv("JAVA_HOME")))
)

if not SPARK_AVAILABLE:
    collect_ignore_glob = ["test_*.py"]


def pytest_collection_modifyitems(config, items):
    for item in items:
        if "tests/lake/" in str(item.fspath).replace("\\", "/"):
            item.add_marker(pytest.mark.spark)


@pytest.fixture(scope="session")
def spark():
    os.environ.setdefault("SPARK_MASTER", "local[2]")
    os.environ.setdefault("SPARK_SHUFFLE_PARTITIONS", "2")

    from src.lake.spark import build_spark

    session = build_spark("tests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def lake(tmp_path):
    """An empty lake root for one test."""
    return tmp_path / "lake"


def land(lake, raw_event_id, extracted_at, prices, symbol="AAPL", status="ok"):
    """Write one bronze response. prices: [(datetime_str, close), ...]."""
    from src.lake.bronze import write_bronze_record

    return write_bronze_record(
        {
            "raw_event_id": raw_event_id,
            "source_system": "TwelveData",
            "symbol": symbol,
            "extracted_at": extracted_at,
            "payload_hash": f"hash-{raw_event_id}",
            "payload": {
                "status": status,
                "meta": {"symbol": symbol, "interval": "1day"},
                "values": [{"datetime": d, "close": c} for d, c in prices],
            },
        },
        root=lake,
    )
