"""Spark small-data mode: on by default, can be switched off, and never overrides a caller."""

import pytest

from src.lake import spark as spark_cfg


def test_small_data_mode_on_by_default(monkeypatch):
    monkeypatch.delenv("SPARK_SMALL_DATA_MODE", raising=False)
    assert spark_cfg.small_data_mode() is True


@pytest.mark.parametrize("value", ["false", "False", "0", "no"])
def test_small_data_mode_can_be_switched_off(monkeypatch, value):
    monkeypatch.setenv("SPARK_SMALL_DATA_MODE", value)
    assert spark_cfg.small_data_mode() is False


def test_settings_are_startup_tuning_only():
    # Nothing here may change results: only JIT, garbage collector and codegen.
    assert set(spark_cfg.SMALL_DATA_CONF) == {
        "spark.driver.extraJavaOptions", "spark.sql.codegen.wholeStage",
    }
    assert "TieredStopAtLevel=1" in spark_cfg.SMALL_DATA_CONF["spark.driver.extraJavaOptions"]
