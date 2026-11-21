"""Gold layer: hand-computable checks of the Spark analytics."""

import math

import pytest

from src.lake.config import gold_path
from src.lake.gold import build_gold
from src.lake.silver import build_silver
from tests.lake.conftest import land


def build(spark, lake, series):
    """series: {symbol: [(date, close), ...]} -> bronze -> silver -> gold."""
    for i, (symbol, prices) in enumerate(series.items(), start=1):
        land(lake, i, "2026-11-15T06:00:00", [(d, str(c)) for d, c in prices], symbol=symbol)
    build_silver(spark, root=lake)
    return build_gold(spark, root=lake)


def days(n, start=1, month="2026-10"):
    return [f"{month}-{d:02d}" for d in range(start, start + n)]


def read(spark, lake, table, symbol=None):
    df = spark.read.parquet(str(gold_path(table, lake)))
    if symbol:
        df = df.where(df.symbol == symbol)
    return df


def test_daily_returns_and_drawdown(spark, lake):
    build(spark, lake, {"TST": list(zip(days(4), [100, 110, 99, 120], strict=True))})

    r = read(spark, lake, "daily_metrics", "TST").orderBy("trade_date").collect()

    assert r[0].daily_return is None
    assert r[1].daily_return == pytest.approx(0.10)
    assert r[2].daily_return == pytest.approx(-0.10)
    assert r[1].log_return == pytest.approx(math.log(1.1))
    assert [x.running_peak for x in r] == [100, 110, 110, 120]
    assert r[2].drawdown == pytest.approx(99 / 110 - 1)
    assert r[3].drawdown == pytest.approx(0.0)


def test_rolling_windows_need_a_full_window(spark, lake):
    build(spark, lake, {"SEQ": list(zip(days(25), range(1, 26), strict=True))})

    r = {x.trading_day_number: x for x in read(spark, lake, "daily_metrics", "SEQ").collect()}

    assert r[4].sma_5 is None and r[5].sma_5 == pytest.approx(3.0)
    assert r[19].sma_20 is None and r[20].sma_20 == pytest.approx(10.5)
    assert r[20].volatility_20d is None and r[21].volatility_20d is not None
    assert r[21].volatility_20d_annualized == pytest.approx(r[21].volatility_20d * math.sqrt(252))


def test_gold_matches_postgres_analytics_definitions(spark, lake):
    """Same inputs as tests/integration/test_analytics.py, same answers."""
    build(spark, lake, {"DD": list(zip(days(4), [100, 120, 90, 130], strict=True))})

    r = read(spark, lake, "daily_metrics", "DD").orderBy("trade_date").collect()

    assert [x.running_peak for x in r] == [100, 120, 120, 130]
    assert r[2].drawdown == pytest.approx(-0.25)


def test_return_correlation(spark, lake):
    up_down = [100, 110, 99, 108.9, 98.01]          # +10%, -10%, +10%, -10%
    same = [2 * p for p in up_down]                  # identical returns
    opposite = [100, 90, 99, 89.1, 98.01]            # -10%, +10%, -10%, +10%

    build(spark, lake, {
        "AAA": list(zip(days(5), up_down, strict=True)),
        "BBB": list(zip(days(5), same, strict=True)),
        "CCC": list(zip(days(5), opposite, strict=True)),
    })

    corr = {
        (r.symbol_a, r.symbol_b): r
        for r in read(spark, lake, "return_correlation").collect()
    }

    assert sorted(corr) == [("AAA", "BBB"), ("AAA", "CCC"), ("BBB", "CCC")]  # each pair once
    assert corr[("AAA", "BBB")].return_correlation == pytest.approx(1.0)
    assert corr[("AAA", "CCC")].return_correlation == pytest.approx(-1.0)
    assert corr[("AAA", "BBB")].overlapping_days == 4


def test_monthly_summary(spark, lake):
    october = list(zip(days(3, start=28), [100, 102, 104], strict=True))          # last Oct close 104
    november = list(zip(days(2, start=2, month="2026-11"), [110, 114.4], strict=True))
    build(spark, lake, {"MON": october + november})

    rows = {r.month: r for r in read(spark, lake, "monthly_summary").collect()}

    assert rows["2026-10"].trading_days == 3
    assert (rows["2026-10"].first_close, rows["2026-10"].last_close) == (100, 104)
    assert rows["2026-10"].monthly_return is None                    # no prior month
    assert rows["2026-11"].monthly_return == pytest.approx(114.4 / 104 - 1)  # +10%


def test_gold_is_fully_rebuilt(spark, lake, tmp_path):
    counts = build(spark, lake, {"AAA": list(zip(days(3), [1, 2, 3], strict=True)),
                                 "BBB": list(zip(days(3), [3, 2, 1], strict=True))})
    assert counts == {"daily_metrics": 6, "monthly_summary": 2, "return_correlation": 1}

    # rebuild from the same silver: identical counts, no leftovers
    assert build_gold(spark, root=lake) == counts
    assert sorted(p.name for p in gold_path("daily_metrics", lake).glob("symbol=*")) == [
        "symbol=AAA", "symbol=BBB",
    ]
