"""
Analytics layer (migration 002) against real PostgreSQL.

Inputs are tiny and hand-computable, so every expected number below can be
checked with a calculator.
"""

import math

import pytest

from src.analytics.analytics import refresh_analytics, symbol_summary
from src.pipeline import steps
from tests.integration.helpers import insert_prices, query, scalar


def rows(engine, view, symbol):
    return query(
        engine,
        f"SELECT * FROM analytics.{view} WHERE symbol = :s ORDER BY trade_date",
        s=symbol,
    )


def test_daily_returns(clean_db):
    insert_prices(clean_db, "TST", [100, 110, 99])

    r = query(
        clean_db,
        "SELECT close_price, prev_close, daily_return, log_return "
        "FROM analytics.daily_returns WHERE symbol = 'TST' ORDER BY trade_date",
    )

    assert r[0][1:] == (None, None, None)                  # no previous day
    assert r[1][2] == pytest.approx(0.10)                  # 110 / 100 - 1
    assert r[1][3] == pytest.approx(math.log(1.1))
    assert r[2][2] == pytest.approx(-0.10)                 # 99 / 110 - 1


def test_returns_are_per_symbol(clean_db):
    """PARTITION BY symbol: one symbol's last day never feeds another's LAG()."""
    insert_prices(clean_db, "AAA", [10, 20])
    insert_prices(clean_db, "BBB", [50, 55])

    first_bbb = query(
        clean_db,
        "SELECT prev_close FROM analytics.daily_returns "
        "WHERE symbol = 'BBB' ORDER BY trade_date LIMIT 1",
    )[0][0]

    assert first_bbb is None


def test_daily_prices_uses_last_observation_of_the_day(clean_db):
    with clean_db.begin() as conn:
        conn.exec_driver_sql("""
            INSERT INTO staging.market_events
                (event_id, entity_id, event_time, event_type, value, source_system)
            VALUES
                ('a', 'INT', '2026-01-05 10:00', 'MARKET_PRICE', 50, 'test'),
                ('b', 'INT', '2026-01-05 15:59', 'MARKET_PRICE', 55, 'test'),
                ('c', 'INT', '2026-01-05 16:00', 'VOLUME',     999, 'test')
        """)

    r = query(clean_db, "SELECT trade_date::text, close_price FROM analytics.daily_prices WHERE symbol = 'INT'")

    assert [tuple(x) for x in r] == [("2026-01-05", 55.0)]


def test_moving_averages_need_a_full_window(clean_db):
    insert_prices(clean_db, "SEQ", list(range(1, 26)))  # prices 1..25

    r = {row.trading_day_number: row for row in rows(clean_db, "price_indicators", "SEQ")}

    assert r[4].sma_5 is None
    assert r[5].sma_5 == pytest.approx(3.0)       # mean(1..5)
    assert r[19].sma_20 is None
    assert r[20].sma_20 == pytest.approx(10.5)    # mean(1..20)
    assert r[25].sma_20 == pytest.approx(15.5)    # mean(6..25)

    # volatility needs 20 *returns*, i.e. 21 prices
    assert r[20].volatility_20d is None
    assert r[21].volatility_20d is not None
    assert r[21].volatility_20d_annualized == pytest.approx(
        r[21].volatility_20d * math.sqrt(252)
    )


def test_drawdown_from_running_peak(clean_db):
    insert_prices(clean_db, "DD", [100, 120, 90, 130])

    r = rows(clean_db, "price_indicators", "DD")

    assert [x.running_peak for x in r] == [100, 120, 120, 130]
    assert r[2].drawdown == pytest.approx(90 / 120 - 1)   # -25%
    assert r[3].drawdown == pytest.approx(0.0)            # new high


def test_symbol_summary_is_materialized_and_refreshed(clean_db):
    insert_prices(clean_db, "TST", [100, 110, 99])

    # materialized: not visible until refreshed
    assert symbol_summary() == []

    counts = refresh_analytics()
    assert counts == {"analytics.symbol_summary": 1}

    (s,) = symbol_summary()
    assert s["trading_days"] == 3
    assert s["first_close"] == 100 and s["last_close"] == 99
    assert s["period_return"] == pytest.approx(-0.01)
    assert s["best_day_return"] == pytest.approx(0.10)
    assert str(s["best_day"]) == "2026-01-02"
    assert s["worst_day_return"] == pytest.approx(-0.10)
    assert str(s["worst_day"]) == "2026-01-03"
    assert s["max_drawdown"] == pytest.approx(-0.10)


def test_refresh_is_repeatable(clean_db):
    insert_prices(clean_db, "TST", [100, 110])
    refresh_analytics()
    insert_prices(clean_db, "NEW", [5, 6])

    assert refresh_analytics() == {"analytics.symbol_summary": 2}
    assert refresh_analytics() == {"analytics.symbol_summary": 2}


class OneBadRecordAPI:
    def extract(self, **kwargs):
        return {
            "meta": {"symbol": "AAPL"},
            "status": "ok",
            "values": [
                {"datetime": "2026-11-13", "close": "230.10"},
                {"datetime": "2026-11-12", "close": "228.40"},
                {"datetime": "2026-11-11", "close": "-1"},
            ],
        }


def test_pipeline_health_and_freshness(clean_db, monkeypatch):
    monkeypatch.setattr(steps, "TwelveDataExtractor", OneBadRecordAPI)
    steps.run_pipeline("AAPL")

    health = query(clean_db, "SELECT * FROM analytics.pipeline_health WHERE symbol = 'AAPL'")[0]

    assert health.api_responses == 1
    assert health.loaded == 1
    assert health.failed == 0
    assert health.records_received == 3
    assert health.records_quarantined == 1
    assert health.new_events_loaded == 2
    assert float(health.quarantine_rate) == pytest.approx(0.3333)

    assert str(scalar(
        clean_db,
        "SELECT latest_trade_date FROM analytics.data_freshness WHERE symbol = 'AAPL'",
    )) == "2026-11-13"
