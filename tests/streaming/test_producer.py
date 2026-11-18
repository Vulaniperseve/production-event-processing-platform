import json
from datetime import datetime, timezone

import pytest

from src.streaming import producer
from src.streaming.producer import (
    BudgetExhausted, CreditBudget, TwelveData, cached_bars, live, min_poll_interval, replay,
)
from tests.streaming.test_messages import bar, payload


class FakePublisher:
    def __init__(self):
        self.sent = []
        self.flushes = 0

    def publish(self, tick):
        self.sent.append(tick)

    def flush(self, timeout=30):
        self.flushes += 1


def test_credit_budget():
    budget = CreditBudget(5)
    budget.spend(3)
    with pytest.raises(BudgetExhausted):
        budget.spend(3)
    assert budget.spent == 3


@pytest.mark.parametrize("symbols, seconds", [(1, 8), (3, 23), (8, 60)])
def test_min_poll_interval_respects_free_tier(symbols, seconds):
    assert min_poll_interval(symbols) == seconds


def test_replay_interleaves_symbols_in_event_time_order_and_paces():
    aapl = payload([bar("2026-11-17 09:31:00", "2"), bar("2026-11-17 09:30:00", "1")])
    msft = payload([bar("2026-11-17 09:30:00", "10"), bar("2026-11-17 09:32:00", "12")], symbol="MSFT")
    pub, sleeps = FakePublisher(), []

    sent = replay(pub, [aapl, msft], speed=60, sleep=sleeps.append)

    assert sent == 4
    assert [(t["symbol"], t["event_ts"][11:16]) for t in pub.sent] == [
        ("AAPL", "14:30"), ("MSFT", "14:30"), ("AAPL", "14:31"), ("MSFT", "14:32")]
    assert sleeps == [1.0, 1.0]      # one minute of event time = 1 s at 60x
    assert pub.flushes == 1
    assert all(t["source"] == "replay" for t in pub.sent)


def test_replay_skips_invalid_bars():
    pub = FakePublisher()
    sent = replay(pub, [payload([bar("2026-11-17 09:30:00", "n/a"), bar("2026-11-17 09:31:00", "5")])],
                  sleep=lambda s: None)
    assert sent == 1 and pub.sent[0]["price"] == 5.0


class FakeApi:
    def __init__(self):
        self.calls = 0

    def latest_prices(self, symbols):
        self.calls += 1
        return {s: 100.0 + i for i, s in enumerate(symbols)}

    def one_minute_bars(self, symbol, outputsize=390):
        self.calls += 1
        return payload([bar("2026-11-17 09:30:00")], symbol=symbol)


def test_live_stops_when_the_budget_runs_out():
    pub, api, sleeps = FakePublisher(), FakeApi(), []
    budget = CreditBudget(7)

    sent = live(pub, api, ["AAPL", "MSFT", "GOOGL"], interval=1, budget=budget, sleep=sleeps.append,
                now=lambda: datetime(2026, 11, 17, 20, 31, 5, 999, tzinfo=timezone.utc))

    assert api.calls == 2                       # 3 + 3 credits; a third poll would need 9
    assert sent == 6 and budget.spent == 6
    assert sleeps == [23, 23]                   # 1 s requested, raised to the free-tier minimum
    assert pub.sent[0]["event_ts"] == "2026-11-17T20:31:05Z"
    assert pub.sent[0]["source"] == "live"


def test_cached_bars_calls_the_api_only_once(tmp_path):
    api = FakeApi()
    first, fetched1 = cached_bars(api, "AAPL", tmp_path)
    second, fetched2 = cached_bars(api, "AAPL", tmp_path)

    assert (fetched1, fetched2) == (True, False)
    assert api.calls == 1
    assert first == second
    assert json.loads((tmp_path / "AAPL_1min.json").read_text())["meta"]["symbol"] == "AAPL"


responses = pytest.importorskip("responses")


def test_twelve_data_client():
    with responses.RequestsMock() as rsps:
        rsps.get(f"{producer.API_URL}/price", json={"AAPL": {"price": "231.5"}, "MSFT": {"price": "500"}})
        rsps.get(f"{producer.API_URL}/price", json={"price": "231.5"})
        rsps.get(f"{producer.API_URL}/time_series", json={"status": "error", "message": "API credits exhausted"})
        api = TwelveData("key")

        assert api.latest_prices(["AAPL", "MSFT"]) == {"AAPL": 231.5, "MSFT": 500.0}
        assert api.latest_prices(["AAPL"]) == {"AAPL": 231.5}
        with pytest.raises(RuntimeError, match="credits exhausted"):
            api.one_minute_bars("AAPL")
        assert "apikey=key" in rsps.calls[0].request.url


def test_missing_api_key_is_rejected():
    with pytest.raises(ValueError, match="TWELVE_DATA_API_KEY"):
        TwelveData(None)
