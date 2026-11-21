import json
import math
from datetime import UTC, datetime, timedelta, timezone

import pytest

from src.streaming.messages import (
    bars_to_ticks,
    encode,
    make_tick,
    tick_event_id,
    tick_problem,
    utc_iso,
)

UTC = UTC


def payload(values, symbol="AAPL", tz="America/New_York"):
    return {"meta": {"symbol": symbol, "interval": "1min", "exchange_timezone": tz},
            "values": values, "status": "ok"}


def bar(dt, close="231.50", volume="1200"):
    return {"datetime": dt, "open": close, "high": close, "low": close, "close": close, "volume": volume}


def test_utc_iso_formats():
    assert utc_iso(datetime(2026, 11, 17, 20, 31, tzinfo=UTC)) == "2026-11-17T20:31:00Z"
    assert utc_iso(datetime(2026, 11, 17, 20, 31, 0, 123456, tzinfo=UTC)) == "2026-11-17T20:31:00.123Z"
    sast = timezone(timedelta(hours=2))
    assert utc_iso(datetime(2026, 11, 17, 22, 31, tzinfo=sast)) == "2026-11-17T20:31:00Z"


def test_event_id_is_deterministic_and_specific():
    a = tick_event_id("AAPL", "2026-11-17T20:31:00Z")
    assert a == tick_event_id("AAPL", "2026-11-17T20:31:00Z")
    assert len(a) == 36
    assert a != tick_event_id("MSFT", "2026-11-17T20:31:00Z")
    assert a != tick_event_id("AAPL", "2026-11-17T20:32:00Z")


def test_make_tick_message_contract():
    tick = make_tick("AAPL", datetime(2026, 11, 17, 20, 31, tzinfo=UTC), 231.5, 1200, "replay",
                     produced_at=datetime(2026, 11, 18, 9, 0, tzinfo=UTC))
    assert tick == {
        "schema_version": 1,
        "event_id": tick_event_id("AAPL", "2026-11-17T20:31:00Z"),
        "symbol": "AAPL",
        "event_ts": "2026-11-17T20:31:00Z",
        "price": 231.5,
        "volume": 1200,
        "source": "replay",
        "produced_at": "2026-11-18T09:00:00Z",
    }
    assert json.loads(encode(tick)) == tick


def test_bars_to_ticks_converts_exchange_time_to_utc_and_sorts():
    ticks = bars_to_ticks(payload([bar("2026-11-17 15:59:00", "231.9"),
                                   bar("2026-11-17 09:30:00", "229.1")]))

    assert [t["event_ts"] for t in ticks] == ["2026-11-17T14:30:00Z", "2026-11-17T20:59:00Z"]
    assert [t["price"] for t in ticks] == [229.1, 231.9]
    assert ticks[0]["volume"] == 1200


def test_bars_to_ticks_handles_daylight_saving():
    # New York is UTC-4 in summer (EDT) and UTC-5 in winter (EST).
    summer = bars_to_ticks(payload([bar("2026-07-01 09:30:00")]))
    assert summer[0]["event_ts"] == "2026-07-01T13:30:00Z"


def test_bars_to_ticks_keeps_only_the_latest_trading_day():
    ticks = bars_to_ticks(payload([bar("2026-11-17 09:30:00"), bar("2026-11-16 15:59:00"),
                                   bar("2026-11-17 09:31:00")]))
    assert len(ticks) == 2
    assert all(t["event_ts"].startswith("2026-11-17") for t in ticks)


def test_bars_to_ticks_unparseable_numbers_become_none():
    ticks = bars_to_ticks(payload([bar("2026-11-17 09:30:00", close="n/a", volume="x")]))
    assert ticks[0]["price"] is None and ticks[0]["volume"] is None
    assert tick_problem(ticks[0]) == "Non-positive or non-finite price"


def test_bars_to_ticks_empty_or_error_payload():
    assert bars_to_ticks({"status": "error", "message": "limit"}) == []
    assert bars_to_ticks(payload([])) == []


GOOD = make_tick("AAPL", datetime(2026, 11, 17, 20, 31, tzinfo=UTC), 231.5, 10, "live")


@pytest.mark.parametrize("tick, reason", [
    (GOOD, None),
    ("not a dict", "Malformed JSON"),
    ({**GOOD, "symbol": ""}, "Missing symbol"),
    ({**GOOD, "symbol": None}, "Missing symbol"),
    ({**GOOD, "event_ts": "yesterday"}, "Invalid timestamp"),
    ({**GOOD, "price": 0}, "Non-positive or non-finite price"),
    ({**GOOD, "price": -1.5}, "Non-positive or non-finite price"),
    ({**GOOD, "price": math.inf}, "Non-positive or non-finite price"),
    ({**GOOD, "price": math.nan}, "Non-positive or non-finite price"),
    ({**GOOD, "price": "231.5"}, "Non-positive or non-finite price"),
    ({**GOOD, "price": True}, "Non-positive or non-finite price"),
])
def test_tick_problem(tick, reason):
    assert tick_problem(tick) == reason
