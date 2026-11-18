"""
The tick message: one price observation for one symbol, as sent to Kafka.

    key   = symbol  (all ticks of a symbol go to the same partition, in order)
    value = JSON:
        {"schema_version": 1, "event_id": "...", "symbol": "AAPL",
         "event_ts": "2026-11-17T20:31:00Z", "price": 231.2, "volume": 1200,
         "source": "replay", "produced_at": "2026-11-18T09:00:01.123Z"}

event_ts is the time the price happened (event time), always UTC. Twelve
Data reports exchange-local times, so they are converted here once.

event_id = sha256("SYMBOL|event_ts|PRICE_TICK")[:36], the same recipe as
the batch pipeline's ids: the same tick sent twice has the same id, which
is what lets the sink drop duplicates.
"""

import hashlib
import json
import math
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

SCHEMA_VERSION = 1
EVENT_TYPE = "PRICE_TICK"


def utc_iso(ts):
    """2026-11-17T20:31:00Z (seconds) or with milliseconds when present."""
    ts = ts.astimezone(timezone.utc)
    if ts.microsecond:
        return ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def tick_event_id(symbol, event_ts_iso):
    return hashlib.sha256(f"{symbol}|{event_ts_iso}|{EVENT_TYPE}".encode()).hexdigest()[:36]


def make_tick(symbol, event_ts, price, volume, source, produced_at=None):
    event_ts_iso = utc_iso(event_ts)
    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": tick_event_id(symbol, event_ts_iso),
        "symbol": symbol,
        "event_ts": event_ts_iso,
        "price": price,
        "volume": volume,
        "source": source,
        "produced_at": utc_iso(produced_at or datetime.now(timezone.utc)),
    }


def encode(tick):
    return json.dumps(tick, separators=(",", ":")).encode()


def _num(value, cast):
    try:
        return cast(value)
    except (TypeError, ValueError):
        return None


def bars_to_ticks(payload, source="replay"):
    """
    A Twelve Data 1-minute time_series response -> ticks for its latest
    trading day, oldest first. Each 1-minute bar becomes one tick at the
    bar's close price.
    """
    meta = payload.get("meta", {})
    symbol = meta.get("symbol")
    tz = ZoneInfo(meta.get("exchange_timezone") or "America/New_York")
    values = payload.get("values") or []
    if not symbol or not values:
        return []

    latest_day = max(v["datetime"][:10] for v in values)
    ticks = []
    for bar in values:
        if bar["datetime"][:10] != latest_day:
            continue
        local = datetime.strptime(bar["datetime"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=tz)
        ticks.append(make_tick(
            symbol, local,
            price=_num(bar.get("close"), float),
            volume=_num(bar.get("volume"), int),
            source=source,
        ))
    return sorted(ticks, key=lambda t: t["event_ts"])


def tick_problem(tick):
    """
    The same record rules as the batch validator and the Spark consumer.
    Returns the rejection reason, or None for a valid tick.
    """
    if not isinstance(tick, dict):
        return "Malformed JSON"
    if not tick.get("symbol"):
        return "Missing symbol"
    try:
        datetime.fromisoformat(str(tick.get("event_ts")).replace("Z", "+00:00"))
    except ValueError:
        return "Invalid timestamp"
    price = tick.get("price")
    if not isinstance(price, (int, float)) or isinstance(price, bool) \
            or not math.isfinite(price) or price <= 0:
        return "Non-positive or non-finite price"
    return None
