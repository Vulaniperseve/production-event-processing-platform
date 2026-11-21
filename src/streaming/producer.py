"""
Kafka producer for market price ticks.

Two modes (STREAM_MODE):

* replay (default, free): fetches one trading day of real 1-minute prices
  per symbol from Twelve Data (1 API credit per symbol, cached on disk so a
  replay never calls the API again) and sends them in event-time order,
  REPLAY_SPEED times faster than real time. Each run of a replay sends the
  same events, so a second run shows the sink dropping duplicates.

* live: polls the latest price of every symbol every STREAM_INTERVAL
  seconds and sends it. Bounded by a daily credit budget (STREAM_MAX_CREDITS)
  and the free tier's 8 requests per minute, so it can never exhaust the
  API key the batch pipeline also needs.

    python -m src.streaming.producer

Producer settings: acks=all + idempotence (no duplicates or reordering from
retries inside Kafka), key = symbol (per-symbol ordering), zstd compression.
"""

import json
import logging
import math
import os
import time
from datetime import UTC, datetime
from pathlib import Path

import requests

from src.streaming.messages import bars_to_ticks, encode, make_tick, tick_problem, utc_iso

log = logging.getLogger("producer")

TOPIC = os.getenv("STREAM_TOPIC", "market.price_ticks")
API_URL = "https://api.twelvedata.com"
FREE_TIER_REQUESTS_PER_MINUTE = 8


class BudgetExhausted(RuntimeError):
    pass


class CreditBudget:
    """Stops the producer before it spends more API credits than allowed."""

    def __init__(self, max_credits):
        self.max_credits = max_credits
        self.spent = 0

    def spend(self, credits):
        if self.spent + credits > self.max_credits:
            raise BudgetExhausted(f"API credit budget of {self.max_credits} reached")
        self.spent += credits


def min_poll_interval(n_symbols, per_minute=FREE_TIER_REQUESTS_PER_MINUTE):
    """Every symbol costs 1 credit per poll; stay under the per-minute limit."""
    return math.ceil(60 * n_symbols / per_minute)


# ------------------------------------------------------------------ API
class TwelveData:
    def __init__(self, api_key, session=None, timeout=15):
        if not api_key:
            raise ValueError("TWELVE_DATA_API_KEY is not set")
        self.api_key = api_key
        self.session = session or requests.Session()
        self.timeout = timeout

    def _get(self, path, **params):
        # Key in a header, never in the URL (URLs end up in errors and logs).
        response = self.session.get(f"{API_URL}/{path}", timeout=self.timeout, params=params,
                                    headers={"Authorization": f"apikey {self.api_key}"})
        response.raise_for_status()
        data = response.json()
        if isinstance(data, dict) and data.get("status") == "error":
            raise RuntimeError(f"Twelve Data error: {data.get('message')}")
        return data

    def one_minute_bars(self, symbol, outputsize=390):
        return self._get("time_series", symbol=symbol, interval="1min", outputsize=outputsize)

    def latest_prices(self, symbols):
        data = self._get("price", symbol=",".join(symbols))
        if len(symbols) == 1:
            data = {symbols[0]: data}
        return {s: float(v["price"]) for s, v in data.items() if isinstance(v, dict) and "price" in v}


def cached_bars(api, symbol, cache_dir, max_age_hours=12):
    """1-minute bars from the cache, or from the API (1 credit) when stale."""
    path = Path(cache_dir) / f"{symbol}_1min.json"
    if path.exists() and time.time() - path.stat().st_mtime < max_age_hours * 3600:
        return json.loads(path.read_text()), False
    payload = api.one_minute_bars(symbol)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload))
    os.replace(tmp, path)
    return payload, True


# -------------------------------------------------------------- publish
class KafkaPublisher:
    def __init__(self, bootstrap, topic=TOPIC):
        from confluent_kafka import Producer

        self.bootstrap = bootstrap
        self.topic = topic
        self.errors = 0
        self.delivered = 0
        self.producer = Producer({
            "bootstrap.servers": bootstrap,
            "acks": "all",
            "enable.idempotence": True,
            "compression.type": "zstd",
            "linger.ms": 20,
            "client.id": "price-tick-producer",
        })

    def ensure_topic(self, partitions=3, retention_hours=24):
        from confluent_kafka.admin import AdminClient, NewTopic

        admin = AdminClient({"bootstrap.servers": self.bootstrap})
        if self.topic in admin.list_topics(timeout=10).topics:
            return False
        futures = admin.create_topics([NewTopic(
            self.topic, num_partitions=partitions, replication_factor=1,
            config={"retention.ms": str(retention_hours * 3600 * 1000)},
        )])
        try:
            futures[self.topic].result(timeout=30)
        except Exception as exc:          # created by someone else meanwhile
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise
        return True

    def _on_delivery(self, err, msg):
        if err is not None:
            self.errors += 1
            log.error("delivery failed: %s", err)
        else:
            self.delivered += 1

    def publish(self, tick):
        self.producer.produce(self.topic, key=tick["symbol"].encode(), value=encode(tick),
                              on_delivery=self._on_delivery)
        self.producer.poll(0)

    def flush(self, timeout=30):
        remaining = self.producer.flush(timeout)
        if remaining:
            raise RuntimeError(f"{remaining} messages were not delivered")


# ---------------------------------------------------------------- modes
def replay(publisher, payloads, speed=60.0, sleep=time.sleep):
    """
    Send every symbol's ticks interleaved in event-time order. The gap
    between two event times is waited for, divided by `speed`.
    """
    ticks = sorted((t for p in payloads for t in bars_to_ticks(p)), key=lambda t: t["event_ts"])
    sent, previous = 0, None
    for tick in ticks:
        current = datetime.fromisoformat(tick["event_ts"].replace("Z", "+00:00"))
        if previous is not None and current > previous:
            sleep((current - previous).total_seconds() / speed)
        previous = current
        if tick_problem(tick):
            log.warning("skipping invalid bar %s: %s", tick, tick_problem(tick))
            continue
        tick["produced_at"] = utc_iso(datetime.now(UTC))   # sent now, happened then
        publisher.publish(tick)
        sent += 1
    publisher.flush()
    return sent


def live(publisher, api, symbols, interval, budget, polls=None, sleep=time.sleep,
         now=lambda: datetime.now(UTC)):
    """Poll latest prices until the credit budget (or `polls`) runs out."""
    interval = max(interval, min_poll_interval(len(symbols)))
    sent, done = 0, 0
    while polls is None or done < polls:
        try:
            budget.spend(len(symbols))
        except BudgetExhausted as exc:
            log.info("%s; stopping live mode", exc)
            break
        observed = now().replace(microsecond=0)
        for symbol, price in api.latest_prices(symbols).items():
            publisher.publish(make_tick(symbol, observed, price, None, "live"))
            sent += 1
        publisher.flush()
        done += 1
        sleep(interval)
    return sent


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    mode = os.getenv("STREAM_MODE", "replay")
    symbols = [s.strip().upper() for s in os.getenv("STREAM_SYMBOLS", "AAPL,MSFT,GOOGL").split(",") if s.strip()]
    publisher = KafkaPublisher(os.getenv("KAFKA_BOOTSTRAP", "localhost:29092"))
    if publisher.ensure_topic():
        log.info("created topic %s", TOPIC)
    api = TwelveData(os.getenv("TWELVE_DATA_API_KEY"))

    if mode == "replay":
        cache = os.getenv("STREAM_CACHE_DIR", "data/stream_cache")
        payloads = []
        for symbol in symbols:
            payload, fetched = cached_bars(api, symbol, cache)
            log.info("%s: %s", symbol, "fetched from API (1 credit)" if fetched else "from cache (0 credits)")
            payloads.append(payload)
        speed = float(os.getenv("REPLAY_SPEED", "60"))
        sent = replay(publisher, payloads, speed=speed)
        log.info("replay finished: %d ticks sent, %d delivered, %d errors",
                 sent, publisher.delivered, publisher.errors)
    elif mode == "live":
        budget = CreditBudget(int(os.getenv("STREAM_MAX_CREDITS", "300")))
        sent = live(publisher, api, symbols, int(os.getenv("STREAM_INTERVAL", "30")), budget)
        log.info("live mode finished: %d ticks sent, %d credits used", sent, budget.spent)
    else:
        raise SystemExit(f"Unknown STREAM_MODE {mode!r} (use replay or live)")


if __name__ == "__main__":
    main()
