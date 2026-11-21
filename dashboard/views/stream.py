"""Live stream page: Kafka -> Spark Structured Streaming -> PostgreSQL, live."""

from datetime import UTC, datetime

import pandas as pd
import streamlit as st

from dashboard.lib import charts
from dashboard.lib import resources as res

st.title("Live stream")
st.caption(
    "Price ticks flow through Kafka (topic `market.price_ticks`, keyed by symbol) into a "
    "Spark Structured Streaming job. Spark validates every tick, drops duplicates and builds "
    "5-minute price bars by event time, then writes to PostgreSQL with exactly-once results."
)

try:
    available = res.stream_available()
except Exception as exc:
    st.error(f"PostgreSQL isn't reachable: {exc}")
    st.stop()

HOW_TO = (
    "Start streaming by double-clicking **`stream.bat`**. It starts Kafka and the Spark "
    "streaming job, then replays a real trading day of 1-minute prices (about 7 minutes). "
    "Run it again to watch duplicates being dropped."
)

if not available:
    st.info(HOW_TO, icon=":material/info:")
    st.stop()


def _ago(ts):
    if ts is None or pd.isna(ts):
        return None
    seconds = (datetime.now(UTC) - pd.Timestamp(ts).to_pydatetime()).total_seconds()
    return max(seconds, 0)


def _fmt_ago(seconds):
    if seconds is None:
        return "never"
    if seconds < 60:
        return f"{int(seconds)}s ago"
    if seconds < 3600:
        return f"{int(seconds // 60)} min ago"
    return f"{int(seconds // 3600)} h ago"


def live_section():
    o = res.stream_overview()
    since_last = _ago(o["last_processed"])
    streaming_now = since_last is not None and since_last < 60

    if int(o["ticks"]) == 0:
        st.info(HOW_TO, icon=":material/info:")
        return
    if streaming_now:
        st.success(f"**Receiving ticks** · last batch processed {_fmt_ago(since_last)}",
                   icon=":material/sensors:")
    else:
        st.info(f"Stream idle · last batch processed {_fmt_ago(since_last)}. {HOW_TO}",
                icon=":material/pause_circle:")

    a, b, c, d, e = st.columns(5)
    a.metric("Ticks stored", f"{int(o['ticks']):,}", help="Unique ticks: one row per event_id")
    b.metric("Last 5 minutes", f"{int(o['ticks_5m']):,}")
    p50, p95 = o["latency_p50"], o["latency_p95"]
    def secs(v):
        return f"{v:.0f}s" if v >= 10 else f"{v:.1f}s"

    c.metric("Latency p50 · p95", "–" if p50 is None or pd.isna(p50) else f"{secs(p50)} · {secs(p95)}",
             help="From the producer sending a tick to it being stored in PostgreSQL, over the "
                  "last 1,000 ticks (includes the 5-second micro-batch interval). Ticks that "
                  "waited in Kafka while Spark was down show up here as high latency.")
    d.metric("Duplicates dropped", f"{int(o['duplicates']):,}",
             help="Ticks received again (a replay or a retried batch) and ignored by event_id")
    e.metric("Rejected", f"{int(o['rejected']):,}", help="Invalid messages, kept with a reason")

    symbols = res.stream_symbols()
    if symbols:
        left, right = st.columns([3, 2])
        with left:
            st.markdown("**5-minute price bars** · `streaming.price_bars_5m` (event-time windows)")
            symbol = st.session_state.get("stream_symbol")
            if symbol not in symbols:
                symbol = symbols[0]
            bars = res.stream_bars(symbol)
            st.plotly_chart(charts.candlestick_chart(bars, symbol), width="stretch",
                            config={"displayModeBar": False}, key=f"bars_{symbol}")
            st.caption(f"{symbol}: {len(bars)} bars, {int(bars['ticks'].sum()) if not bars.empty else 0} "
                       "ticks. Times in UTC.")
        with right:
            st.markdown("**Latest ticks**")
            st.dataframe(res.stream_latest_ticks(), hide_index=True, width="stretch", height=360)

    throughput = res.stream_throughput()
    if not throughput.empty:
        st.markdown("**Throughput per minute** · from `streaming.batch_log`")
        st.plotly_chart(charts.throughput_chart(throughput), width="stretch",
                        config={"displayModeBar": False})

    if streaming_now != st.session_state.get("stream_live"):
        st.session_state["stream_live"] = streaming_now
        st.rerun(scope="app")      # switch between 3 s (live) and 30 s (idle) refresh


try:
    symbols = res.stream_symbols()
except Exception:
    symbols = []
if len(symbols) > 1:
    st.segmented_control("Symbol", symbols, key="stream_symbol", default=symbols[0],
                         label_visibility="collapsed")

interval = 3 if st.session_state.get("stream_live", True) else 30
st.fragment(run_every=interval)(live_section)()

with st.expander("Rejected messages", icon=":material/block:"):
    rejects = res.stream_rejects()
    if rejects.empty:
        st.caption("No invalid messages so far.")
    else:
        st.dataframe(rejects, hide_index=True, width="stretch")

with st.expander("How the stream works", icon=":material/menu_book:"):
    st.markdown(
        """
| Piece | What it does | Why it's built this way |
|---|---|---|
| **Producer** | Replays a real trading day of 1-minute prices (or polls live prices) and sends one JSON message per tick | Keyed by symbol, so each symbol's ticks stay in order in one partition. `acks=all` + idempotence: no lost or duplicated messages inside Kafka |
| **Kafka** | Single broker in KRaft mode (no ZooKeeper), 3 partitions, 24 h retention | Decouples the producer from the processing. If Spark is down, ticks wait in Kafka |
| **Spark Structured Streaming** | Micro-batches every 5 s: parse, validate, deduplicate | Kafka offsets live in a checkpoint, so after a crash it resumes exactly where it stopped |
| **Event-time windows** | 5-minute OHLC bars by the time the price *happened*, with a 10-minute watermark | Late or out-of-order ticks still land in the right bar, and old window state is dropped so memory stays bounded |
| **Idempotent sinks** | `ON CONFLICT` on `event_id`, on Kafka position for rejects, and on `(symbol, window)` for bars | Spark may replay a batch after a failure. Idempotent writes turn at-least-once delivery into exactly-once results |
        """
    )
