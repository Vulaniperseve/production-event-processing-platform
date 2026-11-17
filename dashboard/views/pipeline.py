"""Pipeline page: start a run and follow every stage live."""

import contextlib
import re
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

from dashboard.lib import resources as res
from dashboard.lib.airflow_client import AirflowUnavailable
from dashboard.lib.flow import render_flow
from dashboard.lib.pipeline_status import INGEST_DAG, LAKE_DAG, build_pipeline_view

DEFAULT_SYMBOLS = ["AAPL", "MSFT", "GOOGL"]
SUGGESTED = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "JPM", "V", "NFLX"]
SYMBOL_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")
MAX_SYMBOLS = 8   # the DAG's `symbols` param allows at most 8 (API rate limit)

st.title("Pipeline")
st.caption(
    "One click runs the real pipeline: Airflow extracts from the API, validates, loads "
    "PostgreSQL and refreshes analytics. That emits an Asset event, which starts the Spark "
    "data lake build. The diagram follows the actual Airflow tasks every few seconds."
)


# ---------------------------------------------------------------- helpers
def _parse(ts):
    return datetime.fromisoformat(ts.replace("Z", "+00:00")) if ts else None


def _ago(ts):
    t = _parse(ts)
    if not t:
        return "–"
    seconds = int((datetime.now(timezone.utc) - t).total_seconds())
    if seconds < 60:
        return f"{seconds}s ago"
    if seconds < 3600:
        return f"{seconds // 60} min ago"
    if seconds < 86400:
        return f"{seconds // 3600} h ago"
    return f"{seconds // 86400} d ago"


def _elapsed(start, end=None):
    s, e = _parse(start), _parse(end) or datetime.now(timezone.utc)
    if not s:
        return "–"
    seconds = max(int((e - s).total_seconds()), 0)
    return f"{seconds // 60}m {seconds % 60:02d}s"


def _runs_table(client):
    rows = []
    for dag_id, label in ((INGEST_DAG, "Ingestion"), (LAKE_DAG, "Data lake")):
        for run in client.recent_runs(dag_id, limit=5):
            rows.append({
                "DAG": label,
                "Run": run.get("dag_run_id"),
                "Triggered by": {"manual": "Button / manual", "scheduled": "Schedule (06:00)",
                                 "asset_triggered": "Asset event"}.get(run.get("run_type"), run.get("run_type")),
                "State": run.get("state"),
                "Started": _ago(run.get("start_date") or run.get("queued_at")),
                "Duration": _elapsed(run.get("start_date"), run.get("end_date"))
                if run.get("start_date") else "–",
                "_sort": run.get("run_after") or "",
            })
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values("_sort", ascending=False).drop(columns="_sort")


# -------------------------------------------------------------- run panel
def start_run(symbols):
    client = res.airflow()
    try:
        if client.is_paused(LAKE_DAG):
            client.unpause(LAKE_DAG)      # otherwise the Asset event can't start it
        run = client.trigger(INGEST_DAG, conf={"symbols": symbols})
    except AirflowUnavailable as exc:
        st.error(f"Couldn't start the run: {exc}")
        return
    st.session_state["pipeline_status"] = "running"
    st.toast(f"Run started for {', '.join(symbols)}", icon=":material/rocket_launch:")
    st.session_state["last_trigger"] = run.get("dag_run_id")


with st.container(border=True):
    left, right = st.columns([4, 1], vertical_alignment="bottom")
    with left:
        symbols = st.multiselect(
            "Symbols to ingest",
            options=SUGGESTED,
            default=DEFAULT_SYMBOLS,
            max_selections=MAX_SYMBOLS,
            accept_new_options=True,
            help="Any ticker the Twelve Data API knows. Type to add one. Up to 8 per run.",
        )
    with right:
        clicked = st.button("Run pipeline", type="primary", icon=":material/play_arrow:",
                            width="stretch")
    if clicked:
        cleaned = sorted({s.strip().upper() for s in symbols})
        invalid = [s for s in cleaned if not SYMBOL_RE.match(s)]
        if not cleaned:
            st.warning("Pick at least one symbol.")
        elif invalid:
            st.warning(f"These don't look like tickers: {', '.join(invalid)}")
        else:
            start_run(cleaned)


# ------------------------------------------------------------- live view
def live_view():
    client = res.airflow()
    # Only the first load shows a spinner; the 5-second refreshes stay silent.
    first_load = "pipeline_status" not in st.session_state
    loading = (st.spinner("Reading the pipeline status from Airflow…") if first_load
               else contextlib.nullcontext())
    try:
        with loading:
            ingest = client.latest_run(INGEST_DAG)
            ingest_tis = client.task_instances(INGEST_DAG, ingest["dag_run_id"]) if ingest else []
            lake = client.latest_run(LAKE_DAG)
            lake_tis = client.task_instances(LAKE_DAG, lake["dag_run_id"]) if lake else []
    except AirflowUnavailable as exc:
        st.error(
            f"Airflow isn't reachable, so the live view is paused. {exc}\n\n"
            "Start the platform with `start.bat` (or `docker compose up -d` in `airflow/`)."
        )
        return

    view = build_pipeline_view(ingest, ingest_tis, lake, lake_tis)

    numbers = None
    if ingest and ingest.get("start_date"):
        try:
            numbers = res.run_numbers(ingest["start_date"], ingest.get("end_date"))
        except Exception:   # the diagram still works without the database
            numbers = None
    try:
        lake_stats = res.lake_layers()
    except Exception:
        lake_stats = None

    # Status banner
    if view.status == "running":
        st.info(f"**{view.headline}** · elapsed {_elapsed(ingest.get('start_date') or ingest.get('queued_at'))}",
                icon=":material/autorenew:")
    elif view.status == "failed":
        st.error(f"**{view.headline}**. Open Airflow to read the task log.", icon=":material/error:")
    elif view.status == "success":
        end = (view.lake_run or {}).get("end_date") or ingest.get("end_date")
        st.success(f"**{view.headline}** · finished {_ago(end)}", icon=":material/check_circle:")
    else:
        st.info(view.headline, icon=":material/info:")

    st.html(render_flow(view, numbers, lake_stats))

    # When a run finishes, clear cached data so every page shows the new data,
    # and switch the refresh rate between "live" (5 s) and "idle" (20 s).
    previous = st.session_state.get("pipeline_status")
    st.session_state["pipeline_status"] = view.status
    if previous is not None and previous != view.status:
        if previous == "running":
            res.refresh_all()
            st.toast("Run finished. All pages now show the new data.", icon=":material/done_all:")
        st.rerun(scope="app")

    with st.expander("Recent runs", icon=":material/history:"):
        try:
            table = _runs_table(client)
            if table.empty:
                st.caption("No runs yet.")
            else:
                st.dataframe(table, hide_index=True, width="stretch")
        except AirflowUnavailable as exc:
            st.caption(str(exc))


interval = 5 if st.session_state.get("pipeline_status", "running") == "running" else 20
st.fragment(run_every=interval)(live_view)()

with st.expander("What happens in each stage", icon=":material/menu_book:"):
    st.markdown(
        """
| # | Stage | What it does | Why it's built this way |
|---|---|---|---|
| 1 | **Extract** | Calls the Twelve Data API once per symbol and stores each response unchanged in `raw.raw_api_events` | The raw layer is a receipt: any later stage can be replayed without calling the API again |
| 2 | **Validate** | Checks the response structure, then every record (fields, timestamp, positive price) | Bad records go to **quarantine** with a reason instead of disappearing |
| 3 | **Load staging** | Inserts valid events with a deterministic SHA-256 `event_id` and `ON CONFLICT DO NOTHING` | **Idempotent**: rerunning never creates duplicates |
| 4 | **Analytics** | Refreshes SQL views: returns, moving averages, volatility, drawdown, pipeline health | Materialized view refreshed `CONCURRENTLY`, so readers are never blocked |
| 5 | **Bronze** | Copies new raw responses into the data lake as JSON files | Only responses not already in the lake are copied |
| 6 | **Silver** | Spark validates, deduplicates and writes Parquet partitioned by symbol and month | Only affected partitions are rewritten (incremental) |
| 7 | **Gold** | Spark builds daily metrics, monthly summaries and return correlations | The business-ready tables, verified against PostgreSQL |
| 8 | **Publish to S3** | Syncs the lake to Amazon S3 (changed files only) | Skips itself until an S3 bucket is configured, so it costs $0 |
        """
    )
