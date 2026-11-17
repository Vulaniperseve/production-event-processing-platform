"""Data quality and proof page: can the data be trusted?"""

from datetime import datetime

import pandas as pd
import streamlit as st

from dashboard.lib import charts
from dashboard.lib import resources as res

st.title("Data quality and proof")
st.caption(
    "What was checked, what was rejected and why, how fresh the data is, and proof that the "
    "Spark data lake and the PostgreSQL warehouse agree."
)

try:
    totals = res.warehouse_totals()
except Exception as exc:
    st.error(f"PostgreSQL isn't reachable: {exc}")
    st.stop()

# --------------------------------------------------------------- KPIs
health = res.pipeline_health(90)
received = int(health["records_received"].sum()) if not health.empty else 0
quarantined = int(health["records_quarantined"].sum()) if not health.empty else 0
a, b, c, d = st.columns(4)
a.metric("API responses stored", f"{int(totals['raw_responses']):,}")
b.metric("Events in staging", f"{int(totals['staging_events']):,}",
         help="Unique events. Reruns never add duplicates.")
c.metric("Records quarantined", f"{int(totals['quarantined']):,}")
d.metric("Quarantine rate (90 days)", f"{(quarantined / received if received else 0):.2%}")

# ------------------------------------------------- proof: lake vs warehouse
st.divider()
st.subheader("Proof: the data lake matches the warehouse")
st.caption(
    "Two independent systems process the same raw responses: Python + PostgreSQL, and "
    "Spark + Parquet. If they agree on every event id and every result, the logic is consistent."
)
try:
    ids = res.event_id_comparison()
    from dashboard.lib import data as data_lib
    returns = data_lib.compare_returns(res.symbol_summary(), res.gold("daily_metrics"))
except Exception as exc:
    ids, returns = pd.DataFrame(), pd.DataFrame()
    st.warning(f"Couldn't read the data lake: {exc}")

if not ids.empty:
    in_both = int(ids["matching"].sum())
    lake_total = int(ids["lake"].sum())
    only_lake = int(ids["only_in_lake"].sum())
    max_diff = float(returns["difference"].max()) if not returns.empty else None
    ids_ok = only_lake == 0 and in_both == lake_total
    returns_ok = max_diff is not None and max_diff < 1e-9

    if ids_ok and returns_ok:
        st.success(
            f"**All {in_both:,} events in the lake have the identical event id in PostgreSQL**, "
            + ("and the period returns match exactly." if max_diff == 0
               else f"and the period returns agree to within {max_diff:.0e}."),
            icon=":material/verified:",
        )
    else:
        st.warning("The lake and the warehouse differ. See the tables below.", icon=":material/warning:")

    left, right = st.columns(2)
    with left:
        st.markdown("**Event ids per symbol**")
        st.dataframe(
            ids.rename(columns={"warehouse": "PostgreSQL", "lake": "Spark silver",
                                "matching": "Identical ids", "only_in_warehouse": "Only in PostgreSQL",
                                "only_in_lake": "Only in lake"}),
            hide_index=True, width="stretch",
        )
        if int(ids["only_in_warehouse"].sum()):
            st.caption(
                "Events only in PostgreSQL were loaded before the raw layer existed (no raw "
                "response to rebuild them from), so the lake can't contain them."
            )
    with right:
        st.markdown("**Period return per symbol**")
        if not returns.empty:
            shown = returns.copy()
            for col in ("warehouse_return", "lake_return"):
                shown[col] = shown[col].map(lambda v: f"{v:+.4%}")
            shown["difference"] = shown["difference"].map(lambda v: f"{v:.1e}")
            st.dataframe(shown.rename(columns={"warehouse_return": "SQL analytics",
                                               "lake_return": "Spark gold", "difference": "Difference"}),
                         hide_index=True, width="stretch")

# --------------------------------------------------------- data lake
lake = res.lake_layers()
st.markdown("**Data lake layers**")
cols = st.columns(3)
for col, name, label in zip(cols, ("bronze", "silver", "gold"),
                            ("Bronze · raw JSON", "Silver · Parquet", "Gold · Parquet")):
    layer = lake.get(name, {})
    rows = f"{layer.get('rows'):,} rows · " if layer.get("rows") is not None else ""
    updated = (datetime.fromtimestamp(layer["updated"]).strftime("%d %b %H:%M")
               if layer.get("updated") else "–")
    col.metric(label, f"{layer.get('files', 0):,} files")
    col.caption(f"{rows}{layer.get('partitions', 0)} partitions · "
                f"{layer.get('bytes', 0) / 1024:,.0f} KB · updated {updated}")

# ------------------------------------------------------ quality over time
st.divider()
st.subheader("What happened to every record")
if health.empty:
    st.info("No pipeline runs in the last 90 days.")
else:
    st.plotly_chart(charts.health_chart(health), width="stretch",
                    config={"displayModeBar": False})
    st.caption(
        "Grey = records that were already stored and were skipped on purpose. That's "
        "idempotency at work: rerunning a day never creates duplicates."
    )

left, right = st.columns(2)
with left:
    st.markdown("**Raw response lifecycle**")
    statuses = res.raw_status_counts()
    st.dataframe(statuses, hide_index=True, width="stretch")
    st.caption("PENDING → VALIDATED → LOADED, or NO_VALID_RECORDS / FAILED.")
with right:
    st.markdown("**Freshness**")
    fresh = res.data_freshness()
    if not fresh.empty:
        fresh = pd.DataFrame({
            "Symbol": fresh["symbol"],
            "Latest trade date": fresh["latest_trade_date"],
            "Hours since load": fresh["hours_since_last_load"].astype(float).round(1),
            "Status": fresh["days_behind"].map(
                lambda d: "Current" if d is not None and d <= 3 else "Stale"),
        })
    st.dataframe(fresh, hide_index=True, width="stretch")
    st.caption("Market data is daily, and weekends and holidays have no new prices, so up to 3 days behind counts as current.")

st.markdown("**Quarantine**")
reasons = res.quarantine_reasons()
if reasons.empty:
    st.success("No records have been quarantined. Every record passed validation.", icon=":material/check:")
else:
    st.dataframe(reasons, hide_index=True, width="stretch")
    with st.expander("Latest quarantined records"):
        st.dataframe(res.recent_quarantine(), hide_index=True, width="stretch")
