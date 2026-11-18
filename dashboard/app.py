"""
Production Event Processing Platform: dashboard.

    streamlit run dashboard/app.py

Three pages:
  * Pipeline: start a run and watch every stage live (from the Airflow API)
  * Market results: what the data says (PostgreSQL analytics + Spark gold)
  * Data quality and proof: quarantine, freshness, warehouse vs lake checks
  * Live stream: Kafka -> Spark Structured Streaming -> PostgreSQL, live
"""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import os  # noqa: E402

import streamlit as st  # noqa: E402

st.set_page_config(
    page_title="Event Processing Platform",
    page_icon=":material/hub:",
    layout="wide",
)

st.markdown(
    """
    <style>
      .block-container {padding-top: 2.2rem; max-width: 1400px;}
      [data-testid="stMetric"] {background: #f6f7f9; border-radius: 12px; padding: 12px 14px;}
      [data-testid="stMetricLabel"] p {font-size: 13px; color: #6b6f76;}
      h1 {font-weight: 650; letter-spacing: -0.01em;}
      /* The live view refreshes every few seconds: don't fade it while it does. */
      [data-stale="true"] {opacity: 1 !important; transition: none !important;}
    </style>
    """,
    unsafe_allow_html=True,
)

pages = [
    st.Page(str(ROOT / "dashboard/views/pipeline.py"), title="Pipeline",
            icon=":material/account_tree:", default=True),
    st.Page(str(ROOT / "dashboard/views/results.py"), title="Market results",
            icon=":material/monitoring:"),
    st.Page(str(ROOT / "dashboard/views/quality.py"), title="Data quality and proof",
            icon=":material/verified:"),
    st.Page(str(ROOT / "dashboard/views/stream.py"), title="Live stream",
            icon=":material/sensors:"),
]
nav = st.navigation(pages)

with st.sidebar:
    st.markdown("### Event Processing Platform")
    st.caption("Market data: API → PostgreSQL → Spark data lake")
    st.divider()
    airflow_ui = os.getenv("AIRFLOW_UI_URL", "http://localhost:8080")
    st.page_link(airflow_ui, label="Open Airflow", icon=":material/open_in_new:")
    st.page_link("https://github.com/Vulaniperseve/production-event-processing-platform",
                 label="Source code", icon=":material/code:")

nav.run()
