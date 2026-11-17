"""
Shared Streamlit resources: one database engine and one Airflow client per
server process, and short-lived caches for query results.

Caches expire after a few seconds (live pipeline numbers) or 60 seconds
(charts), and the Pipeline page clears them when a run finishes, so the
other pages show the new data straight away.
"""

import streamlit as st

from dashboard.lib import data
from dashboard.lib.airflow_client import AirflowClient


@st.cache_resource
def engine():
    return data.make_engine()


@st.cache_resource
def airflow():
    return AirflowClient()


@st.cache_data(ttl=60, show_spinner=False)
def symbol_summary():
    return data.symbol_summary(engine())


@st.cache_data(ttl=60, show_spinner=False)
def price_indicators(symbol):
    return data.price_indicators(engine(), symbol)


@st.cache_data(ttl=60, show_spinner=False)
def pipeline_health(days=30):
    return data.pipeline_health(engine(), days)


@st.cache_data(ttl=60, show_spinner=False)
def data_freshness():
    return data.data_freshness(engine())


@st.cache_data(ttl=60, show_spinner=False)
def raw_status_counts():
    return data.raw_status_counts(engine())


@st.cache_data(ttl=60, show_spinner=False)
def quarantine_reasons():
    return data.quarantine_reasons(engine())


@st.cache_data(ttl=60, show_spinner=False)
def recent_quarantine(limit=20):
    return data.recent_quarantine(engine(), limit)


@st.cache_data(ttl=10, show_spinner=False)
def warehouse_totals():
    return data.warehouse_totals(engine())


@st.cache_data(ttl=5, show_spinner=False)
def run_numbers(since, until):
    return data.run_numbers(engine(), since, until)


# Reading Parquet footers through a Docker bind mount on Windows is slow, so
# this is cached longer; refresh_all() clears it as soon as a run finishes.
@st.cache_data(ttl=300, show_spinner=False)
def lake_layers():
    return data.lake_layers()


@st.cache_data(ttl=60, show_spinner=False)
def gold(table):
    return data.read_gold(table)


@st.cache_data(ttl=60, show_spinner=False)
def event_id_comparison():
    return data.compare_event_ids(data.staging_event_ids(engine()), data.silver_event_ids())


def refresh_all():
    st.cache_data.clear()
