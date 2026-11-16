"""
Production market event pipeline.

    get_symbols ─> extract_to_raw[n] ─> validate_raw[n] ─> transform_and_load_staging[n] ─> refresh_analytics
                                                                                              │
                                                    Asset "market_events_staging" ─> market_data_lakehouse DAG

* Dynamic task mapping: one extract/validate/load instance per symbol,
  created at runtime from the `symbols` param. Each instance retries on
  its own, and the Grid view shows each symbol separately.
* The API is called ONCE per symbol, in extract_to_raw. The response is
  stored in raw.raw_api_events and only its raw_event_id (an int) goes
  through XCom. Downstream tasks read the payload from the raw layer, so
  retries never hit the API again and every task sees the same data.
* Structurally invalid responses fail fast (no pointless retries).
* refresh_analytics runs once at the end even if a symbol failed
  (trigger_rule=all_done): the refresh is idempotent and the other
  symbols' new data should still reach the analytics layer.
"""

from datetime import datetime, timedelta

from airflow.exceptions import AirflowFailException
from airflow.sdk import Asset, Param, dag, task

# Updated by this DAG; consumed by the market_data_lakehouse DAG
STAGING_ASSET = Asset("market_events_staging")


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=10),
}


@dag(
    dag_id="production_event_pipeline",
    default_args=default_args,
    description="Market events: raw -> validate -> staging -> analytics",
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    params={
        "symbols": Param(
            ["AAPL", "MSFT", "GOOGL"],
            type="array",
            items={"type": "string"},
            minItems=1,
            maxItems=8,  # Twelve Data free tier: 8 requests per minute
        ),
        "interval": Param("1day", type="string"),
        # Credits are charged per request, not per row, so fetching 100
        # days costs the same as 10. Overlap is deduplicated in staging.
        "outputsize": Param(100, type="integer", minimum=1, maximum=5000),
    },
    tags=["production", "market-data", "etl", "analytics"],
)
def production_event_pipeline():

    @task(task_id="get_symbols")
    def get_symbols(**context):
        symbols = [s.strip().upper() for s in context["params"]["symbols"] if s.strip()]
        return sorted(set(symbols))

    @task(task_id="extract_to_raw", map_index_template="{{ task.op_kwargs['symbol'] }}")
    def extract_to_raw(symbol: str, **context):
        from src.pipeline.steps import extract_to_raw as extract_step

        params = context["params"]

        return extract_step(
            symbol=symbol,
            interval=params["interval"],
            outputsize=params["outputsize"],
        )

    @task(task_id="validate_raw")
    def validate_raw(raw_event_id: int):
        from src.pipeline.steps import StructureValidationError, validate_raw as validate_step

        try:
            validate_step(raw_event_id)
        except StructureValidationError as e:
            # Bad API response structure: retrying will not fix it.
            raise AirflowFailException(f"Invalid API response structure: {e}") from e

        # If every record was quarantined, the load step records
        # NO_VALID_RECORDS and loads nothing (no skip needed, which keeps
        # the mapped instances aligned).
        return raw_event_id

    @task(task_id="transform_and_load_staging")
    def transform_and_load(raw_event_id: int):
        from src.pipeline.steps import transform_and_load as load_step

        return load_step(raw_event_id)

    # Emitting the Asset event triggers the market_data_lakehouse DAG.
    @task(task_id="refresh_analytics", trigger_rule="all_done", outlets=[STAGING_ASSET])
    def refresh_analytics():
        from src.analytics.analytics import refresh_analytics as refresh_step

        return refresh_step()

    raw_ids = extract_to_raw.expand(symbol=get_symbols())
    validated = validate_raw.expand(raw_event_id=raw_ids)
    loaded = transform_and_load.expand(raw_event_id=validated)

    loaded >> refresh_analytics()


production_event_pipeline()
