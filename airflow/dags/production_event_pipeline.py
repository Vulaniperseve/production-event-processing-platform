"""
Production market event pipeline.

    extract_to_raw ──> validate_raw ──> transform_and_load_staging

* The API is called ONCE, in extract_to_raw. The response is stored in
  raw.raw_api_events and only its raw_event_id (an int) goes through XCom.
* Downstream tasks read the payload from the raw layer, so retries and
  manual reruns never hit the API again and every task sees the same data.
* Structurally invalid responses fail fast (no pointless retries).
* If no records pass validation, the load task is skipped, not failed.
"""

from datetime import datetime, timedelta

from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.sdk import Param, dag, task


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
    description="Production market event processing pipeline (raw -> validate -> staging)",
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    params={
        "symbol": Param("AAPL", type="string"),
        "interval": Param("1day", type="string"),
        "outputsize": Param(10, type="integer", minimum=1, maximum=5000),
    },
    tags=["production", "market-data", "etl"],
)
def production_event_pipeline():

    @task(task_id="extract_to_raw")
    def extract_to_raw(**context):
        from src.pipeline.steps import extract_to_raw as extract_step

        params = context["params"]

        return extract_step(
            symbol=params["symbol"],
            interval=params["interval"],
            outputsize=params["outputsize"],
        )

    @task(task_id="validate_raw")
    def validate_raw(raw_event_id: int):
        from src.pipeline.steps import StructureValidationError, validate_raw as validate_step

        try:
            summary = validate_step(raw_event_id)
        except StructureValidationError as e:
            # Bad API response structure: retrying will not fix it.
            raise AirflowFailException(f"Invalid API response structure: {e}") from e

        if summary["valid_count"] == 0:
            raise AirflowSkipException(
                f"raw_event_id={raw_event_id}: all records quarantined, nothing to load."
            )

        return raw_event_id

    @task(task_id="transform_and_load_staging")
    def transform_and_load(raw_event_id: int):
        from src.pipeline.steps import transform_and_load as load_step

        return load_step(raw_event_id)

    transform_and_load(validate_raw(extract_to_raw()))


production_event_pipeline()
