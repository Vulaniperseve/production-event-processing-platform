from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def extract_data():
    from src.extractors.twelve_data_extractor import TwelveDataExtractor

    extractor = TwelveDataExtractor()
    data = extractor.extract()

    if data is None:
        raise ValueError("Extraction returned no data.")

    return data


def validate_data():
    from src.extractors.twelve_data_extractor import TwelveDataExtractor
    from src.validators.validator import DataValidator

    extractor = TwelveDataExtractor()
    data = extractor.extract()

    DataValidator.validate_structure(data)


def transform_data():
    from src.extractors.twelve_data_extractor import TwelveDataExtractor
    from src.transformers.transformer import DataTransformer

    extractor = TwelveDataExtractor()
    data = extractor.extract()

    DataTransformer.transform(data)


def load_data():
    from src.extractors.twelve_data_extractor import TwelveDataExtractor
    from src.transformers.transformer import DataTransformer
    from src.loaders.postgres_loader import PostgresLoader

    extractor = TwelveDataExtractor()
    data = extractor.extract()

    df = DataTransformer.transform(data)

    PostgresLoader.load(df)


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="production_event_pipeline",
    default_args=default_args,
    description="Production market event processing pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",
    catchup=False,
    tags=["production", "market-data", "etl"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_market_data",
        python_callable=extract_data,
    )

    validate_task = PythonOperator(
        task_id="validate_market_data",
        python_callable=validate_data,
    )

    transform_task = PythonOperator(
        task_id="transform_market_data",
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id="load_to_postgresql",
        python_callable=load_data,
    )

    extract_task >> validate_task >> transform_task >> load_task