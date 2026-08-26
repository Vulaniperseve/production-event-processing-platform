# Current Pipeline Architecture

The Production Event Processing Platform is being developed as a production-oriented data engineering pipeline for ingesting, validating, transforming, and loading market event data.

## Pipeline Flow

```text
Twelve Data API
       |
       v
Data Extraction
       |
       v
Structure & Record Validation
       |
       v
Data Transformation
       |
       v
PostgreSQL Staging
       |
       v
Idempotent Loading
       |
       v
Apache Airflow Orchestration
```

## Technologies

* Python
* Pandas
* SQLAlchemy
* PostgreSQL
* Apache Airflow
* Docker / Docker Compose
* Twelve Data API
* Git / GitHub

## Completed Components

* API data extraction
* API response structure validation
* Individual record validation
* Invalid/bad-record detection
* Data transformation
* PostgreSQL staging
* Idempotent PostgreSQL loading
* Duplicate-event detection and skipping
* Dockerized Airflow environment
* Airflow DAG creation
* Airflow integration with the existing Python project
* Airflow access to the project's `src`, `config`, and `database` modules
* Initial end-to-end Airflow orchestration

## Airflow Workflow

The current Airflow DAG is:

```text
extract_market_data
        |
        v
validate_market_data
        |
        v
transform_market_data
        |
        v
load_to_postgresql
```

The DAG is named:

```text
production_event_pipeline
```

Airflow is currently running through Docker Compose.

## Data Quality

The pipeline includes validation for:

* Missing required fields
* Invalid market values
* Invalid timestamps
* Invalid API response structures

Invalid records are identified separately rather than silently entering the processing pipeline.

## Idempotency

The PostgreSQL loading process prevents duplicate events from being inserted.

If an event already exists, the loader skips it rather than creating another copy.

This allows the pipeline to safely process repeated data without creating duplicate records.

## Current Development Status

The core ETL pipeline and idempotent PostgreSQL loading have been implemented.

Apache Airflow orchestration and Docker integration are currently being tested as the next stage of the project.

Future development will include:

* Improved Airflow task design
* Retry and failure handling
* Monitoring and observability
* Automated testing
* Cloud integration
* Distributed/big-data processing with Apache Spark
* Production deployment
