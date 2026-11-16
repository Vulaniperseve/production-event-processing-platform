# Production Event Processing Platform

[![CI](https://github.com/Vulaniperseve/production-event-processing-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/Vulaniperseve/production-event-processing-platform/actions/workflows/ci.yml)

A production-oriented data engineering platform that ingests market data from a REST API, preserves every raw response, validates and quarantines bad records, loads clean events idempotently into PostgreSQL, builds an SQL analytics layer, and runs a Spark **Bronze → Silver → Gold** data lake in Parquet. Apache Airflow orchestrates it in Docker, and every change runs unit, integration and Spark tests in GitHub Actions.

## Architecture

```text
Twelve Data REST API
        │   one request per symbol (retries, timeout)
        ▼
┌──────────────────────┐
│ raw.raw_api_events   │  every response as JSONB, append-only,
│                      │  payload hash + processing status
└──────────┬───────────┘
           │  raw_event_id  (the only thing passed between Airflow tasks)
           ▼
   structure + record validation ──────► quarantine.quarantined_records
           │                               (bad records + reason, linked to raw)
           ▼
┌──────────────────────┐
│ staging.market_events│  deterministic event_id, INSERT ... ON CONFLICT DO NOTHING,
│                      │  raw_event_id foreign key = lineage
└──────────┬───────────┘
           ▼
┌──────────────────────────────────────────────────────────────┐
│ analytics                                                    │
│   daily_prices → daily_returns → price_indicators            │
│   symbol_summary (materialized, refreshed after each load)   │
│   pipeline_health · data_freshness                           │
└──────────────────────────────────────────────────────────────┘

        raw.raw_api_events  ──(Airflow Asset event)──►  data lake (Spark, Parquet)
┌─────────────────────────────────────────────────────────────────────────────┐
│ BRONZE  bronze/twelvedata_time_series/ingest_date=…/raw_event_id=….json     │
│         raw responses, unchanged (Python copy job)                          │
│ SILVER  silver/market_prices/symbol=…/trade_month=…/*.parquet               │
│         validated, deduplicated (latest extraction wins), same event_id     │
│         as PostgreSQL; rejects in silver/market_prices_rejected             │
│ GOLD    gold/daily_metrics/symbol=…   gold/monthly_summary                  │
│         gold/return_correlation                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Airflow DAG: `production_event_pipeline`

```text
get_symbols ─► extract_to_raw[n] ─► validate_raw[n] ─► transform_and_load_staging[n] ─► refresh_analytics
```

- **Dynamic task mapping:** one extract/validate/load instance per symbol, created at runtime from the `symbols` parameter (default `AAPL, MSFT, GOOGL`).
- **Extract once:** the API is called only in `extract_to_raw`. Downstream tasks read the stored payload by `raw_event_id`, so retries never call the API again and every task sees the same data.
- **Fail fast:** a structurally invalid API response raises `AirflowFailException` (no pointless retries) and is recorded as `FAILED` in the raw layer.
- **`refresh_analytics`** runs with `trigger_rule="all_done"`, so a failure on one symbol doesn't keep the others' new data out of the analytics.
- **Right-sized:** LocalExecutor on a single host (no Celery worker or Redis broker), with exponential-backoff retries and task timeouts.
- **`refresh_analytics`** emits the `market_events_staging` **Asset** event, which triggers the lakehouse DAG.

## Airflow DAG: `market_data_lakehouse`

```text
land_bronze ─► build_silver (Spark) ─► build_gold (Spark)
```

- **Data-aware scheduling:** `schedule=[Asset("market_events_staging")]`. It runs whenever ingestion updates staging, not on a guessed cron time, and a Spark failure never blocks ingestion.
- **Incremental:** `land_bronze` copies only responses not yet in the lake (set difference of ids, not a max-id watermark, which can skip rows committed out of order). `build_silver` processes only the newly landed ingest dates and rewrites only the affected `(symbol, trade_month)` partitions. Trigger with `{"full_refresh": true}` to rebuild from all of bronze.
- **One Spark JVM at a time:** sequential tasks and `max_active_runs=1` (local mode, 1 GB driver) keep it within a small machine's memory.

## Data lake (PySpark 4.1, Parquet)

| Layer | What it holds | Spark techniques |
|---|---|---|
| Bronze | Raw API responses as JSON Lines, partitioned by `ingest_date`, written atomically (temp file + rename) | — (plain Python copy) |
| Silver | One validated row per event, partitioned by `symbol` / `trade_month` | Explicit schema (no inference), `explode`, `try_cast` / `try_to_timestamp`, `row_number()` deduplication, `sha2` event ids, broadcast join for partition pruning, **dynamic partition overwrite** for incremental upserts, `localCheckpoint` before overwriting partitions it reads |
| Gold | `daily_metrics`, `monthly_summary`, `return_correlation` | Window functions (`lag`, rolling `rowsBetween`, running max), **Spark SQL** (`min_by` / `max_by`, CTE + `LAG`), self-join with `corr()` |

Consistency between the two systems is verified: silver applies the same quality rules as the Python validator, produces the **same `event_id`s** as `staging.market_events`, and gold returns match the PostgreSQL analytics.

Spark is configured for the data size: 4 shuffle partitions instead of the default 200, adaptive query execution, one file per partition (`repartition` by partition columns), and `coalesce(1)` for small gold tables.

## Data reliability

| Concern | How it is handled |
|---|---|
| Raw data preservation | Every API response is stored unchanged as JSONB, including error responses |
| Idempotency | Deterministic `event_id` (SHA-256 of symbol + time + type) with `ON CONFLICT DO NOTHING`. Reruns, retries and overlapping extractions never duplicate |
| Data quality | Structure checks (keys, API status, non-empty) plus record checks (required fields, valid timestamp, positive finite price) |
| Bad data | Invalid records are quarantined with a reason, linked to their raw response |
| Lineage | Every staging and quarantine row references the raw response it came from (FK-enforced) |
| Observability | Raw status lifecycle `PENDING → VALIDATED → LOADED / NO_VALID_RECORDS / FAILED`, plus the `pipeline_health` and `data_freshness` views |

## Analytics layer (SQL window functions)

| View | Contents |
|---|---|
| `analytics.daily_prices` | Last observed price per symbol per day (`DISTINCT ON`) |
| `analytics.daily_returns` | Simple and log returns using `LAG()` |
| `analytics.price_indicators` | 5/20-day moving averages, 20-day volatility (raw and annualized), running peak and drawdown. Rolling windows use `ROWS` (trading days) and stay `NULL` until the window is full |
| `analytics.symbol_summary` | **Materialized**: period return, volatility, max drawdown, best and worst day per symbol. Refreshed with `REFRESH MATERIALIZED VIEW CONCURRENTLY`, so readers are never blocked |
| `analytics.pipeline_health` | API responses, statuses, records received / quarantined / loaded, and quarantine rate per day and symbol |
| `analytics.data_freshness` | Latest trade date and hours since the last load per symbol |

Example query:

```sql
SELECT symbol, trading_days, period_return, annualized_volatility, max_drawdown
FROM analytics.symbol_summary
ORDER BY period_return DESC;
```

## Database migrations

Versioned SQL migrations in `database/migrations/` are the single source of truth for the schema. `python -m database.migrate` applies pending migrations in order and records them in `public.schema_migrations`, with a checksum to detect edits to already-applied files. Every script is also idempotent.

## Testing and CI

- **Unit tests** (`tests/`): validation, transformation, deterministic IDs, and the pipeline steps with in-memory fakes. No database needed.
- **Integration tests** (`tests/integration/`): a real PostgreSQL 16 covering migrations (idempotency, legacy upgrade, ORM/SQL schema-drift check), storage, end-to-end pipeline replays, and the analytics calculations checked against hand-computed values. These tests drop schemas, so they only run with `RUN_INTEGRATION_TESTS=1` and refuse any database whose name doesn't contain `test`.
- **Spark tests** (`tests/lake/`): silver validation and parity with the Python validator, identical event ids, latest-wins deduplication, idempotent reruns, incremental runs that leave untouched partitions' files alone, full refresh equal to incremental, and the gold calculations. They skip automatically when PySpark or Java is unavailable.
- **GitHub Actions** runs three jobs on every push and pull request: unit tests, integration tests against a `postgres:16` service container, and Spark tests on Java 17. Python 3.12 matches the Airflow image.

## Running locally

```bash
# 1. Start Airflow + PostgreSQL (from airflow/; needs airflow/.env)
docker compose up -d

# 2. Create / upgrade the schema
docker compose exec -w /opt/airflow airflow-scheduler python -m database.migrate

# 3. Trigger the pipeline (or use the UI at http://localhost:8080)
docker compose exec airflow-scheduler airflow dags trigger production_event_pipeline
```

Data lake by hand (inside the Airflow image, which has Java and PySpark):

```bash
docker compose exec -w /opt/airflow airflow-scheduler python -m src.lake.run all
```

Tests:

```bash
python -m pytest                      # unit tests (Spark tests skip without Java)
docker compose exec -w /opt/airflow airflow-scheduler \
  python -m pytest /opt/airflow/project/tests -c /opt/airflow/project/pytest.ini -p no:cacheprovider -m spark
docker run -d --name pg-test -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=pipeline_test -p 5433:5432 postgres:16
RUN_INTEGRATION_TESTS=1 DB_HOST=localhost DB_PORT=5433 DB_NAME=pipeline_test \
  DB_USER=postgres DB_PASSWORD=postgres python -m pytest -m integration
```

## Tech stack

Python · SQL · PostgreSQL (JSONB, window functions, materialized views) · SQLAlchemy · pandas · Apache Spark / PySpark 4.1 (DataFrames, Spark SQL) · Parquet · Apache Airflow 3 (TaskFlow, dynamic task mapping, Assets) · Docker Compose · pytest · GitHub Actions

## Roadmap

- [x] Production pipeline: raw → validation → quarantine → staging, with lineage
- [x] Automated testing: unit and PostgreSQL integration tests
- [x] CI with GitHub Actions
- [x] Analytics layer
- [x] Apache Spark / PySpark
- [x] Data lake: Bronze → Silver → Gold, Parquet and partitioning
- [ ] Cloud deployment (AWS)
- [ ] Streaming: Kafka / Spark Structured Streaming
- [ ] Monitoring and alerting
- [ ] Performance benchmarking
