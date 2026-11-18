# Production Event Processing Platform

[![CI](https://github.com/Vulaniperseve/production-event-processing-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/Vulaniperseve/production-event-processing-platform/actions/workflows/ci.yml)

A production-oriented data engineering platform that ingests market data from a REST API, preserves every raw response, validates and quarantines bad records, loads clean events idempotently into PostgreSQL, builds an SQL analytics layer, and streams live price ticks through **Kafka** and **Spark Structured Streaming**, runs a Spark **Bronze → Silver → Gold** data lake in Parquet, published to **Amazon S3** and queryable with **Athena** (infrastructure as code with Terraform). Apache Airflow orchestrates it in Docker, and every change runs unit, integration and Spark tests in GitHub Actions.

## Quick start

1. Install [Docker Desktop](https://www.docker.com/products/docker-desktop/), clone the repo and create `airflow/.env` (Twelve Data API key and database settings).
2. Double-click **`start.bat`**. It starts Docker if needed, starts the platform and opens the dashboard at **http://localhost:8501**.
3. Press **Run pipeline** and watch each stage run live, from the API to the Spark data lake.

Streaming (optional, needs more memory): double-click **`stream.bat`** to start Kafka and the Spark streaming job and replay a real trading day. Watch it on the dashboard's *Live stream* page.

Stop everything with `stop.bat` (your data is kept).

## Dashboard

A Streamlit app that runs the platform and shows what it produces, without any terminal commands.

| Page | What it shows |
|---|---|
| **Pipeline** | A **Run pipeline** button that starts the real Airflow DAG through the Airflow REST API, then an animated diagram of all 8 stages that follows the actual Airflow tasks every 5 seconds: per-symbol progress, durations, records loaded, records quarantined, and the Asset event that starts the Spark lake build. A rerun shows *0 new · 300 already stored*: idempotency, visible |
| **Market results** | Performance of all symbols rebased to 100, price with 5- and 20-day moving averages, drawdown, daily returns, a monthly returns heatmap and a correlation matrix from the Spark gold layer |
| **Live stream** | Ticks stored, end-to-end latency (p50/p95), duplicates dropped, rejected messages, live 5-minute candlestick bars, and throughput per minute from the streaming job's batch log |
| **Data quality and proof** | Quarantine reasons, the raw response lifecycle, freshness, records loaded vs skipped vs quarantined per day, and a **live consistency check**: every event id in the Spark lake must exist in PostgreSQL, and period returns must agree to within 1e-9 |

Design choices:
- The dashboard opens PostgreSQL sessions with `default_transaction_read_only=on` and mounts the lake read-only, so it can't change data.
- It talks to Airflow only through the public REST API with a JWT, never to Airflow's internal database.
- The stage-status logic (mapped tasks, retries, skipped and blocked stages, which lake run belongs to which ingestion) is pure Python with unit tests. The pages have headless Streamlit `AppTest` smoke tests.
- It runs as its own small image (`python:3.12-slim`, non-root user, health check, 512 MB memory limit).

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
        │  publish_to_s3 (incremental sync)
        ▼
  s3://<project>-lake-<account>/lake/{bronze,silver,gold}/  ──►  Glue Data Catalog  ──►  Athena SQL
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
land_bronze ─► build_silver (Spark) ─► build_gold (Spark) ─► publish_to_s3
```

- **Data-aware scheduling:** `schedule=[Asset("market_events_staging")]`. It runs whenever ingestion updates staging, not on a guessed cron time, and a Spark failure never blocks ingestion.
- **Incremental:** `land_bronze` copies only responses not yet in the lake (set difference of ids, not a max-id watermark, which can skip rows committed out of order). `build_silver` processes only the newly landed ingest dates and rewrites only the affected `(symbol, trade_month)` partitions. Trigger with `{"full_refresh": true}` to rebuild from all of bronze.
- **`publish_to_s3`** runs with `trigger_rule="none_failed"`: even when there was nothing new to build, it re-checks S3, so a publish that failed earlier catches up. It skips itself when `LAKE_S3_BUCKET` is not set, so the platform still runs fully offline.
- **One Spark JVM at a time:** sequential tasks and `max_active_runs=1` (local mode, 1 GB driver) keep it within a small machine's memory.

## Data lake (PySpark 4.1, Parquet)

| Layer | What it holds | Spark techniques |
|---|---|---|
| Bronze | Raw API responses as JSON Lines, partitioned by `ingest_date`, written atomically (temp file + rename) | — (plain Python copy) |
| Silver | One validated row per event, partitioned by `symbol` / `trade_month` | Explicit schema (no inference), `explode`, `try_cast` / `try_to_timestamp`, `row_number()` deduplication, `sha2` event ids, broadcast join for partition pruning, **dynamic partition overwrite** for incremental upserts, `localCheckpoint` before overwriting partitions it reads |
| Gold | `daily_metrics`, `monthly_summary`, `return_correlation` | Window functions (`lag`, rolling `rowsBetween`, running max), **Spark SQL** (`min_by` / `max_by`, CTE + `LAG`), self-join with `corr()` |

Consistency between the two systems is verified: silver applies the same quality rules as the Python validator, produces the **same `event_id`s** as `staging.market_events`, and gold returns match the PostgreSQL analytics.

Spark is configured for the data size: 4 shuffle partitions instead of the default 200, adaptive query execution, one file per partition (`repartition` by partition columns), and `coalesce(1)` for small gold tables.

## Cloud: AWS (S3, IAM, Glue, Athena) with Terraform

Spark runs locally, where compute is free. **S3 is the durable, shared copy of the lake**, and Athena queries the gold tables with serverless SQL. Everything in AWS is declared in [`infra/terraform`](infra/terraform):

| Resource | Why it is configured this way |
|---|---|
| S3 bucket `<project>-lake-<account id>` | Public access blocked, ACLs disabled (`BucketOwnerEnforced`), SSE-S3 encryption, a bucket policy that denies non-TLS requests |
| Versioning + lifecycle | A bad sync can be undone for 7 days, then old versions expire. Athena results expire after 7 days. There is **no** move to Infrequent Access: IA bills each object as at least 128 KB for 30 days, so for files of a few KB it costs more |
| IAM user `…-lake-publisher` | **Least privilege**: list/get/put/delete only under `lake/`. It has no other prefix, bucket, or service. Its access key is created by hand, not by Terraform, so the secret never sits in the state file |
| Glue tables + Athena workgroup | Gold schemas are declared in code, so no crawler (which is billed per run) is needed. **Partition projection** on `symbol` means new data needs no `MSCK REPAIR`. The workgroup enforces encrypted results and cancels any query that scans more than 100 MB |
| AWS Budget | An email alert at 50% of a $5/month budget and when the month is forecast to go over it |

**The sync** (`src/lake/s3_sync.py`) is incremental and idempotent:
- It uploads a file only when its MD5 differs from the object's ETag. A single `PUT` with SSE-S3 makes the ETag the content MD5, and `Content-MD5` makes S3 reject corrupted uploads.
- It deletes objects whose local file is gone. Spark's partition overwrite renames files, so without this step Athena would count rewritten partitions twice.
- It uploads before it deletes, so readers never see a partition with its data missing.
- A guard refuses to run if a local layer is empty while S3 has data, so an unmounted volume can't wipe the bucket.

It is tested against moto's in-memory S3 (no account, no network). Table formats such as Apache Iceberg or Delta Lake would remove the remaining window where a reader can see old and new files together; that is the next step for multi-writer use.

### Deploying to AWS

1. Create an AWS account, turn on MFA for the root user, and use an admin IAM identity for Terraform (not the root user).
2. Install Terraform and the AWS CLI, and run `aws configure` with that admin identity.
3. Run `cp infra/terraform/terraform.tfvars.example infra/terraform/terraform.tfvars` and set `alert_email`.
4. Run `terraform -chdir=infra/terraform init`, then `terraform -chdir=infra/terraform apply`.
5. In the IAM console, create an access key for the `…-lake-publisher` user. Put `LAKE_S3_BUCKET` (Terraform output), `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_DEFAULT_REGION` in `airflow/.env`, then run `docker compose up -d`.
6. Run `python -m src.lake.run publish --dry-run` to preview the changes, then trigger the lakehouse DAG. Query the data in Athena with the `event-platform-lake` workgroup:

```sql
SELECT symbol, month, monthly_return
FROM event_platform_gold.monthly_summary
ORDER BY symbol, month;
```

At this data size (~0.3 MB), S3 storage and Athena scans cost cents per month.

## Streaming: Kafka → Spark Structured Streaming → PostgreSQL

```text
producer (replay | live) ──► Kafka  market.price_ticks  (3 partitions, key = symbol, 24 h retention)
                                   │
                    Spark Structured Streaming (micro-batch every 5 s, offsets in a checkpoint)
                                   │  parse + validate (same rules as the batch validator)
             ┌─────────────────────┼──────────────────────────────┐
             ▼                     ▼                              ▼
 streaming.price_ticks   streaming.rejected_ticks   streaming.price_bars_5m
 (one row per event_id)  (invalid + reason)         (event-time OHLC, 10-min watermark)
                                   └──► streaming.batch_log (throughput, duplicates, duration)
```

| Concern | How it is handled |
|---|---|
| Real data, zero cost | **Replay mode** (default) fetches one trading day of real 1-minute prices once (3 API credits), caches it, and replays it in event-time order 60× faster. **Live mode** polls real prices within a daily credit budget and the free tier's 8 requests per minute |
| Ordering | Messages are keyed by symbol, so each symbol's ticks stay in order within one partition |
| No loss or duplication inside Kafka | Producer uses `acks=all` and `enable.idempotence=true` |
| Exactly-once results | Spark stores Kafka offsets in a checkpoint and replays an unfinished micro-batch after a crash. Every sink is idempotent: ticks by `event_id`, rejects by Kafka partition+offset, bars by `(symbol, window)`, batch log by `(query, batch id)`. Each micro-batch is one PostgreSQL transaction |
| Duplicates | Spark recomputes `event_id` from the message (a wrong id can't break deduplication) and drops duplicates within the watermark before aggregating, so a re-sent tick is never counted twice in a bar |
| Late and out-of-order data | Bars are built by **event time** (when the price happened), not arrival time. A 10-minute watermark bounds the state Spark keeps |
| Bad messages | Same validation rules as the batch pipeline (tested for parity). Invalid messages go to `streaming.rejected_ticks` with the reason instead of stopping the stream |
| Startup order | Spark subscribes by topic pattern, so it waits for the topic instead of failing if it starts before the producer |
| Small machine | Opt-in Compose profile; Kafka in KRaft mode (no ZooKeeper) with a 256 MB heap; Spark driver 768 MB |

**Verified end to end:** one replay sent 1,170 ticks (3 symbols × 390 minutes), and all 1,170 were delivered and stored once, building 234 five-minute bars. Restarting the processor mid-stream resumed from the checkpoint. Replaying the same day again stored 0 new ticks and dropped every message as a duplicate.

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
- **Dashboard tests** (`tests/dashboard/`): stage-status aggregation, the Airflow API client (token refresh, unpause then trigger, error handling) against mocked HTTP, the lake and consistency calculations, the HTML diagram, and every page rendered headless with fake data, including a page detecting a deliberate lake/warehouse mismatch.
- **Streaming tests** (`tests/streaming/`, `tests/lake/test_stream_processor.py`, `tests/integration/test_streaming_sinks.py`): the message contract and time-zone handling (including daylight saving), replay ordering and pacing, the live-mode credit budget, Spark validation parity with the Python rules, event-time OHLC with out-of-order ticks, a full streaming run on a file source that restarts from its checkpoint and drops a re-sent duplicate, and the idempotent PostgreSQL sinks.
- **S3 publish tests** (`tests/test_s3_sync.py`): first sync, no-op re-sync, rewritten partitions, dry run, objects outside the prefix untouched, and the empty-layer guard, all against moto.
- **GitHub Actions** runs four jobs on every push and pull request: unit tests, integration tests against a `postgres:16` service container, Spark tests on Java 17, and `terraform fmt` + `validate`. Python 3.12 matches the Airflow image.

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

## Engineering notes

Problems hit while building this, and how they were solved:

- **Airflow 3.1.0 scheduler crash loop.** A mapped task (one per symbol) waiting to retry with exponential backoff crashed the scheduler with `'MappedOperator' object has no attribute 'max_retry_delay'`. Traced through the Airflow source to `TaskInstance.next_retry_datetime`. The fix was a fixed retry delay instead of exponential backoff, the stuck task was cleared through the REST API, and `tests/test_dag_settings.py` keeps it from coming back.
- **Docker Desktop wouldn't start after an unclean shutdown** (stale Unix-socket files that Windows can't delete). `start.bat` moves them aside while Docker isn't running.
- **Counting duplicates in streaming bars.** A test showed a re-sent tick counted twice in a 5-minute bar, because deduplication happened only at the sink. The fix deduplicates within the watermark before aggregating.
- **Time zones.** PySpark's `collect()` returns naive datetimes in the *Python process's* local time. The streaming job writes timestamps as explicit UTC strings, so results don't depend on the machine's time zone.

## Tech stack

Python · SQL · Apache Kafka (KRaft) · Spark Structured Streaming · Streamlit · Plotly · PostgreSQL (JSONB, window functions, materialized views) · SQLAlchemy · pandas · Apache Spark / PySpark 4.1 (DataFrames, Spark SQL) · Parquet · Apache Airflow 3 (TaskFlow, dynamic task mapping, Assets) · AWS (S3, IAM, Glue Data Catalog, Athena, Budgets) · Terraform · boto3 / moto · Docker Compose · pytest · GitHub Actions

## Roadmap

- [x] Production pipeline: raw → validation → quarantine → staging, with lineage
- [x] Automated testing: unit and PostgreSQL integration tests
- [x] CI with GitHub Actions
- [x] Analytics layer
- [x] Apache Spark / PySpark
- [x] Data lake: Bronze → Silver → Gold, Parquet and partitioning
- [x] Cloud: S3 data lake, least-privilege IAM, Athena, Terraform
- [x] Dashboard: live pipeline view, results, data-quality proof
- [x] Streaming: Kafka / Spark Structured Streaming
- [ ] Monitoring and alerting
- [ ] Performance benchmarking
