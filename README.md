# Production Event Processing Platform

[![CI](https://github.com/Vulaniperseve/production-event-processing-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/Vulaniperseve/production-event-processing-platform/actions/workflows/ci.yml)

A production-oriented data engineering platform that ingests market data from a REST API, preserves every raw response, validates and quarantines bad records, loads clean events idempotently into PostgreSQL, builds an SQL analytics layer, and streams live price ticks through **Kafka** and **Spark Structured Streaming**, runs a Spark **Bronze → Silver → Gold** data lake in Parquet, published to **Amazon S3** and queryable with **Athena** (infrastructure as code with Terraform). Prometheus and Grafana monitor data freshness, quality, lake/warehouse reconciliation and streaming lag with tested alert rules. Apache Airflow orchestrates it in Docker, and every change runs unit, integration and Spark tests in GitHub Actions.

## Quick start

1. Install [Docker Desktop](https://www.docker.com/products/docker-desktop/) and clone the repo. Copy `airflow/.env.example` to `airflow/.env`, add your free Twelve Data API key, then run `python scripts/generate_secrets.py` to create Airflow's secret keys.
2. Double-click **`start.bat`**. It starts Docker if needed, starts the platform and opens the dashboard at **http://localhost:8501**.
3. Press **Run pipeline** and watch each stage run live, from the API to the Spark data lake.

Streaming (optional, needs more memory): double-click **`stream.bat`** to start Kafka and the Spark streaming job and replay a real trading day. Watch it on the dashboard's *Live stream* page.

Monitoring runs by default: Grafana is at **http://localhost:3000**, and firing alerts also appear in the dashboard's sidebar.

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
- **Right-sized:** LocalExecutor on a single host (no Celery worker or Redis broker), with retries on a fixed delay and task timeouts.
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

## Monitoring and alerting: Prometheus + Grafana

```text
metrics exporter (Python) ──► Prometheus (scrape every 30 s, 11 alert rules) ──► Grafana dashboard
   reads PostgreSQL, the Airflow REST API,               │
   the lake, Kafka and Spark's checkpoint                └──► firing alerts also shown in the Streamlit sidebar
```

Monitoring watches **the data, not just the servers**. The exporter (`monitoring/exporter.py`) publishes:

| Area | Metrics | Alert when |
|---|---|---|
| Pipeline runs | last result, last success time and duration per DAG | the latest run failed and no retry is running; no success in 26 h |
| Freshness | hours since load and days behind, per symbol | more than 4 days behind (weekends allowed) |
| Quality | raw statuses, quarantined records, quarantine rate | over 5% quarantined |
| Reconciliation | events only in the lake or only in PostgreSQL, largest return difference | the Spark lake and PostgreSQL disagree |
| Streaming | ticks, duplicates, rejects, latency p95, last batch age, **consumer lag** (Kafka end offsets vs Spark's committed checkpoint), offset resets | lag > 500; messages waiting but no batch for 2 min; offsets went backwards |
| Monitoring itself | each source's up/down and collection time | the exporter or a source is down |

Design choices:
- Every rule has a `for:` duration, so a single slow moment doesn't page anyone. On this laptop that suppressed several real blips.
- Every alert says what is wrong *and what to do* (a runbook in the annotation).
- The rules are **unit-tested with `promtool test rules`** (fake metric histories in, expected alerts out), and a Python test checks that every metric used by a rule or a Grafana panel exists in the exporter. A typo there would otherwise fail silently: the alert would simply never fire.
- The Grafana datasource and dashboard are provisioned from files in Git, not clicked together.
- Collectors are independent: one source being down is reported as a metric, and the rest keep working.

**Tested with a real incident:** with Spark stopped and a replay running, lag climbed to 1,170 and `StreamStalled` fired after its 2-minute window, in Grafana and in the dashboard sidebar. After Spark started it caught up, and both alerts resolved by themselves.

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

## CI/CD and security

```text
push / PR ──► CI: unit · PostgreSQL integration · Spark · Terraform · promtool · lint + security
                     │ all green on main
                     ▼
              CD: build 3 images ──► Trivy vulnerability scan ──► publish to ghcr.io (commit SHA + latest)
```

| Check | Tool | Fails the build when |
|---|---|---|
| Secrets in code | **gitleaks**, over the *entire Git history* | any new secret appears, in any commit |
| Bugs and insecure code | **Ruff**, including Bandit's security rules | SQL built from strings, unsafe subprocess calls, hard-coded passwords, unused or undefined names |
| Vulnerable dependencies | **pip-audit** on every pinned requirements file | a package has a known vulnerability (CVE/PYSEC) |
| Misconfiguration | **Trivy** on Dockerfiles and Terraform | a HIGH or CRITICAL issue isn't fixed or explicitly accepted |
| Vulnerable images | **Trivy** on each built image, before publishing | a CRITICAL vulnerability that has a fix |
| Outdated dependencies | **Dependabot** | (opens a weekly PR, and CI decides whether it's safe) |

- **Delivery is gated:** images are only published after CI has passed on `main`, built from the exact commit CI tested, and tagged with that commit's SHA, so every running image traces back to one tested commit.
- **Least privilege in CI:** workflows read the repository only. The publish job alone may write packages.
- **Secrets:** real values live only in `airflow/.env` (git-ignored). Docker Compose refuses to start without Airflow's keys, and the Twelve Data key travels in an HTTP header so it can't leak into URLs, errors or logs. See [SECURITY.md](SECURITY.md).
- **Accepted risks are written down, not silenced:** `.gitleaksignore` and `.trivyignore.yaml` list each reviewed finding with its reason. Anything new still fails.

**What the first scans found, and the fixes:**
1. Airflow's internal signing and encryption keys had been committed in `airflow.cfg`. All three were rotated, moved to the environment, and encryption was switched on: the Fernet key had been overridden to *empty*, so Airflow wasn't encrypting anything.
2. The API key could leak into logs through HTTP error messages, which include the URL. Fixed with header authentication, and tested.
3. Five advisories in `urllib3` and `certifi`. Upgraded.
4. A possible SQL injection warning: a safe, constant identifier now also gets an explicit allow-list check.

## Testing and CI

- **Unit tests** (`tests/`): validation, transformation, deterministic IDs, and the pipeline steps with in-memory fakes. No database needed.
- **Integration tests** (`tests/integration/`): a real PostgreSQL 16 covering migrations (idempotency, legacy upgrade, ORM/SQL schema-drift check), storage, end-to-end pipeline replays, and the analytics calculations checked against hand-computed values. These tests drop schemas, so they only run with `RUN_INTEGRATION_TESTS=1` and refuse any database whose name doesn't contain `test`.
- **Spark tests** (`tests/lake/`): silver validation and parity with the Python validator, identical event ids, latest-wins deduplication, idempotent reruns, incremental runs that leave untouched partitions' files alone, full refresh equal to incremental, and the gold calculations. They skip automatically when PySpark or Java is unavailable.
- **Dashboard tests** (`tests/dashboard/`): stage-status aggregation, the Airflow API client (token refresh, unpause then trigger, error handling) against mocked HTTP, the lake and consistency calculations, the HTML diagram, and every page rendered headless with fake data, including a page detecting a deliberate lake/warehouse mismatch.
- **Streaming tests** (`tests/streaming/`, `tests/lake/test_stream_processor.py`, `tests/integration/test_streaming_sinks.py`): the message contract and time-zone handling (including daylight saving), replay ordering and pacing, the live-mode credit budget, Spark validation parity with the Python rules, event-time OHLC with out-of-order ticks, a full streaming run on a file source that restarts from its checkpoint and drops a re-sent duplicate, and the idempotent PostgreSQL sinks.
- **Monitoring tests** (`tests/monitoring/`, `monitoring/prometheus/alerts_test.yml`): Spark-checkpoint offset parsing and consumer lag (including the topic-recreated case), every collector against fakes, a failing source being reported rather than crashing, consistency between the exporter, alert rules and Grafana panels, and promtool unit tests for the alert rules.
- **S3 publish tests** (`tests/test_s3_sync.py`): first sync, no-op re-sync, rewritten partitions, dry run, objects outside the prefix untouched, and the empty-layer guard, all against moto.
- **GitHub Actions** runs six jobs on every push and pull request: unit tests, integration tests against a `postgres:16` service container, Spark tests on Java 17, `terraform fmt` + `validate`, `promtool` checks plus alert-rule unit tests, and lint + security (Ruff, gitleaks, pip-audit, Trivy). Python 3.12 matches the Airflow image.

## Performance

Every stage was benchmarked, the slow ones fixed, and the results checked to be identical. Full method and numbers: [`docs/PERFORMANCE.md`](docs/PERFORMANCE.md).

| What | Before | After |
|---|---|---|
| Ingest 10,000 records end to end | 33.2 s | 1.4 s (**24×**) |
| Validate 10,000 records | 12.6 s | 0.03 s (date parsing fast path, same results as pandas) |
| Load 50,000 rows into PostgreSQL | 26 s | 3.2 s (`COPY` into a temp table + `INSERT … ON CONFLICT`) |
| Spark silver / gold, first build | 27.5 s / 12.9 s | 17.3 s / 7.7 s (JVM tuned for small data) |
| Airflow triggerer / dag-processor, idle | ~90% CPU, 1.1 GB each | ~3% CPU, 200–340 MB |
| Dashboard, first thing on screen | ~10 s blank | ~2 s |

```bash
# throwaway PostgreSQL (see Running locally), then:
DB_HOST=localhost DB_PORT=5433 DB_NAME=pipeline_test DB_USER=postgres DB_PASSWORD=postgres \
  python -m benchmarks.bench_ingestion --label mine
python -m benchmarks.bench_ingestion --compare baseline mine
```

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
- **A version pin silently broke an Airflow upgrade.** Upgrading to Airflow 3.3.2 (to clear critical CVEs the image scan found) made the database migration fail with `cannot import name 'mapped_column'`. Cause: `requirements.txt` pinned SQLAlchemy 1.4, and pip downgraded Airflow's SQLAlchemy 2.0 without complaint. Fixes: the pins now match the versions the Airflow image ships, so tests run on the same libraries as production, and the Dockerfile installs `apache-airflow==<version>` alongside them, so a conflicting pin fails the build instead of the running system.
- **Health checks that starved the services they checked.** The triggerer and dag-processor used ~90% CPU and 1.1 GB each while idle. Their Docker health check ran `airflow jobs check`, which boots the whole Airflow CLI and took longer than the check's timeout on this laptop. Docker doesn't kill a timed-out check, so about 20 CLI processes piled up in each container, and after the 3.3.2 upgrade the services never even finished starting. The check is now `airflow/config/job_healthcheck.py`, the same heartbeat query against the metadata database in under a second, and the redundant startup `airflow db check` is skipped. Idle usage dropped to ~3% CPU and 200–340 MB per service.
- **Noisy benchmarks.** On a busy laptop, identical Spark runs differed by up to 2× (JVM startup ranged from 21 s to 45 s with the same settings), so a single before/after pair proved nothing. The configurations were run alternately, three times each, and only the medians were reported; the tuned setup won every paired run.
- **Docker Desktop wouldn't start after an unclean shutdown** (stale Unix-socket files that Windows can't delete). `start.bat` moves them aside while Docker isn't running.
- **Counting duplicates in streaming bars.** A test showed a re-sent tick counted twice in a 5-minute bar, because deduplication happened only at the sink. The fix deduplicates within the watermark before aggregating.
- **Silent data loss in streaming, found by monitoring.** Kafka originally kept its data inside the container. Recreating the container restarted the topic's offsets at 0, while Spark's checkpoint still pointed to offset 1,560, so Spark skipped every new message ("some data may have been lost") and stored nothing. The lag metric even showed 0, because it clamped negative values. Fixes: Kafka data on a named volume, 7-day retention, the exporter detects offsets going backwards (`KafkaOffsetsReset` alert) and counts those messages as lag. Recovery was safe *because the sinks are idempotent*: resetting Spark's checkpoint and reprocessing the whole topic restored the missing trading day, and already-stored ticks were dropped as duplicates.
- **Kafka health check slower than its timeout.** `kafka-broker-api-versions.sh` starts a JVM (about 30 s of CPU on this laptop), so the broker was always "unhealthy". Replaced with a port check.
- **Time zones.** PySpark's `collect()` returns naive datetimes in the *Python process's* local time. The streaming job writes timestamps as explicit UTC strings, so results don't depend on the machine's time zone.

## Tech stack

Python · SQL · Prometheus · Grafana · Apache Kafka (KRaft) · Spark Structured Streaming · Streamlit · Plotly · PostgreSQL (JSONB, window functions, materialized views) · SQLAlchemy · pandas · Apache Spark / PySpark 4.1 (DataFrames, Spark SQL) · Parquet · Apache Airflow 3 (TaskFlow, dynamic task mapping, Assets) · AWS (S3, IAM, Glue Data Catalog, Athena, Budgets) · Terraform · boto3 / moto · Docker Compose · pytest · GitHub Actions

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
- [x] Monitoring and alerting: Prometheus, Grafana, tested alert rules
- [x] CI/CD and security: secret, dependency, code and image scanning; gated image publishing
- [x] Performance benchmarking: measured, fixed and verified (ingestion up to 24× faster)

## Author

**Vulani Makwakwa** · B.Eng.Tech Computer Engineering (CPUT, 2026) · Cape Town · [GitHub](https://github.com/Vulaniperseve) · [LinkedIn](https://www.linkedin.com/in/vulani-makwakwa-24229a23a/)
