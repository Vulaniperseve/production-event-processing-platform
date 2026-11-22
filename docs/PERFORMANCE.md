# Performance

Phase 11: measure every stage, find what is actually slow, fix it, and prove
that the results didn't change.

**Machine:** Intel Core i5-6300U laptop (2 cores, 4 threads), 7.8 GB RAM,
Windows 11 with Docker Desktop (WSL 2, capped at 5 GB). The whole platform
(Airflow, PostgreSQL, Kafka, Prometheus, Grafana, dashboard) was running
during every measurement, so absolute numbers are modest; the before/after
ratios are what matter.

**Method**

- Ingestion: `benchmarks/bench_ingestion.py` runs the *real* pipeline steps
  on synthetic Twelve Data-shaped responses (no API calls), 1% deliberately
  invalid records, against a throwaway PostgreSQL. Each number is the median
  of 3 runs. Raw results: `benchmarks/results/{baseline,batched,optimized}.json`.
- Spark: `benchmarks/bench_lake.py` runs silver and gold on a *copy* of the
  real bronze layer inside the Airflow image. Timings on this laptop varied
  by up to 2× between identical runs, so the two configurations were run
  alternately, 3 times each, and medians are reported.
  Raw results: `benchmarks/results/lake_runs.jsonl`.
- Correctness: unit, integration and Spark test suites, plus new tests that
  pin the optimized behaviour to the old one (see each fix).

## Results

### Batch ingestion (raw → validate → transform → load)

| Records | Before | After | Speed-up | Throughput after |
|---:|---:|---:|---:|---:|
| 1,000 | 3.4 s | 0.40 s | 8.7× | ~2,500 records/s |
| 10,000 | 33.2 s | 1.37 s | **24×** | ~7,300 records/s |
| 50,000 | 114.3 s | 10.2 s | 11× | ~4,900 records/s |

Per stage, 10,000 records:

| Stage | Before | After | Speed-up |
|---|---:|---:|---:|
| Validate records | 12,632 ms | 33 ms | 383× |
| Transform | 4,602 ms | 114 ms | 41× |
| Load (new rows) | 4,288 ms | 687 ms | 6.2× |
| Rerun (all duplicates skipped) | 3,474 ms | 444 ms | 7.8× |

### Spark data lake (medians of 3)

| Step | Before | After | Speed-up |
|---|---:|---:|---:|
| Start Spark (JVM) | 19.5 s | 15.9 s | 1.2× |
| Silver, first build in a JVM | 27.5 s | 17.3 s | 1.6× |
| Gold, first build in a JVM | 12.9 s | 7.7 s | 1.7× |
| Silver, warm JVM | 14.9 s | 5.2 s | 2.8× |

The Spark test suite went from 5 min 25 s to 3 min 24 s with the same
settings. A real full-refresh lake run through Airflow afterwards took
90 s (silver) and 49 s (gold); earlier incremental runs took 79–148 s and
69–166 s. The lake still matched PostgreSQL exactly: all 315 event ids, and
a 0.0 difference in returns.

### Dashboard first load

| | Before | After |
|---|---|---|
| First thing on screen after a restart | blank white page for ~10 s | themed sidebar and a loading card within ~2 s |

## What was slow, and the fix

**1. Validation parsed every date with pandas (~1.3 ms per record).**
`pd.to_datetime` on one string at a time spends nearly all its time on
format inference. The validator now tries `datetime.fromisoformat` (about a
microsecond) for values that start with a plain `YYYY-MM-DD` date and fall
inside the range pandas supports, and falls back to `pd.to_datetime` for
everything else, so the set of accepted values is unchanged.
`tests/test_datetime_fast_path.py` checks 26 edge cases (time zones,
fractional seconds, ISO week dates, out-of-range years, invalid dates)
against pandas itself. This mattered twice over, because the pipeline
validates each response twice by design (so `transform_and_load` depends
only on the raw layer).

**2. The transformer hashed row by row with `DataFrame.apply(axis=1)`**,
which builds a pandas Series for every row. A list comprehension over the
columns computes the same `sha256(symbol|datetime|type)[:36]` ids (pinned by
a test). It also stopped writing `data/processed/events.csv` on every run, a
debugging leftover from phase 1 that nothing read.

**3. The loader built one huge INSERT statement.** SQLAlchemy compiled a SQL
string with 7 bind parameters per row before sending it. Step one
(`batched`) passed the rows as a parameter list, which SQLAlchemy 2.0 sends
as pages of multi-row INSERTs: 3.7× faster. Step two (`optimized`) uses
PostgreSQL's bulk loader: `COPY` into a temporary table, then one
`INSERT … SELECT … ON CONFLICT (event_id) DO NOTHING`. That kept every
guarantee: idempotency, an exact inserted count, and the lineage foreign
key (the integration tests still get an `IntegrityError` for an unknown
`raw_event_id`). Loading 50,000 rows went from 26 s to 3.2 s.

**4. Spark spent its time starting up, not processing.** With a few thousand
rows, a job's cost is the JVM starting, loading classes and compiling hot
code. "Small data mode" in `src/lake/spark.py` (on by default,
`SPARK_SMALL_DATA_MODE=false` to disable):

- `-XX:TieredStopAtLevel=1`: only the quick JIT compiler, so code is fast
  sooner. The slower optimising compiler only pays off in long jobs.
- `-XX:+UseSerialGC`: the simplest garbage collector, plenty for a 1 GB heap.
- `spark.sql.codegen.wholeStage=false`: don't generate and compile Java code
  for each query plan, which costs more than it saves on tiny data.

These trade peak throughput for fast startup. On a cluster with real data
volumes, turn the mode off. Single runs of each setting on its own landed
in between, but were too noisy to say how much each contributes.

**5. The dashboard drew nothing until the whole page was ready.** The first
visit after a restart imports pandas and SQLAlchemy (~2 s) and reads the
lake's file stats (~1.9 s through the Windows–Docker file bridge) before
the page can draw. `dashboard/app.py` now shows a themed loading card
immediately, the Prometheus health check moved below the page so it never
delays it, and the lake stats call `stat()` once per file instead of twice.

## Earlier performance work (phase 10)

**Airflow idle CPU: ~90% → ~3% per service.** The triggerer's and
dag-processor's Docker health checks booted the whole Airflow CLI, ran past
their timeout, and piled up (about 20 processes per container). They now
run a sub-second heartbeat query (`airflow/config/job_healthcheck.py`).
Memory dropped from 1.1 GB to 200–340 MB per service.

## What I'd look at next

- **Raw store save** (now the largest single step at 50,000 records, ~1.5 s):
  it serialises the whole response to JSONB and hashes it.
  Fine at the real batch size (100 records per symbol).
- **One Spark session for silver and gold** would save one JVM start
  (~16 s) per lake run, at the cost of the two steps no longer retrying or
  showing progress separately in Airflow and the dashboard. Not worth it yet.
- **Data volume:** at the real daily volume (300 records) every stage is now
  well under a second except Spark, whose cost is dominated by startup.
