"""
Benchmark the batch ingestion path: raw store -> validate -> transform -> load.

Runs the real pipeline steps (src/pipeline/steps.py) against synthetic API
responses of increasing size, so no API calls (and no API credits) are used.
About 1% of the generated records are invalid, so the quarantine path is
exercised too.

It needs a throwaway PostgreSQL whose name contains "test" (the schemas are
dropped and rebuilt), the same rule as the integration tests:

    docker run -d --name pg-test -e POSTGRES_PASSWORD=postgres \
        -e POSTGRES_DB=pipeline_test -p 5433:5432 postgres:16

    DB_HOST=localhost DB_PORT=5433 DB_NAME=pipeline_test DB_USER=postgres \
    DB_PASSWORD=postgres python -m benchmarks.bench_ingestion --label baseline

    python -m benchmarks.bench_ingestion --compare baseline optimized

Each timing is the median of --repeat runs. Results go to
benchmarks/results/<label>.json.
"""

import argparse
import json
import logging
import os
import platform
import statistics
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

RESULTS_DIR = Path(__file__).parent / "results"
DEFAULT_SIZES = (1_000, 10_000, 50_000)
INVALID_EVERY = 100  # 1% of records are invalid


def make_payload(n_records, symbol="BENCH", invalid_every=INVALID_EVERY):
    """A Twelve Data-shaped response with n_records 1-minute bars."""
    start = datetime(2020, 1, 1, 9, 30)
    values = []
    for i in range(n_records):
        record = {
            "datetime": (start + timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M:%S"),
            "open": "100.0", "high": "101.0", "low": "99.0",
            "close": f"{100 + (i % 500) / 10:.2f}",
            "volume": "1000",
        }
        if invalid_every and i % invalid_every == invalid_every - 1:
            # rotate through the validator's rejection reasons
            kind = (i // invalid_every) % 4
            if kind == 0:
                record["close"] = ""
            elif kind == 1:
                record["datetime"] = "not-a-date"
            elif kind == 2:
                record["close"] = "abc"
            else:
                record["close"] = "-5"
        values.append(record)
    return {
        "meta": {"symbol": symbol, "interval": "1min", "currency": "USD"},
        "values": values,
        "status": "ok",
    }


def timed(fn, repeat):
    """Median wall time of fn() over `repeat` runs, plus the last result."""
    times, result = [], None
    for _ in range(repeat):
        t0 = time.perf_counter()
        result = fn()
        times.append(time.perf_counter() - t0)
    return statistics.median(times), result


def check_database():
    db_name = os.getenv("DB_NAME", "")
    if "test" not in db_name.lower():
        sys.exit(f"Refusing to benchmark against DB_NAME={db_name!r}: "
                 "the name must contain 'test' (the schemas are dropped).")


def reset_database(engine):
    from tests.integration.helpers import drop_schemas, run_migration
    drop_schemas(engine)
    run_migration(engine)


def truncate(engine):
    with engine.begin() as conn:
        conn.exec_driver_sql(
            "TRUNCATE staging.market_events, quarantine.quarantined_records, "
            "raw.raw_api_events RESTART IDENTITY CASCADE"
        )


def bench_size(engine, n, repeat):
    from src.loaders.postgres_loader import PostgresLoader
    from src.loaders.raw_store import RawStore
    from src.pipeline import steps
    from src.transformers.transformer import DataTransformer
    from src.validators.validator import DataValidator

    payload = make_payload(n)
    row = {"records": n}

    # --- pure Python stages (no database) --------------------------------
    row["validate_s"], (valid, invalid) = timed(
        lambda: DataValidator.validate_records(payload), repeat)
    row["valid"], row["invalid"] = len(valid), len(invalid)

    clean = {"meta": payload["meta"], "status": "ok", "values": valid}
    row["transform_s"], df = timed(lambda: DataTransformer.transform(clean), repeat)

    # --- database stages ----------------------------------------------------
    def fresh_load():
        truncate(engine)
        t0 = time.perf_counter()
        result = PostgresLoader.load(df)
        return time.perf_counter() - t0, result

    load_times = []
    for _ in range(repeat):
        seconds, (inserted, _) = fresh_load()
        load_times.append(seconds)
    row["load_s"] = statistics.median(load_times)
    row["inserted"] = inserted

    # rerun of the same batch: every row is a duplicate (idempotency path)
    row["reload_s"], (inserted_again, skipped) = timed(lambda: PostgresLoader.load(df), repeat)
    row["reload_skipped"] = skipped
    if inserted_again != 0:
        raise RuntimeError("a rerun must not insert anything (idempotency broken)")

    # --- the real pipeline steps, end to end (what one Airflow run does) -----
    def end_to_end():
        truncate(engine)
        t0 = time.perf_counter()
        raw_id = RawStore.save(payload)
        t1 = time.perf_counter()
        summary = steps.validate_raw(raw_id)
        t2 = time.perf_counter()
        steps.transform_and_load(raw_id)
        t3 = time.perf_counter()
        return {"raw_save_s": t1 - t0, "validate_step_s": t2 - t1,
                "transform_load_step_s": t3 - t2, "total_s": t3 - t0,
                "quarantined": summary["invalid_count"]}

    runs = [end_to_end() for _ in range(repeat)]
    for key in ("raw_save_s", "validate_step_s", "transform_load_step_s", "total_s"):
        row[key] = statistics.median(r[key] for r in runs)
    row["records_per_s"] = n / row["total_s"]
    return row


def run(label, sizes, repeat):
    check_database()
    logging.getLogger("production_event_platform").setLevel(logging.WARNING)

    from database.database import engine

    reset_database(engine)
    rows = []
    for n in sizes:
        print(f"  {n:>7,} records ...", end="", flush=True)
        try:
            row = bench_size(engine, n, repeat)
            print(f" {row['total_s']:.2f} s end to end, {row['records_per_s']:,.0f} records/s")
        except Exception as exc:  # a stage that can't handle the size is a result too
            row = {"records": n, "error": f"{type(exc).__name__}: {str(exc)[:200]}"}
            print(f" FAILED ({row['error']})")
        rows.append(row)

    result = {
        "label": label,
        "date": datetime.now().isoformat(timespec="seconds"),
        "repeat": repeat,
        "machine": f"{platform.processor() or platform.machine()} ({os.cpu_count()} logical CPUs), "
                   f"Python {platform.python_version()}",
        "results": rows,
    }
    RESULTS_DIR.mkdir(exist_ok=True)
    out = RESULTS_DIR / f"{label}.json"
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"Saved {out}")
    return result


STAGES = [
    ("validate_s", "Validate records"),
    ("transform_s", "Transform"),
    ("load_s", "Load (new rows)"),
    ("reload_s", "Reload (all duplicates)"),
    ("raw_save_s", "Raw store save"),
    ("validate_step_s", "validate_raw step"),
    ("transform_load_step_s", "transform_and_load step"),
    ("total_s", "End to end"),
]


def compare(before_label, after_label):
    """Print a markdown before/after table for every size both runs have."""
    before = json.loads((RESULTS_DIR / f"{before_label}.json").read_text(encoding="utf-8"))
    after = json.loads((RESULTS_DIR / f"{after_label}.json").read_text(encoding="utf-8"))
    after_by_n = {r["records"]: r for r in after["results"]}

    for b in before["results"]:
        a = after_by_n.get(b["records"])
        if a is None:
            continue
        print(f"\n**{b['records']:,} records**\n")
        print(f"| Stage | {before_label} | {after_label} | Speed-up |")
        print("|---|---:|---:|---:|")
        for key, name in STAGES:
            bv, av = b.get(key), a.get(key)
            fmt = lambda v: "failed" if v is None else f"{v * 1000:,.0f} ms"  # noqa: E731
            speed = f"{bv / av:.1f}×" if bv and av else "–"
            print(f"| {name} | {fmt(bv)} | {fmt(av)} | {speed} |")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--label", default="latest", help="name of the results file")
    parser.add_argument("--sizes", type=int, nargs="+", default=list(DEFAULT_SIZES))
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--compare", nargs=2, metavar=("BEFORE", "AFTER"))
    args = parser.parse_args(argv)

    if args.compare:
        compare(*args.compare)
    else:
        run(args.label, args.sizes, args.repeat)


if __name__ == "__main__":
    main()
