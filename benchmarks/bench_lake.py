"""
Benchmark the Spark lake jobs: where do silver and gold spend their time?

Runs on a throwaway COPY of the real bronze layer (the real lake is never
written), inside the Airflow image, which has Java and PySpark:

    docker compose -f airflow/docker-compose.yaml --profile streaming run --rm --no-deps -T \
        -v ../benchmarks:/opt/airflow/project/benchmarks:ro \
        --entrypoint python stream-processor -m benchmarks.bench_lake --label tuned

Add `-e SPARK_SMALL_DATA_MODE=false` to measure without the small-data
settings in src/lake/spark.py (the original baseline).

Times: starting a Spark session (the JVM), a full silver build, a gold build,
and both again in the same session (what a warm JVM costs). Results go to
stdout as JSON, to paste into docs/PERFORMANCE.md.
"""

import argparse
import json
import logging
import shutil
import tempfile
import time
from pathlib import Path


def timed(fn):
    t0 = time.perf_counter()
    result = fn()
    return round(time.perf_counter() - t0, 2), result


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", default="latest")
    parser.add_argument("--lake", default="/opt/airflow/lake", help="real lake (read only)")
    parser.add_argument("--conf", action="append", default=[], metavar="KEY=VALUE",
                        help="extra Spark setting to try (repeatable)")
    args = parser.parse_args(argv)
    extra = dict(item.split("=", 1) for item in args.conf)
    logging.getLogger("production_event_platform").setLevel(logging.WARNING)

    from src.lake.gold import build_gold
    from src.lake.silver import build_silver
    from src.lake.spark import build_spark

    work = Path(tempfile.mkdtemp(prefix="lakebench-"))
    try:
        shutil.copytree(Path(args.lake) / "bronze", work / "bronze")
        files = sum(1 for _ in (work / "bronze").rglob("*.json"))

        result = {"label": args.label, "bronze_files": files, "extra_conf": extra}
        result["spark_start_s"], spark = timed(lambda: build_spark("bench", extra or None))
        spark.sparkContext.setLogLevel("ERROR")
        try:
            result["silver_cold_s"], metrics = timed(lambda: build_silver(spark, None, root=work))
            result["silver_rows"] = metrics.get("silver_rows_written")
            result["gold_s"], counts = timed(lambda: build_gold(spark, root=work))
            result["gold_rows"] = sum(counts.values())
            # same session again: no JVM start, Spark's code already warmed up
            result["silver_warm_s"], _ = timed(lambda: build_silver(spark, None, root=work))
            result["gold_warm_s"], _ = timed(lambda: build_gold(spark, root=work))
        finally:
            t0 = time.perf_counter()
            spark.stop()
            result["spark_stop_s"] = round(time.perf_counter() - t0, 2)
    finally:
        shutil.rmtree(work, ignore_errors=True)

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
