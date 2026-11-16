"""
Run lake jobs by hand (Airflow runs them via the lakehouse DAG):

    python -m src.lake.run bronze
    python -m src.lake.run silver [--full-refresh]
    python -m src.lake.run gold
    python -m src.lake.run all [--full-refresh]
"""

import argparse


def main():
    parser = argparse.ArgumentParser(description="Market data lake jobs")
    parser.add_argument("stage", choices=["bronze", "silver", "gold", "all"])
    parser.add_argument("--full-refresh", action="store_true",
                        help="rebuild silver from all of bronze")
    args = parser.parse_args()

    ingest_dates = None

    if args.stage in ("bronze", "all"):
        from src.lake.bronze import land_raw_to_bronze

        landed = land_raw_to_bronze()
        print(f"bronze: {landed}")
        ingest_dates = landed["ingest_dates"]

    if args.stage in ("silver", "gold", "all"):
        from src.lake.gold import build_gold
        from src.lake.silver import build_silver
        from src.lake.spark import spark_session

        with spark_session() as spark:
            if args.stage in ("silver", "all"):
                dates = None if (args.full_refresh or args.stage == "silver") else ingest_dates
                print(f"silver: {build_silver(spark, dates)}")
            if args.stage in ("gold", "all"):
                print(f"gold: {build_gold(spark)}")


if __name__ == "__main__":
    main()
