"""
Run the pipeline locally (without Airflow):

    python main.py            # default symbol AAPL
    python main.py MSFT
"""

import sys

from src.pipeline.steps import run_pipeline


def main():

    symbol = sys.argv[1] if len(sys.argv) > 1 else "AAPL"

    result = run_pipeline(symbol=symbol)

    validation = result["validation"]
    load = result["load"]

    print("\nPipeline completed.")
    print(f"  raw_event_id : {validation['raw_event_id']}")
    print(f"  valid        : {validation['valid_count']}")
    print(f"  quarantined  : {validation['invalid_count']}")

    if load is None:
        print("  No valid records were loaded.")
    else:
        print(f"  inserted     : {load['inserted']}")
        print(f"  duplicates   : {load['skipped']}")


if __name__ == "__main__":
    main()
