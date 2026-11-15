"""
Run the pipeline locally (without Airflow):

    python main.py                  # default symbol AAPL
    python main.py AAPL MSFT GOOGL  # several symbols, then refresh analytics
"""

import sys

from src.analytics.analytics import refresh_analytics, symbol_summary
from src.pipeline.steps import run_pipeline


def main():

    symbols = [s.upper() for s in sys.argv[1:]] or ["AAPL"]

    for symbol in symbols:

        result = run_pipeline(symbol=symbol, outputsize=100)

        validation = result["validation"]
        load = result["load"]

        print(f"\n{symbol}:")
        print(f"  raw_event_id : {validation['raw_event_id']}")
        print(f"  valid        : {validation['valid_count']}")
        print(f"  quarantined  : {validation['invalid_count']}")

        if load is None:
            print("  No valid records were loaded.")
        else:
            print(f"  inserted     : {load['inserted']}")
            print(f"  duplicates   : {load['skipped']}")

    refresh_analytics()

    def pct(value):
        return "n/a" if value is None else f"{value:+.2%}"

    print("\nSymbol summary:")
    for row in symbol_summary():
        print(
            f"  {row['symbol']:<6} {row['first_date']} -> {row['last_date']}  "
            f"days={row['trading_days']:<4} "
            f"return={pct(row['period_return'])}  "
            f"max_drawdown={pct(row['max_drawdown'])}"
        )


if __name__ == "__main__":
    main()
