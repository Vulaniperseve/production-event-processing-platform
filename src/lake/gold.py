"""
Gold layer (Spark): business-ready datasets built from silver.

    gold/daily_metrics/symbol=AAPL/      returns, moving averages, volatility, drawdown
    gold/monthly_summary/                monthly returns and risk per symbol (Spark SQL)
    gold/return_correlation/             pairwise correlation of daily returns (self-join)

Gold is fully recomputed from silver on each run: rolling windows and
running peaks depend on a symbol's whole history, and silver is small
enough that a full rebuild is the simplest correct choice.
"""

import math

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.lake.config import gold_path, silver_path
from src.logger import logger


def daily_metrics(silver):
    """One row per symbol per trading day, with window-function indicators."""

    # Last observation per day (handles intraday intervals too)
    last_of_day = Window.partitionBy("symbol", "trade_date").orderBy(F.col("trade_ts").desc())
    daily = (
        silver.withColumn("_rn", F.row_number().over(last_of_day))
        .where(F.col("_rn") == 1)
        .select("symbol", "trade_date", "close_price")
    )

    by_day = Window.partitionBy("symbol").orderBy("trade_date")
    w5 = by_day.rowsBetween(-4, Window.currentRow)
    w20 = by_day.rowsBetween(-19, Window.currentRow)
    running = by_day.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    close = F.col("close_price")
    prev = F.col("prev_close")

    return (
        daily
        .withColumn("prev_close", F.lag("close_price").over(by_day))
        .withColumn("daily_return", F.when(prev > 0, close / prev - 1))
        .withColumn("log_return", F.when((prev > 0) & (close > 0), F.log(close / prev)))
        # Rolling statistics stay NULL until the window is full
        .withColumn("sma_5", F.when(F.count("*").over(w5) == 5, F.avg(close).over(w5)))
        .withColumn("sma_20", F.when(F.count("*").over(w20) == 20, F.avg(close).over(w20)))
        .withColumn(
            "volatility_20d",
            F.when(F.count("daily_return").over(w20) == 20, F.stddev_samp("daily_return").over(w20)),
        )
        .withColumn("volatility_20d_annualized", F.col("volatility_20d") * F.lit(math.sqrt(252)))
        .withColumn("running_peak", F.max(close).over(running))
        .withColumn("drawdown", close / F.col("running_peak") - 1)
        .withColumn("trading_day_number", F.row_number().over(by_day))
    )


MONTHLY_SUMMARY_SQL = """
WITH monthly AS (
    SELECT
        symbol,
        date_format(trade_date, 'yyyy-MM')        AS month,
        COUNT(*)                                  AS trading_days,
        min_by(close_price, trade_date)           AS first_close,
        max_by(close_price, trade_date)           AS last_close,
        AVG(daily_return)                         AS avg_daily_return,
        STDDEV_SAMP(daily_return)                 AS daily_volatility,
        MAX(daily_return)                         AS best_day_return,
        MIN(daily_return)                         AS worst_day_return,
        MIN(drawdown)                             AS deepest_drawdown
    FROM daily_metrics
    GROUP BY symbol, date_format(trade_date, 'yyyy-MM')
)
SELECT
    *,
    -- month-over-month: this month's last close vs last month's last close
    last_close / LAG(last_close) OVER (PARTITION BY symbol ORDER BY month) - 1 AS monthly_return
FROM monthly
"""


def monthly_summary(spark, daily):
    daily.createOrReplaceTempView("daily_metrics")
    return spark.sql(MONTHLY_SUMMARY_SQL)


def return_correlation(daily):
    """Pearson correlation of daily returns for every pair of symbols."""

    returns = daily.where(F.col("daily_return").isNotNull()).select(
        "symbol", "trade_date", "daily_return"
    )
    a, b = returns.alias("a"), returns.alias("b")

    return (
        a.join(
            b,
            (F.col("a.trade_date") == F.col("b.trade_date"))
            & (F.col("a.symbol") < F.col("b.symbol")),   # each pair once, no self-pairs
        )
        .groupBy(F.col("a.symbol").alias("symbol_a"), F.col("b.symbol").alias("symbol_b"))
        .agg(
            F.corr("a.daily_return", "b.daily_return").alias("return_correlation"),
            F.count("*").alias("overlapping_days"),
        )
    )


def build_gold(spark, root=None):
    """Rebuild all gold datasets from silver. Returns row counts."""

    source = silver_path(root)
    if not source.exists():
        logger.info("Gold: silver is empty, nothing to build.")
        return {}

    silver = spark.read.parquet(str(source))

    daily = daily_metrics(silver).cache()

    outputs = {
        "daily_metrics": daily,
        "monthly_summary": monthly_summary(spark, daily),
        "return_correlation": return_correlation(daily),
    }

    counts = {}
    for name, df in outputs.items():
        writer = (
            df.repartition("symbol").write.partitionBy("symbol")
            if name == "daily_metrics"
            else df.coalesce(1).write  # small tables: a single file
        )
        # Full rebuild: replace the whole dataset, not just touched partitions
        writer.mode("overwrite").option("partitionOverwriteMode", "static").parquet(
            str(gold_path(name, root))
        )
        counts[name] = spark.read.parquet(str(gold_path(name, root))).count()

    daily.unpersist()

    logger.info(f"Gold build complete: {counts}")
    return counts
