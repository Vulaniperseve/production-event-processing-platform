"""
Data lake locations.

LAKE_ROOT is a local directory today. The layout (and the Spark code)
works unchanged on object storage such as s3a://bucket/lake later.
"""

import os
from pathlib import Path


def lake_root():
    return Path(os.getenv("LAKE_ROOT", "data/lake")).resolve()


def bronze_path(root=None):
    return (Path(root) if root else lake_root()) / "bronze" / "twelvedata_time_series"


def silver_path(root=None):
    return (Path(root) if root else lake_root()) / "silver" / "market_prices"


def silver_rejected_path(root=None):
    return (Path(root) if root else lake_root()) / "silver" / "market_prices_rejected"


def gold_path(table, root=None):
    return (Path(root) if root else lake_root()) / "gold" / table
