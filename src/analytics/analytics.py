"""
Analytics layer operations.

The analytics layer itself is SQL (database/migrations/002_analytics_layer.sql):
plain views that are always current, plus the materialized
analytics.symbol_summary, which is refreshed here after each load.
"""

from sqlalchemy import text

from database.database import engine
from src.logger import logger

MATERIALIZED_VIEWS = ("analytics.symbol_summary",)


def refresh_analytics():
    """
    Refresh materialized analytics views.

    CONCURRENTLY rebuilds the view in the background and swaps it in, so
    dashboards/queries reading it are never blocked during the refresh
    (this requires the unique index created in migration 002).

    Returns {view_name: row_count}.
    """

    counts = {}

    with engine.begin() as conn:
        for view in MATERIALIZED_VIEWS:
            conn.execute(text(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}"))
            counts[view] = conn.execute(text(f"SELECT count(*) FROM {view}")).scalar()

    logger.info(f"Analytics refreshed: {counts}")

    return counts


def symbol_summary():
    """Return analytics.symbol_summary as a list of dicts."""

    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT * FROM analytics.symbol_summary ORDER BY symbol")
        ).mappings().all()

    return [dict(row) for row in rows]
