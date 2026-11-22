"""
The validator's fast datetime check must accept exactly what pandas accepts.

Phase 11 replaced pd.to_datetime (about 1 ms per record) with a
datetime.fromisoformat fast path that falls back to pandas. These tests pin
the behaviour: for every input, the answer equals "does pd.to_datetime parse
it?", so the speed-up can never change which records are quarantined.
"""

import hashlib

import pandas as pd
import pytest

from src.transformers.transformer import DataTransformer
from src.validators.validator import is_valid_datetime

CASES = [
    # what Twelve Data sends
    "2026-11-20", "2026-11-20 15:30:00", "2026-11-20T15:30:00",
    # other ISO variants
    "2026-11-20 15:30", "2026-11-20T15:30:00.123456", "2026-11-20T15:30:00Z",
    "2026-11-20T15:30:00+02:00",
    # pandas' range is 1677-09-21 .. 2262-04-11: the fast path must defer
    "1500-01-01", "1677-09-22", "2262-04-10", "2300-01-01", "0001-01-01",
    # not ISO calendar dates: handled by the pandas fallback
    "2026/11/20", "20 Nov 2026", "Nov 20, 2026", "20261120", "2026-W47-5", "2026-324",
    # invalid
    "not-a-date", "2026-13-01", "2026-02-30", "2026-11-20 25:00:00", "", "   ",
]


def pandas_accepts(value):
    try:
        pd.to_datetime(value)
    except Exception:
        return False
    return True


@pytest.mark.parametrize("value", CASES)
def test_fast_path_agrees_with_pandas(value):
    assert is_valid_datetime(value) == pandas_accepts(value)


@pytest.mark.parametrize("value", [None, 20261120, 1.5])
def test_non_strings_go_through_pandas(value):
    assert is_valid_datetime(value) == pandas_accepts(value)


def test_transformer_event_ids_unchanged():
    """Same deterministic id as before the speed-up (sha256 of SYMBOL|datetime|TYPE)."""
    df = DataTransformer.transform({
        "meta": {"symbol": "AAPL"}, "status": "ok",
        "values": [{"datetime": "2026-11-20", "close": "100.5"}],
    })
    expected = hashlib.sha256(b"AAPL|2026-11-20|MARKET_PRICE").hexdigest()[:36]
    assert df.loc[0, "event_id"] == expected
    assert list(df.columns) == ["event_id", "entity_id", "event_time", "event_type", "value",
                                "source_system"]
