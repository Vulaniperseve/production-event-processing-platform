from datetime import date

import pandas as pd
import pytest

from dashboard.lib import data

pq = pytest.importorskip("pyarrow.parquet")
pa = pytest.importorskip("pyarrow")


def write_parquet(path, df):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path)


def test_compare_event_ids():
    wh = pd.DataFrame({"symbol": ["A", "A", "B", "OLD"], "event_id": ["1", "2", "3", "9"]})
    lake = pd.DataFrame({"symbol": ["A", "A", "B"], "event_id": ["1", "2", "4"]})

    out = data.compare_event_ids(wh, lake).set_index("symbol")

    assert out.loc["A"].to_dict() == {"warehouse": 2, "lake": 2, "matching": 2,
                                      "only_in_warehouse": 0, "only_in_lake": 0}
    assert out.loc["B", "only_in_lake"] == 1 and out.loc["B", "only_in_warehouse"] == 1
    assert out.loc["OLD", "lake"] == 0


def test_compare_returns_uses_first_and_last_close():
    summary = pd.DataFrame({"symbol": ["A"], "period_return": [0.5]})
    gold = pd.DataFrame({"symbol": ["A", "A", "A"],
                         "trade_date": [date(2026, 1, 3), date(2026, 1, 1), date(2026, 1, 2)],
                         "close_price": [150.0, 100.0, 120.0]})

    out = data.compare_returns(summary, gold).iloc[0]

    assert out["lake_return"] == pytest.approx(0.5)
    assert out["difference"] == pytest.approx(0.0)


def test_compare_returns_handles_empty_inputs():
    assert data.compare_returns(pd.DataFrame(), pd.DataFrame()).empty


def test_lake_layers_reads_footers_and_ignores_bookkeeping(tmp_path):
    rows = pd.DataFrame({"event_id": ["a", "b"], "close_price": [1.0, 2.0]})
    write_parquet(tmp_path / "silver/market_prices/symbol=A/trade_month=2026-01/p0.parquet", rows)
    write_parquet(tmp_path / "silver/market_prices/symbol=B/trade_month=2026-01/p0.parquet", rows)
    write_parquet(tmp_path / "gold/monthly_summary/p0.parquet", rows)
    (tmp_path / "silver/market_prices/_SUCCESS").write_text("")
    (tmp_path / "silver/market_prices/symbol=A/trade_month=2026-01/.p0.parquet.crc").write_text("x")
    bronze = tmp_path / "bronze/t/ingest_date=2026-01-01/raw_event_id=1.json"
    bronze.parent.mkdir(parents=True)
    bronze.write_text("{}")

    layers = data.lake_layers(tmp_path)

    assert layers["silver"]["files"] == 2
    assert layers["silver"]["rows"] == 4
    assert layers["silver"]["partitions"] == 2
    assert layers["gold"]["rows"] == 2
    assert layers["bronze"]["files"] == 1 and layers["bronze"]["rows"] is None


def test_missing_lake_is_empty_not_an_error(tmp_path):
    layers = data.lake_layers(tmp_path / "nothing")
    assert layers["silver"]["files"] == 0
    assert data.read_gold("monthly_summary", tmp_path).empty
    assert data.silver_event_ids(tmp_path).empty


def test_naive_utc_conversion():
    assert str(data._naive_utc("2026-11-17T06:00:00Z")) == "2026-11-17 06:00:00"
    assert str(data._naive_utc("2026-11-17T08:00:00+02:00")) == "2026-11-17 06:00:00"
    assert data._naive_utc(None) is None
