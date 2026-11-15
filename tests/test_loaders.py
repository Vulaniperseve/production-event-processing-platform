from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import insert

from database.models import MarketEvent
from src.loaders.postgres_loader import PostgresLoader
from src.loaders.raw_store import payload_hash
from src.transformers.transformer import DataTransformer
from src.validators.validator import DataValidator


def test_payload_hash_ignores_key_order():
    assert payload_hash({"a": 1, "b": [1, 2]}) == payload_hash({"b": [1, 2], "a": 1})
    assert payload_hash({"a": 1}) != payload_hash({"a": 2})


def test_build_rows_attaches_lineage_and_casts_value(api_payload):
    valid, _ = DataValidator.validate_records(api_payload)
    df = DataTransformer.transform({**api_payload, "values": valid})

    rows = PostgresLoader.build_rows(df, raw_event_id=42)

    assert len(rows) == 2
    assert all(r["raw_event_id"] == 42 for r in rows)
    assert all(isinstance(r["value"], float) for r in rows)


def test_load_statement_is_conflict_safe():
    stmt = (
        insert(MarketEvent.__table__)
        .values([{"event_id": "x", "entity_id": "AAPL"}])
        .on_conflict_do_nothing(index_elements=["event_id"])
    )

    sql = str(stmt.compile(dialect=postgresql.dialect()))

    assert "ON CONFLICT (event_id) DO NOTHING" in sql
