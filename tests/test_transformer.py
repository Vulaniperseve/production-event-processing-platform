from src.transformers.transformer import DataTransformer
from src.validators.validator import DataValidator


def _valid_payload(api_payload):
    valid, _ = DataValidator.validate_records(api_payload)
    return {**api_payload, "values": valid}


def test_transform_output_columns(api_payload):
    df = DataTransformer.transform(_valid_payload(api_payload))

    assert list(df.columns) == [
        "event_id", "entity_id", "event_time", "event_type", "value", "source_system",
    ]
    assert len(df) == 2
    assert (df["entity_id"] == "AAPL").all()


def test_event_ids_are_deterministic(api_payload):
    """Same input must always produce the same IDs: the basis of idempotency."""
    first = DataTransformer.transform(_valid_payload(api_payload))
    second = DataTransformer.transform(_valid_payload(api_payload))

    assert first["event_id"].tolist() == second["event_id"].tolist()


def test_event_ids_are_unique_per_timestamp(api_payload):
    df = DataTransformer.transform(_valid_payload(api_payload))

    assert df["event_id"].is_unique
    assert df["event_id"].str.len().eq(36).all()
