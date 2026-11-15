import pytest

from src.validators.validator import DataValidator


def test_valid_structure_passes(api_payload):
    assert DataValidator.validate_structure(api_payload) is True


@pytest.mark.parametrize("missing_key", ["meta", "values", "status"])
def test_missing_top_level_key_fails(api_payload, missing_key):
    del api_payload[missing_key]
    with pytest.raises(ValueError, match=missing_key):
        DataValidator.validate_structure(api_payload)


def test_none_payload_fails():
    with pytest.raises(ValueError):
        DataValidator.validate_structure(None)


def test_error_status_fails(api_payload):
    api_payload["status"] = "error"
    with pytest.raises(ValueError):
        DataValidator.validate_structure(api_payload)


def test_empty_values_fails(api_payload):
    api_payload["values"] = []
    with pytest.raises(ValueError):
        DataValidator.validate_structure(api_payload)


def test_record_validation_splits_good_and_bad(api_payload):
    valid, invalid = DataValidator.validate_records(api_payload)

    assert [r["datetime"] for r in valid] == ["2026-08-26", "2026-08-25"]
    assert len(invalid) == 3

    reasons = [item["reason"] for item in invalid]
    assert any("datetime" in r for r in reasons)
    assert "Invalid close price" in reasons
