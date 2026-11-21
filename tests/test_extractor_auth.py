"""The API key must never be part of a URL: URLs end up in errors and logs."""

import pytest
import requests

responses = pytest.importorskip("responses")

from src.extractors import twelve_data_extractor  # noqa: E402
from src.extractors.twelve_data_extractor import TwelveDataExtractor  # noqa: E402

URL = "https://api.twelvedata.com/time_series"


def test_key_is_sent_in_a_header_not_the_url(monkeypatch):
    monkeypatch.setattr(twelve_data_extractor, "API_KEY", "secret-123")
    with responses.RequestsMock() as rsps:
        rsps.get(URL, json={"status": "ok", "meta": {"symbol": "AAPL"}, "values": []})
        TwelveDataExtractor().extract("AAPL")

        request = rsps.calls[0].request
        assert request.headers["Authorization"] == "apikey secret-123"
        assert "secret-123" not in request.url


def test_a_failed_request_does_not_leak_the_key_in_the_error(monkeypatch):
    monkeypatch.setattr(twelve_data_extractor, "API_KEY", "secret-123")
    monkeypatch.setattr(twelve_data_extractor.time, "sleep", lambda s: None)
    with responses.RequestsMock() as rsps:
        rsps.get(URL, status=500)
        with pytest.raises(requests.HTTPError) as error:
            TwelveDataExtractor().extract("AAPL")

    assert "secret-123" not in str(error.value)
