import pytest

responses = pytest.importorskip("responses")

from dashboard.lib.alerts import firing_alerts  # noqa: E402

URL = "http://prometheus.test"


def alert(name, state="firing", severity="warning"):
    return {"labels": {"alertname": name, "severity": severity}, "state": state,
            "annotations": {"summary": f"{name} summary"}, "activeAt": "2026-11-21T08:00:00Z"}


def test_only_firing_alerts_most_severe_first():
    with responses.RequestsMock() as rsps:
        rsps.get(f"{URL}/api/v1/alerts", json={"status": "success", "data": {"alerts": [
            alert("DataStale"), alert("StreamStalled", severity="critical"),
            alert("HighQuarantineRate", state="pending")]}})
        result = firing_alerts(URL)

    assert [a["name"] for a in result] == ["StreamStalled", "DataStale"]
    assert result[0]["summary"] == "StreamStalled summary"


def test_no_alerts_is_an_empty_list():
    with responses.RequestsMock() as rsps:
        rsps.get(f"{URL}/api/v1/alerts", json={"status": "success", "data": {"alerts": []}})
        assert firing_alerts(URL) == []


def test_unreachable_or_unconfigured_prometheus_is_none(monkeypatch):
    monkeypatch.delenv("PROMETHEUS_URL", raising=False)
    assert firing_alerts() is None
    assert firing_alerts("http://127.0.0.1:9", timeout=0.5) is None
