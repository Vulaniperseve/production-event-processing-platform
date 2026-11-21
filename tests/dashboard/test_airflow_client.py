import json

import pytest

responses = pytest.importorskip("responses")

from dashboard.lib.airflow_client import AirflowClient, AirflowUnavailable  # noqa: E402

BASE = "http://airflow.test"


@pytest.fixture
def api():
    with responses.RequestsMock() as rsps:
        rsps.post(f"{BASE}/auth/token", json={"access_token": "t1"}, status=201)
        yield rsps


def client():
    return AirflowClient(BASE, "user", "pw")


def test_login_once_and_send_bearer_token(api):
    api.get(f"{BASE}/api/v2/dags/d/dagRuns", json={"dag_runs": [{"dag_run_id": "r1"}]})
    api.get(f"{BASE}/api/v2/dags/d/dagRuns", json={"dag_runs": []})
    c = client()

    assert c.latest_run("d") == {"dag_run_id": "r1"}
    assert c.latest_run("d") is None

    token_calls = [call for call in api.calls if call.request.url.endswith("/auth/token")]
    assert len(token_calls) == 1
    assert json.loads(token_calls[0].request.body) == {"username": "user", "password": "pw"}
    assert api.calls[1].request.headers["Authorization"] == "Bearer t1"
    assert "order_by=-run_after" in api.calls[1].request.url


@pytest.mark.parametrize("status", [401, 403])
def test_rejected_token_is_refreshed_once(api, status):
    # 401 = expired; 403 = signature no longer verifies (Airflow 3.3 after a restart).
    api.get(f"{BASE}/api/v2/dags/d", status=status)
    api.post(f"{BASE}/auth/token", json={"access_token": "t2"}, status=201)
    api.get(f"{BASE}/api/v2/dags/d", json={"is_paused": True})

    assert client().is_paused("d") is True
    assert api.calls[-1].request.headers["Authorization"] == "Bearer t2"


def test_trigger_unpauses_then_starts_run(api):
    api.get(f"{BASE}/api/v2/dags/d", json={"is_paused": True})
    api.patch(f"{BASE}/api/v2/dags/d", json={"is_paused": False})
    api.post(f"{BASE}/api/v2/dags/d/dagRuns", json={"dag_run_id": "manual__x"})

    run = client().trigger("d", conf={"symbols": ["AAPL"]})

    assert run["dag_run_id"] == "manual__x"
    assert json.loads(api.calls[2].request.body) == {"is_paused": False}
    assert json.loads(api.calls[3].request.body) == {"logical_date": None,
                                                      "conf": {"symbols": ["AAPL"]}}


def test_trigger_does_not_touch_an_active_dag(api):
    api.get(f"{BASE}/api/v2/dags/d", json={"is_paused": False})
    api.post(f"{BASE}/api/v2/dags/d/dagRuns", json={"dag_run_id": "manual__y"})

    client().trigger("d")

    assert [c.request.method for c in api.calls] == ["POST", "GET", "POST"]


def test_api_error_is_reported_clearly(api):
    api.get(f"{BASE}/api/v2/dags/d/dagRuns/r/taskInstances", json={"detail": "nope"}, status=404)

    with pytest.raises(AirflowUnavailable, match="HTTP 404"):
        client().task_instances("d", "r")


def test_unreachable_airflow():
    c = AirflowClient("http://127.0.0.1:9", "u", "p", timeout=0.5)
    with pytest.raises(AirflowUnavailable, match="not reachable"):
        c.latest_run("d")


def test_bad_credentials():
    with responses.RequestsMock() as rsps:
        rsps.post(f"{BASE}/auth/token", json={"detail": "bad"}, status=401)
        with pytest.raises(AirflowUnavailable, match="login failed"):
            client().latest_run("d")
