"""
Small client for the Airflow 3 REST API (/api/v2), used by the dashboard to
start the pipeline and follow its tasks live.

Auth: Airflow 3 issues a JWT from /auth/token for a username and password;
every API call then sends it as a Bearer token. The token is cached and
fetched again once if the API rejects it: 401 when it has expired, 403 when
its signature no longer verifies (Airflow 3.3 answers that way after a
restart or a key rotation).
"""

import os

import requests


class AirflowUnavailable(RuntimeError):
    """Airflow could not be reached or rejected the request."""


class AirflowClient:
    def __init__(self, base_url=None, username=None, password=None,
                 session=None, timeout=10, login_timeout=60):
        self.base_url = (base_url or os.getenv("AIRFLOW_API_URL", "http://localhost:8080")).rstrip("/")
        self.username = username or os.getenv("AIRFLOW_USERNAME", "airflow")
        self.password = password or os.getenv("AIRFLOW_PASSWORD", "airflow")
        self.session = session or requests.Session()
        self.timeout = timeout
        # Login hashes the password, which can take 20+ s on a busy laptop.
        self.login_timeout = login_timeout
        self._token = None

    # ------------------------------------------------------------------ auth
    def _fetch_token(self):
        try:
            response = self.session.post(
                f"{self.base_url}/auth/token",
                json={"username": self.username, "password": self.password},
                timeout=self.login_timeout,
            )
        except requests.RequestException as exc:
            raise AirflowUnavailable(f"Airflow is not reachable at {self.base_url}") from exc
        if response.status_code not in (200, 201):
            raise AirflowUnavailable(f"Airflow login failed (HTTP {response.status_code})")
        self._token = response.json()["access_token"]
        return self._token

    def _request(self, method, path, **kwargs):
        for attempt in (1, 2):
            token = self._token or self._fetch_token()
            try:
                response = self.session.request(
                    method, f"{self.base_url}{path}",
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=self.timeout, **kwargs,
                )
            except requests.RequestException as exc:
                raise AirflowUnavailable(f"Airflow is not reachable at {self.base_url}") from exc
            if response.status_code in (401, 403) and attempt == 1:
                self._token = None          # stale token: log in again once
                continue
            if response.status_code >= 400:
                detail = response.text[:200]
                raise AirflowUnavailable(f"Airflow API {method} {path} → HTTP {response.status_code}: {detail}")
            return response.json() if response.content else {}
        raise AirflowUnavailable("Airflow rejected the credentials")

    # ------------------------------------------------------------------ reads
    def latest_run(self, dag_id):
        data = self._request(
            "GET", f"/api/v2/dags/{dag_id}/dagRuns",
            params={"order_by": "-run_after", "limit": 1},
        )
        runs = data.get("dag_runs", [])
        return runs[0] if runs else None

    def recent_runs(self, dag_id, limit=10):
        data = self._request(
            "GET", f"/api/v2/dags/{dag_id}/dagRuns",
            params={"order_by": "-run_after", "limit": limit},
        )
        return data.get("dag_runs", [])

    def task_instances(self, dag_id, run_id):
        data = self._request(
            "GET", f"/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances",
            params={"limit": 100},
        )
        return data.get("task_instances", [])

    def is_paused(self, dag_id):
        return bool(self._request("GET", f"/api/v2/dags/{dag_id}").get("is_paused"))

    # ----------------------------------------------------------------- writes
    def unpause(self, dag_id):
        self._request("PATCH", f"/api/v2/dags/{dag_id}", json={"is_paused": False})

    def trigger(self, dag_id, conf=None):
        """Start a run now. Unpauses the DAG first, or the run would never start."""
        if self.is_paused(dag_id):
            self.unpause(dag_id)
        return self._request(
            "POST", f"/api/v2/dags/{dag_id}/dagRuns",
            json={"logical_date": None, "conf": conf or {}},
        )
