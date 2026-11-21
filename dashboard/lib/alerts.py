"""Firing alerts from Prometheus, for the dashboard's health indicator."""

import os

import requests

SEVERITY_ORDER = {"critical": 0, "warning": 1}


def firing_alerts(url=None, session=None, timeout=3):
    """
    Firing alerts, most severe first, or None when Prometheus can't be
    reached (monitoring is off), which is different from "no alerts".
    """
    url = (url or os.getenv("PROMETHEUS_URL", "")).rstrip("/")
    if not url:
        return None
    try:
        response = (session or requests).get(f"{url}/api/v1/alerts", timeout=timeout)
        response.raise_for_status()
        alerts = response.json()["data"]["alerts"]
    except (requests.RequestException, ValueError, KeyError):
        return None
    firing = [
        {
            "name": a["labels"].get("alertname", "Alert"),
            "severity": a["labels"].get("severity", "warning"),
            "summary": a.get("annotations", {}).get("summary", ""),
            "since": a.get("activeAt"),
        }
        for a in alerts if a.get("state") == "firing"
    ]
    return sorted(firing, key=lambda a: (SEVERITY_ORDER.get(a["severity"], 9), a["name"]))
