"""
Consistency between the exporter, the alert rules and the Grafana dashboard.

A typo in a metric name doesn't fail anywhere at runtime: the alert just
never fires and the panel just shows "no data". These tests catch that.
"""

import json
import re
from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")
pytest.importorskip("prometheus_client")

from monitoring.exporter import METRIC_NAMES  # noqa: E402

ROOT = Path(__file__).resolve().parents[2] / "monitoring"


def metrics_in(expr):
    expr = re.sub(r'"[^"]*"', "", expr)                 # drop label values
    expr = re.sub(r"\{[^}]*\}", "{}", expr)             # drop label matchers
    expr = re.sub(r"\[[^\]]*\]", "[]", expr)            # drop range windows
    names = set(re.findall(r"[a-zA-Z_:][a-zA-Z0-9_:]*", expr))
    return {n for n in names if n.startswith("platform_")}


def alert_rules():
    config = yaml.safe_load((ROOT / "prometheus" / "alerts.yml").read_text(encoding="utf-8"))
    return [rule for group in config["groups"] for rule in group["rules"]]


def dashboard():
    return json.loads((ROOT / "grafana" / "dashboards" / "platform.json").read_text(encoding="utf-8"))


def test_every_alert_uses_metrics_the_exporter_provides():
    for rule in alert_rules():
        unknown = metrics_in(rule["expr"]) - set(METRIC_NAMES)
        assert not unknown, f"{rule['alert']} uses unknown metrics {unknown}"


def test_every_alert_is_actionable():
    names = [r["alert"] for r in alert_rules()]
    assert len(names) == len(set(names))
    for rule in alert_rules():
        assert rule["labels"]["severity"] in ("critical", "warning"), rule["alert"]
        assert rule["annotations"]["summary"] and rule["annotations"]["description"], rule["alert"]
        assert "for" in rule, f"{rule['alert']} should wait before firing (no flapping)"


def test_every_dashboard_panel_uses_known_metrics_and_the_provisioned_datasource():
    provisioned = yaml.safe_load((ROOT / "grafana" / "provisioning" / "datasources" / "prometheus.yml")
                                 .read_text(encoding="utf-8"))
    datasource = provisioned["datasources"][0]["uid"]
    panels = [p for p in dashboard()["panels"] if p["type"] != "row"]
    assert panels
    for panel in panels:
        assert panel["datasource"]["uid"] == datasource, panel["title"]
        for target in panel["targets"]:
            unknown = metrics_in(target["expr"]) - set(METRIC_NAMES)
            assert not unknown, f"panel {panel['title']!r} uses unknown metrics {unknown}"


def test_dashboard_layout_has_no_overlapping_panels():
    cells = set()
    for panel in dashboard()["panels"]:
        g = panel["gridPos"]
        assert g["x"] + g["w"] <= 24, panel["title"]
        for x in range(g["x"], g["x"] + g["w"]):
            for y in range(g["y"], g["y"] + g["h"]):
                assert (x, y) not in cells, f"{panel['title']} overlaps another panel"
                cells.add((x, y))


def test_prometheus_scrapes_the_exporter_and_loads_the_rules():
    config = yaml.safe_load((ROOT / "prometheus" / "prometheus.yml").read_text(encoding="utf-8"))
    targets = [t for job in config["scrape_configs"] for sc in job["static_configs"] for t in sc["targets"]]
    assert "metrics-exporter:9200" in targets
    assert config["rule_files"] == ["/etc/prometheus/alerts.yml"]
