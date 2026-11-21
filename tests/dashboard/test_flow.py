from dashboard.lib.flow import render_flow, stage_metric
from dashboard.lib.pipeline_status import (
    RUNNING,
    SKIPPED,
    SUCCESS,
    WAITING,
    build_pipeline_view,
)
from tests.dashboard.test_pipeline_status import INGEST_RUN, LAKE_RUN, full_ingest, full_lake, ti

NUMBERS = {"api_responses": 3, "records_received": 300, "quarantined": 2, "valid": 298,
           "new_events": 298, "already_present": 0}
LAKE = {"bronze": {"files": 14}, "silver": {"rows": 300, "partitions": 18},
        "gold": {"rows": 321}}


def test_metrics_only_for_finished_stages():
    assert stage_metric("extract", RUNNING, NUMBERS, LAKE) == ""
    assert stage_metric("extract", SUCCESS, NUMBERS, LAKE) == "3 API calls · 300 records"
    assert stage_metric("silver", SUCCESS, NUMBERS, LAKE) == "300 rows · 18 partitions"
    assert stage_metric("silver", SKIPPED, NUMBERS, LAKE) == ""


def test_rerun_load_metric_shows_idempotent_skip():
    rerun = {**NUMBERS, "new_events": 0, "already_present": 298}
    assert stage_metric("load", SUCCESS, rerun, LAKE) == "0 new · 298 already stored"


def test_render_complete_run():
    view = build_pipeline_view(INGEST_RUN, full_ingest(), LAKE_RUN, full_lake())
    html = render_flow(view, NUMBERS, LAKE)

    assert html.count('class="pf-card') == 8
    assert html.count("st-ok") >= 7
    assert "2 quarantined" in html
    assert "No S3 bucket configured" in html
    assert "pf-link done" in html
    assert "<script" not in html


def test_running_stage_animates_its_incoming_connector():
    tis = [ti("get_symbols", "success"), *[ti("extract_to_raw", "success", i) for i in range(3)],
           *[ti("validate_raw", "running", i) for i in range(3)]]
    view = build_pipeline_view({**INGEST_RUN, "state": "running"}, tis, None, [])
    html = render_flow(view, NUMBERS, LAKE)

    assert view.stages[1].status == RUNNING
    assert html.count("pf-link flow") == 1
    assert "pf-spin" in html


def test_waiting_lake_shows_asset_trigger_hint():
    view = build_pipeline_view(None, [], None, [])
    html = render_flow(view)
    assert all(s.status == WAITING for s in view.stages)
    assert "starts automatically" in html


def test_user_text_is_escaped():
    view = build_pipeline_view({**INGEST_RUN, "dag_run_id": "<b>x</b>"}, full_ingest(), None, [])
    assert "<b>x</b>" not in render_flow(view)
