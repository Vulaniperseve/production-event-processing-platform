import pytest

from dashboard.lib.pipeline_status import (
    BLOCKED,
    FAILED,
    QUEUED,
    RETRYING,
    RUNNING,
    SKIPPED,
    SUCCESS,
    WAITING,
    aggregate_states,
    build_pipeline_view,
    lake_run_belongs_to,
)


@pytest.mark.parametrize("states, expected", [
    ([], WAITING),
    ([None, None], WAITING),
    (["scheduled", "queued"], QUEUED),
    (["running"], RUNNING),
    (["success", "running", "queued"], RUNNING),        # mapped: 1 of 3 done
    (["success", None], RUNNING),                        # part done, rest not started
    (["success", "success", "success"], SUCCESS),
    (["success", "skipped"], SUCCESS),
    (["skipped"], SKIPPED),
    (["success", "failed", "success"], FAILED),          # one symbol failed
    (["up_for_retry", "success"], RETRYING),
    (["upstream_failed"], BLOCKED),
    (["deferred"], RUNNING),
])
def test_aggregate_states(states, expected):
    assert aggregate_states(states) == expected


def ti(task_id, state, map_index=-1, start=None, end=None):
    return {"task_id": task_id, "state": state, "map_index": map_index,
            "start_date": start, "end_date": end}


INGEST_RUN = {"dag_run_id": "manual__1", "state": "success",
              "start_date": "2026-11-17T06:00:00Z", "end_date": "2026-11-17T06:01:00Z"}
LAKE_RUN = {"dag_run_id": "asset__1", "state": "success",
            "queued_at": "2026-11-17T06:01:01Z", "start_date": "2026-11-17T06:01:02Z",
            "end_date": "2026-11-17T06:05:00Z"}
OLD_LAKE_RUN = {"dag_run_id": "asset__0", "state": "success",
                "queued_at": "2026-11-16T06:01:01Z", "start_date": "2026-11-16T06:01:02Z"}


def full_ingest(state="success"):
    return [ti("get_symbols", "success", start="2026-11-17T06:00:01Z", end="2026-11-17T06:00:02Z")] + [
        ti(task, state, i, "2026-11-17T06:00:03Z", "2026-11-17T06:00:20Z")
        for task in ("extract_to_raw", "validate_raw", "transform_and_load_staging")
        for i in range(3)
    ] + [ti("refresh_analytics", state, start="2026-11-17T06:00:30Z", end="2026-11-17T06:00:40Z")]


def full_lake(publish="skipped"):
    return [ti("land_bronze", "success"), ti("build_silver", "success"),
            ti("build_gold", "success"), ti("publish_to_s3", publish)]


def test_no_runs_is_idle():
    view = build_pipeline_view(None, [], None, [])
    assert view.status == "idle"
    assert all(s.status == WAITING for s in view.stages)


def test_complete_run_end_to_end():
    view = build_pipeline_view(INGEST_RUN, full_ingest(), LAKE_RUN, full_lake())

    assert view.status == "success"
    assert [s.status for s in view.stages] == [SUCCESS] * 7 + [SKIPPED]
    publish = view.stages[-1]
    assert "No S3 bucket" in publish.note


def test_mapped_progress_counts_symbols_not_get_symbols():
    tis = [ti("get_symbols", "success"), ti("extract_to_raw", "success", 0),
           ti("extract_to_raw", "running", 1), ti("extract_to_raw", "queued", 2)]
    view = build_pipeline_view({**INGEST_RUN, "state": "running", "end_date": None}, tis, None, [])

    extract = view.stages[0]
    assert (extract.status, extract.done, extract.total) == (RUNNING, 1, 3)
    assert view.status == "running"
    assert view.headline == "Running: Extract"


def test_old_lake_run_is_not_shown_for_a_new_ingestion():
    view = build_pipeline_view(INGEST_RUN, full_ingest(), OLD_LAKE_RUN, full_lake())

    assert view.lake_run is None
    assert view.lake_pending is True            # ingestion done, lake DAG not started yet
    assert all(s.status == WAITING for s in view.stages[4:])
    assert view.status == "running"
    assert "Waiting for the lake DAG" in view.headline


def test_failed_symbol_marks_run_failed():
    tis = full_ingest()
    tis[4]["state"] = "failed"                   # validate_raw for one symbol
    run = {**INGEST_RUN, "state": "failed"}

    view = build_pipeline_view(run, tis, None, [])

    assert view.status == "failed"
    assert view.headline == "Run failed at Validate"
    assert view.lake_pending is False


def test_rerun_with_no_new_data_skips_spark_with_reason():
    lake = [ti("land_bronze", "success"), ti("build_silver", "skipped"),
            ti("build_gold", "skipped"), ti("publish_to_s3", "skipped")]
    view = build_pipeline_view(INGEST_RUN, full_ingest(), LAKE_RUN, lake)

    silver = view.stages[5]
    assert silver.status == SKIPPED
    assert silver.note == "Nothing new to build"


def test_stage_duration_spans_all_its_instances():
    view = build_pipeline_view(INGEST_RUN, full_ingest(), LAKE_RUN, full_lake())
    extract = view.stages[0]
    assert extract.duration_s == 19.0            # 06:00:01 → 06:00:20


def test_lake_run_belongs_to():
    assert lake_run_belongs_to(INGEST_RUN, LAKE_RUN)
    assert not lake_run_belongs_to(INGEST_RUN, OLD_LAKE_RUN)
    assert not lake_run_belongs_to(None, LAKE_RUN)
    assert not lake_run_belongs_to(INGEST_RUN, None)
