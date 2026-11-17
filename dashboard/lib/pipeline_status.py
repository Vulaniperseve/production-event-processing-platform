"""
Turn Airflow task instances into the 8 stages shown on the Pipeline page.

Pure functions only (no Streamlit, no HTTP), so the logic is unit-tested.

A stage can cover several Airflow task instances: extract_to_raw is mapped
over the symbols (one instance per symbol), and "Extract" also includes the
small get_symbols task. The stage status summarizes all of them.
"""

from dataclasses import dataclass, field
from datetime import datetime

INGEST_DAG = "production_event_pipeline"
LAKE_DAG = "market_data_lakehouse"


@dataclass(frozen=True)
class Stage:
    key: str
    label: str
    subtitle: str
    dag_id: str
    tasks: tuple


STAGES = (
    Stage("extract", "Extract", "Twelve Data API → raw layer", INGEST_DAG,
          ("get_symbols", "extract_to_raw")),
    Stage("validate", "Validate", "Quality checks + quarantine", INGEST_DAG, ("validate_raw",)),
    Stage("load", "Load staging", "Idempotent insert to PostgreSQL", INGEST_DAG,
          ("transform_and_load_staging",)),
    Stage("analytics", "Analytics", "SQL views refreshed", INGEST_DAG, ("refresh_analytics",)),
    Stage("bronze", "Bronze", "Raw responses into the lake", LAKE_DAG, ("land_bronze",)),
    Stage("silver", "Silver", "Spark · validated Parquet", LAKE_DAG, ("build_silver",)),
    Stage("gold", "Gold", "Spark · business tables", LAKE_DAG, ("build_gold",)),
    Stage("publish", "Publish to S3", "Cloud copy (optional)", LAKE_DAG, ("publish_to_s3",)),
)

# Display statuses
WAITING, QUEUED, RUNNING, RETRYING, SUCCESS, SKIPPED, FAILED, BLOCKED = (
    "waiting", "queued", "running", "retrying", "success", "skipped", "failed", "blocked",
)

_ACTIVE = {"running", "deferred", "restarting", "up_for_reschedule"}
_PENDING = {None, "", "none", "scheduled", "queued"}
_FINISHED_OK = {"success", "skipped"}
_ACTIVE_RUN = {"queued", "running"}


def aggregate_states(states):
    """One display status for a stage, from the states of its task instances."""
    states = list(states)
    if not states:
        return WAITING
    present = set(states)
    if "failed" in present:
        return FAILED
    if "up_for_retry" in present:
        return RETRYING
    if present & _ACTIVE:
        return RUNNING
    if present <= _FINISHED_OK:
        return SUCCESS if "success" in present else SKIPPED
    if "upstream_failed" in present:
        # Nothing of this stage can still run (no active or pending instance
        # would be left here otherwise), so it was blocked by an earlier failure.
        return BLOCKED if not (present & _PENDING) else RUNNING
    if present <= _PENDING:
        return QUEUED if present & {"scheduled", "queued"} else WAITING
    # Some instances finished, others still pending: the stage is in progress.
    return RUNNING


def _parse(ts):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None


def _duration(tis):
    starts = [_parse(t.get("start_date")) for t in tis]
    ends = [_parse(t.get("end_date")) for t in tis]
    starts = [s for s in starts if s]
    ends = [e for e in ends if e]
    if not starts or not ends or len(ends) < len(tis):
        return None
    return max((max(ends) - min(starts)).total_seconds(), 0.0)


@dataclass
class StageView:
    key: str
    label: str
    subtitle: str
    status: str
    done: int = 0
    total: int = 0
    duration_s: float | None = None
    note: str = ""


@dataclass
class PipelineView:
    stages: list
    status: str                  # idle | running | success | failed
    headline: str
    ingest_run: dict | None = None
    lake_run: dict | None = None
    lake_pending: bool = False   # ingestion finished, lake DAG not started yet
    meta: dict = field(default_factory=dict)


def lake_run_belongs_to(ingest_run, lake_run):
    """
    The lake DAG is triggered by the Asset event that ingestion emits at its
    end, so its run belongs to this ingestion run when it was queued after
    the ingestion run started.
    """
    if not ingest_run or not lake_run:
        return False
    ingest_start = _parse(ingest_run.get("start_date") or ingest_run.get("run_after"))
    lake_queued = _parse(lake_run.get("queued_at") or lake_run.get("run_after")
                         or lake_run.get("start_date"))
    if not ingest_start or not lake_queued:
        return False
    return lake_queued >= ingest_start


def build_pipeline_view(ingest_run, ingest_tis, lake_run, lake_tis):
    """Combine the latest run of each DAG into one end-to-end view."""
    ingest_tis = ingest_tis or []
    linked = lake_run_belongs_to(ingest_run, lake_run)
    lake_tis = (lake_tis or []) if linked else []

    by_task = {}
    for ti in ingest_tis + lake_tis:
        by_task.setdefault(ti.get("task_id"), []).append(ti)

    stages = []
    for stage in STAGES:
        tis = [ti for task in stage.tasks for ti in by_task.get(task, [])]
        # get_symbols is plumbing; progress counts the per-symbol extracts.
        counted = [ti for ti in tis if ti.get("task_id") != "get_symbols"] or tis
        status = aggregate_states(ti.get("state") for ti in tis)
        done = sum(1 for ti in counted if ti.get("state") in _FINISHED_OK)
        note = ""
        if stage.key == "publish" and status == SKIPPED:
            note = "No S3 bucket configured (local only, $0)"
        elif stage.key in ("silver", "gold") and status == SKIPPED:
            note = "Nothing new to build"
        stages.append(StageView(stage.key, stage.label, stage.subtitle, status,
                                done=done, total=len(counted),
                                duration_s=_duration(tis), note=note))

    ingest_state = (ingest_run or {}).get("state")
    lake_state = (lake_run or {}).get("state") if linked else None
    lake_pending = ingest_state == "success" and not linked
    if ingest_state == "failed" and not linked:
        lake_pending = False

    if not ingest_run:
        status, headline = "idle", "No runs yet. Press Run pipeline to start one."
    elif ingest_state in _ACTIVE_RUN or lake_state in _ACTIVE_RUN or lake_pending:
        current = next((s.label for s in stages if s.status in (RUNNING, RETRYING, QUEUED)), None)
        if current:
            status, headline = "running", f"Running: {current}"
        elif lake_pending:
            status, headline = "running", "Ingestion done. Waiting for the lake DAG to start"
        else:
            status, headline = "running", "Starting…"
    elif ingest_state == "failed" or lake_state == "failed":
        failed = next((s.label for s in stages if s.status == FAILED), "a stage")
        status, headline = "failed", f"Run failed at {failed}"
    else:
        status, headline = "success", "Last run complete: API → warehouse → lake"

    return PipelineView(stages, status, headline, ingest_run,
                        lake_run if linked else None, lake_pending)
