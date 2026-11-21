"""
Guards on DAG settings, checked by reading the DAG files (no Airflow needed).

Airflow 3.1.0 bug: a mapped task (extract/validate/load run once per
symbol) that is waiting to retry with exponential backoff makes the
scheduler crash in a loop ('MappedOperator' object has no attribute
'max_retry_delay'). It happened on 2026-11-17; these tests keep it fixed.
"""

import ast
from pathlib import Path

DAGS = Path(__file__).resolve().parents[1] / "airflow" / "dags"


def default_args(dag_file):
    tree = ast.parse((DAGS / dag_file).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if (isinstance(node, ast.Assign) and any(getattr(t, "id", None) == "default_args"
                                                 for t in node.targets)):
            # Keep the plain values (numbers, booleans, strings); skip timedelta(...) calls.
            return {k.value: v.value for k, v in zip(node.value.keys, node.value.values, strict=True)
                    if isinstance(v, ast.Constant)}
    raise AssertionError(f"no default_args in {dag_file}")


def uses_mapping(dag_file):
    return ".expand(" in (DAGS / dag_file).read_text(encoding="utf-8")


def test_ingestion_dag_is_mapped_and_has_retries():
    args = default_args("production_event_pipeline.py")
    assert uses_mapping("production_event_pipeline.py")
    assert args["retries"] >= 1


def test_no_exponential_backoff_on_dags_with_mapped_tasks():
    for dag_file in DAGS.glob("*.py"):
        if not uses_mapping(dag_file.name):
            continue
        source = dag_file.read_text(encoding="utf-8")
        assert '"retry_exponential_backoff": True' not in source, dag_file.name
        assert "retry_exponential_backoff=True" not in source, dag_file.name
