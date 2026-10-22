"""Integrity tests of the Airflow DAG: it loads without errors and has the expected
steps, order and safety rules, in both local and cloud modes."""

import importlib.util
from pathlib import Path

import pytest

pytest.importorskip("airflow")

DAG_FILE = Path(__file__).resolve().parents[1] / "orchestration" / "airflow" / "dags" / "public_spending_pipeline.py"

EXPECTED_ORDER = [
    "define_months",
    "ingestion.ingest_raw_files",
    "ingestion.raw_to_bronze",
    "ingestion.load_snowflake_bronze",
    "transformation.dbt_build_silver",
    "transformation.dbt_build_gold",
    "quality_and_release.shadow_validation",
    "quality_and_release.publish_to_bi",
]


def load_dag(monkeypatch, target):
    """Imports the DAG file fresh, with the requested mode (local or cloud)."""
    monkeypatch.setenv("PSDP_TARGET", target)
    spec = importlib.util.spec_from_file_location(f"psdp_dag_{target}", DAG_FILE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.dag


@pytest.mark.parametrize("target", ["local", "cloud"])
def test_dag_has_expected_steps_in_order(monkeypatch, target):
    dag = load_dag(monkeypatch, target)
    assert set(dag.task_ids) == set(EXPECTED_ORDER)
    # Each step must run right after the previous one.
    for upstream, downstream in zip(EXPECTED_ORDER, EXPECTED_ORDER[1:], strict=False):
        assert downstream in dag.get_task(upstream).downstream_task_ids, f"{upstream} -> {downstream}"


def test_cloud_mode_uses_glue_and_snowflake(monkeypatch):
    dag = load_dag(monkeypatch, "cloud")
    assert type(dag.get_task("ingestion.raw_to_bronze")).__name__ == "MappedOperator"  # one Glue run per month
    assert type(dag.get_task("ingestion.load_snowflake_bronze")).__name__ == "SQLExecuteQueryOperator"
    assert "--target snowflake" in dag.get_task("transformation.dbt_build_gold").bash_command


def test_safety_rules(monkeypatch):
    dag = load_dag(monkeypatch, "local")
    assert dag.catchup is False and dag.max_active_runs == 1
    assert dag.timetable.summary == "0 6 10 * *"
    assert str(dag.timezone) in {"America/Sao_Paulo", "Timezone('America/Sao_Paulo')"}
    for task in dag.tasks:
        assert task.on_failure_callback, f"{task.task_id} has no failure alert"
    # Data checks must not be retried: a difference in numbers does not fix itself.
    assert dag.get_task("quality_and_release.shadow_validation").retries == 0
    assert dag.get_task("transformation.dbt_build_silver").retries == 2
