"""Monthly pipeline of the Public Spending Data Platform, orchestrated by Apache Airflow.

In plain words: once a month, after the government publishes the spending of the
month that has just closed, this pipeline

1. decides which month(s) to load,
2. copies the official files into the data lake,
3. standardizes them,
4. loads them into Snowflake,
5. builds the business tables (silver and gold layers) with dbt, testing the data,
6. compares the numbers with the old platform (Athena) and only continues if they match,
7. refreshes the Power BI report used by the business.

If any step fails, it is retried automatically; if it still fails, the team receives
an alert and the following steps do not run, so wrong numbers never reach the reports.

The same DAG runs in two modes (environment variable PSDP_TARGET):
``local`` uses the sample files and DuckDB; ``cloud`` uses AWS and Snowflake.
"""

from __future__ import annotations

import os
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Asset, Param, TaskGroup, get_current_context, task

# --------------------------------------------------------------------------------------
# Make the project code (ingestion, validation, steps...) importable by Airflow.
# The project root is three folders above this file, or the PSDP_PROJECT_ROOT variable
# when Airflow runs inside Docker.
# --------------------------------------------------------------------------------------
PROJECT_ROOT = Path(os.environ.get("PSDP_PROJECT_ROOT", Path(__file__).resolve().parents[3]))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from orchestration import steps  # noqa: E402

TARGET = steps.target()
IS_CLOUD = TARGET == "cloud"
LOCAL_TZ = pendulum.timezone("America/Sao_Paulo")

# The final business table. Declaring it as an "asset" lets other pipelines
# (e.g. a forecasting model) start automatically as soon as it is updated.
GOLD_MONTHLY_EXECUTION = Asset(name="psdp.gold.agg_execucao_mensal")


# --------------------------------------------------------------------------------------
# Alerts sent to the team channel
# --------------------------------------------------------------------------------------
def notify_failure(context) -> None:
    """Called by Airflow when a step fails for good (after all retries)."""
    ti = context["ti"]
    steps.send_alert(
        f"[FAILED] Public spending pipeline stopped at step '{ti.task_id}' "
        f"(run {context['run_id']}, attempt {ti.try_number}). "
        "The following steps were stopped and the reports keep the previous data."
    )


# --------------------------------------------------------------------------------------
# Rules applied to every step unless a step says otherwise
# --------------------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "data-engineering",
    # Temporary problems (network, a busy server) usually solve themselves:
    # try again twice, waiting 5 then 10 minutes, before declaring a failure.
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    # No single step should take more than 1 hour; if it does, something is wrong.
    "execution_timeout": timedelta(hours=1),
    "on_failure_callback": notify_failure,
}

# Environment passed to dbt: where the local files are, or the Snowflake credentials
# already present in the Airflow environment for the cloud mode.
DBT_ENV = {
    "BRONZE_PATH": str(steps.DATA_DIR / "bronze"),
    "DUCKDB_PATH": str(steps.DATA_DIR / "warehouse.duckdb"),
    "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
}
DBT_TARGET = "snowflake" if IS_CLOUD else "local"
# dbt may live in its own Python environment (see the Dockerfile); DBT_BIN points to it.
DBT_BIN = os.environ.get("DBT_BIN", "dbt")
# "buildable" means: run each data test only in the step where all the tables it
# checks are already built (e.g. the silver-vs-gold reconciliation runs with gold).
DBT_CMD = (
    f"cd {PROJECT_ROOT / 'dbt'} && {DBT_BIN} build --profiles-dir . "
    f"--target {DBT_TARGET} --indirect-selection buildable"
)


with DAG(
    dag_id="public_spending_monthly",
    description="Monthly load of federal public spending: lake -> Snowflake -> dbt -> validation -> Power BI",
    # Runs on the 10th of every month at 06:00 (Brasília time), when the previous
    # month is already published and before the business day starts.
    schedule="0 6 10 * *",
    start_date=pendulum.datetime(2026, 1, 1, tz=LOCAL_TZ),
    # Do not try to "catch up" on past months automatically; past months are
    # reprocessed on purpose, by triggering the pipeline with the 'months' parameter.
    catchup=False,
    # Only one monthly load at a time, so two runs never write the same tables together.
    max_active_runs=1,
    # The whole monthly load must finish within 3 hours.
    dagrun_timeout=timedelta(hours=3),
    default_args=DEFAULT_ARGS,
    # Lets SQL files be referenced by a short path (sql/copy_into_bronze.sql).
    template_searchpath=[str(Path(__file__).resolve().parents[1])],
    params={
        "months": Param(
            default=[],
            type="array",
            items={"type": "string", "pattern": "^[0-9]{6}$"},
            title="Months to (re)process",
            description="Optional. List of months in YYYYMM format, e.g. ['202603']. "
            "Leave empty to load the month that has just closed.",
        ),
    },
    tags=["public-spending", "snowflake", "dbt", TARGET],
    doc_md=__doc__,
) as dag:
    # ----------------------------------------------------------------------------------
    # 1. Decide which months this run will load
    # ----------------------------------------------------------------------------------
    @task(task_id="define_months", retries=0)
    def define_months() -> list[str]:
        """Returns the list of months to load (manual choice or the month just closed)."""
        context = get_current_context()
        # Reference date of the run; manual runs may not have one, so use "now".
        run_date = context.get("data_interval_end") or pendulum.now(LOCAL_TZ)
        return steps.reference_months(run_date.date(), context["params"].get("months"))

    months = define_months()

    # ----------------------------------------------------------------------------------
    # 2-4. Ingestion: official files -> raw area -> standardized (bronze) -> Snowflake
    # ----------------------------------------------------------------------------------
    with TaskGroup("ingestion", tooltip="Bring the official files into the platform") as ingestion:

        @task(task_id="ingest_raw_files")
        def ingest_raw_files(months: list[str]) -> list[str]:
            """Copies the official monthly files, untouched, into the raw area of the lake."""
            landed = steps.ingest(months)
            if not landed:
                # Without files there is nothing to process: stop here with a clear message.
                raise AirflowFailException(f"No files found for months {months}")
            return landed

        landed_files = ingest_raw_files(months)

        if IS_CLOUD:
            from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
            from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

            # One Glue job run per month (the job rewrites only that month's folder,
            # so other months are never touched). Runs one month at a time because the
            # Glue job allows a single concurrent run.
            raw_to_bronze = GlueJobOperator.partial(
                task_id="raw_to_bronze",
                job_name=os.environ.get("GLUE_JOB_NAME", "psdp-raw-to-bronze"),
                region_name=os.environ.get("AWS_REGION", "us-east-1"),
                wait_for_completion=True,
                max_active_tis_per_dag=1,
            ).expand(script_args=months.map(lambda month: {"--ANO_MES": month}))

            # Loads the new bronze files into Snowflake (safe to repeat, never duplicates).
            load_snowflake = SQLExecuteQueryOperator(
                task_id="load_snowflake_bronze",
                conn_id="snowflake_default",
                sql="sql/copy_into_bronze.sql",
                split_statements=True,
            )
        else:

            @task(task_id="raw_to_bronze")
            def raw_to_bronze_local() -> int:
                """Local equivalent of the Glue job: raw CSV -> standardized Parquet files."""
                return steps.build_bronze_local()

            raw_to_bronze = raw_to_bronze_local()

            # Locally there is no Snowflake: the local warehouse (DuckDB) reads the
            # bronze files directly, so this step only keeps the same flow as the cloud.
            load_snowflake = EmptyOperator(task_id="load_snowflake_bronze")

        landed_files >> raw_to_bronze >> load_snowflake

    # ----------------------------------------------------------------------------------
    # 5. Transformation with dbt: business tables + automatic data tests
    # ----------------------------------------------------------------------------------
    with TaskGroup("transformation", tooltip="Build the business tables and test the data") as transformation:
        # Silver: cleans and standardizes every expenditure line, and tests it
        # (unique keys, valid states, valid expense categories...).
        dbt_silver = BashOperator(
            task_id="dbt_build_silver",
            bash_command=f"{DBT_CMD} --select staging",
            env=DBT_ENV,
            append_env=True,
        )

        # Gold: builds the star schema and the monthly execution table used by
        # Power BI, and checks that not a single cent was lost from silver to gold.
        dbt_gold = BashOperator(
            task_id="dbt_build_gold",
            bash_command=f"{DBT_CMD} --select marts",
            env=DBT_ENV,
            append_env=True,
            outlets=[GOLD_MONTHLY_EXECUTION],
        )

        dbt_silver >> dbt_gold

    # ----------------------------------------------------------------------------------
    # 6-7. Quality gate and release to the business
    # ----------------------------------------------------------------------------------
    with TaskGroup("quality_and_release", tooltip="Validate the numbers, then publish") as release:

        @task(task_id="shadow_validation", retries=0)
        def shadow_validation(months: list[str]) -> dict:
            """Compares old (Athena) and new (Snowflake) numbers; stops the release if
            less than 99.5% of the values match. No retry: a data difference does not
            fix itself and must be investigated."""
            summary = steps.shadow_validation(months)
            if not summary["passed"]:
                raise AirflowFailException(
                    f"Adherence below {steps.MIN_ADHERENCE:.1%}: {summary['adherence']} "
                    f"(key coverage {summary['coverage']:.2%}). Data NOT released."
                )
            return summary

        @task(task_id="publish_to_bi")
        def publish_to_bi(summary: dict) -> str:
            """Makes the validated month visible to the business (Power BI refresh)
            and tells the team that the new data is available."""
            if IS_CLOUD:
                result = steps.refresh_power_bi()
                if result is None:
                    steps.send_alert(
                        f"Data for {', '.join(summary['months'])} is validated in Snowflake "
                        "(Power BI refresh not configured)."
                    )
                    raise AirflowSkipException("Power BI is not configured; data is available in Snowflake.")
            else:
                result = str(steps.publish_local(summary))
            loaded = ", ".join(summary["months"])
            steps.send_alert(f"[OK] Public spending data for {loaded} is validated and published.")
            return result

        publish_to_bi(shadow_validation(months))

    # ----------------------------------------------------------------------------------
    # Order of execution
    # ----------------------------------------------------------------------------------
    months >> ingestion >> transformation >> release
