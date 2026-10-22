"""Business steps executed by the Airflow pipeline.

This module holds the "what to do" of each step of the monthly pipeline, written as
plain Python functions. The Airflow DAG only decides "when" and "in which order"
they run. Keeping the logic here means every step can be tested on its own,
without Airflow, and reused by other tools.

The pipeline can run in two modes, chosen by the PSDP_TARGET environment variable:

- ``local`` (default): everything runs on one computer with the sample files,
  DuckDB playing the role of Athena/Snowflake. Used for development and CI.
- ``cloud``: the same steps run on AWS (S3, Glue, Athena) and Snowflake.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime
from pathlib import Path

# Folder where the project lives. Every path below is built from it, so the
# pipeline works the same on a laptop, in Docker or in the CI server.
PROJECT_ROOT = Path(os.environ.get("PSDP_PROJECT_ROOT", Path(__file__).resolve().parents[1]))

# Local working area: raw files, bronze files, the local warehouse and reports.
DATA_DIR = PROJECT_ROOT / "data"
SAMPLE_DIR = PROJECT_ROOT / "sample_data" / "raw"

# Minimum share of the numbers that must match between the old platform (Athena)
# and the new one (Snowflake) before the data is released to the business.
MIN_ADHERENCE = 0.995

log = logging.getLogger(__name__)


def target() -> str:
    """Tells which environment the pipeline is running against: 'local' or 'cloud'."""
    value = os.environ.get("PSDP_TARGET", "local").lower()
    if value not in {"local", "cloud"}:
        raise ValueError(f"PSDP_TARGET must be 'local' or 'cloud', got '{value}'")
    return value


# --------------------------------------------------------------------------------------
# Step 1 - Decide which months to process
# --------------------------------------------------------------------------------------
def previous_month(reference: date) -> str:
    """Returns the month before the given date, in the YYYYMM format used by the source.

    The government publishes each month's spending only after the month closes,
    so a run in May processes April.
    """
    year, month = (reference.year, reference.month - 1) if reference.month > 1 else (reference.year - 1, 12)
    return f"{year}{month:02d}"


def sample_months() -> list[str]:
    """Lists the months available in the sample files shipped with the project."""
    return sorted(path.name[:6] for path in SAMPLE_DIR.glob("*_Despesas.csv"))


def reference_months(run_date: date, requested: list[str] | None = None) -> list[str]:
    """Defines the months this run will load.

    Priority:
    1. Months typed by a user when triggering the pipeline manually
       (used to reprocess a past month after a correction at the source).
    2. Local mode: every month available in the sample files.
    3. Cloud mode: the month that has just closed.
    """
    if requested:
        # Keep only valid YYYYMM values and remove duplicates, so a typo
        # never triggers a wrong or duplicated load.
        months = sorted({m.strip() for m in requested if len(m.strip()) == 6 and m.strip().isdigit()})
        if not months:
            raise ValueError(f"no valid YYYYMM month in {requested}")
        return months
    if target() == "local":
        return sample_months()
    return [previous_month(run_date)]


# --------------------------------------------------------------------------------------
# Step 2 - Bring the official files into the raw area of the data lake
# --------------------------------------------------------------------------------------
def ingest(months: list[str]) -> list[str]:
    """Copies the monthly spending files, untouched, into the raw area.

    Local mode copies the sample files. Cloud mode downloads the requested months
    from the Portal da Transparência and saves them in S3. Files are stored exactly
    as published, so any number can always be traced back to the original source.
    """
    from ingestion.ingest_despesas import DEFAULT_URL_TEMPLATE, run

    if target() == "local":
        return run([], SAMPLE_DIR, str(DATA_DIR / "raw"), DEFAULT_URL_TEMPLATE)
    bucket = os.environ["DATA_LAKE_BUCKET"]
    url_template = os.environ.get("DESPESAS_URL_TEMPLATE", DEFAULT_URL_TEMPLATE)
    return run(months, None, f"s3://{bucket}/raw", url_template)


# --------------------------------------------------------------------------------------
# Step 3 - Standardize the raw files (bronze layer)
# --------------------------------------------------------------------------------------
def build_bronze_local() -> int:
    """Local stand-in for the AWS Glue job: converts the raw files into clean, typed
    Parquet files (column names in English-like codes, amounts as numbers).
    Returns how many expenditure lines were processed."""
    from local.build_bronze import build

    return build(DATA_DIR / "raw", DATA_DIR / "bronze")


# --------------------------------------------------------------------------------------
# Step 5 - Compare old and new platforms before releasing the data
# --------------------------------------------------------------------------------------
def shadow_validation(months: list[str]) -> dict:
    """Checks that the new platform reports the same numbers as the old one.

    For every month and government body, the row count and the committed, settled
    and paid amounts are compared. The data is only released if at least 99.5% of
    the values match; otherwise the pipeline stops and the team is alerted.
    """
    from validation import shadow_validation as sv

    if target() == "local":
        legacy = sv.legacy_local(months, DATA_DIR / "raw")
        new = sv.new_local(months, DATA_DIR / "warehouse.duckdb")
        names = ("raw files", "local warehouse")
    else:
        legacy = sv.legacy_athena(months)
        new = sv.new_snowflake(months)
        names = ("athena", "snowflake")

    result = sv.compare(legacy, new)
    report = sv.render_report(result, MIN_ADHERENCE, *names)

    # Keep a copy of the report with the run date, for audit purposes.
    report_dir = DATA_DIR / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / f"shadow_validation_{datetime.now():%Y%m%d_%H%M%S}.md").write_text(report, encoding="utf-8")
    log.info("\n%s", report)

    return {
        "months": months,
        "passed": result.passed(MIN_ADHERENCE),
        "coverage": round(result.coverage, 6),
        "adherence": {metric: round(value, 6) for metric, value in result.adherence.items()},
    }


# --------------------------------------------------------------------------------------
# Step 6 - Tell the BI layer that fresh, validated data is available
# --------------------------------------------------------------------------------------
def publish_local(summary: dict) -> Path:
    """Local stand-in for the Power BI refresh: writes a 'data ready' file with the
    months loaded and the validation results, which a dashboard or person can check."""
    path = DATA_DIR / "publish" / "latest_release.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"released_at": datetime.now().isoformat(timespec="seconds"), **summary}
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def refresh_power_bi() -> str | None:
    """Asks Power BI to refresh the published report, so business users see the new
    month as soon as it is validated.

    Uses a service principal (an application account, no personal password) and the
    Power BI REST API. If Power BI is not configured, the step is skipped instead of
    failing, because the data itself is already correct in the warehouse.
    """
    import requests

    settings = {
        key: os.environ.get(key)
        for key in (
            "POWERBI_TENANT_ID",
            "POWERBI_CLIENT_ID",
            "POWERBI_CLIENT_SECRET",
            "POWERBI_WORKSPACE_ID",
            "POWERBI_DATASET_ID",
        )
    }
    if not all(settings.values()):
        return None

    # 1. Get a temporary access token for the application account.
    token = requests.post(
        f"https://login.microsoftonline.com/{settings['POWERBI_TENANT_ID']}/oauth2/v2.0/token",
        data={
            "grant_type": "client_credentials",
            "client_id": settings["POWERBI_CLIENT_ID"],
            "client_secret": settings["POWERBI_CLIENT_SECRET"],
            "scope": "https://analysis.windows.net/powerbi/api/.default",
        },
        timeout=30,
    )
    token.raise_for_status()

    # 2. Ask Power BI to refresh the dataset behind the report.
    url = (
        f"https://api.powerbi.com/v1.0/myorg/groups/{settings['POWERBI_WORKSPACE_ID']}"
        f"/datasets/{settings['POWERBI_DATASET_ID']}/refreshes"
    )
    response = requests.post(
        url,
        headers={"Authorization": f"Bearer {token.json()['access_token']}"},
        json={"notifyOption": "MailOnFailure"},
        timeout=30,
    )
    response.raise_for_status()
    return response.headers.get("RequestId", "accepted")


# --------------------------------------------------------------------------------------
# Alerts
# --------------------------------------------------------------------------------------
def send_alert(text: str) -> bool:
    """Sends a short message to the team channel (Microsoft Teams or Slack webhook).

    If no channel is configured, the message is only written to the log, so a
    missing configuration never breaks the pipeline itself.
    """
    url = os.environ.get("ALERT_WEBHOOK_URL")
    if not url:
        log.warning("ALERT (no webhook configured): %s", text)
        return False
    import requests

    requests.post(url, json={"text": text}, timeout=15).raise_for_status()
    return True
