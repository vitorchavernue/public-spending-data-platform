"""Tests of the business steps used by the Airflow pipeline (no Airflow needed)."""

from datetime import date

import pytest

from orchestration import steps


def test_previous_month_handles_january():
    # A run in January must load December of the previous year.
    assert steps.previous_month(date(2026, 1, 10)) == "202512"
    assert steps.previous_month(date(2026, 5, 10)) == "202604"


def test_requested_months_are_cleaned_and_deduplicated():
    # Typos and duplicates typed by a user must never trigger a wrong load.
    assert steps.reference_months(date(2026, 5, 10), [" 202603", "202603", "202604"]) == ["202603", "202604"]


def test_invalid_requested_months_are_rejected():
    with pytest.raises(ValueError):
        steps.reference_months(date(2026, 5, 10), ["2026-03", "abc"])


def test_local_mode_uses_sample_months(monkeypatch):
    monkeypatch.setenv("PSDP_TARGET", "local")
    assert steps.reference_months(date(2026, 5, 10)) == ["202602", "202603", "202604"]


def test_cloud_mode_uses_month_just_closed(monkeypatch):
    monkeypatch.setenv("PSDP_TARGET", "cloud")
    assert steps.reference_months(date(2026, 5, 10)) == ["202604"]


def test_unknown_target_is_rejected(monkeypatch):
    monkeypatch.setenv("PSDP_TARGET", "production")
    with pytest.raises(ValueError):
        steps.target()


def test_publish_local_writes_release_file(monkeypatch, tmp_path):
    monkeypatch.setattr(steps, "DATA_DIR", tmp_path)
    path = steps.publish_local({"months": ["202604"], "passed": True})
    assert path.exists()
    assert '"202604"' in path.read_text()


def test_alert_without_webhook_only_logs(monkeypatch):
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)
    assert steps.send_alert("test message") is False


def test_power_bi_refresh_is_skipped_when_not_configured(monkeypatch):
    for key in (
        "POWERBI_TENANT_ID",
        "POWERBI_CLIENT_ID",
        "POWERBI_CLIENT_SECRET",
        "POWERBI_WORKSPACE_ID",
        "POWERBI_DATASET_ID",
    ):
        monkeypatch.delenv(key, raising=False)
    assert steps.refresh_power_bi() is None
