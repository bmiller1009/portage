"""Unit tests for control_plane/metrics.py's reconciliation-lag gauge — the
one instrument that isn't exercised by any router/reconciler test, since
it's driven by reconciler/main.py's loop rather than reconciler/service.py."""

import time

from opentelemetry.metrics import CallbackOptions

from control_plane import metrics


def test_reconciliation_lag_reports_time_since_last_successful_reconcile():
    metrics.record_successful_reconcile()
    time.sleep(0.05)

    text = metrics.render_prometheus_text().decode()
    lines = [line for line in text.splitlines() if line.startswith("portage_reconciliation_lag_seconds{")]

    assert lines, text
    value = float(lines[0].rsplit(" ", 1)[1])
    assert value >= 0.05


def test_reconciliation_lag_absent_before_first_reconcile(monkeypatch):
    monkeypatch.setattr(metrics, "_last_successful_reconcile", None)

    assert metrics._reconciliation_lag_callback(CallbackOptions()) == []
