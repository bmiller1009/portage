"""Unit tests for PortageRunOperator -- PortageHook itself is
monkeypatched (already covered directly by tests/test_hooks.py), so
these tests focus purely on the operator's own orchestration logic:
register -> submit -> poll -> raise/return."""

from unittest.mock import MagicMock

import pytest
import yaml
from airflow.exceptions import AirflowException
from airflow_provider_portage.operators.portage import PortageRunOperator


@pytest.fixture
def workload_file(tmp_path):
    path = tmp_path / "workload.yaml"
    path.write_text(yaml.safe_dump({"metadata": {"name": "wordcount", "version": "0.1.0"}}))
    return str(path)


def _context():
    return {
        "dag": MagicMock(dag_id="test_dag"),
        "run_id": "manual__2026-08-19",
        "task": MagicMock(task_id="submit_wordcount"),
    }


def test_execute_returns_run_on_success(monkeypatch, workload_file):
    mock_hook = MagicMock()
    mock_hook.submit_run.return_value = {"id": "run-1"}
    mock_hook.get_run.return_value = {"id": "run-1", "state": "SUCCEEDED"}
    monkeypatch.setattr(
        "airflow_provider_portage.operators.portage.PortageHook", MagicMock(return_value=mock_hook)
    )

    op = PortageRunOperator(
        task_id="submit_wordcount", workload_file=workload_file, environment_name="k8s-remote"
    )
    result = op.execute(_context())

    assert result["state"] == "SUCCEEDED"
    mock_hook.register_workload.assert_called_once()
    mock_hook.submit_run.assert_called_once_with(
        workload_name="wordcount",
        workload_version="0.1.0",
        environment_name="k8s-remote",
        idempotency_key="test_dag-manual__2026-08-19-submit_wordcount",
    )


def test_execute_raises_on_failed_state(monkeypatch, workload_file):
    mock_hook = MagicMock()
    mock_hook.submit_run.return_value = {"id": "run-1"}
    mock_hook.get_run.return_value = {"id": "run-1", "state": "FAILED"}
    monkeypatch.setattr(
        "airflow_provider_portage.operators.portage.PortageHook", MagicMock(return_value=mock_hook)
    )

    op = PortageRunOperator(
        task_id="submit_wordcount", workload_file=workload_file, environment_name="k8s-remote"
    )
    with pytest.raises(AirflowException, match="ended in FAILED"):
        op.execute(_context())


def test_execute_polls_until_terminal(monkeypatch, workload_file):
    mock_hook = MagicMock()
    mock_hook.submit_run.return_value = {"id": "run-1"}
    mock_hook.get_run.side_effect = [
        {"id": "run-1", "state": "QUEUED"},
        {"id": "run-1", "state": "RUNNING"},
        {"id": "run-1", "state": "SUCCEEDED"},
    ]
    monkeypatch.setattr(
        "airflow_provider_portage.operators.portage.PortageHook", MagicMock(return_value=mock_hook)
    )
    monkeypatch.setattr("airflow_provider_portage.operators.portage.time.sleep", MagicMock())

    op = PortageRunOperator(
        task_id="submit_wordcount",
        workload_file=workload_file,
        environment_name="k8s-remote",
        poll_interval_seconds=0,
    )
    result = op.execute(_context())

    assert result["state"] == "SUCCEEDED"
    assert mock_hook.get_run.call_count == 3
