"""Unit tests for PortageRunSensor -- PortageHook is monkeypatched."""

from unittest.mock import MagicMock

import pytest
from airflow.exceptions import AirflowException
from airflow_provider_portage.sensors.portage import PortageRunSensor


def _sensor_with_hook(monkeypatch, run_state: str) -> PortageRunSensor:
    mock_hook = MagicMock()
    mock_hook.get_run.return_value = {"id": "run-1", "state": run_state}
    monkeypatch.setattr(
        "airflow_provider_portage.sensors.portage.PortageHook", MagicMock(return_value=mock_hook)
    )
    return PortageRunSensor(task_id="wait_for_run", run_id="run-1")


def test_poke_returns_false_while_running(monkeypatch):
    sensor = _sensor_with_hook(monkeypatch, "RUNNING")
    assert sensor.poke({}) is False


def test_poke_returns_true_on_succeeded(monkeypatch):
    sensor = _sensor_with_hook(monkeypatch, "SUCCEEDED")
    assert sensor.poke({}) is True


def test_poke_raises_on_failed(monkeypatch):
    sensor = _sensor_with_hook(monkeypatch, "FAILED")
    with pytest.raises(AirflowException, match="ended in FAILED"):
        sensor.poke({})


def test_poke_raises_on_canceled(monkeypatch):
    sensor = _sensor_with_hook(monkeypatch, "CANCELED")
    with pytest.raises(AirflowException, match="ended in CANCELED"):
        sensor.poke({})
