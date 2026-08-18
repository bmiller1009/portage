"""Tests plane's run/status/cancel/logs commands as pure REST clients (spec
§31) — httpx.post/get/delete are monkeypatched so no real network or API
server is involved, only the request shapes and response handling."""

from pathlib import Path
from typing import cast
from unittest.mock import Mock

import httpx
import pytest
import typer

from cli.main import cancel as plane_cancel
from cli.main import logs as plane_logs
from cli.main import run as plane_run
from cli.main import status as plane_status

EXAMPLES_DIR = Path(__file__).resolve().parents[2] / "examples"
RUN_ID = "11111111-1111-1111-1111-111111111111"


class FakeResponse:
    def __init__(self, status_code: int, json_data):
        self.status_code = status_code
        self._json = json_data

    def json(self):
        return self._json

    def raise_for_status(self):
        if self.status_code >= 400:
            request = httpx.Request("GET", "http://test")
            raise httpx.HTTPStatusError("error", request=request, response=cast(httpx.Response, self))


def _install_run_fakes(monkeypatch, states: list[str]):
    """POST /v1/workloads -> 201, POST /v1/runs -> 202, then GET
    /v1/runs/{id} returns each of `states` in turn (holding on the last)."""
    calls: dict[str, list] = {"post": [], "get": []}
    remaining = list(states)

    def fake_post(url, json=None, headers=None, **kwargs):
        calls["post"].append((url, json, headers))
        if url.endswith("/v1/workloads"):
            return FakeResponse(201, {})
        if url.endswith("/v1/runs"):
            return FakeResponse(202, {"id": RUN_ID})
        raise AssertionError(f"unexpected POST {url}")

    def fake_get(url, **kwargs):
        calls["get"].append(url)
        state = remaining.pop(0) if len(remaining) > 1 else remaining[0]
        return FakeResponse(200, {"id": RUN_ID, "state": state})

    monkeypatch.setattr(httpx, "post", fake_post)
    monkeypatch.setattr(httpx, "get", fake_get)
    return calls


def test_run_registers_workload_and_submits(monkeypatch):
    calls = _install_run_fakes(monkeypatch, ["QUEUED", "RUNNING", "SUCCEEDED"])

    plane_run(
        workload_file=str(EXAMPLES_DIR / "wordcount.yaml"),
        environment="k8s-remote",
        timeout_seconds=5,
        poll_interval_seconds=0,
    )

    workload_url, workload_body, _ = calls["post"][0]
    assert workload_url.endswith("/v1/workloads")
    assert workload_body["metadata"]["name"] == "wordcount"

    run_url, run_body, run_headers = calls["post"][1]
    assert run_url.endswith("/v1/runs")
    assert run_body == {
        "workload_name": "wordcount",
        "workload_version": workload_body["metadata"]["version"],
        "environment_name": "k8s-remote",
    }
    assert "Idempotency-Key" in run_headers


def test_run_treats_workload_already_registered_as_success(monkeypatch):
    """A 409 from POST /v1/workloads means the workload is already
    registered — not an error the CLI should surface."""
    remaining = ["SUCCEEDED"]

    def fake_post(url, json=None, headers=None, **kwargs):
        if url.endswith("/v1/workloads"):
            return FakeResponse(409, {"detail": "already exists"})
        return FakeResponse(202, {"id": RUN_ID})

    def fake_get(url, **kwargs):
        state = remaining.pop(0) if len(remaining) > 1 else remaining[0]
        return FakeResponse(200, {"id": RUN_ID, "state": state})

    monkeypatch.setattr(httpx, "post", fake_post)
    monkeypatch.setattr(httpx, "get", fake_get)

    plane_run(
        workload_file=str(EXAMPLES_DIR / "wordcount.yaml"),
        environment="k8s-remote",
        timeout_seconds=5,
        poll_interval_seconds=0,
    )


def test_run_exits_nonzero_on_failure(monkeypatch):
    _install_run_fakes(monkeypatch, ["RUNNING", "FAILED"])

    with pytest.raises(typer.Exit) as exc_info:
        plane_run(
            workload_file=str(EXAMPLES_DIR / "wordcount.yaml"),
            environment="k8s-remote",
            timeout_seconds=5,
            poll_interval_seconds=0,
        )
    assert exc_info.value.exit_code == 1


def test_status_prints_run_and_events(monkeypatch):
    run_resp = FakeResponse(
        200,
        {
            "id": RUN_ID,
            "state": "RUNNING",
            "workload_name": "wordcount",
            "workload_version": "0.1.0",
            "environment_name": "k8s-remote",
        },
    )
    events_resp = FakeResponse(
        200, [{"from_state": "ACCEPTED", "to_state": "QUEUED", "message": "submitted"}]
    )
    fake_get = Mock(side_effect=[run_resp, events_resp])
    monkeypatch.setattr(httpx, "get", fake_get)

    plane_status(RUN_ID)

    assert fake_get.call_count == 2


def test_status_exits_nonzero_on_missing_run(monkeypatch):
    monkeypatch.setattr(httpx, "get", Mock(return_value=FakeResponse(404, {"detail": "not found"})))

    with pytest.raises(typer.Exit) as exc_info:
        plane_status(RUN_ID)
    assert exc_info.value.exit_code == 1


def test_cancel_calls_delete_and_prints_state(monkeypatch):
    fake_delete = Mock(return_value=FakeResponse(202, {"id": RUN_ID, "state": "CANCELING"}))
    monkeypatch.setattr(httpx, "delete", fake_delete)

    plane_cancel(RUN_ID)

    fake_delete.assert_called_once()
    assert fake_delete.call_args[0][0].endswith(f"/v1/runs/{RUN_ID}")


def test_cancel_exits_nonzero_on_conflict(monkeypatch):
    monkeypatch.setattr(
        httpx, "delete", Mock(return_value=FakeResponse(409, {"detail": "already terminal"}))
    )

    with pytest.raises(typer.Exit) as exc_info:
        plane_cancel(RUN_ID)
    assert exc_info.value.exit_code == 1


def test_logs_prints_reference(monkeypatch, capsys):
    monkeypatch.setattr(
        httpx,
        "get",
        Mock(return_value=FakeResponse(200, {"description": "driver pod logs", "uri": "kubectl logs ..."})),
    )

    plane_logs(RUN_ID)

    assert "driver pod logs" in capsys.readouterr().out


def test_logs_exits_nonzero_when_not_submitted(monkeypatch):
    monkeypatch.setattr(
        httpx, "get", Mock(return_value=FakeResponse(409, {"detail": "not submitted yet"}))
    )

    with pytest.raises(typer.Exit) as exc_info:
        plane_logs(RUN_ID)
    assert exc_info.value.exit_code == 1
