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
from cli.main import workload_validate as plane_workload_validate

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


def test_workload_validate_without_environment_stays_local(monkeypatch):
    """No --environment: pure schema check, zero network calls."""
    fake_post = Mock()
    monkeypatch.setattr(httpx, "post", fake_post)

    plane_workload_validate(str(EXAMPLES_DIR / "wordcount.yaml"), environment=[])

    fake_post.assert_not_called()


def test_workload_validate_with_environment_posts_to_validate_endpoint(monkeypatch):
    fake_post = Mock(return_value=FakeResponse(200, {"valid": True, "errors": []}))
    monkeypatch.setattr(httpx, "post", fake_post)

    plane_workload_validate(str(EXAMPLES_DIR / "wordcount.yaml"), environment=["k8s-remote-v2"])

    fake_post.assert_called_once()
    url, kwargs = fake_post.call_args[0][0], fake_post.call_args[1]
    assert url.endswith("/v1/validate")
    assert kwargs["json"]["environment_name"] == "k8s-remote-v2"


def test_workload_validate_exits_nonzero_on_capability_mismatch(monkeypatch):
    monkeypatch.setattr(
        httpx,
        "post",
        Mock(return_value=FakeResponse(200, {"valid": False, "errors": ["workload requires GPU"]})),
    )

    with pytest.raises(typer.Exit) as exc_info:
        plane_workload_validate(str(EXAMPLES_DIR / "wordcount.yaml"), environment=["k8s-remote-v2"])
    assert exc_info.value.exit_code == 1


def test_workload_validate_exits_nonzero_on_unknown_environment(monkeypatch):
    monkeypatch.setattr(
        httpx, "post", Mock(return_value=FakeResponse(422, {"detail": "environment 'ghost' not found"}))
    )

    with pytest.raises(typer.Exit) as exc_info:
        plane_workload_validate(str(EXAMPLES_DIR / "wordcount.yaml"), environment=["ghost"])
    assert exc_info.value.exit_code == 1


def test_workload_validate_multiple_environments_all_pass(monkeypatch, capsys):
    """Cross-environment static portability validation (spec §66): one
    workload checked against several environments in a single command."""
    fake_post = Mock(return_value=FakeResponse(200, {"valid": True, "errors": []}))
    monkeypatch.setattr(httpx, "post", fake_post)

    plane_workload_validate(
        str(EXAMPLES_DIR / "wordcount.yaml"), environment=["k8s-remote-v2", "azure-dbx-v1"]
    )

    assert fake_post.call_count == 2
    called_envs = [c.kwargs["json"]["environment_name"] for c in fake_post.call_args_list]
    assert called_envs == ["k8s-remote-v2", "azure-dbx-v1"]
    out = capsys.readouterr().out
    assert "PASS: compatible with environment 'k8s-remote-v2'" in out
    assert "PASS: compatible with environment 'azure-dbx-v1'" in out


def test_workload_validate_multiple_environments_mixed_exits_nonzero(monkeypatch, capsys):
    responses = [
        FakeResponse(200, {"valid": True, "errors": []}),
        FakeResponse(200, {"valid": False, "errors": ["unsupported Spark version: 4.2"]}),
    ]
    monkeypatch.setattr(httpx, "post", Mock(side_effect=responses))

    with pytest.raises(typer.Exit) as exc_info:
        plane_workload_validate(
            str(EXAMPLES_DIR / "wordcount.yaml"), environment=["k8s-remote-v2", "azure-dbx-v1"]
        )
    assert exc_info.value.exit_code == 1
    out = capsys.readouterr().out
    assert "PASS: compatible with environment 'k8s-remote-v2'" in out
    assert "CAPABILITY MISMATCH: azure-dbx-v1: unsupported Spark version: 4.2" in out


def test_workload_validate_prints_nothing_extra_without_provider_overrides(monkeypatch, capsys):
    monkeypatch.setattr(httpx, "post", Mock())
    plane_workload_validate(str(EXAMPLES_DIR / "wordcount.yaml"), environment=[])
    out = capsys.readouterr().out
    assert "PORTABLE WITH PROVIDER-SPECIFIC OVERRIDES" not in out


def test_workload_validate_surfaces_provider_overrides(tmp_path, monkeypatch, capsys):
    """spec §19's own worked example, driven through the actual CLI
    command — computed locally from the workload's own providerOverrides
    field, no --environment or network call needed (ADR 0010)."""
    workload_file = tmp_path / "with-overrides.yaml"
    workload_file.write_text(
        (EXAMPLES_DIR / "wordcount.yaml").read_text()
        + "\nproviderOverrides:\n"
        "  kubernetes:\n"
        "    nodeSelector: {}\n"
        "    tolerations: []\n"
        "  databricks: {}\n"
    )
    monkeypatch.setattr(httpx, "post", Mock())

    plane_workload_validate(str(workload_file), environment=[])

    out = capsys.readouterr().out
    assert "PORTABLE WITH PROVIDER-SPECIFIC OVERRIDES" in out
    assert "kubernetes\t2" in out
    assert "databricks\t0" in out
