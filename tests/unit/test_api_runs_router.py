"""Router-layer tests for /v1/runs — same pattern as test_api_routers.py:
control_plane.run_service is monkeypatched, no database. The real
create/transition logic is exercised for real in
tests/integration/db/test_runs.py."""

import uuid
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from api.main import app
from control_plane import audit, repositories, run_service
from control_plane.db import get_db_session
from control_plane.models import Run, RunEvent
from tests.unit.conftest import fake_session

app.dependency_overrides[get_db_session] = fake_session
client = TestClient(app)


@pytest.fixture(autouse=True)
def _no_op_audit(monkeypatch):
    """This file's fake session (None, no real DB) can't back a real
    AuditEvent write — these are router-wiring tests, not audit tests."""
    monkeypatch.setattr(audit, "record_audit_event", AsyncMock())


def test_create_run_returns_202_for_new_run(monkeypatch):
    run = Run(
        id=uuid.uuid4(),
        workload_name="wordcount",
        workload_version="0.1.0",
        environment_name="k8s-remote",
        state="ACCEPTED",
    )
    monkeypatch.setattr(run_service, "create_run", AsyncMock(return_value=(run, True)))

    resp = client.post(
        "/v1/runs", json={"workload_name": "wordcount", "environment_name": "k8s-remote"}
    )

    assert resp.status_code == 202
    assert resp.json()["state"] == "ACCEPTED"


def test_list_runs_returns_runs(monkeypatch):
    mock_list = AsyncMock(
        return_value=[
            Run(
                id=uuid.uuid4(),
                workload_name="wordcount",
                workload_version="0.1.0",
                environment_name="k8s-remote",
                state="SUCCEEDED",
            )
        ]
    )
    monkeypatch.setattr(run_service, "list_runs", mock_list)

    resp = client.get("/v1/runs", params={"environment_name": "k8s-remote"})

    assert resp.status_code == 200
    assert resp.json()[0]["state"] == "SUCCEEDED"
    mock_list.assert_awaited_once()
    assert mock_list.await_args is not None
    assert mock_list.await_args.kwargs["environment_name"] == "k8s-remote"


def test_create_run_replay_returns_200_for_existing_run(monkeypatch):
    run = Run(
        id=uuid.uuid4(),
        workload_name="wordcount",
        workload_version="0.1.0",
        environment_name="k8s-remote",
        state="QUEUED",
    )
    monkeypatch.setattr(run_service, "create_run", AsyncMock(return_value=(run, False)))

    resp = client.post(
        "/v1/runs",
        json={"workload_name": "wordcount", "environment_name": "k8s-remote"},
        headers={"Idempotency-Key": "already-used-key"},
    )

    assert resp.status_code == 200
    assert resp.json()["state"] == "QUEUED"


def test_create_run_unknown_environment_returns_422(monkeypatch):
    monkeypatch.setattr(
        run_service,
        "create_run",
        AsyncMock(side_effect=repositories.NotFoundError("environment 'ghost' not found")),
    )

    resp = client.post("/v1/runs", json={"workload_name": "wordcount", "environment_name": "ghost"})

    assert resp.status_code == 422


def test_get_run_not_found_returns_404(monkeypatch):
    monkeypatch.setattr(
        run_service, "get_run", AsyncMock(side_effect=repositories.NotFoundError("run 'x' not found"))
    )

    resp = client.get(f"/v1/runs/{uuid.uuid4()}")

    assert resp.status_code == 404


def test_get_run_omits_failure_for_a_healthy_run(monkeypatch):
    run = Run(
        id=uuid.uuid4(),
        workload_name="wordcount",
        workload_version="0.1.0",
        environment_name="k8s-remote",
        state="RUNNING",
    )
    monkeypatch.setattr(run_service, "get_run", AsyncMock(return_value=run))
    monkeypatch.setattr(run_service, "get_run_failure", AsyncMock(return_value=None))

    resp = client.get(f"/v1/runs/{run.id}")

    assert resp.status_code == 200
    assert resp.json()["failure"] is None


def test_get_run_surfaces_structured_failure(monkeypatch):
    run = Run(
        id=uuid.uuid4(),
        workload_name="wordcount",
        workload_version="0.1.0",
        environment_name="k8s-remote",
        state="FAILED",
    )
    monkeypatch.setattr(run_service, "get_run", AsyncMock(return_value=run))
    monkeypatch.setattr(
        run_service,
        "get_run_failure",
        AsyncMock(
            return_value=run_service.RunFailure(
                category="WORKLOAD_EXECUTION",
                disposition="terminal",
                retryable=False,
                summary="Spark driver exited non-zero",
                provider="kubernetes",
                diagnostic_reference=f"/v1/runs/{run.id}/events",
            )
        ),
    )

    resp = client.get(f"/v1/runs/{run.id}")

    assert resp.status_code == 200
    failure = resp.json()["failure"]
    assert failure["category"] == "WORKLOAD_EXECUTION"
    assert failure["retryable"] is False
    assert failure["provider"] == "kubernetes"
    assert failure["summary"] == "Spark driver exited non-zero"


def test_list_run_events(monkeypatch):
    events = [
        RunEvent(run_id=uuid.uuid4(), from_state=None, to_state="ACCEPTED", message="run accepted"),
        RunEvent(run_id=uuid.uuid4(), from_state="ACCEPTED", to_state="QUEUED", message="submitted"),
    ]
    monkeypatch.setattr(run_service, "list_run_events", AsyncMock(return_value=events))

    resp = client.get(f"/v1/runs/{uuid.uuid4()}/events")

    assert resp.status_code == 200
    assert [e["to_state"] for e in resp.json()] == ["ACCEPTED", "QUEUED"]


def test_cancel_run_returns_202_when_pending(monkeypatch):
    run = Run(
        id=uuid.uuid4(),
        workload_name="wordcount",
        workload_version="0.1.0",
        environment_name="k8s-remote",
        state="CANCELING",
    )
    monkeypatch.setattr(run_service, "cancel_run", AsyncMock(return_value=(run, True)))

    resp = client.delete(f"/v1/runs/{run.id}")

    assert resp.status_code == 202
    assert resp.json()["state"] == "CANCELING"


def test_cancel_run_returns_200_when_already_final(monkeypatch):
    run = Run(
        id=uuid.uuid4(),
        workload_name="wordcount",
        workload_version="0.1.0",
        environment_name="k8s-remote",
        state="CANCELED",
    )
    monkeypatch.setattr(run_service, "cancel_run", AsyncMock(return_value=(run, False)))

    resp = client.delete(f"/v1/runs/{run.id}")

    assert resp.status_code == 200
    assert resp.json()["state"] == "CANCELED"


def test_cancel_run_not_found_returns_404(monkeypatch):
    monkeypatch.setattr(
        run_service, "cancel_run", AsyncMock(side_effect=repositories.NotFoundError("run 'x' not found"))
    )

    resp = client.delete(f"/v1/runs/{uuid.uuid4()}")

    assert resp.status_code == 404


def test_cancel_run_terminal_state_returns_409(monkeypatch):
    monkeypatch.setattr(
        run_service,
        "cancel_run",
        AsyncMock(side_effect=run_service.InvalidRunStateError("already terminal")),
    )

    resp = client.delete(f"/v1/runs/{uuid.uuid4()}")

    assert resp.status_code == 409


def test_get_run_logs_returns_reference(monkeypatch):
    from control_plane.execution_provider import LogReference

    monkeypatch.setattr(
        run_service,
        "get_run_logs",
        AsyncMock(return_value=LogReference(description="driver pod logs", uri="kubectl logs ...")),
    )

    resp = client.get(f"/v1/runs/{uuid.uuid4()}/logs")

    assert resp.status_code == 200
    assert resp.json() == {"description": "driver pod logs", "uri": "kubectl logs ..."}


def test_get_run_logs_not_found_returns_404(monkeypatch):
    monkeypatch.setattr(
        run_service,
        "get_run_logs",
        AsyncMock(side_effect=repositories.NotFoundError("run 'x' not found")),
    )

    resp = client.get(f"/v1/runs/{uuid.uuid4()}/logs")

    assert resp.status_code == 404


def test_get_run_logs_not_submitted_returns_409(monkeypatch):
    monkeypatch.setattr(
        run_service,
        "get_run_logs",
        AsyncMock(side_effect=run_service.RunNotSubmittedError("not submitted yet")),
    )

    resp = client.get(f"/v1/runs/{uuid.uuid4()}/logs")

    assert resp.status_code == 409
