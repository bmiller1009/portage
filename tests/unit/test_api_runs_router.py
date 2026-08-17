"""Router-layer tests for /v1/runs — same pattern as test_api_routers.py:
control_plane.run_service is monkeypatched, no database. The real
create/transition logic is exercised for real in
tests/integration/db/test_runs.py."""

import uuid
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from api.main import app
from control_plane import repositories, run_service
from control_plane.db import get_db_session
from control_plane.models import Run, RunEvent


async def _fake_session():
    yield None


app.dependency_overrides[get_db_session] = _fake_session
client = TestClient(app)


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


def test_list_run_events(monkeypatch):
    events = [
        RunEvent(run_id=uuid.uuid4(), from_state=None, to_state="ACCEPTED", message="run accepted"),
        RunEvent(run_id=uuid.uuid4(), from_state="ACCEPTED", to_state="QUEUED", message="submitted"),
    ]
    monkeypatch.setattr(run_service, "list_run_events", AsyncMock(return_value=events))

    resp = client.get(f"/v1/runs/{uuid.uuid4()}/events")

    assert resp.status_code == 200
    assert [e["to_state"] for e in resp.json()] == ["ACCEPTED", "QUEUED"]
