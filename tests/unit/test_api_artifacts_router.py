"""Router-layer tests for /v1/artifacts — mirrors test_api_routers.py's
dataset-binding tests exactly, since the two endpoints share the same
shape of problem (a logical binding CRUD). No database — repository
functions are monkeypatched; the real persistence is exercised in
tests/integration/db/test_artifacts.py."""

from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from api.main import app
from control_plane import audit, repositories
from control_plane.db import get_db_session
from control_plane.models import ArtifactBinding
from tests.unit.conftest import fake_session

app.dependency_overrides[get_db_session] = fake_session
client = TestClient(app)


@pytest.fixture(autouse=True)
def _no_op_audit(monkeypatch):
    """This file's fake session (no real DB) can't back a real
    AuditEvent write — these are router-wiring tests, not audit tests."""
    monkeypatch.setattr(audit, "record_audit_event", AsyncMock())


def test_create_artifact_binding_success(monkeypatch):
    mock_audit = AsyncMock()
    monkeypatch.setattr(audit, "record_audit_event", mock_audit)
    monkeypatch.setattr(
        repositories,
        "create_artifact_binding",
        AsyncMock(
            return_value=ArtifactBinding(
                artifact_name="wordcount",
                artifact_version="0.1.0",
                environment_name="k8s-remote",
                kind="path",
                uri="s3a://portage-phase0/artifacts/wordcount-0.1.0.whl",
            )
        ),
    )

    resp = client.post(
        "/v1/artifacts",
        json={
            "artifact_name": "wordcount",
            "artifact_version": "0.1.0",
            "environment_name": "k8s-remote",
            "uri": "s3a://portage-phase0/artifacts/wordcount-0.1.0.whl",
        },
    )

    assert resp.status_code == 201
    assert resp.json()["artifact_name"] == "wordcount"
    mock_audit.assert_awaited_once()
    assert mock_audit.await_args is not None
    assert mock_audit.await_args.kwargs["action"] == "ARTIFACT_BINDING_CREATE"
    assert mock_audit.await_args.kwargs["result"] == audit.RESULT_SUCCESS
    assert mock_audit.await_args.kwargs["resource"] == "wordcount/0.1.0/k8s-remote"


def test_create_artifact_binding_duplicate_returns_409(monkeypatch):
    monkeypatch.setattr(
        repositories,
        "create_artifact_binding",
        AsyncMock(side_effect=repositories.AlreadyExistsError("already bound")),
    )

    resp = client.post(
        "/v1/artifacts",
        json={
            "artifact_name": "wordcount",
            "artifact_version": "0.1.0",
            "environment_name": "k8s-remote",
            "uri": "s3a://bucket/x",
        },
    )

    assert resp.status_code == 409


def test_create_artifact_binding_unknown_environment_returns_422(monkeypatch):
    monkeypatch.setattr(
        repositories,
        "create_artifact_binding",
        AsyncMock(side_effect=repositories.NotFoundError("environment 'ghost' not found")),
    )

    resp = client.post(
        "/v1/artifacts",
        json={
            "artifact_name": "wordcount",
            "artifact_version": "0.1.0",
            "environment_name": "ghost",
            "uri": "s3a://bucket/x",
        },
    )

    assert resp.status_code == 422


def test_list_artifact_bindings_filters_by_artifact_name(monkeypatch):
    mock_list = AsyncMock(
        return_value=[
            ArtifactBinding(
                artifact_name="wordcount",
                artifact_version="0.1.0",
                environment_name="k8s-remote",
                kind="path",
                uri="s3a://portage-phase0/artifacts/wordcount-0.1.0.whl",
            )
        ]
    )
    monkeypatch.setattr(repositories, "list_artifact_bindings", mock_list)

    resp = client.get("/v1/artifacts", params={"artifact_name": "wordcount"})

    assert resp.status_code == 200
    mock_list.assert_awaited_once()
    assert mock_list.await_args is not None
    assert mock_list.await_args.kwargs["artifact_name"] == "wordcount"


def test_get_artifact_binding_not_found_returns_404(monkeypatch):
    monkeypatch.setattr(
        repositories,
        "get_artifact_binding",
        AsyncMock(side_effect=repositories.NotFoundError("no binding")),
    )

    resp = client.get("/v1/artifacts/wordcount/0.1.0/k8s-remote")

    assert resp.status_code == 404
