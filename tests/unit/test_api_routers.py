"""Router-layer tests: HTTP status/error mapping, request validation.
No database — control_plane.db.get_db_session is overridden to a no-op,
and repository functions are monkeypatched, since the repository
functions themselves are exercised for real against Postgres in
tests/integration/db (this layer only needs to prove the routers wire
errors and responses correctly)."""

from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from api.main import app
from control_plane import audit, repositories
from control_plane.db import get_db_session
from control_plane.models import DatasetBinding, Environment, ExecutionProfile
from tests.unit.conftest import fake_session

EXAMPLES_DIR = Path(__file__).resolve().parents[2] / "examples"


app.dependency_overrides[get_db_session] = fake_session
client = TestClient(app)


@pytest.fixture(autouse=True)
def _no_op_audit(monkeypatch):
    """This file's fake session (None, no real DB) can't back a real
    AuditEvent write — these are router-wiring tests, not audit tests
    (see tests/unit/test_api_audit_router.py for those)."""
    monkeypatch.setattr(audit, "record_audit_event", AsyncMock())


def test_create_execution_profile_success(monkeypatch):
    monkeypatch.setattr(
        repositories,
        "create_execution_profile",
        AsyncMock(return_value=ExecutionProfile(name="phase0-remote", provider="kubernetes", config={})),
    )
    resp = client.post(
        "/v1/execution-profiles",
        json={"name": "phase0-remote", "provider": "kubernetes", "config": {"namespace": "default"}},
    )
    assert resp.status_code == 201
    assert resp.json()["name"] == "phase0-remote"


def test_create_execution_profile_duplicate_returns_409(monkeypatch):
    monkeypatch.setattr(
        repositories,
        "create_execution_profile",
        AsyncMock(side_effect=repositories.AlreadyExistsError("execution profile 'x' already exists")),
    )
    resp = client.post(
        "/v1/execution-profiles", json={"name": "x", "provider": "kubernetes", "config": {}}
    )
    assert resp.status_code == 409


def test_get_environment_not_found_returns_404(monkeypatch):
    monkeypatch.setattr(
        repositories,
        "get_environment",
        AsyncMock(side_effect=repositories.NotFoundError("environment 'nope' not found")),
    )
    resp = client.get("/v1/environments/nope")
    assert resp.status_code == 404


def test_create_environment_with_dangling_profile_returns_422(monkeypatch):
    monkeypatch.setattr(
        repositories,
        "create_environment",
        AsyncMock(side_effect=repositories.NotFoundError("execution profile 'ghost' not found")),
    )
    resp = client.post(
        "/v1/environments",
        json={
            "name": "k8s-remote",
            "execution_provider": "kubernetes",
            "execution_profile_name": "ghost",
            "storage_provider": "s3",
            "storage_profile_name": "minio",
        },
    )
    assert resp.status_code == 422


def test_list_environments(monkeypatch):
    monkeypatch.setattr(
        repositories,
        "list_environments",
        AsyncMock(
            return_value=[
                Environment(
                    name="k8s-remote",
                    execution_provider="kubernetes",
                    execution_profile_name="phase0-remote",
                    storage_provider="s3",
                    storage_profile_name="phase0-minio",
                )
            ]
        ),
    )
    resp = client.get("/v1/environments")
    assert resp.status_code == 200
    assert resp.json()[0]["name"] == "k8s-remote"


def test_create_workload_validates_body_against_spark_workload_schema():
    # Missing required fields (application/datasets/resources/execution) —
    # FastAPI/Pydantic should reject before the handler (and repositories)
    # ever run, per POST /v1/workloads only accepting real SparkWorkloads.
    resp = client.post("/v1/workloads", json={"apiVersion": "runtime/v1alpha1", "kind": "SparkWorkload"})
    assert resp.status_code == 422


def test_create_workload_success(monkeypatch):
    import yaml

    from control_plane.models import WorkloadDefinition

    workload_dict = yaml.safe_load((EXAMPLES_DIR / "wordcount.yaml").read_text())
    monkeypatch.setattr(
        repositories,
        "create_workload_definition",
        AsyncMock(
            return_value=WorkloadDefinition(
                name="wordcount", version="0.1.0", definition=workload_dict
            )
        ),
    )
    resp = client.post("/v1/workloads", json=workload_dict)
    assert resp.status_code == 201
    assert resp.json()["name"] == "wordcount"


def test_list_dataset_bindings_filters_by_dataset_name(monkeypatch):
    mock_list = AsyncMock(
        return_value=[
            DatasetBinding(
                dataset_name="wordcount.raw",
                environment_name="k8s-remote",
                kind="path",
                uri="s3a://portage-phase0/wordcount/input.txt",
            )
        ]
    )
    monkeypatch.setattr(repositories, "list_dataset_bindings", mock_list)

    resp = client.get("/v1/datasets", params={"dataset_name": "wordcount.raw"})

    assert resp.status_code == 200
    mock_list.assert_awaited_once()
    assert mock_list.await_args is not None
    assert mock_list.await_args.kwargs["dataset_name"] == "wordcount.raw"


def test_create_dataset_binding_records_audit_event(monkeypatch):
    monkeypatch.setattr(
        repositories,
        "create_dataset_binding",
        AsyncMock(
            return_value=DatasetBinding(
                dataset_name="wordcount.raw",
                environment_name="k8s-remote",
                kind="path",
                uri="s3a://portage-phase0/wordcount/input.txt",
            )
        ),
    )
    mock_audit = AsyncMock()
    monkeypatch.setattr(audit, "record_audit_event", mock_audit)

    resp = client.post(
        "/v1/datasets",
        json={
            "dataset_name": "wordcount.raw",
            "environment_name": "k8s-remote",
            "kind": "path",
            "uri": "s3a://portage-phase0/wordcount/input.txt",
        },
    )

    assert resp.status_code == 201
    mock_audit.assert_awaited_once()
    assert mock_audit.await_args is not None
    assert mock_audit.await_args.kwargs["action"] == "DATASET_BINDING_CREATE"
    assert mock_audit.await_args.kwargs["resource"] == "wordcount.raw/k8s-remote"
    assert mock_audit.await_args.kwargs["result"] == audit.RESULT_SUCCESS
