"""Router-layer tests for the v0.6.2 PUT/DELETE endpoints across every
config resource — repositories is monkeypatched, no database. Real
IntegrityError -> InUseError behavior is exercised for real in
tests/integration/db/test_persistence.py; this file only checks that
each router translates the already-classified exceptions to the right
HTTP status."""

from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from api.main import app
from control_plane import audit, repositories
from control_plane.db import get_db_session
from control_plane.models import (
    DatasetBinding,
    Environment,
    ExecutionProfile,
    StorageProfile,
    WorkloadDefinition,
)
from tests.unit.conftest import fake_session

app.dependency_overrides[get_db_session] = fake_session
client = TestClient(app)


@pytest.fixture(autouse=True)
def _mock_audit(monkeypatch):
    # FakeSession has no .add()/.commit() -- every success-path handler in
    # this file eventually calls audit.record_audit_event(), so it needs
    # mocking here the same way the *_repositories functions themselves
    # are mocked per test.
    monkeypatch.setattr(audit, "record_audit_event", AsyncMock())


def test_update_execution_profile(monkeypatch):
    monkeypatch.setattr(
        repositories,
        "update_execution_profile",
        AsyncMock(return_value=ExecutionProfile(name="exec-a", provider="databricks", config={"x": 1})),
    )
    resp = client.put("/v1/execution-profiles/exec-a", json={"provider": "databricks", "config": {"x": 1}})
    assert resp.status_code == 200
    assert resp.json()["provider"] == "databricks"


def test_update_execution_profile_404(monkeypatch):
    async def _raise(session, name, **kwargs):
        raise repositories.NotFoundError(f"execution profile '{name}' not found")

    monkeypatch.setattr(repositories, "update_execution_profile", _raise)
    resp = client.put("/v1/execution-profiles/ghost", json={"provider": "databricks", "config": {}})
    assert resp.status_code == 404


def test_delete_execution_profile_409_when_in_use(monkeypatch):
    async def _raise(session, name):
        raise repositories.InUseError(f"execution profile '{name}' is still referenced by an environment")

    monkeypatch.setattr(repositories, "delete_execution_profile", _raise)
    resp = client.delete("/v1/execution-profiles/exec-a")
    assert resp.status_code == 409


def test_update_storage_profile(monkeypatch):
    monkeypatch.setattr(
        repositories,
        "update_storage_profile",
        AsyncMock(
            return_value=StorageProfile(name="storage-a", provider="adls", config={}, credential_reference={})
        ),
    )
    resp = client.put(
        "/v1/storage-profiles/storage-a",
        json={"provider": "adls", "config": {}, "credential_reference": {}},
    )
    assert resp.status_code == 200


def test_delete_storage_profile(monkeypatch):
    monkeypatch.setattr(repositories, "delete_storage_profile", AsyncMock())
    resp = client.delete("/v1/storage-profiles/storage-a")
    assert resp.status_code == 204


def test_update_environment(monkeypatch):
    monkeypatch.setattr(
        repositories,
        "update_environment",
        AsyncMock(
            return_value=Environment(
                name="env-a",
                execution_provider="kubernetes",
                execution_profile_name="exec-a",
                storage_provider="s3",
                storage_profile_name="storage-a",
            )
        ),
    )
    resp = client.put(
        "/v1/environments/env-a",
        json={
            "execution_provider": "kubernetes",
            "execution_profile_name": "exec-a",
            "storage_provider": "s3",
            "storage_profile_name": "storage-a",
        },
    )
    assert resp.status_code == 200


def test_delete_environment_409_when_in_use(monkeypatch):
    async def _raise(session, name):
        raise repositories.InUseError(f"environment '{name}' is still referenced by a run")

    monkeypatch.setattr(repositories, "delete_environment", _raise)
    resp = client.delete("/v1/environments/env-a")
    assert resp.status_code == 409


def test_update_dataset_binding(monkeypatch):
    monkeypatch.setattr(
        repositories,
        "update_dataset_binding",
        AsyncMock(
            return_value=DatasetBinding(
                dataset_name="ds-a", environment_name="env-a", kind="table", uri="analytics.x"
            )
        ),
    )
    mock_audit = AsyncMock()
    monkeypatch.setattr(audit, "record_audit_event", mock_audit)

    resp = client.put("/v1/datasets/ds-a/env-a", json={"kind": "table", "uri": "analytics.x"})

    assert resp.status_code == 200
    assert resp.json()["kind"] == "table"
    mock_audit.assert_awaited_once()
    assert mock_audit.await_args is not None
    assert mock_audit.await_args.kwargs["action"] == "DATASET_BINDING_UPDATE"
    assert mock_audit.await_args.kwargs["resource"] == "ds-a/env-a"


def test_delete_dataset_binding(monkeypatch):
    monkeypatch.setattr(repositories, "delete_dataset_binding", AsyncMock())
    mock_audit = AsyncMock()
    monkeypatch.setattr(audit, "record_audit_event", mock_audit)

    resp = client.delete("/v1/datasets/ds-a/env-a")

    assert resp.status_code == 204
    mock_audit.assert_awaited_once()
    assert mock_audit.await_args is not None
    assert mock_audit.await_args.kwargs["action"] == "DATASET_BINDING_DELETE"
    assert mock_audit.await_args.kwargs["result"] == audit.RESULT_SUCCESS


def test_update_workload_requires_version_query_param():
    resp = client.put("/v1/workloads/wordcount", json={})
    assert resp.status_code == 422


def test_update_workload(monkeypatch):
    monkeypatch.setattr(
        repositories,
        "update_workload_definition",
        AsyncMock(
            return_value=WorkloadDefinition(name="wordcount", version="0.1.0", definition={"metadata": {}})
        ),
    )
    body = {
        "apiVersion": "runtime/v1alpha1",
        "kind": "SparkWorkload",
        "metadata": {"name": "wordcount", "version": "0.1.0"},
        "runtime": {"spark": "4.2"},
        "application": {"type": "python-wheel", "artifact": "artifact://wordcount/0.1.0", "entryPoint": "x.y"},
        "datasets": {},
        "resources": {
            "driver": {"cores": 1, "memory": "1g"},
            "executor": {"cores": 1, "memory": "1g"},
            "scaling": {"minExecutors": 1, "maxExecutors": 1},
        },
        "execution": {"timeout": "10m"},
    }
    resp = client.put("/v1/workloads/wordcount?version=0.1.0", json=body)
    assert resp.status_code == 200


def test_delete_workload_409_when_in_use(monkeypatch):
    async def _raise(session, name, version):
        raise repositories.InUseError(f"workload '{name}' version '{version}' is still referenced by a run")

    monkeypatch.setattr(repositories, "delete_workload_definition", _raise)
    resp = client.delete("/v1/workloads/wordcount?version=0.1.0")
    assert resp.status_code == 409


def test_delete_workload_requires_version_query_param():
    resp = client.delete("/v1/workloads/wordcount")
    assert resp.status_code == 422
