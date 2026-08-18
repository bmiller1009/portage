"""Router-layer tests for /v1/providers — no database, repositories and
provider_factory are monkeypatched."""

from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from api.main import app
from control_plane import provider_factory, repositories
from control_plane.db import get_db_session
from control_plane.execution_provider import CapabilitySet
from control_plane.models import ExecutionProfile, StorageProfile
from control_plane.storage_provider import StorageCapabilitySet
from tests.unit.conftest import fake_session

app.dependency_overrides[get_db_session] = fake_session
client = TestClient(app)


def test_list_providers_combines_execution_and_storage(monkeypatch):
    monkeypatch.setattr(
        repositories,
        "list_execution_profiles",
        AsyncMock(return_value=[ExecutionProfile(name="phase0-remote", provider="kubernetes", config={})]),
    )
    monkeypatch.setattr(
        repositories,
        "list_storage_profiles",
        AsyncMock(
            return_value=[
                StorageProfile(name="phase0-minio", provider="s3", config={}, credential_reference={})
            ]
        ),
    )

    resp = client.get("/v1/providers")

    assert resp.status_code == 200
    body = resp.json()
    assert {"name": "phase0-remote", "kind": "execution", "provider": "kubernetes"} in body
    assert {"name": "phase0-minio", "kind": "storage", "provider": "s3"} in body


def test_get_provider_capabilities_execution(monkeypatch):
    monkeypatch.setattr(
        repositories,
        "get_execution_profile",
        AsyncMock(return_value=ExecutionProfile(name="phase0-remote", provider="kubernetes", config={})),
    )

    class FakeProvider:
        async def capabilities(self):
            return CapabilitySet(
                spark_versions=["4.2"], languages=["python", "jvm"], dynamic_allocation=True,
                gpu=False, streaming=False, local_disk=True, spark_connect=False,
            )

    monkeypatch.setattr(provider_factory, "build_execution_provider", lambda profile: FakeProvider())

    resp = client.get("/v1/providers/phase0-remote/capabilities")

    assert resp.status_code == 200
    body = resp.json()
    assert body["kind"] == "execution"
    assert body["capabilities"]["spark_versions"] == ["4.2"]


def test_get_provider_capabilities_storage(monkeypatch):
    monkeypatch.setattr(
        repositories, "get_execution_profile", AsyncMock(return_value=None)
    )
    monkeypatch.setattr(
        repositories,
        "get_storage_profile",
        AsyncMock(
            return_value=StorageProfile(name="phase0-minio", provider="s3", config={}, credential_reference={})
        ),
    )

    class FakeStorageProvider:
        def capabilities(self):
            return StorageCapabilitySet(protocol="s3", path_bindings=True, table_bindings=False)

    monkeypatch.setattr(provider_factory, "build_storage_provider", lambda profile: FakeStorageProvider())

    resp = client.get("/v1/providers/phase0-minio/capabilities")

    assert resp.status_code == 200
    body = resp.json()
    assert body["kind"] == "storage"
    assert body["capabilities"]["protocol"] == "s3"


def test_get_provider_capabilities_not_found(monkeypatch):
    monkeypatch.setattr(repositories, "get_execution_profile", AsyncMock(return_value=None))
    monkeypatch.setattr(repositories, "get_storage_profile", AsyncMock(return_value=None))

    resp = client.get("/v1/providers/ghost/capabilities")

    assert resp.status_code == 404
