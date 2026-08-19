"""Unit tests for control_plane/certification.py — repositories and
provider_factory are monkeypatched, no database and no real provider
construction."""

from typing import cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from control_plane import certification, provider_factory, repositories
from control_plane.execution_provider import CapabilitySet
from control_plane.models import Environment, ExecutionProfile, StorageProfile


def _environment(name: str, *, execution_profile: str, storage_profile: str) -> Environment:
    return Environment(
        name=name,
        execution_provider="kubernetes",
        execution_profile_name=execution_profile,
        storage_provider="s3",
        storage_profile_name=storage_profile,
    )


class _FakeExecutionProvider:
    def __init__(self, spark_versions=("4.1", "4.2")):
        self._spark_versions = list(spark_versions)

    async def capabilities(self):
        return CapabilitySet(
            spark_versions=self._spark_versions,
            languages=["python"],
            dynamic_allocation=False,
            gpu=False,
            streaming=False,
            local_disk=True,
            spark_connect=False,
        )


def test_storage_protocol_label_distinguishes_vast_modes():
    s3_mode = StorageProfile(name="x", provider="vast", config={"protocol": "s3"}, credential_reference={})
    nfs_mode = StorageProfile(name="x", provider="vast", config={"protocol": "nfs"}, credential_reference={})
    plain_s3 = StorageProfile(name="x", provider="s3", config={}, credential_reference={})
    adls = StorageProfile(name="x", provider="adls", config={}, credential_reference={})

    assert certification._storage_protocol_label(s3_mode) == "vast-s3"
    assert certification._storage_protocol_label(nfs_mode) == "vast-nfs"
    assert certification._storage_protocol_label(plain_s3) == "s3"
    assert certification._storage_protocol_label(adls) == "adls"


@pytest.mark.parametrize(
    "execution_provider,storage_protocol,expected",
    [
        ("kubernetes", "s3", certification.STATUS_PASS),
        ("kubernetes", "vast-s3", certification.STATUS_PASS),
        ("kubernetes", "vast-nfs", certification.STATUS_BLOCKED),
        ("kubernetes", "adls", certification.STATUS_BLOCKED),
        # Live-verified since v0.3 (real OAuth M2M runs), including
        # v1.0.0's Spark 4.2 run — paired with Unity Catalog Volumes
        # storage, registered under the "s3" storage-provider type.
        ("databricks", "s3", certification.STATUS_PASS),
        # ADLS itself remains translation-layer-only (no real Azure
        # subscription available to this project) regardless of which
        # execution provider it's paired with.
        ("databricks", "adls", certification.STATUS_BLOCKED),
    ],
)
def test_status_for_matches_this_projects_live_verification_history(
    execution_provider, storage_protocol, expected
):
    assert certification._status_for(execution_provider, storage_protocol) == expected


@pytest.mark.asyncio
async def test_check_execution_side_reports_no_fail_for_healthy_provider(monkeypatch):
    async def fake_get_execution_profile(session, name, *, required=True):
        return ExecutionProfile(name=name, provider="kubernetes", config={})

    monkeypatch.setattr(repositories, "get_execution_profile", fake_get_execution_profile)
    monkeypatch.setattr(provider_factory, "build_execution_provider", lambda ep: _FakeExecutionProvider())

    provider_name, fail_detail = await certification._check_execution_side(
        cast(AsyncSession, None), "exec"
    )

    assert provider_name == "kubernetes"
    assert fail_detail is None


@pytest.mark.asyncio
async def test_check_execution_side_reports_fail_on_construction_error(monkeypatch):
    async def fake_get_execution_profile(session, name, *, required=True):
        return ExecutionProfile(name=name, provider="kubernetes", config={})

    def raise_construction_error(ep):
        raise RuntimeError("kubeconfig not found")

    monkeypatch.setattr(repositories, "get_execution_profile", fake_get_execution_profile)
    monkeypatch.setattr(provider_factory, "build_execution_provider", raise_construction_error)

    provider_name, fail_detail = await certification._check_execution_side(
        cast(AsyncSession, None), "exec"
    )

    assert provider_name == "kubernetes"
    assert fail_detail is not None
    assert "kubeconfig" in fail_detail


@pytest.mark.asyncio
async def test_check_execution_side_reports_fail_on_no_spark_versions(monkeypatch):
    async def fake_get_execution_profile(session, name, *, required=True):
        return ExecutionProfile(name=name, provider="kubernetes", config={})

    monkeypatch.setattr(repositories, "get_execution_profile", fake_get_execution_profile)
    monkeypatch.setattr(
        provider_factory, "build_execution_provider", lambda ep: _FakeExecutionProvider(spark_versions=())
    )

    _provider_name, fail_detail = await certification._check_execution_side(cast(AsyncSession, None), "exec")

    assert fail_detail is not None


@pytest.mark.asyncio
async def test_generate_report_deduplicates_by_provider_type_not_by_environment(monkeypatch):
    """This project's shared database accumulates many uniquely-named
    test environments for the SAME underlying provider combination — the
    report must produce one row per combination, not one per
    environment (confirmed live — see issue #42's closing comment)."""

    async def fake_get_storage_profile(session, name, *, required=True):
        return StorageProfile(name=name, provider="s3", config={}, credential_reference={})

    async def fake_get_execution_profile(session, name, *, required=True):
        return ExecutionProfile(name=name, provider="kubernetes", config={})

    monkeypatch.setattr(repositories, "get_storage_profile", fake_get_storage_profile)
    monkeypatch.setattr(repositories, "get_execution_profile", fake_get_execution_profile)
    monkeypatch.setattr(provider_factory, "build_execution_provider", lambda ep: _FakeExecutionProvider())

    async def fake_list_environments(session):
        return [
            _environment(f"env-{i}", execution_profile=f"exec-{i}", storage_profile=f"storage-{i}")
            for i in range(50)
        ]

    monkeypatch.setattr(repositories, "list_environments", fake_list_environments)

    report = await certification.generate_certification_report(cast(AsyncSession, None))

    assert len(report.rows) == 1
    assert report.rows[0].status == certification.STATUS_PASS
