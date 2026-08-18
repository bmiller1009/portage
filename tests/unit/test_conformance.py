"""Unit tests for control_plane/conformance.py — repositories and
conformance.comparison.compare_semantic are monkeypatched, no database
and no real Parquet I/O. The real end-to-end path (real runs, real
Postgres, real MinIO) is live-verified separately, matching this
project's established pattern for every other reconciliation-adjacent
module."""

import uuid
from typing import cast

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from conformance.comparison import ComparisonResult
from control_plane import conformance, provider_factory, repositories
from control_plane.models import (
    DatasetBinding,
    Environment,
    Run,
    StorageProfile,
    WorkloadDefinition,
)

WORKLOAD_DEFINITION = {
    "apiVersion": "runtime/v1alpha1",
    "kind": "SparkWorkload",
    "metadata": {"name": "wordcount", "version": "0.1.0"},
    "runtime": {"spark": "4.2"},
    "application": {
        "type": "python-wheel",
        "artifact": "artifact://wordcount/0.1.0",
        "entryPoint": "wordcount.jobs.count",
    },
    "arguments": [],
    "datasets": {
        "inputs": {"text": {"dataset": "wordcount.raw"}},
        "outputs": {"counts": {"dataset": "wordcount.counts"}},
    },
    "resources": {
        "driver": {"cores": 1, "memory": "2Gi"},
        "executor": {"cores": 1, "memory": "2Gi"},
        "scaling": {"minExecutors": 1, "maxExecutors": 2},
    },
    "execution": {"timeout": "30m", "retries": 0},
}


def _run(run_id: uuid.UUID, *, environment_name: str, state: str = "SUCCEEDED") -> Run:
    return Run(
        id=run_id,
        workload_name="wordcount",
        workload_version="0.1.0",
        environment_name=environment_name,
        state=state,
    )


@pytest.fixture
def two_run_ids() -> tuple[uuid.UUID, uuid.UUID]:
    return uuid.uuid4(), uuid.uuid4()


def _wire_happy_path(monkeypatch, run_id_a, run_id_b):
    runs = {
        run_id_a: _run(run_id_a, environment_name="env-a"),
        run_id_b: _run(run_id_b, environment_name="env-b"),
    }

    async def fake_get_run(session, run_id):
        return runs[run_id]

    async def fake_get_workload_definition(session, name, *, version=None, required=True):
        return WorkloadDefinition(name=name, version=version or "0.1.0", definition=WORKLOAD_DEFINITION)

    async def fake_get_dataset_binding(session, dataset_name, environment_name, *, required=True):
        return DatasetBinding(
            dataset_name=dataset_name,
            environment_name=environment_name,
            kind="path",
            uri=f"s3a://bucket/{environment_name}/counts",
        )

    async def fake_get_environment(session, name, *, required=True):
        return Environment(
            name=name,
            execution_provider="kubernetes",
            execution_profile_name="exec",
            storage_provider="s3",
            storage_profile_name=f"storage-{name}",
        )

    async def fake_get_storage_profile(session, name, *, required=True):
        return StorageProfile(
            name=name, provider="s3", config={"endpoint_url": "http://minio.local:9000"}, credential_reference={}
        )

    monkeypatch.setattr(repositories, "get_run", fake_get_run)
    monkeypatch.setattr(repositories, "get_workload_definition", fake_get_workload_definition)
    monkeypatch.setattr(repositories, "get_dataset_binding", fake_get_dataset_binding)
    monkeypatch.setattr(repositories, "get_environment", fake_get_environment)
    monkeypatch.setattr(repositories, "get_storage_profile", fake_get_storage_profile)
    monkeypatch.setattr(provider_factory, "build_storage_config", lambda storage_profile: {})


@pytest.mark.asyncio
async def test_compare_runs_reports_pass_when_matched(monkeypatch, two_run_ids):
    run_id_a, run_id_b = two_run_ids
    _wire_happy_path(monkeypatch, run_id_a, run_id_b)
    monkeypatch.setattr(
        conformance, "compare_semantic", lambda *a, **k: ComparisonResult(matched=True, mismatches=[])
    )

    report = await conformance.compare_runs(session=cast(AsyncSession, None), run_ids=[run_id_a, run_id_b])

    assert len(report.results) == 1
    result = report.results[0]
    assert result.status == conformance.STATUS_PASS
    assert result.left_environment == "env-a"
    assert result.right_environment == "env-b"
    assert result.mismatches == []


@pytest.mark.asyncio
async def test_compare_runs_reports_fail_when_mismatched(monkeypatch, two_run_ids):
    run_id_a, run_id_b = two_run_ids
    _wire_happy_path(monkeypatch, run_id_a, run_id_b)
    monkeypatch.setattr(
        conformance,
        "compare_semantic",
        lambda *a, **k: ComparisonResult(matched=False, mismatches=["row count mismatch"]),
    )

    report = await conformance.compare_runs(session=cast(AsyncSession, None), run_ids=[run_id_a, run_id_b])

    assert report.results[0].status == conformance.STATUS_FAIL
    assert report.results[0].mismatches == ["row count mismatch"]


@pytest.mark.asyncio
async def test_compare_runs_blocks_a_non_succeeded_run(monkeypatch, two_run_ids):
    run_id_a, run_id_b = two_run_ids
    _wire_happy_path(monkeypatch, run_id_a, run_id_b)
    runs = {
        run_id_a: _run(run_id_a, environment_name="env-a", state="RUNNING"),
        run_id_b: _run(run_id_b, environment_name="env-b"),
    }

    async def fake_get_run(session, run_id):
        return runs[run_id]

    monkeypatch.setattr(repositories, "get_run", fake_get_run)

    report = await conformance.compare_runs(session=cast(AsyncSession, None), run_ids=[run_id_a, run_id_b])

    assert report.results[0].status == conformance.STATUS_BLOCKED
    assert "not SUCCEEDED" in report.results[0].mismatches[0]


@pytest.mark.asyncio
async def test_compare_runs_blocks_when_no_dataset_binding(monkeypatch, two_run_ids):
    run_id_a, run_id_b = two_run_ids
    _wire_happy_path(monkeypatch, run_id_a, run_id_b)

    async def fake_get_dataset_binding(session, dataset_name, environment_name, *, required=True):
        return None

    monkeypatch.setattr(repositories, "get_dataset_binding", fake_get_dataset_binding)

    report = await conformance.compare_runs(session=cast(AsyncSession, None), run_ids=[run_id_a, run_id_b])

    assert report.results[0].status == conformance.STATUS_BLOCKED
    assert "no dataset binding" in report.results[0].mismatches[0]


@pytest.mark.asyncio
async def test_compare_runs_uses_named_output_when_given(monkeypatch, two_run_ids):
    run_id_a, run_id_b = two_run_ids
    _wire_happy_path(monkeypatch, run_id_a, run_id_b)
    requested_datasets = []

    async def fake_get_dataset_binding(session, dataset_name, environment_name, *, required=True):
        requested_datasets.append(dataset_name)
        return DatasetBinding(
            dataset_name=dataset_name, environment_name=environment_name, kind="path", uri="s3a://bucket/x"
        )

    monkeypatch.setattr(repositories, "get_dataset_binding", fake_get_dataset_binding)
    monkeypatch.setattr(
        conformance, "compare_semantic", lambda *a, **k: ComparisonResult(matched=True, mismatches=[])
    )

    await conformance.compare_runs(session=cast(AsyncSession, None), run_ids=[run_id_a, run_id_b], output_name="counts")

    assert requested_datasets == ["wordcount.counts", "wordcount.counts"]


@pytest.mark.asyncio
async def test_compare_runs_blocks_when_named_output_not_found(monkeypatch, two_run_ids):
    run_id_a, run_id_b = two_run_ids
    _wire_happy_path(monkeypatch, run_id_a, run_id_b)

    report = await conformance.compare_runs(
        session=cast(AsyncSession, None), run_ids=[run_id_a, run_id_b], output_name="not-a-real-output"
    )

    assert report.results[0].status == conformance.STATUS_BLOCKED
    assert "no output named" in report.results[0].mismatches[0]
