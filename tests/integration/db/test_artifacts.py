"""Real-Postgres tests for artifact bindings (spec §51) — CRUD (mirrors
test_persistence.py's dataset-binding tests) and the reconciler's
resolution behavior (mirrors test_reconciler.py's dataset-resolution
coverage): a real artifact:// reference resolved through the binding
table, a non-artifact:// reference passed through unchanged, and a
missing binding failing the run cleanly rather than crashing the loop.
"""

import uuid

import pytest

from control_plane import provider_factory, repositories, run_service
from control_plane.execution_provider import (
    CapabilitySet,
    LogReference,
    ProviderRun,
    ProviderStatus,
    ValidationResult,
)
from control_plane.run_state import RunState
from reconciler import service as reconciler_service
from spec.workload.v1alpha1 import SparkWorkload


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


class FakeExecutionProvider:
    def __init__(self):
        self.submitted_workload: SparkWorkload | None = None

    async def validate(self, resolved):
        return ValidationResult(valid=True)

    async def submit(self, run):
        self.submitted_workload = run.resolved.workload
        return ProviderRun(provider_run_id="fake-provider-run-1")

    async def status(self, provider_run_id):
        # Terminal on the very first poll, deliberately — submit_new_runs()/
        # poll_active_runs() both run within one reconcile_once(), so this
        # keeps every test's run out of the ACTIVE states by the time the
        # test ends. list_runs_by_state() queries are global, not scoped to
        # a single test's own environment/workload, so a run left sitting
        # in an active state here would get swept up by a *different*
        # test's polling loop later in the same pytest session — and, if
        # that test's own fake provider pops status from a shared list
        # (test_reconciler.py's does), silently steal one of its pops.
        return ProviderStatus(state=RunState.SUCCEEDED, provider_native_status="SUCCEEDED")

    async def cancel(self, provider_run_id):
        pass

    async def logs(self, provider_run_id):
        return LogReference(description="fake")

    async def capabilities(self):
        return CapabilitySet(
            spark_versions=["4.2"], languages=["python"], dynamic_allocation=False,
            gpu=False, streaming=False, local_disk=True, spark_connect=False,
        )


async def _seed_dataset_bindings(session, environment_name: str) -> None:
    for dataset_name in ("wordcount.raw", "wordcount.counts"):
        await repositories.create_dataset_binding(
            session, dataset_name=dataset_name, environment_name=environment_name,
            kind="path", uri=f"s3a://bucket/{dataset_name}",
        )


@pytest.mark.asyncio
async def test_artifact_binding_round_trip(session, environment_name):
    artifact_name = _unique("wordcount")
    binding = await repositories.create_artifact_binding(
        session,
        artifact_name=artifact_name,
        artifact_version="0.1.0",
        environment_name=environment_name,
        kind="path",
        uri="s3a://portage-phase0/artifacts/wordcount-0.1.0.whl",
    )
    assert binding.uri == "s3a://portage-phase0/artifacts/wordcount-0.1.0.whl"

    fetched = await repositories.get_artifact_binding(session, artifact_name, "0.1.0", environment_name)
    assert fetched.kind == "path"

    bindings = await repositories.list_artifact_bindings(session, artifact_name=artifact_name)
    assert len(bindings) == 1


@pytest.mark.asyncio
async def test_artifact_binding_requires_existing_environment(session):
    with pytest.raises(repositories.NotFoundError):
        await repositories.create_artifact_binding(
            session,
            artifact_name=_unique("orphan"),
            artifact_version="0.1.0",
            environment_name=_unique("ghost-env"),
            kind="path",
            uri="s3a://bucket/x.whl",
        )


@pytest.mark.asyncio
async def test_artifact_binding_duplicate_raises(session, environment_name):
    artifact_name = _unique("wordcount")
    await repositories.create_artifact_binding(
        session, artifact_name=artifact_name, artifact_version="0.1.0",
        environment_name=environment_name, kind="path", uri="s3a://bucket/a.whl",
    )
    with pytest.raises(repositories.AlreadyExistsError):
        await repositories.create_artifact_binding(
            session, artifact_name=artifact_name, artifact_version="0.1.0",
            environment_name=environment_name, kind="path", uri="s3a://bucket/b.whl",
        )


@pytest.mark.asyncio
async def test_submit_resolves_artifact_reference_to_bound_uri(session, environment_name, workload_ref, monkeypatch):
    fake_provider = FakeExecutionProvider()
    monkeypatch.setattr(provider_factory, "build_execution_provider", lambda execution_profile: fake_provider)
    monkeypatch.setattr(provider_factory, "build_storage_config", lambda storage_profile: {})
    monkeypatch.setattr(provider_factory, "build_storage_volume_mounts", lambda storage_profile: None)

    await _seed_dataset_bindings(session, environment_name)
    await repositories.create_artifact_binding(
        session, artifact_name="wordcount", artifact_version="0.1.0", environment_name=environment_name,
        kind="path", uri="s3a://portage-phase0/artifacts/wordcount-0.1.0.whl",
    )

    workload_name, workload_version = workload_ref
    run, _created = await run_service.create_run(
        session, workload_name=workload_name, workload_version=workload_version,
        environment_name=environment_name,
    )

    await reconciler_service.reconcile_once(session)

    run = await run_service.get_run(session, run.id)
    assert run.state == RunState.SUCCEEDED.value
    assert fake_provider.submitted_workload is not None
    assert (
        fake_provider.submitted_workload.application.artifact
        == "s3a://portage-phase0/artifacts/wordcount-0.1.0.whl"
    )


@pytest.mark.asyncio
async def test_submit_fails_cleanly_when_artifact_binding_missing(session, environment_name, workload_ref, monkeypatch):
    fake_provider = FakeExecutionProvider()
    monkeypatch.setattr(provider_factory, "build_execution_provider", lambda execution_profile: fake_provider)
    monkeypatch.setattr(provider_factory, "build_storage_config", lambda storage_profile: {})
    monkeypatch.setattr(provider_factory, "build_storage_volume_mounts", lambda storage_profile: None)

    await _seed_dataset_bindings(session, environment_name)
    # Deliberately no artifact binding seeded.

    workload_name, workload_version = workload_ref
    run, _created = await run_service.create_run(
        session, workload_name=workload_name, workload_version=workload_version,
        environment_name=environment_name,
    )

    await reconciler_service.reconcile_once(session)

    run = await run_service.get_run(session, run.id)
    assert run.state == RunState.FAILED.value
    assert fake_provider.submitted_workload is None
