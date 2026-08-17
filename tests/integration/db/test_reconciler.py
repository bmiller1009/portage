"""Real-Postgres test that reconcile_once() actually advances a run through
its full lifecycle. Uses a fake ExecutionProvider (not real Kubernetes) so
this stays CI-fast and infra-free — the live version of this same flow was
verified by hand against the real remote cluster (see docs/providers/
kubernetes.md and the plan for this slice)."""

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


class FakeExecutionProvider:
    def __init__(self, statuses: list[RunState]):
        self._statuses = list(statuses)

    async def validate(self, workload):
        return ValidationResult(valid=True)

    async def submit(self, run):
        return ProviderRun(provider_run_id="fake-provider-run-1", raw={"ok": True})

    async def status(self, provider_run_id):
        state = self._statuses.pop(0) if len(self._statuses) > 1 else self._statuses[0]
        return ProviderStatus(state=state, provider_native_status=state.value)

    async def cancel(self, provider_run_id):
        pass

    async def logs(self, provider_run_id):
        return LogReference(description="fake")

    async def capabilities(self):
        return CapabilitySet(
            spark_versions=["4.2"],
            languages=["python"],
            dynamic_allocation=False,
            gpu=False,
            streaming=False,
            local_disk=True,
            spark_connect=False,
        )


async def _seed_dataset_bindings(session, environment_name: str) -> None:
    for dataset_name in ("wordcount.raw", "wordcount.counts"):
        await repositories.create_dataset_binding(
            session,
            dataset_name=dataset_name,
            environment_name=environment_name,
            kind="path",
            uri=f"s3a://bucket/{dataset_name}",
        )


@pytest.mark.asyncio
async def test_reconcile_once_advances_run_to_succeeded(
    session, environment_name, workload_ref, monkeypatch
):
    fake_provider = FakeExecutionProvider([RunState.QUEUED, RunState.RUNNING, RunState.SUCCEEDED])
    monkeypatch.setattr(
        provider_factory, "build_execution_provider", lambda execution_profile: fake_provider
    )
    monkeypatch.setattr(provider_factory, "build_storage_config", lambda storage_profile: {})

    await _seed_dataset_bindings(session, environment_name)
    workload_name, workload_version = workload_ref
    run, _created = await run_service.create_run(
        session,
        workload_name=workload_name,
        workload_version=workload_version,
        environment_name=environment_name,
    )

    await reconciler_service.reconcile_once(session)
    run = await run_service.get_run(session, run.id)
    assert run.state == RunState.QUEUED.value

    await reconciler_service.reconcile_once(session)
    run = await run_service.get_run(session, run.id)
    assert run.state == RunState.RUNNING.value

    await reconciler_service.reconcile_once(session)
    run = await run_service.get_run(session, run.id)
    assert run.state == RunState.SUCCEEDED.value

    events = await run_service.list_run_events(session, run.id)
    assert [e.to_state for e in events] == [
        RunState.ACCEPTED.value,
        RunState.QUEUED.value,
        RunState.RUNNING.value,
        RunState.SUCCEEDED.value,
    ]

    provider_run = await repositories.get_latest_provider_run(session, run.id)
    assert provider_run is not None
    assert provider_run.provider_run_id == "fake-provider-run-1"


@pytest.mark.asyncio
async def test_reconcile_once_transitions_to_failed_on_validation_rejection(
    session, environment_name, workload_ref, monkeypatch
):
    class RejectingProvider(FakeExecutionProvider):
        async def validate(self, workload):
            return ValidationResult(valid=False, errors=["unsupported artifact type"])

    monkeypatch.setattr(
        provider_factory,
        "build_execution_provider",
        lambda execution_profile: RejectingProvider([RunState.QUEUED]),
    )
    monkeypatch.setattr(provider_factory, "build_storage_config", lambda storage_profile: {})

    await _seed_dataset_bindings(session, environment_name)
    workload_name, workload_version = workload_ref
    run, _created = await run_service.create_run(
        session,
        workload_name=workload_name,
        workload_version=workload_version,
        environment_name=environment_name,
    )

    await reconciler_service.reconcile_once(session)

    run = await run_service.get_run(session, run.id)
    assert run.state == RunState.FAILED.value


@pytest.mark.asyncio
async def test_reconcile_once_transitions_to_failed_on_missing_dataset_binding(
    session, environment_name, workload_ref, monkeypatch
):
    """No _seed_dataset_bindings call — dataset resolution should fail
    cleanly (FAILED transition), not crash the reconciler loop."""
    monkeypatch.setattr(
        provider_factory,
        "build_execution_provider",
        lambda execution_profile: FakeExecutionProvider([RunState.QUEUED]),
    )
    monkeypatch.setattr(provider_factory, "build_storage_config", lambda storage_profile: {})

    workload_name, workload_version = workload_ref
    run, _created = await run_service.create_run(
        session,
        workload_name=workload_name,
        workload_version=workload_version,
        environment_name=environment_name,
    )

    await reconciler_service.reconcile_once(session)

    run = await run_service.get_run(session, run.id)
    assert run.state == RunState.FAILED.value
