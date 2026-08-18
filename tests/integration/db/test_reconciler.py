"""Real-Postgres test that reconcile_once() actually advances a run through
its full lifecycle. Uses a fake ExecutionProvider (not real Kubernetes) so
this stays CI-fast and infra-free — the live version of this same flow was
verified by hand against the real remote cluster (see docs/providers/
kubernetes.md and the plan for this slice)."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import update

from control_plane import metrics, provider_factory, repositories, run_service
from control_plane.execution_provider import (
    CapabilitySet,
    LogReference,
    ProviderRun,
    ProviderStatus,
    RetryableProviderError,
    ValidationResult,
)
from control_plane.models import Run
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
    # examples/wordcount.yaml's application.artifact is the fixed literal
    # "artifact://wordcount/0.1.0" regardless of what unique workload NAME
    # workload_ref registers it under — submit_new_runs() now resolves
    # this through ArtifactBinding, so tests expecting a successful
    # submission need one seeded, same as the dataset bindings above.
    await repositories.create_artifact_binding(
        session,
        artifact_name="wordcount",
        artifact_version="0.1.0",
        environment_name=environment_name,
        kind="path",
        uri="s3a://bucket/artifacts/wordcount-0.1.0.whl",
    )


def _metric_sample_count(name: str) -> float:
    """Parses the `_count` line the Prometheus histogram exposition format
    produces for `name`, e.g. `portage_run_submission_latency_seconds_count
    {...} 3.0` -> 3.0. Used to assert a metric moved by a known delta rather
    than asserting an absolute value, since the OTel registry accumulates
    across every test in the same process."""
    text = metrics.render_prometheus_text().decode()
    for line in text.splitlines():
        if line.startswith(f"{name}_count{{"):
            return float(line.rsplit(" ", 1)[1])
    return 0.0


def _counter_value(name: str) -> float:
    text = metrics.render_prometheus_text().decode()
    for line in text.splitlines():
        if line.startswith((f"{name}{{", f"{name} ")):
            return float(line.rsplit(" ", 1)[1])
    return 0.0


@pytest.mark.asyncio
async def test_reconcile_once_advances_run_to_succeeded(
    session, environment_name, workload_ref, monkeypatch
):
    fake_provider = FakeExecutionProvider([RunState.QUEUED, RunState.RUNNING, RunState.SUCCEEDED])
    monkeypatch.setattr(
        provider_factory, "build_execution_provider", lambda execution_profile: fake_provider
    )
    monkeypatch.setattr(provider_factory, "build_storage_config", lambda storage_profile: {})
    monkeypatch.setattr(provider_factory, "build_storage_volume_mounts", lambda storage_profile: None)

    await _seed_dataset_bindings(session, environment_name)
    workload_name, workload_version = workload_ref
    run, _created = await run_service.create_run(
        session,
        workload_name=workload_name,
        workload_version=workload_version,
        environment_name=environment_name,
    )

    submission_before = _metric_sample_count("portage_run_submission_latency_seconds")
    queue_before = _metric_sample_count("portage_run_queue_latency_seconds")
    execution_before = _metric_sample_count("portage_run_execution_duration_seconds")

    await reconciler_service.reconcile_once(session)
    run = await run_service.get_run(session, run.id)
    assert run.state == RunState.QUEUED.value
    assert _metric_sample_count("portage_run_submission_latency_seconds") == submission_before + 1

    await reconciler_service.reconcile_once(session)
    run = await run_service.get_run(session, run.id)
    assert run.state == RunState.RUNNING.value
    assert _metric_sample_count("portage_run_queue_latency_seconds") == queue_before + 1

    await reconciler_service.reconcile_once(session)
    run = await run_service.get_run(session, run.id)
    assert run.state == RunState.SUCCEEDED.value
    assert _metric_sample_count("portage_run_execution_duration_seconds") == execution_before + 1

    events = await run_service.list_run_events(session, run.id)
    assert [e.to_state for e in events] == [
        RunState.ACCEPTED.value,
        RunState.SUBMITTING.value,  # claimed for submission (spec §26/§57/§67)
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
    monkeypatch.setattr(provider_factory, "build_storage_volume_mounts", lambda storage_profile: None)

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
async def test_cancel_runs_calls_provider_cancel_and_finalizes(
    session, environment_name, workload_ref, monkeypatch
):
    fake_provider = FakeExecutionProvider([RunState.RUNNING])
    cancelled_ids = []

    async def _cancel(provider_run_id):
        cancelled_ids.append(provider_run_id)

    fake_provider.cancel = _cancel
    monkeypatch.setattr(
        provider_factory, "build_execution_provider", lambda execution_profile: fake_provider
    )
    monkeypatch.setattr(provider_factory, "build_storage_config", lambda storage_profile: {})
    monkeypatch.setattr(provider_factory, "build_storage_volume_mounts", lambda storage_profile: None)

    await _seed_dataset_bindings(session, environment_name)
    workload_name, workload_version = workload_ref
    run, _created = await run_service.create_run(
        session,
        workload_name=workload_name,
        workload_version=workload_version,
        environment_name=environment_name,
    )

    # Get the run submitted and into an active state first.
    await reconciler_service.reconcile_once(session)
    run = await run_service.get_run(session, run.id)
    assert run.state == RunState.RUNNING.value

    result, pending = await run_service.cancel_run(session, run.id)
    assert pending is True
    assert result.state == RunState.CANCELING.value

    await reconciler_service.cancel_runs(session)

    run = await run_service.get_run(session, run.id)
    assert run.state == RunState.CANCELED.value
    assert cancelled_ids == ["fake-provider-run-1"]

    events = await run_service.list_run_events(session, run.id)
    assert [e.to_state for e in events] == [
        RunState.ACCEPTED.value,
        RunState.SUBMITTING.value,  # claimed for submission (spec §26/§57/§67)
        RunState.QUEUED.value,
        RunState.RUNNING.value,
        RunState.CANCELING.value,
        RunState.CANCELED.value,
    ]


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
    monkeypatch.setattr(provider_factory, "build_storage_volume_mounts", lambda storage_profile: None)

    workload_name, workload_version = workload_ref
    run, _created = await run_service.create_run(
        session,
        workload_name=workload_name,
        workload_version=workload_version,
        environment_name=environment_name,
    )

    errors_before = _counter_value("portage_provider_errors_total")

    await reconciler_service.reconcile_once(session)

    run = await run_service.get_run(session, run.id)
    assert run.state == RunState.FAILED.value
    assert _counter_value("portage_provider_errors_total") == errors_before + 1


@pytest.mark.asyncio
async def test_stuck_submitting_run_is_recovered_not_reblown(
    session, environment_name, workload_ref, monkeypatch
):
    """Simulates the spec §57 Control Plane Recovery Test crash window: a
    run left in SUBMITTING with no ProviderRun row (the reconciler died
    between provider.submit() and persisting the result) must be picked
    back up and successfully submitted, not left stuck or duplicated."""
    fake_provider = FakeExecutionProvider([RunState.QUEUED])
    monkeypatch.setattr(
        provider_factory, "build_execution_provider", lambda execution_profile: fake_provider
    )
    monkeypatch.setattr(provider_factory, "build_storage_config", lambda storage_profile: {})
    monkeypatch.setattr(provider_factory, "build_storage_volume_mounts", lambda storage_profile: None)

    await _seed_dataset_bindings(session, environment_name)
    workload_name, workload_version = workload_ref
    run, _created = await run_service.create_run(
        session,
        workload_name=workload_name,
        workload_version=workload_version,
        environment_name=environment_name,
    )

    # Force the run into the crash state directly: SUBMITTING, no
    # ProviderRun row, updated_at backdated past the stuck-recovery grace
    # period — a raw Core UPDATE (not the ORM attribute + commit path) so
    # the explicit updated_at value isn't clobbered by the column's own
    # onupdate=func.now() default.
    stale = datetime.now(UTC) - timedelta(seconds=60)
    await session.execute(
        update(Run).where(Run.id == run.id).values(state=RunState.SUBMITTING.value, updated_at=stale)
    )
    await session.commit()
    assert await repositories.get_latest_provider_run(session, run.id) is None

    await reconciler_service.reconcile_once(session)

    run = await run_service.get_run(session, run.id)
    assert run.state == RunState.QUEUED.value
    provider_run = await repositories.get_latest_provider_run(session, run.id)
    assert provider_run is not None
    assert provider_run.provider_run_id == "fake-provider-run-1"


@pytest.mark.asyncio
async def test_retryable_submission_error_requeues_run_to_accepted(
    session, environment_name, workload_ref, monkeypatch
):
    class FlakyThenOkProvider(FakeExecutionProvider):
        def __init__(self):
            super().__init__([RunState.QUEUED])
            self.submit_calls = 0

        async def submit(self, run):
            self.submit_calls += 1
            if self.submit_calls == 1:
                raise RetryableProviderError("transient 503")
            return await super().submit(run)

    flaky_provider = FlakyThenOkProvider()
    monkeypatch.setattr(
        provider_factory, "build_execution_provider", lambda execution_profile: flaky_provider
    )
    monkeypatch.setattr(provider_factory, "build_storage_config", lambda storage_profile: {})
    monkeypatch.setattr(provider_factory, "build_storage_volume_mounts", lambda storage_profile: None)

    await _seed_dataset_bindings(session, environment_name)
    workload_name, workload_version = workload_ref
    run, _created = await run_service.create_run(
        session,
        workload_name=workload_name,
        workload_version=workload_version,
        environment_name=environment_name,
    )

    # First tick: submit() raises RetryableProviderError -> back to ACCEPTED.
    await reconciler_service.reconcile_once(session)
    run = await run_service.get_run(session, run.id)
    assert run.state == RunState.ACCEPTED.value
    assert run.submission_attempts == 1

    # Second tick: submit() succeeds -> QUEUED, same as the happy path.
    await reconciler_service.reconcile_once(session)
    run = await run_service.get_run(session, run.id)
    assert run.state == RunState.QUEUED.value
    assert flaky_provider.submit_calls == 2


@pytest.mark.asyncio
async def test_retryable_submission_error_fails_after_max_attempts(
    session, environment_name, workload_ref, monkeypatch
):
    class AlwaysFlakyProvider(FakeExecutionProvider):
        def __init__(self):
            super().__init__([RunState.QUEUED])

        async def submit(self, run):
            raise RetryableProviderError("permanently flaky 503")

    monkeypatch.setattr(
        provider_factory, "build_execution_provider", lambda execution_profile: AlwaysFlakyProvider()
    )
    monkeypatch.setattr(provider_factory, "build_storage_config", lambda storage_profile: {})
    monkeypatch.setattr(provider_factory, "build_storage_volume_mounts", lambda storage_profile: None)

    await _seed_dataset_bindings(session, environment_name)
    workload_name, workload_version = workload_ref
    run, _created = await run_service.create_run(
        session,
        workload_name=workload_name,
        workload_version=workload_version,
        environment_name=environment_name,
    )

    for _ in range(reconciler_service.MAX_SUBMISSION_ATTEMPTS - 1):
        await reconciler_service.reconcile_once(session)
        run = await run_service.get_run(session, run.id)
        assert run.state == RunState.ACCEPTED.value

    # The MAX_SUBMISSION_ATTEMPTS-th failure crosses the threshold -> FAILED.
    await reconciler_service.reconcile_once(session)
    run = await run_service.get_run(session, run.id)
    assert run.state == RunState.FAILED.value
    assert run.submission_attempts == reconciler_service.MAX_SUBMISSION_ATTEMPTS
    last_event = (await run_service.list_run_events(session, run.id))[-1]
    assert last_event.message is not None
    assert "exceeded max submission attempts" in last_event.message
