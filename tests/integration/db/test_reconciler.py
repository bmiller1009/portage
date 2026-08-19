"""Real-Postgres test that reconcile_once() actually advances a run through
its full lifecycle. Uses a fake ExecutionProvider (not real Kubernetes) so
this stays CI-fast and infra-free — the live version of this same flow was
verified by hand against the real remote cluster (see docs/providers/
kubernetes.md and the plan for this slice)."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import update

from control_plane import metrics, provider_factory, repositories, run_service
from control_plane.execution_provider import (
    CapabilitySet,
    LogReference,
    ProviderRun,
    ProviderStatus,
    RetryableProviderError,
    TerminalProviderError,
    ValidationResult,
)
from control_plane.models import Run
from control_plane.run_state import RunState
from reconciler import service as reconciler_service
from spec.workload.v1alpha1 import SparkWorkload


class FakeExecutionProvider:
    """list_runs_by_state() (used by both submit_new_runs() and
    poll_active_runs()) is a GLOBAL query against this long-lived, shared
    Postgres, and provider_factory.build_execution_provider() gets
    monkeypatched to return THIS instance regardless of which run's
    execution_profile is actually asking — so an unrelated leftover
    active run from a different test (past or concurrent) can genuinely
    get routed through this same fake within a single poll_active_runs()
    sweep. A unique, per-instance provider_run_id (not a hardcoded
    literal every test's FakeExecutionProvider used to share) plus
    ignoring status()/cancel() calls for any other id keeps this
    instance's own stateful behavior (_statuses popping) from being
    corrupted by — or corrupting — a run it doesn't actually own."""

    def __init__(self, statuses: list[RunState]):
        self._statuses = list(statuses)
        self.provider_run_id = f"fake-provider-run-{uuid4().hex[:8]}"

    async def validate(self, workload):
        return ValidationResult(valid=True)

    async def submit(self, run):
        return ProviderRun(provider_run_id=self.provider_run_id, raw={"ok": True})

    async def status(self, provider_run_id):
        if provider_run_id != self.provider_run_id:
            # Not mine — a safe no-op (retryable, no state transition)
            # rather than guessing at a state that could incorrectly
            # advance or fail a run this fake doesn't actually own.
            raise RetryableProviderError(f"unrecognized provider_run_id: {provider_run_id}")
        state = self._statuses.pop(0) if len(self._statuses) > 1 else self._statuses[0]
        return ProviderStatus(state=state, provider_native_status=state.value)

    async def cancel(self, provider_run_id):
        if provider_run_id != self.provider_run_id:
            raise RetryableProviderError(f"unrecognized provider_run_id: {provider_run_id}")

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
    assert provider_run.provider_run_id == fake_provider.provider_run_id


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
    events = await run_service.list_run_events(session, run.id)
    failed_event = next(e for e in events if e.to_state == RunState.FAILED.value)
    assert failed_event.category == "VALIDATION"
    assert failed_event.disposition == "user_action_required"

    failure = await run_service.get_run_failure(session, run)
    assert failure is not None
    assert failure.category == "VALIDATION"
    assert failure.retryable is False
    assert "unsupported artifact type" in failure.summary


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
    assert cancelled_ids == [fake_provider.provider_run_id]

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
    # >= rather than == : this counter is process-global, and a stray
    # active run from elsewhere in this long-lived shared Postgres can
    # legitimately add its own (safe, no-op) retryable-error increment if
    # it gets swept into this same poll_active_runs() call — see
    # FakeExecutionProvider's docstring above. The one guarantee this
    # test actually cares about is that ITS OWN error was counted at all.
    assert _counter_value("portage_provider_errors_total") >= errors_before + 1

    events = await run_service.list_run_events(session, run.id)
    failed_event = next(e for e in events if e.to_state == RunState.FAILED.value)
    assert failed_event.category == "STORAGE_RESOLUTION"
    assert failed_event.disposition == "user_action_required"


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
    assert provider_run.provider_run_id == fake_provider.provider_run_id


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


@pytest.mark.asyncio
async def test_poll_survives_a_transient_status_error(session, environment_name, workload_ref, monkeypatch):
    """Spec §56's "network interruption after submission": a transient
    status() blip must not fail a run that's actually fine -- it should
    just be retried on the next tick.

    list_runs_by_state() is a GLOBAL query (not scoped to this test's own
    run) against this long-lived, shared Postgres — a different, unrelated
    leftover active run from another test could be polled through this
    same fake provider in the same tick. This fake is deliberately keyed
    by provider_run_id (not a shared call counter or a shared _statuses
    list) so that kind of cross-test interference can't change its
    behavior for THIS test's own run — see test_artifacts.py's
    FakeExecutionProvider for the same hazard documented independently."""

    class FlakyThenOkStatusProvider:
        def __init__(self, *, target_provider_run_id: str):
            self._target = target_provider_run_id
            self._calls_for_target = 0

        async def validate(self, workload):
            return ValidationResult(valid=True)

        async def submit(self, run):
            return ProviderRun(provider_run_id=self._target, raw={"ok": True})

        async def status(self, provider_run_id):
            if provider_run_id != self._target:
                return ProviderStatus(state=RunState.RUNNING, provider_native_status="RUNNING")
            self._calls_for_target += 1
            if self._calls_for_target == 1:
                raise RetryableProviderError("transient 503")
            return ProviderStatus(state=RunState.RUNNING, provider_native_status="RUNNING")

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

    flaky_provider = FlakyThenOkStatusProvider(target_provider_run_id=f"fake-provider-run-{uuid4().hex[:8]}")
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

    # First tick: submits -> QUEUED, then the same-tick poll_active_runs()
    # call already reaches this run and hits its first (raising) status()
    # call -> stays QUEUED, NOT FAILED.
    await reconciler_service.reconcile_once(session)
    run = await run_service.get_run(session, run.id)
    assert run.state == RunState.QUEUED.value

    # Second tick: status() succeeds -> RUNNING, same as the happy path.
    await reconciler_service.reconcile_once(session)
    run = await run_service.get_run(session, run.id)
    assert run.state == RunState.RUNNING.value


@pytest.mark.asyncio
async def test_poll_fails_run_with_classified_message_on_terminal_provider_error(
    session, environment_name, workload_ref, monkeypatch
):
    """poll_active_runs() must catch TerminalProviderError explicitly, not
    just let it fall through to the generic "unclassified error" handler
    — the provider deliberately classified this one (e.g. a real 401/403
    from the platform), so the run's failure message should say so, the
    same distinction submit_new_runs() already makes. Confirmed live as a
    real gap (tests/chaos/test_provider_outage_recovery.py) before this
    except clause existed."""

    class TerminalOnStatusProvider:
        def __init__(self, *, target_provider_run_id: str):
            self._target = target_provider_run_id

        async def validate(self, workload):
            return ValidationResult(valid=True)

        async def submit(self, run):
            return ProviderRun(provider_run_id=self._target, raw={"ok": True})

        async def status(self, provider_run_id):
            if provider_run_id != self._target:
                return ProviderStatus(state=RunState.RUNNING, provider_native_status="RUNNING")
            raise TerminalProviderError("401 Unauthorized")

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

    terminal_provider = TerminalOnStatusProvider(target_provider_run_id=f"fake-provider-run-{uuid4().hex[:8]}")
    monkeypatch.setattr(
        provider_factory, "build_execution_provider", lambda execution_profile: terminal_provider
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

    # First tick: submits -> QUEUED. Second tick: poll_active_runs() hits
    # the classified TerminalProviderError -> FAILED, with the provider's
    # own message, not "unclassified error: ...".
    await reconciler_service.reconcile_once(session)
    await reconciler_service.reconcile_once(session)
    run = await run_service.get_run(session, run.id)
    assert run.state == RunState.FAILED.value
    events = await repositories.list_run_events(session, run.id)
    failure_event = next(e for e in events if e.to_state == RunState.FAILED.value)
    assert failure_event.message == "401 Unauthorized"
    assert failure_event.category == "WORKLOAD_EXECUTION"
    assert failure_event.disposition == "terminal"


@pytest.mark.asyncio
async def test_cancel_survives_a_transient_cancel_error(session, environment_name, workload_ref, monkeypatch):
    """Same cross-test-pollution caution as test_poll_survives_a_transient_
    status_error above — keyed by provider_run_id, not a shared counter."""

    class FlakyThenOkCancelProvider:
        def __init__(self, *, target_provider_run_id: str):
            self._target = target_provider_run_id
            self._cancel_calls_for_target = 0

        async def validate(self, workload):
            return ValidationResult(valid=True)

        async def submit(self, run):
            return ProviderRun(provider_run_id=self._target, raw={"ok": True})

        async def status(self, provider_run_id):
            return ProviderStatus(state=RunState.RUNNING, provider_native_status="RUNNING")

        async def cancel(self, provider_run_id):
            if provider_run_id != self._target:
                return
            self._cancel_calls_for_target += 1
            if self._cancel_calls_for_target == 1:
                raise RetryableProviderError("transient 429")

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

    flaky_provider = FlakyThenOkCancelProvider(target_provider_run_id=f"fake-provider-run-{uuid4().hex[:8]}")
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

    await reconciler_service.reconcile_once(session)
    run = await run_service.get_run(session, run.id)
    assert run.state == RunState.RUNNING.value

    _result, pending = await run_service.cancel_run(session, run.id)
    assert pending is True

    # First cancel_runs() tick: cancel() raises RetryableProviderError ->
    # stays CANCELING, NOT FAILED.
    await reconciler_service.cancel_runs(session)
    run = await run_service.get_run(session, run.id)
    assert run.state == RunState.CANCELING.value

    # Second tick: cancel() succeeds -> CANCELED.
    await reconciler_service.cancel_runs(session)
    run = await run_service.get_run(session, run.id)
    assert run.state == RunState.CANCELED.value


@pytest.mark.asyncio
async def test_resolve_dataset_config_reads_table_binding_from_real_db(session, workload_ref):
    """A real end-to-end read of a persisted kind="table" DatasetBinding
    row (uri column repurposed to hold the table identifier, spec §11) —
    confirms reconciler_service._resolve_dataset_config's new kind branch
    against real Postgres, not just the pure spec-layer unit tests."""
    workload_name, workload_version = workload_ref
    workload_row = await repositories.get_workload_definition(
        session, workload_name, version=workload_version
    )
    workload = SparkWorkload.model_validate(workload_row.definition)

    exec_profile = await repositories.create_execution_profile(
        session,
        name=f"exec-{uuid4().hex[:8]}",
        provider="kubernetes",
        config={"namespace": "default", "service_account": "spark", "image": "portage/wordcount:0.1.0"},
    )
    storage_profile = await repositories.create_storage_profile(
        session,
        name=f"storage-{uuid4().hex[:8]}",
        provider="s3",
        config={
            "endpoint_url": "http://minio.local:9000",
            "iceberg_catalog_uri": "http://iceberg-rest:8181",
            "iceberg_warehouse": "s3://portage-local/iceberg-warehouse",
        },
        credential_reference={"provider": "env", "reference": "PORTAGE_TEST"},
    )
    environment_name = f"env-{uuid4().hex[:8]}"
    await repositories.create_environment(
        session,
        name=environment_name,
        execution_provider="kubernetes",
        execution_profile_name=exec_profile.name,
        storage_provider="s3",
        storage_profile_name=storage_profile.name,
    )

    await repositories.create_dataset_binding(
        session,
        dataset_name="wordcount.raw",
        environment_name=environment_name,
        kind="table",
        uri="analytics.wordcount.raw",
    )
    await repositories.create_dataset_binding(
        session,
        dataset_name="wordcount.counts",
        environment_name=environment_name,
        kind="path",
        uri="s3a://bucket/wordcount.counts",
    )

    storage_config = {
        "spark.hadoop.fs.s3a.access.key": "test-access-key",
        "spark.hadoop.fs.s3a.secret.key": "test-secret-key",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.endpoint": "http://minio.local:9000",
    }
    config = await reconciler_service._resolve_dataset_config(
        session, workload, environment_name, storage_profile, storage_config
    )

    assert config == {
        "spark.portable.dataset.wordcount.raw.identifier": "portage_iceberg.analytics.wordcount.raw",
        "spark.portable.dataset.wordcount.counts.uri": "s3a://bucket/wordcount.counts",
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.portage_iceberg": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.portage_iceberg.type": "rest",
        "spark.sql.catalog.portage_iceberg.uri": "http://iceberg-rest:8181",
        "spark.sql.catalog.portage_iceberg.warehouse": "s3://portage-local/iceberg-warehouse",
        "spark.sql.catalog.portage_iceberg.client.region": "us-east-1",
        "spark.sql.catalog.portage_iceberg.s3.endpoint": "http://minio.local:9000",
        "spark.sql.catalog.portage_iceberg.s3.access-key-id": "test-access-key",
        "spark.sql.catalog.portage_iceberg.s3.secret-access-key": "test-secret-key",
        "spark.sql.catalog.portage_iceberg.s3.path-style-access": "true",
    }
