"""The async submit/poll/converge worker (docs/architecture/spec.md §24).
Two testable stages, called in sequence by reconcile_once(): submit_new_runs
picks up ACCEPTED runs and submits them to a real execution provider;
poll_active_runs advances already-submitted runs toward a terminal state.

Every per-run exception is caught and turned into a FAILED transition
rather than crashing the loop — control-plane resilience (ADR 0007) applies
to the reconciler itself, not just to the API staying up during an outage.
"""

from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession

from control_plane import metrics, provider_factory, repositories, run_service
from control_plane.execution_provider import ResolvedWorkload, RunRequest
from control_plane.run_state import TERMINAL_STATES, RunState
from spec.dataset.v1alpha1 import Dataset, DatasetMetadata, PathBinding, resolve_dataset_config
from spec.workload.v1alpha1 import SparkWorkload

_ACTIVE_STATES = [RunState.SUBMITTING.value, RunState.QUEUED.value, RunState.RUNNING.value]


async def _event_timestamp(session: AsyncSession, run_id, to_state: str) -> datetime | None:
    """Latest event that transitioned into to_state, or None if it never
    has — used to compute latency histograms from RunEvent's own timestamps
    rather than tracking duration state separately."""
    events = await repositories.list_run_events(session, run_id)
    for event in reversed(events):
        if event.to_state == to_state:
            return event.created_at
    return None


async def _resolve_dataset_config(
    session: AsyncSession, workload: SparkWorkload, environment_name: str
) -> dict[str, str]:
    """Adapts persisted DatasetBinding rows into the shape
    spec/dataset/v1alpha1.py's resolve_dataset_config() already expects,
    reusing that function rather than duplicating its logic."""
    refs = list(workload.datasets.inputs.values()) + list(workload.datasets.outputs.values())
    datasets: dict[str, Dataset] = {}
    for ref in refs:
        binding_row = await repositories.get_dataset_binding(
            session, ref.dataset, environment_name, required=False
        )
        if binding_row is not None:
            # Path bindings only for now (spec §11 — table bindings are a
            # later milestone); DatasetBinding.kind exists for forward
            # compatibility but PathBinding itself only accepts "path".
            datasets[ref.dataset] = Dataset(
                apiVersion="runtime/v1alpha1",
                kind="Dataset",
                metadata=DatasetMetadata(name=ref.dataset),
                bindings={environment_name: PathBinding(uri=binding_row.uri)},
            )
    return resolve_dataset_config(workload, datasets, environment_name)


async def submit_new_runs(session: AsyncSession) -> None:
    for run in await repositories.list_runs_by_state(session, [RunState.ACCEPTED.value]):
        try:
            workload_row = await repositories.get_workload_definition(
                session, run.workload_name, version=run.workload_version
            )
            workload = SparkWorkload.model_validate(workload_row.definition)
            environment = await repositories.get_environment(session, run.environment_name)
            execution_profile = await repositories.get_execution_profile(
                session, environment.execution_profile_name
            )
            storage_profile = await repositories.get_storage_profile(
                session, environment.storage_profile_name
            )

            resolved = ResolvedWorkload(
                workload=workload,
                dataset_config=await _resolve_dataset_config(session, workload, run.environment_name),
                storage_config=provider_factory.build_storage_config(storage_profile),
                environment_name=run.environment_name,
                volume_mounts=provider_factory.build_storage_volume_mounts(storage_profile),
            )
            provider = provider_factory.build_execution_provider(execution_profile)

            validation = await provider.validate(resolved)
            if not validation.valid:
                await run_service.transition_run_state(
                    session, run, RunState.FAILED, message="; ".join(validation.errors)
                )
                continue

            provider_run = await provider.submit(RunRequest(run_id=str(run.id), resolved=resolved))
            await repositories.create_provider_run(
                session,
                run_id=run.id,
                provider_run_id=provider_run.provider_run_id,
                provider=execution_profile.provider,
                raw=provider_run.raw or {},
            )
            accepted_at = await _event_timestamp(session, run.id, RunState.ACCEPTED.value)
            if accepted_at is not None:
                metrics.submission_latency_seconds.record(
                    (datetime.now(UTC) - accepted_at).total_seconds()
                )
            await run_service.transition_run_state(
                session, run, RunState.QUEUED, message=f"submitted as {provider_run.provider_run_id}"
            )
        except Exception as e:  # noqa: BLE001 - deliberate: one run's failure must not stop the loop
            metrics.provider_errors_total.add(1)
            await run_service.transition_run_state(session, run, RunState.FAILED, message=str(e))


async def poll_active_runs(session: AsyncSession) -> None:
    for run in await repositories.list_runs_by_state(session, _ACTIVE_STATES):
        try:
            provider_run = await repositories.get_latest_provider_run(session, run.id)
            if provider_run is None:
                continue

            environment = await repositories.get_environment(session, run.environment_name)
            execution_profile = await repositories.get_execution_profile(
                session, environment.execution_profile_name
            )
            provider = provider_factory.build_execution_provider(execution_profile)

            status = await provider.status(provider_run.provider_run_id)
            if status.state.value != run.state:
                if status.state == RunState.RUNNING:
                    queued_at = await _event_timestamp(session, run.id, RunState.QUEUED.value)
                    if queued_at is not None:
                        metrics.queue_latency_seconds.record(
                            (datetime.now(UTC) - queued_at).total_seconds()
                        )
                elif status.state in TERMINAL_STATES:
                    running_at = await _event_timestamp(session, run.id, RunState.RUNNING.value)
                    if running_at is not None:
                        metrics.execution_duration_seconds.record(
                            (datetime.now(UTC) - running_at).total_seconds()
                        )
                await run_service.transition_run_state(
                    session, run, status.state, message=status.provider_native_status
                )
        except Exception as e:  # noqa: BLE001 - same rationale as submit_new_runs
            metrics.provider_errors_total.add(1)
            await run_service.transition_run_state(session, run, RunState.FAILED, message=str(e))


async def cancel_runs(session: AsyncSession) -> None:
    for run in await repositories.list_runs_by_state(session, [RunState.CANCELING.value]):
        try:
            provider_run = await repositories.get_latest_provider_run(session, run.id)
            if provider_run is None:
                # Never submitted — nothing at a provider to cancel.
                await run_service.transition_run_state(session, run, RunState.CANCELED, message="canceled")
                continue

            environment = await repositories.get_environment(session, run.environment_name)
            execution_profile = await repositories.get_execution_profile(
                session, environment.execution_profile_name
            )
            provider = provider_factory.build_execution_provider(execution_profile)

            await provider.cancel(provider_run.provider_run_id)
            await run_service.transition_run_state(session, run, RunState.CANCELED, message="canceled")
        except Exception as e:  # noqa: BLE001 - same rationale as submit_new_runs
            metrics.provider_errors_total.add(1)
            await run_service.transition_run_state(session, run, RunState.FAILED, message=str(e))


async def reconcile_once(session: AsyncSession) -> None:
    await submit_new_runs(session)
    await poll_active_runs(session)
    await cancel_runs(session)
