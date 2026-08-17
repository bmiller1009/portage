"""The async submit/poll/converge worker (docs/architecture/spec.md §24).
Two testable stages, called in sequence by reconcile_once(): submit_new_runs
picks up ACCEPTED runs and submits them to a real execution provider;
poll_active_runs advances already-submitted runs toward a terminal state.

Every per-run exception is caught and turned into a FAILED transition
rather than crashing the loop — control-plane resilience (ADR 0007) applies
to the reconciler itself, not just to the API staying up during an outage.
"""

from sqlalchemy.ext.asyncio import AsyncSession

from control_plane import provider_factory, repositories, run_service
from control_plane.execution_provider import ResolvedWorkload, RunRequest
from control_plane.run_state import RunState
from spec.dataset.v1alpha1 import Dataset, DatasetMetadata, PathBinding, resolve_dataset_config
from spec.workload.v1alpha1 import SparkWorkload

_ACTIVE_STATES = [RunState.SUBMITTING.value, RunState.QUEUED.value, RunState.RUNNING.value]


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
            await run_service.transition_run_state(
                session, run, RunState.QUEUED, message=f"submitted as {provider_run.provider_run_id}"
            )
        except Exception as e:  # noqa: BLE001 - deliberate: one run's failure must not stop the loop
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
                await run_service.transition_run_state(
                    session, run, status.state, message=status.provider_native_status
                )
        except Exception as e:  # noqa: BLE001 - same rationale as submit_new_runs
            await run_service.transition_run_state(session, run, RunState.FAILED, message=str(e))


async def reconcile_once(session: AsyncSession) -> None:
    await submit_new_runs(session)
    await poll_active_runs(session)
