"""Run lifecycle logic shared by api/routers/runs.py and
reconciler/service.py — idempotent creation (spec §25) and state
transitions that always append a run_event (the audit trail behind
GET /v1/runs/{id}/events).
"""

import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from control_plane import provider_factory, repositories
from control_plane.execution_provider import LogReference, ResolvedWorkload, ValidationResult
from control_plane.models import Run
from control_plane.run_state import TERMINAL_STATES, RunState
from spec.workload.v1alpha1 import SparkWorkload


class InvalidRunStateError(Exception):
    pass


class RunNotSubmittedError(Exception):
    pass


async def create_run(
    session: AsyncSession,
    *,
    workload_name: str,
    workload_version: str | None,
    environment_name: str,
    idempotency_key: str | None = None,
) -> tuple[Run, bool]:
    """Creates a new run in ACCEPTED state, or — if idempotency_key was
    already used — returns the existing run instead of creating a
    duplicate (spec §25). Returns (run, created) so the API layer can
    report 202 for a fresh submission vs. 200 for a replayed one."""
    if idempotency_key is not None:
        existing_key = await repositories.get_idempotency_key(session, idempotency_key)
        if existing_key is not None:
            return await repositories.get_run(session, existing_key.run_id), False

    # Raises NotFoundError if either doesn't exist — surfaced by the API
    # layer as 422/404, same pattern as create_environment's profile checks.
    workload = await repositories.get_workload_definition(session, workload_name, version=workload_version)
    await repositories.get_environment(session, environment_name)

    run = await repositories.create_run(
        session,
        workload_name=workload.name,
        workload_version=workload.version,
        environment_name=environment_name,
        state=RunState.ACCEPTED.value,
    )
    await repositories.create_run_event(
        session, run_id=run.id, from_state=None, to_state=RunState.ACCEPTED.value, message="run accepted"
    )
    if idempotency_key is not None:
        await repositories.create_idempotency_key(session, key=idempotency_key, run_id=run.id)
    return run, True


async def get_run(session: AsyncSession, run_id: uuid.UUID) -> Run:
    return await repositories.get_run(session, run_id)


async def list_run_events(session: AsyncSession, run_id: uuid.UUID):
    return await repositories.list_run_events(session, run_id)


async def transition_run_state(
    session: AsyncSession, run: Run, new_state: RunState, *, message: str | None = None
) -> Run:
    old_state = run.state
    await repositories.update_run_state(session, run, new_state.value)
    await repositories.create_run_event(
        session, run_id=run.id, from_state=old_state, to_state=new_state.value, message=message
    )
    return run


async def cancel_run(session: AsyncSession, run_id: uuid.UUID) -> tuple[Run, bool]:
    """Requests cancellation. A run that was never submitted (still
    ACCEPTED) is finalized immediately — there's nothing at a provider to
    cancel. Otherwise this only records intent (CANCELING); reconciler/
    service.py's cancel_runs() is what actually calls provider.cancel()
    and finalizes to CANCELED, same async-only-the-reconciler-touches-
    providers shape as submission. Returns (run, pending) so the API layer
    can report 202 (still in flight) vs 200 (already final)."""
    run = await repositories.get_run(session, run_id)
    if run.state in {s.value for s in TERMINAL_STATES}:
        raise InvalidRunStateError(f"run '{run_id}' is already in terminal state {run.state}")
    if run.state == RunState.ACCEPTED.value:
        await transition_run_state(session, run, RunState.CANCELED, message="canceled before submission")
        return run, False
    await transition_run_state(session, run, RunState.CANCELING, message="cancellation requested")
    return run, True


async def validate_workload(
    session: AsyncSession, workload: SparkWorkload, environment_name: str
) -> ValidationResult:
    """Static, pre-submission capability check (spec §21) — resolves the
    named environment to a live provider and calls its validate(), the same
    call reconciler/service.py's submit_new_runs() makes right before
    submitting, so a passing plane workload validate really does predict
    what submission would do."""
    environment = await repositories.get_environment(session, environment_name)
    execution_profile = await repositories.get_execution_profile(session, environment.execution_profile_name)
    provider = provider_factory.build_execution_provider(execution_profile)
    resolved = ResolvedWorkload(workload=workload, dataset_config={}, environment_name=environment_name)
    return await provider.validate(resolved)


async def get_run_logs(session: AsyncSession, run_id: uuid.UUID) -> LogReference:
    """Read-only against the provider, so unlike cancel this is answered
    synchronously rather than deferred to the reconciler."""
    run = await repositories.get_run(session, run_id)
    provider_run = await repositories.get_latest_provider_run(session, run.id)
    if provider_run is None:
        raise RunNotSubmittedError(f"run '{run_id}' has not been submitted to a provider yet")
    environment = await repositories.get_environment(session, run.environment_name)
    execution_profile = await repositories.get_execution_profile(session, environment.execution_profile_name)
    provider = provider_factory.build_execution_provider(execution_profile)
    return await provider.logs(provider_run.provider_run_id)
