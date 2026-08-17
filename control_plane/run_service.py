"""Run lifecycle logic shared by api/routers/runs.py and
reconciler/service.py — idempotent creation (spec §25) and state
transitions that always append a run_event (the audit trail behind
GET /v1/runs/{id}/events).
"""

import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from control_plane import repositories
from control_plane.models import Run
from control_plane.run_state import RunState


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
