"""Real-Postgres tests for run creation, idempotency, and state transitions
(docs/architecture/spec.md §23-25)."""

import uuid

import pytest

from control_plane import repositories, run_service
from control_plane.run_state import RunState


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.mark.asyncio
async def test_create_run_starts_accepted_and_transitions_log_events(
    session, environment_name, workload_ref
):
    workload_name, workload_version = workload_ref

    run, created = await run_service.create_run(
        session,
        workload_name=workload_name,
        workload_version=workload_version,
        environment_name=environment_name,
    )
    assert created is True
    assert run.state == RunState.ACCEPTED.value

    await run_service.transition_run_state(session, run, RunState.QUEUED, message="submitted")
    fetched = await run_service.get_run(session, run.id)
    assert fetched.state == RunState.QUEUED.value

    events = await run_service.list_run_events(session, run.id)
    assert [e.to_state for e in events] == [RunState.ACCEPTED.value, RunState.QUEUED.value]
    assert events[1].from_state == RunState.ACCEPTED.value
    assert events[1].message == "submitted"


@pytest.mark.asyncio
async def test_create_run_defaults_to_latest_workload_version(session, environment_name, workload_ref):
    workload_name, _version = workload_ref

    run, created = await run_service.create_run(
        session, workload_name=workload_name, workload_version=None, environment_name=environment_name
    )

    assert created is True
    assert run.workload_version == "0.1.0"


@pytest.mark.asyncio
async def test_idempotency_key_replay_returns_existing_run_not_a_duplicate(
    session, environment_name, workload_ref
):
    workload_name, workload_version = workload_ref
    key = _unique("idem")

    run1, created1 = await run_service.create_run(
        session,
        workload_name=workload_name,
        workload_version=workload_version,
        environment_name=environment_name,
        idempotency_key=key,
    )
    run2, created2 = await run_service.create_run(
        session,
        workload_name=workload_name,
        workload_version=workload_version,
        environment_name=environment_name,
        idempotency_key=key,
    )

    assert created1 is True
    assert created2 is False
    assert run1.id == run2.id


@pytest.mark.asyncio
async def test_create_run_requires_existing_workload(session, environment_name):
    with pytest.raises(repositories.NotFoundError):
        await run_service.create_run(
            session,
            workload_name=_unique("ghost-workload"),
            workload_version="0.0.0",
            environment_name=environment_name,
        )


@pytest.mark.asyncio
async def test_create_run_requires_existing_environment(session, workload_ref):
    workload_name, workload_version = workload_ref
    with pytest.raises(repositories.NotFoundError):
        await run_service.create_run(
            session,
            workload_name=workload_name,
            workload_version=workload_version,
            environment_name=_unique("ghost-env"),
        )
