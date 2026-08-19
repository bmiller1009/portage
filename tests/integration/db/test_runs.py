"""Real-Postgres tests for run creation, idempotency, and state transitions
(docs/architecture/spec.md §23-25)."""

import asyncio
import uuid

import pytest

from control_plane import repositories, run_service
from control_plane.db import make_engine, make_session_maker
from control_plane.run_state import RunState


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.mark.asyncio
async def test_list_runs_filters_by_environment_and_orders_newest_first(
    session, environment_name, workload_ref
):
    workload_name, workload_version = workload_ref
    run1, _ = await run_service.create_run(
        session, workload_name=workload_name, workload_version=workload_version,
        environment_name=environment_name,
    )
    run2, _ = await run_service.create_run(
        session, workload_name=workload_name, workload_version=workload_version,
        environment_name=environment_name,
    )

    runs = await run_service.list_runs(session, environment_name=environment_name)

    assert [r.id for r in runs] == [run2.id, run1.id]


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
async def test_concurrent_idempotency_key_race_still_creates_exactly_one_run(
    session, environment_name, workload_ref
):
    """Two genuinely concurrent create_run() calls (separate sessions —
    the same shape as two API replicas each handling their own request)
    racing on the same idempotency_key. The naive get-then-create check
    isn't atomic, so both can pass it before either commits; this proves
    the loser is cleaned up (not left as an orphaned duplicate Run) and
    every caller converges on the same run — confirmed as a real bug via
    tests/chaos/test_idempotent_submission.py before this was fixed."""
    workload_name, workload_version = workload_ref
    key = _unique("idem-race")

    engine = make_engine()
    session_maker = make_session_maker(engine)

    async def create_with_own_session():
        async with session_maker() as s:
            return await run_service.create_run(
                s,
                workload_name=workload_name,
                workload_version=workload_version,
                environment_name=environment_name,
                idempotency_key=key,
            )

    try:
        (run1, created1), (run2, created2) = await asyncio.gather(
            create_with_own_session(), create_with_own_session()
        )
    finally:
        await engine.dispose()

    assert run1.id == run2.id
    assert {created1, created2} == {True, False}

    all_runs = await repositories.list_runs(session, environment_name=environment_name)
    matching = [r for r in all_runs if r.id == run1.id]
    assert len(matching) == 1


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


@pytest.mark.asyncio
async def test_cancel_run_never_submitted_finalizes_immediately(session, environment_name, workload_ref):
    workload_name, workload_version = workload_ref
    run, _created = await run_service.create_run(
        session, workload_name=workload_name, workload_version=workload_version,
        environment_name=environment_name,
    )

    result, pending = await run_service.cancel_run(session, run.id)

    assert pending is False
    assert result.state == RunState.CANCELED.value
    events = await run_service.list_run_events(session, run.id)
    assert [e.to_state for e in events] == [RunState.ACCEPTED.value, RunState.CANCELED.value]


@pytest.mark.asyncio
async def test_cancel_run_active_run_moves_to_canceling(session, environment_name, workload_ref):
    workload_name, workload_version = workload_ref
    run, _created = await run_service.create_run(
        session, workload_name=workload_name, workload_version=workload_version,
        environment_name=environment_name,
    )
    await repositories.create_provider_run(
        session, run_id=run.id, provider_run_id="fake-run-1", provider="kubernetes", raw={}
    )
    await run_service.transition_run_state(session, run, RunState.RUNNING, message="running")

    result, pending = await run_service.cancel_run(session, run.id)

    assert pending is True
    assert result.state == RunState.CANCELING.value


@pytest.mark.asyncio
async def test_cancel_run_rejects_terminal_state(session, environment_name, workload_ref):
    workload_name, workload_version = workload_ref
    run, _created = await run_service.create_run(
        session, workload_name=workload_name, workload_version=workload_version,
        environment_name=environment_name,
    )
    await run_service.transition_run_state(session, run, RunState.SUCCEEDED, message="done")

    with pytest.raises(run_service.InvalidRunStateError):
        await run_service.cancel_run(session, run.id)


@pytest.mark.asyncio
async def test_get_run_logs_raises_before_submission(session, environment_name, workload_ref):
    workload_name, workload_version = workload_ref
    run, _created = await run_service.create_run(
        session, workload_name=workload_name, workload_version=workload_version,
        environment_name=environment_name,
    )

    with pytest.raises(run_service.RunNotSubmittedError):
        await run_service.get_run_logs(session, run.id)
