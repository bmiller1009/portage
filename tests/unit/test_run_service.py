"""Unit tests for run_service.cancel_run/get_run_logs — pure logic against
mocked repositories/provider_factory, no database. The real state-machine
behavior (reconciler picking up CANCELING, etc.) is exercised for real in
tests/integration/db/test_reconciler.py and test_runs.py."""

import uuid
from typing import cast
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from control_plane import provider_factory, repositories, run_service
from control_plane.execution_provider import LogReference
from control_plane.models import ProviderRun, Run
from control_plane.run_state import RunState

# No real session is ever used — repositories.* is fully mocked in every
# test, so this is just a placeholder satisfying the AsyncSession type.
_SESSION = cast(AsyncSession, None)


def _run(state: str) -> Run:
    return Run(
        id=uuid.uuid4(),
        workload_name="wordcount",
        workload_version="0.1.0",
        environment_name="k8s-remote",
        state=state,
    )


@pytest.fixture(autouse=True)
def _mock_transition(monkeypatch):
    """transition_run_state() -> update_run_state()/create_run_event(); mock
    both, with update_run_state actually mutating run.state like the real
    repository function does, so assertions on the returned run work."""

    async def _update_state(session, run, new_state):
        run.state = new_state

    monkeypatch.setattr(repositories, "update_run_state", AsyncMock(side_effect=_update_state))
    monkeypatch.setattr(repositories, "create_run_event", AsyncMock())
    # transition_run_state() also dispatches webhook deliveries -- no
    # subscriptions exist in these tests, so an empty list is enough to
    # keep record_webhook_deliveries() a no-op.
    monkeypatch.setattr(repositories, "list_webhook_subscriptions", AsyncMock(return_value=[]))


@pytest.mark.asyncio
async def test_cancel_run_rejects_terminal_state(monkeypatch):
    run = _run(RunState.SUCCEEDED.value)
    monkeypatch.setattr(repositories, "get_run", AsyncMock(return_value=run))

    with pytest.raises(run_service.InvalidRunStateError):
        await run_service.cancel_run(_SESSION, run.id)


@pytest.mark.asyncio
async def test_cancel_run_finalizes_immediately_when_never_submitted(monkeypatch):
    run = _run(RunState.ACCEPTED.value)
    monkeypatch.setattr(repositories, "get_run", AsyncMock(return_value=run))

    result, pending = await run_service.cancel_run(_SESSION, run.id)

    assert pending is False
    assert result.state == RunState.CANCELED.value


@pytest.mark.asyncio
async def test_cancel_run_active_run_moves_to_canceling(monkeypatch):
    run = _run(RunState.RUNNING.value)
    monkeypatch.setattr(repositories, "get_run", AsyncMock(return_value=run))

    result, pending = await run_service.cancel_run(_SESSION, run.id)

    assert pending is True
    assert result.state == RunState.CANCELING.value


@pytest.mark.asyncio
async def test_get_run_logs_raises_when_never_submitted(monkeypatch):
    run = _run(RunState.ACCEPTED.value)
    monkeypatch.setattr(repositories, "get_run", AsyncMock(return_value=run))
    monkeypatch.setattr(repositories, "get_latest_provider_run", AsyncMock(return_value=None))

    with pytest.raises(run_service.RunNotSubmittedError):
        await run_service.get_run_logs(_SESSION, run.id)


@pytest.mark.asyncio
async def test_get_run_logs_returns_provider_reference(monkeypatch):
    run = _run(RunState.RUNNING.value)
    provider_run = ProviderRun(
        run_id=run.id, provider_run_id="fake-run-1", provider="kubernetes", raw={}
    )
    monkeypatch.setattr(repositories, "get_run", AsyncMock(return_value=run))
    monkeypatch.setattr(repositories, "get_latest_provider_run", AsyncMock(return_value=provider_run))
    monkeypatch.setattr(repositories, "get_environment", AsyncMock())
    monkeypatch.setattr(repositories, "get_execution_profile", AsyncMock())

    class FakeProvider:
        async def logs(self, provider_run_id):
            assert provider_run_id == "fake-run-1"
            return LogReference(description="driver pod logs", uri="kubectl logs ...")

    monkeypatch.setattr(
        provider_factory, "build_execution_provider", lambda execution_profile: FakeProvider()
    )

    result = await run_service.get_run_logs(_SESSION, run.id)

    assert result == LogReference(description="driver pod logs", uri="kubectl logs ...")
