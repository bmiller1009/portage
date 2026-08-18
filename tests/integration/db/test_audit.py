"""Real-Postgres tests for the audit trail (spec §36) — record_audit_event()
persists correctly and list_audit_events()'s filters (resource,
environment_name, since/until) work against real rows, not mocks.

Every resource/environment_name here is unique per test run (conftest's
unique() helper) rather than a fixed literal — this Postgres instance is
long-lived and shared across the whole session's live verification work,
so a fixed resource string would accumulate matching rows across repeated
test runs and silently break the exact-count assertions below."""

from datetime import UTC, datetime, timedelta

import pytest

from control_plane import audit, repositories
from tests.integration.db.conftest import unique


@pytest.mark.asyncio
async def test_record_audit_event_persists_with_correct_fields(session):
    resource = unique("claims-normalization/8fa22")
    await audit.record_audit_event(
        session,
        identity="brad@example.com",
        action="RUN_CANCEL",
        resource=resource,
        environment_name="onprem-prod",
        result=audit.RESULT_SUCCESS,
        source="oidc",
    )

    events = await repositories.list_audit_events(session, resource=resource)

    assert len(events) == 1
    event = events[0]
    assert event.identity == "brad@example.com"
    assert event.action == "RUN_CANCEL"
    assert event.environment_name == "onprem-prod"
    assert event.result == "SUCCESS"
    assert event.source == "oidc"
    assert event.correlation_id
    assert event.created_at is not None


@pytest.mark.asyncio
async def test_record_audit_event_records_failures_too(session):
    """Spec §59's "audit completeness" — a failed privileged action must
    still produce an audit event, not just successes."""
    resource = unique("dangling-profile-env")
    await audit.record_audit_event(
        session,
        identity="brad@example.com",
        action="ENVIRONMENT_CREATE",
        resource=resource,
        environment_name=resource,
        result=audit.RESULT_FAILURE,
        source="oidc",
    )

    events = await repositories.list_audit_events(session, resource=resource)

    assert len(events) == 1
    assert events[0].result == "FAILURE"


@pytest.mark.asyncio
async def test_list_audit_events_filters_by_environment_name(session):
    env_a, env_b = unique("env-a"), unique("env-b")
    resource_a, resource_b = unique("r"), unique("r")
    await audit.record_audit_event(
        session,
        identity="x",
        action="RUN_SUBMIT",
        resource=resource_a,
        environment_name=env_a,
        result=audit.RESULT_SUCCESS,
        source="unauthenticated",
    )
    await audit.record_audit_event(
        session,
        identity="x",
        action="RUN_SUBMIT",
        resource=resource_b,
        environment_name=env_b,
        result=audit.RESULT_SUCCESS,
        source="unauthenticated",
    )

    events = await repositories.list_audit_events(session, environment_name=env_a)

    assert [e.resource for e in events] == [resource_a]


@pytest.mark.asyncio
async def test_list_audit_events_filters_by_time_range(session):
    resource = unique("time-range-test-resource")
    await audit.record_audit_event(
        session,
        identity="x",
        action="WORKLOAD_REGISTER",
        resource=resource,
        environment_name=None,
        result=audit.RESULT_SUCCESS,
        source="unauthenticated",
    )

    future = datetime.now(UTC) + timedelta(days=1)
    past = datetime.now(UTC) - timedelta(days=1)

    events_in_range = await repositories.list_audit_events(
        session, resource=resource, since=past, until=future
    )
    events_out_of_range = await repositories.list_audit_events(session, resource=resource, since=future)

    assert len(events_in_range) == 1
    assert len(events_out_of_range) == 0
