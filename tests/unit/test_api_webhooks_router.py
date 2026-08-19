"""Router-layer tests for /v1/webhooks — same pattern as
test_api_audit_router.py: repositories is monkeypatched, no database."""

import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

from api.main import app
from control_plane import audit, repositories
from control_plane.db import get_db_session
from control_plane.models import WebhookSubscription
from tests.unit.conftest import fake_session

app.dependency_overrides[get_db_session] = fake_session
client = TestClient(app)


@pytest.fixture(autouse=True)
def _no_op_audit(monkeypatch):
    """This file's fake session (no real DB) can't back a real
    AuditEvent write — these are router-wiring tests, not audit tests."""
    monkeypatch.setattr(audit, "record_audit_event", AsyncMock())


def test_create_webhook_subscription(monkeypatch):
    subscription_id = uuid.uuid4()
    mock_create = AsyncMock(
        return_value=WebhookSubscription(
            id=subscription_id,
            url="https://example.com/hook",
            event_types=["run.succeeded"],
            secret="shh",
            enabled=True,
            created_at=datetime(2026, 8, 19, tzinfo=UTC),
        )
    )
    monkeypatch.setattr(repositories, "create_webhook_subscription", mock_create)
    mock_audit = AsyncMock()
    monkeypatch.setattr(audit, "record_audit_event", mock_audit)

    resp = client.post(
        "/v1/webhooks",
        json={"url": "https://example.com/hook", "event_types": ["run.succeeded"], "secret": "shh"},
    )

    assert resp.status_code == 201
    body = resp.json()
    assert body["url"] == "https://example.com/hook"
    assert body["event_types"] == ["run.succeeded"]
    assert "secret" not in body
    mock_audit.assert_awaited_once()
    assert mock_audit.await_args is not None
    # The webhook's HMAC-signing secret must never end up in an audit
    # record any Viewer-role reader of GET /v1/audit could see.
    assert mock_audit.await_args.kwargs["action"] == "WEBHOOK_SUBSCRIPTION_CREATE"
    assert mock_audit.await_args.kwargs["resource"] == str(subscription_id)
    assert "shh" not in str(mock_audit.await_args.kwargs)


def test_list_webhook_subscriptions(monkeypatch):
    mock_list = AsyncMock(
        return_value=[
            WebhookSubscription(
                id=uuid.uuid4(),
                url="https://example.com/hook",
                event_types=["run.state_changed"],
                secret="shh",
                enabled=True,
                created_at=datetime(2026, 8, 19, tzinfo=UTC),
            )
        ]
    )
    monkeypatch.setattr(repositories, "list_webhook_subscriptions", mock_list)

    resp = client.get("/v1/webhooks")

    assert resp.status_code == 200
    assert len(resp.json()) == 1


def test_delete_webhook_subscription(monkeypatch):
    mock_delete = AsyncMock()
    monkeypatch.setattr(repositories, "delete_webhook_subscription", mock_delete)

    resp = client.delete(f"/v1/webhooks/{uuid.uuid4()}")

    assert resp.status_code == 204


def test_delete_webhook_subscription_404_when_missing(monkeypatch):
    async def _raise(session, subscription_id):
        raise repositories.NotFoundError(f"webhook subscription '{subscription_id}' not found")

    monkeypatch.setattr(repositories, "delete_webhook_subscription", _raise)

    resp = client.delete(f"/v1/webhooks/{uuid.uuid4()}")

    assert resp.status_code == 404
