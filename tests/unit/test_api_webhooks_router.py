"""Router-layer tests for /v1/webhooks — same pattern as
test_api_audit_router.py: repositories is monkeypatched, no database."""

import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from api.main import app
from control_plane import repositories
from control_plane.db import get_db_session
from control_plane.models import WebhookSubscription
from tests.unit.conftest import fake_session

app.dependency_overrides[get_db_session] = fake_session
client = TestClient(app)


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

    resp = client.post(
        "/v1/webhooks",
        json={"url": "https://example.com/hook", "event_types": ["run.succeeded"], "secret": "shh"},
    )

    assert resp.status_code == 201
    body = resp.json()
    assert body["url"] == "https://example.com/hook"
    assert body["event_types"] == ["run.succeeded"]
    assert "secret" not in body


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
