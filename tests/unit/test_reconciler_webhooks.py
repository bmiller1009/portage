"""Unit tests for reconciler/service.py's deliver_webhooks() — httpx is
monkeypatched (same pattern as tests/unit/test_cli_certification.py), no
real network, no database."""

import uuid
from typing import cast
from unittest.mock import AsyncMock

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from control_plane import repositories
from control_plane.models import WebhookDelivery, WebhookSubscription
from reconciler.service import MAX_WEBHOOK_DELIVERY_ATTEMPTS, deliver_webhooks

_SESSION = cast(AsyncSession, None)


class _FakeResponse:
    def __init__(self, status_code: int):
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            request = httpx.Request("POST", "http://test")
            raise httpx.HTTPStatusError("error", request=request, response=cast(httpx.Response, self))


class _FakeAsyncClient:
    def __init__(self, status_code: int = 200, raises: Exception | None = None):
        self._status_code = status_code
        self._raises = raises

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def post(self, url, *, content, headers):
        if self._raises is not None:
            raise self._raises
        return _FakeResponse(self._status_code)


def _subscription() -> WebhookSubscription:
    return WebhookSubscription(
        id=uuid.uuid4(), url="https://example.com/hook", event_types=["run.succeeded"], secret="shh", enabled=True
    )


def _delivery(subscription_id, attempts: int = 0) -> WebhookDelivery:
    return WebhookDelivery(
        id=uuid.uuid4(),
        subscription_id=subscription_id,
        run_id=uuid.uuid4(),
        event_type="run.succeeded",
        payload={"run_id": "x"},
        status="pending",
        attempts=attempts,
    )


@pytest.mark.asyncio
async def test_deliver_webhooks_marks_delivered_on_success(monkeypatch):
    subscription = _subscription()
    delivery = _delivery(subscription.id)
    monkeypatch.setattr(repositories, "list_pending_webhook_deliveries", AsyncMock(return_value=[delivery]))
    monkeypatch.setattr(repositories, "get_webhook_subscription", AsyncMock(return_value=subscription))
    mock_mark = AsyncMock()
    monkeypatch.setattr(repositories, "mark_webhook_delivery", mock_mark)
    monkeypatch.setattr(httpx, "AsyncClient", lambda **kwargs: _FakeAsyncClient(status_code=200))

    await deliver_webhooks(_SESSION)

    mock_mark.assert_awaited_once_with(_SESSION, delivery, status="delivered", attempts=1)


@pytest.mark.asyncio
async def test_deliver_webhooks_retries_on_failure_below_max_attempts(monkeypatch):
    subscription = _subscription()
    delivery = _delivery(subscription.id, attempts=1)
    monkeypatch.setattr(repositories, "list_pending_webhook_deliveries", AsyncMock(return_value=[delivery]))
    monkeypatch.setattr(repositories, "get_webhook_subscription", AsyncMock(return_value=subscription))
    mock_mark = AsyncMock()
    monkeypatch.setattr(repositories, "mark_webhook_delivery", mock_mark)
    monkeypatch.setattr(
        httpx, "AsyncClient", lambda **kwargs: _FakeAsyncClient(raises=httpx.ConnectError("refused"))
    )

    await deliver_webhooks(_SESSION)

    mock_mark.assert_awaited_once_with(_SESSION, delivery, status="pending", attempts=2)


@pytest.mark.asyncio
async def test_deliver_webhooks_fails_past_max_attempts(monkeypatch):
    subscription = _subscription()
    delivery = _delivery(subscription.id, attempts=MAX_WEBHOOK_DELIVERY_ATTEMPTS - 1)
    monkeypatch.setattr(repositories, "list_pending_webhook_deliveries", AsyncMock(return_value=[delivery]))
    monkeypatch.setattr(repositories, "get_webhook_subscription", AsyncMock(return_value=subscription))
    mock_mark = AsyncMock()
    monkeypatch.setattr(repositories, "mark_webhook_delivery", mock_mark)
    monkeypatch.setattr(
        httpx, "AsyncClient", lambda **kwargs: _FakeAsyncClient(raises=httpx.ConnectError("refused"))
    )

    await deliver_webhooks(_SESSION)

    mock_mark.assert_awaited_once_with(
        _SESSION, delivery, status="failed", attempts=MAX_WEBHOOK_DELIVERY_ATTEMPTS
    )


@pytest.mark.asyncio
async def test_deliver_webhooks_fails_immediately_when_subscription_disabled(monkeypatch):
    subscription = _subscription()
    subscription.enabled = False
    delivery = _delivery(subscription.id, attempts=0)
    monkeypatch.setattr(repositories, "list_pending_webhook_deliveries", AsyncMock(return_value=[delivery]))
    monkeypatch.setattr(repositories, "get_webhook_subscription", AsyncMock(return_value=subscription))
    mock_mark = AsyncMock()
    monkeypatch.setattr(repositories, "mark_webhook_delivery", mock_mark)

    await deliver_webhooks(_SESSION)

    mock_mark.assert_awaited_once_with(_SESSION, delivery, status="failed", attempts=0)
