"""Unit tests for control_plane/webhooks.py — repositories is
monkeypatched, no database."""

import uuid
from typing import cast
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from control_plane import repositories, webhooks
from control_plane.models import Run, WebhookSubscription

_SESSION = cast(AsyncSession, None)


def _run() -> Run:
    return Run(
        id=uuid.uuid4(),
        workload_name="wordcount",
        workload_version="0.1.0",
        environment_name="k8s-remote",
        state="SUCCEEDED",
    )


def _subscription(event_types: list[str]) -> WebhookSubscription:
    return WebhookSubscription(
        id=uuid.uuid4(), url="https://example.com/hook", event_types=event_types, secret="shh", enabled=True
    )


def test_event_type_for_state():
    assert webhooks.event_type_for_state("SUCCEEDED") == "run.succeeded"
    assert webhooks.event_type_for_state("FAILED") == "run.failed"


@pytest.mark.asyncio
async def test_record_webhook_deliveries_matches_specific_event_type(monkeypatch):
    subscription = _subscription(["run.succeeded"])
    monkeypatch.setattr(repositories, "list_webhook_subscriptions", AsyncMock(return_value=[subscription]))
    mock_create = AsyncMock()
    monkeypatch.setattr(repositories, "create_webhook_delivery", mock_create)

    run = _run()
    await webhooks.record_webhook_deliveries(
        _SESSION, run, from_state="RUNNING", to_state="SUCCEEDED", message="done"
    )

    mock_create.assert_awaited_once()
    _args, kwargs = mock_create.call_args
    assert kwargs["subscription_id"] == subscription.id
    assert kwargs["event_type"] == "run.succeeded"
    assert kwargs["payload"]["run_id"] == str(run.id)
    assert kwargs["payload"]["to_state"] == "SUCCEEDED"


@pytest.mark.asyncio
async def test_record_webhook_deliveries_matches_wildcard(monkeypatch):
    subscription = _subscription(["run.state_changed"])
    monkeypatch.setattr(repositories, "list_webhook_subscriptions", AsyncMock(return_value=[subscription]))
    mock_create = AsyncMock()
    monkeypatch.setattr(repositories, "create_webhook_delivery", mock_create)

    await webhooks.record_webhook_deliveries(
        _SESSION, _run(), from_state="RUNNING", to_state="FAILED", message=None
    )

    mock_create.assert_awaited_once()


@pytest.mark.asyncio
async def test_record_webhook_deliveries_skips_non_matching_subscription(monkeypatch):
    subscription = _subscription(["run.failed"])
    monkeypatch.setattr(repositories, "list_webhook_subscriptions", AsyncMock(return_value=[subscription]))
    mock_create = AsyncMock()
    monkeypatch.setattr(repositories, "create_webhook_delivery", mock_create)

    await webhooks.record_webhook_deliveries(
        _SESSION, _run(), from_state="RUNNING", to_state="SUCCEEDED", message=None
    )

    mock_create.assert_not_awaited()


def test_sign_payload_is_deterministic_and_secret_dependent():
    body = b'{"a": 1}'
    sig1 = webhooks.sign_payload(body, "secret-a")
    sig2 = webhooks.sign_payload(body, "secret-a")
    sig3 = webhooks.sign_payload(body, "secret-b")

    assert sig1 == sig2
    assert sig1 != sig3
