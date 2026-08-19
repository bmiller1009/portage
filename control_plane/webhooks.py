"""Matches run state transitions to registered webhook subscriptions and
persists a `pending` WebhookDelivery row per match (spec §39/§69 — "very
small primitives... webhooks/events", deliberately not a general event
bus). No network I/O here: reconciler/service.py's deliver_webhooks()
tick function is what actually sends the HTTP POST, keeping this call —
made from run_service.transition_run_state(), the single choke point
every state transition already flows through — cheap and synchronous.
"""

import hashlib
import hmac
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession

from control_plane import repositories
from control_plane.models import Run

# Every subscription implicitly matches this, regardless of its own
# event_types list — the simplest way to say "notify me on any state
# change" without enumerating every RunState value.
WILDCARD_EVENT_TYPE = "run.state_changed"


def event_type_for_state(to_state: str) -> str:
    return f"run.{to_state.lower()}"


async def record_webhook_deliveries(
    session: AsyncSession, run: Run, *, from_state: str | None, to_state: str, message: str | None
) -> None:
    event_type = event_type_for_state(to_state)
    subscriptions = await repositories.list_webhook_subscriptions(session, enabled_only=True)
    matching = [
        s for s in subscriptions if event_type in s.event_types or WILDCARD_EVENT_TYPE in s.event_types
    ]
    if not matching:
        return

    payload = {
        "run_id": str(run.id),
        "workload_name": run.workload_name,
        "workload_version": run.workload_version,
        "environment_name": run.environment_name,
        "from_state": from_state,
        "to_state": to_state,
        "message": message,
        "timestamp": datetime.now(UTC).isoformat(),
    }
    for subscription in matching:
        await repositories.create_webhook_delivery(
            session,
            subscription_id=subscription.id,
            run_id=run.id,
            event_type=event_type,
            payload=payload,
        )


def sign_payload(payload_bytes: bytes, secret: str) -> str:
    """HMAC-SHA256 over the raw JSON body, hex-encoded — standard webhook
    signing practice, sent as the X-Portage-Signature header so a
    receiver can verify the request actually came from this server."""
    return hmac.new(secret.encode(), payload_bytes, hashlib.sha256).hexdigest()
