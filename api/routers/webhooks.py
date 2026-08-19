import uuid

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from api.auth import ROLE_OPERATOR, Identity, require_role
from api.schemas import WebhookSubscriptionCreate, WebhookSubscriptionOut
from control_plane import audit, repositories
from control_plane.db import get_db_session

router = APIRouter(prefix="/v1/webhooks", tags=["webhooks"])


@router.post("", response_model=WebhookSubscriptionOut, status_code=201)
async def create_webhook_subscription(
    body: WebhookSubscriptionCreate,
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_OPERATOR)),
):
    """Operator+ only — a webhook URL is effectively a place this server
    sends run data, the same privilege level as cancel/environment
    management (spec §67's own RBAC matrix)."""
    subscription = await repositories.create_webhook_subscription(
        session,
        url=body.url,
        event_types=body.event_types,
        secret=body.secret,
        enabled=body.enabled,
    )
    # resource is the subscription id/URL, never body.secret — the HMAC
    # signing secret must never end up in an audit record any Viewer-role
    # reader of GET /v1/audit could see.
    await audit.record_audit_event(
        session,
        identity=identity.email or identity.subject,
        action="WEBHOOK_SUBSCRIPTION_CREATE",
        resource=str(subscription.id),
        environment_name=None,
        result=audit.RESULT_SUCCESS,
        source=identity.source,
    )
    await session.refresh(subscription)
    return subscription


@router.get("", response_model=list[WebhookSubscriptionOut])
async def list_webhook_subscriptions(
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_OPERATOR)),
):
    return await repositories.list_webhook_subscriptions(session)


@router.delete("/{subscription_id}", status_code=204)
async def delete_webhook_subscription(
    subscription_id: uuid.UUID,
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_OPERATOR)),
):
    try:
        await repositories.delete_webhook_subscription(session, subscription_id)
    except repositories.NotFoundError as e:
        await audit.record_audit_event(
            session,
            identity=identity.email or identity.subject,
            action="WEBHOOK_SUBSCRIPTION_DELETE",
            resource=str(subscription_id),
            environment_name=None,
            result=audit.RESULT_FAILURE,
            source=identity.source,
        )
        raise HTTPException(status_code=404, detail=str(e)) from e

    await audit.record_audit_event(
        session,
        identity=identity.email or identity.subject,
        action="WEBHOOK_SUBSCRIPTION_DELETE",
        resource=str(subscription_id),
        environment_name=None,
        result=audit.RESULT_SUCCESS,
        source=identity.source,
    )
