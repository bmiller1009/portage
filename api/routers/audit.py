from datetime import datetime

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from api.auth import ROLE_OPERATOR, Identity, require_role
from api.schemas import AuditEventOut
from control_plane import repositories
from control_plane.db import get_db_session

router = APIRouter(prefix="/v1/audit", tags=["audit"])


@router.get("", response_model=list[AuditEventOut])
async def list_audit_events(
    resource: str | None = Query(default=None),
    environment_name: str | None = Query(default=None),
    since: datetime | None = Query(default=None),
    until: datetime | None = Query(default=None),
    limit: int = Query(default=100, le=1000),
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_OPERATOR)),
):
    """Export for SIEM ingestion (spec §36: "do not build a SIEM") —
    filterable so an external system can pull incrementally rather than
    re-reading the whole table. Operator+ only: the audit trail itself is
    a privileged resource."""
    return await repositories.list_audit_events(
        session,
        resource=resource,
        environment_name=environment_name,
        since=since,
        until=until,
        limit=limit,
    )
