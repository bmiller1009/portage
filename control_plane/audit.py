"""Records AuditEvent rows for privileged operations (spec §36). Router
handlers call this with already-extracted identity/source strings (from
api.auth.Identity) rather than the Identity object itself, so this module
never imports from api/ — the dependency runs the other way everywhere
else in this codebase, and this keeps it that way.
"""

import uuid

from sqlalchemy.ext.asyncio import AsyncSession

from control_plane import repositories

RESULT_SUCCESS = "SUCCESS"
RESULT_FAILURE = "FAILURE"


async def record_audit_event(
    session: AsyncSession,
    *,
    identity: str,
    action: str,
    resource: str,
    environment_name: str | None,
    result: str,
    source: str,
) -> None:
    await repositories.create_audit_event(
        session,
        identity=identity,
        action=action,
        resource=resource,
        environment_name=environment_name,
        result=result,
        source=source,
        correlation_id=str(uuid.uuid4()),
    )
