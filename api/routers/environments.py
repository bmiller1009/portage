from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from api.auth import ROLE_OPERATOR, ROLE_VIEWER, Identity, require_role
from api.schemas import EnvironmentCreate, EnvironmentOut
from control_plane import audit, repositories
from control_plane.db import get_db_session

router = APIRouter(prefix="/v1/environments", tags=["environments"])


@router.post("", response_model=EnvironmentOut, status_code=201)
async def create_environment(
    body: EnvironmentCreate,
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_OPERATOR)),
):
    try:
        environment = await repositories.create_environment(
            session,
            name=body.name,
            execution_provider=body.execution_provider,
            execution_profile_name=body.execution_profile_name,
            storage_provider=body.storage_provider,
            storage_profile_name=body.storage_profile_name,
        )
    except repositories.AlreadyExistsError as e:
        await audit.record_audit_event(
            session,
            identity=identity.email or identity.subject,
            action="ENVIRONMENT_CREATE",
            resource=body.name,
            environment_name=body.name,
            result=audit.RESULT_FAILURE,
            source=identity.source,
        )
        raise HTTPException(status_code=409, detail=str(e)) from e
    except repositories.NotFoundError as e:
        await audit.record_audit_event(
            session,
            identity=identity.email or identity.subject,
            action="ENVIRONMENT_CREATE",
            resource=body.name,
            environment_name=body.name,
            result=audit.RESULT_FAILURE,
            source=identity.source,
        )
        raise HTTPException(status_code=422, detail=str(e)) from e

    await audit.record_audit_event(
        session,
        identity=identity.email or identity.subject,
        action="ENVIRONMENT_CREATE",
        resource=environment.name,
        environment_name=environment.name,
        result=audit.RESULT_SUCCESS,
        source=identity.source,
    )
    # record_audit_event() commits, which expires `environment` — see
    # api/routers/runs.py's create_run() for the full explanation.
    await session.refresh(environment)
    return environment


@router.get("", response_model=list[EnvironmentOut])
async def list_environments(
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_VIEWER)),
):
    return await repositories.list_environments(session)


@router.get("/{name}", response_model=EnvironmentOut)
async def get_environment(
    name: str,
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_VIEWER)),
):
    try:
        return await repositories.get_environment(session, name)
    except repositories.NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
