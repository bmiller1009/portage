from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from api.auth import ROLE_OPERATOR, ROLE_VIEWER, Identity, require_role
from api.schemas import ExecutionProfileCreate, ExecutionProfileOut, ExecutionProfileUpdate
from control_plane import audit, repositories
from control_plane.db import get_db_session

router = APIRouter(prefix="/v1/execution-profiles", tags=["execution-profiles"])


@router.post("", response_model=ExecutionProfileOut, status_code=201)
async def create_execution_profile(
    body: ExecutionProfileCreate,
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_OPERATOR)),
):
    try:
        profile = await repositories.create_execution_profile(
            session, name=body.name, provider=body.provider, config=body.config
        )
    except repositories.AlreadyExistsError as e:
        await audit.record_audit_event(
            session,
            identity=identity.email or identity.subject,
            action="EXECUTION_PROFILE_CREATE",
            resource=body.name,
            environment_name=None,
            result=audit.RESULT_FAILURE,
            source=identity.source,
        )
        raise HTTPException(status_code=409, detail=str(e)) from e

    await audit.record_audit_event(
        session,
        identity=identity.email or identity.subject,
        action="EXECUTION_PROFILE_CREATE",
        resource=profile.name,
        environment_name=None,
        result=audit.RESULT_SUCCESS,
        source=identity.source,
    )
    # record_audit_event() commits, which expires `profile` — see
    # api/routers/runs.py's create_run() for the full explanation.
    await session.refresh(profile)
    return profile


@router.get("", response_model=list[ExecutionProfileOut])
async def list_execution_profiles(
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_VIEWER)),
):
    return await repositories.list_execution_profiles(session)


@router.get("/{name}", response_model=ExecutionProfileOut)
async def get_execution_profile(
    name: str,
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_VIEWER)),
):
    try:
        return await repositories.get_execution_profile(session, name)
    except repositories.NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.put("/{name}", response_model=ExecutionProfileOut)
async def update_execution_profile(
    name: str,
    body: ExecutionProfileUpdate,
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_OPERATOR)),
):
    try:
        profile = await repositories.update_execution_profile(
            session, name, provider=body.provider, config=body.config
        )
    except repositories.NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e

    await audit.record_audit_event(
        session,
        identity=identity.email or identity.subject,
        action="EXECUTION_PROFILE_UPDATE",
        resource=name,
        environment_name=None,
        result=audit.RESULT_SUCCESS,
        source=identity.source,
    )
    await session.refresh(profile)
    return profile


@router.delete("/{name}", status_code=204)
async def delete_execution_profile(
    name: str,
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_OPERATOR)),
):
    try:
        await repositories.delete_execution_profile(session, name)
    except repositories.NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except repositories.InUseError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e

    await audit.record_audit_event(
        session,
        identity=identity.email or identity.subject,
        action="EXECUTION_PROFILE_DELETE",
        resource=name,
        environment_name=None,
        result=audit.RESULT_SUCCESS,
        source=identity.source,
    )
