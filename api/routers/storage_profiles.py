from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from api.auth import ROLE_OPERATOR, ROLE_VIEWER, Identity, require_role
from api.schemas import StorageProfileCreate, StorageProfileOut, StorageProfileUpdate
from control_plane import audit, repositories
from control_plane.db import get_db_session

router = APIRouter(prefix="/v1/storage-profiles", tags=["storage-profiles"])


@router.post("", response_model=StorageProfileOut, status_code=201)
async def create_storage_profile(
    body: StorageProfileCreate,
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_OPERATOR)),
):
    try:
        profile = await repositories.create_storage_profile(
            session,
            name=body.name,
            provider=body.provider,
            config=body.config,
            credential_reference=body.credential_reference,
        )
    except repositories.AlreadyExistsError as e:
        await audit.record_audit_event(
            session,
            identity=identity.email or identity.subject,
            action="STORAGE_PROFILE_CREATE",
            resource=body.name,
            environment_name=None,
            result=audit.RESULT_FAILURE,
            source=identity.source,
        )
        raise HTTPException(status_code=409, detail=str(e)) from e

    await audit.record_audit_event(
        session,
        identity=identity.email or identity.subject,
        action="STORAGE_PROFILE_CREATE",
        resource=profile.name,
        environment_name=None,
        result=audit.RESULT_SUCCESS,
        source=identity.source,
    )
    # record_audit_event() commits, which expires `profile` — see
    # api/routers/runs.py's create_run() for the full explanation.
    await session.refresh(profile)
    return profile


@router.get("", response_model=list[StorageProfileOut])
async def list_storage_profiles(
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_VIEWER)),
):
    return await repositories.list_storage_profiles(session)


@router.get("/{name}", response_model=StorageProfileOut)
async def get_storage_profile(
    name: str,
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_VIEWER)),
):
    try:
        return await repositories.get_storage_profile(session, name)
    except repositories.NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.put("/{name}", response_model=StorageProfileOut)
async def update_storage_profile(
    name: str,
    body: StorageProfileUpdate,
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_OPERATOR)),
):
    try:
        profile = await repositories.update_storage_profile(
            session,
            name,
            provider=body.provider,
            config=body.config,
            credential_reference=body.credential_reference,
        )
    except repositories.NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e

    await audit.record_audit_event(
        session,
        identity=identity.email or identity.subject,
        action="STORAGE_PROFILE_UPDATE",
        resource=name,
        environment_name=None,
        result=audit.RESULT_SUCCESS,
        source=identity.source,
    )
    await session.refresh(profile)
    return profile


@router.delete("/{name}", status_code=204)
async def delete_storage_profile(
    name: str,
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_OPERATOR)),
):
    try:
        await repositories.delete_storage_profile(session, name)
    except repositories.NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except repositories.InUseError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e

    await audit.record_audit_event(
        session,
        identity=identity.email or identity.subject,
        action="STORAGE_PROFILE_DELETE",
        resource=name,
        environment_name=None,
        result=audit.RESULT_SUCCESS,
        source=identity.source,
    )
