from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from api.auth import ROLE_OPERATOR, ROLE_VIEWER, Identity, require_role
from api.schemas import ExecutionProfileCreate, ExecutionProfileOut
from control_plane import repositories
from control_plane.db import get_db_session

router = APIRouter(prefix="/v1/execution-profiles", tags=["execution-profiles"])


@router.post("", response_model=ExecutionProfileOut, status_code=201)
async def create_execution_profile(
    body: ExecutionProfileCreate,
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_OPERATOR)),
):
    try:
        return await repositories.create_execution_profile(
            session, name=body.name, provider=body.provider, config=body.config
        )
    except repositories.AlreadyExistsError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e


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
