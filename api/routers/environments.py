from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from api.schemas import EnvironmentCreate, EnvironmentOut
from control_plane import repositories
from control_plane.db import get_db_session

router = APIRouter(prefix="/v1/environments", tags=["environments"])


@router.post("", response_model=EnvironmentOut, status_code=201)
async def create_environment(body: EnvironmentCreate, session: AsyncSession = Depends(get_db_session)):
    try:
        return await repositories.create_environment(
            session,
            name=body.name,
            execution_provider=body.execution_provider,
            execution_profile_name=body.execution_profile_name,
            storage_provider=body.storage_provider,
            storage_profile_name=body.storage_profile_name,
        )
    except repositories.AlreadyExistsError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
    except repositories.NotFoundError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e


@router.get("", response_model=list[EnvironmentOut])
async def list_environments(session: AsyncSession = Depends(get_db_session)):
    return await repositories.list_environments(session)


@router.get("/{name}", response_model=EnvironmentOut)
async def get_environment(name: str, session: AsyncSession = Depends(get_db_session)):
    try:
        return await repositories.get_environment(session, name)
    except repositories.NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
