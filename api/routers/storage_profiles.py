from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from api.schemas import StorageProfileCreate, StorageProfileOut
from control_plane import repositories
from control_plane.db import get_db_session

router = APIRouter(prefix="/v1/storage-profiles", tags=["storage-profiles"])


@router.post("", response_model=StorageProfileOut, status_code=201)
async def create_storage_profile(
    body: StorageProfileCreate, session: AsyncSession = Depends(get_db_session)
):
    try:
        return await repositories.create_storage_profile(
            session,
            name=body.name,
            provider=body.provider,
            config=body.config,
            credential_reference=body.credential_reference,
        )
    except repositories.AlreadyExistsError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e


@router.get("", response_model=list[StorageProfileOut])
async def list_storage_profiles(session: AsyncSession = Depends(get_db_session)):
    return await repositories.list_storage_profiles(session)


@router.get("/{name}", response_model=StorageProfileOut)
async def get_storage_profile(name: str, session: AsyncSession = Depends(get_db_session)):
    try:
        return await repositories.get_storage_profile(session, name)
    except repositories.NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
