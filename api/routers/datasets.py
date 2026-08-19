from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from api.auth import ROLE_OPERATOR, ROLE_VIEWER, Identity, require_role
from api.schemas import DatasetBindingCreate, DatasetBindingOut, DatasetBindingUpdate
from control_plane import repositories
from control_plane.db import get_db_session

router = APIRouter(prefix="/v1/datasets", tags=["datasets"])


@router.post("", response_model=DatasetBindingOut, status_code=201)
async def create_dataset_binding(
    body: DatasetBindingCreate,
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_OPERATOR)),
):
    try:
        return await repositories.create_dataset_binding(
            session,
            dataset_name=body.dataset_name,
            environment_name=body.environment_name,
            kind=body.kind,
            uri=body.uri,
        )
    except repositories.AlreadyExistsError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
    except repositories.NotFoundError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e


@router.get("", response_model=list[DatasetBindingOut])
async def list_dataset_bindings(
    dataset_name: str | None = Query(default=None),
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_VIEWER)),
):
    return await repositories.list_dataset_bindings(session, dataset_name=dataset_name)


@router.get("/{dataset_name}/{environment_name}", response_model=DatasetBindingOut)
async def get_dataset_binding(
    dataset_name: str,
    environment_name: str,
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_VIEWER)),
):
    try:
        return await repositories.get_dataset_binding(session, dataset_name, environment_name)
    except repositories.NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.put("/{dataset_name}/{environment_name}", response_model=DatasetBindingOut)
async def update_dataset_binding(
    dataset_name: str,
    environment_name: str,
    body: DatasetBindingUpdate,
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_OPERATOR)),
):
    try:
        return await repositories.update_dataset_binding(
            session, dataset_name, environment_name, kind=body.kind, uri=body.uri
        )
    except repositories.NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.delete("/{dataset_name}/{environment_name}", status_code=204)
async def delete_dataset_binding(
    dataset_name: str,
    environment_name: str,
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_OPERATOR)),
):
    try:
        await repositories.delete_dataset_binding(session, dataset_name, environment_name)
    except repositories.NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
