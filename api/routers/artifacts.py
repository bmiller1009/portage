from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from api.schemas import ArtifactBindingCreate, ArtifactBindingOut
from control_plane import repositories
from control_plane.db import get_db_session

router = APIRouter(prefix="/v1/artifacts", tags=["artifacts"])


@router.post("", response_model=ArtifactBindingOut, status_code=201)
async def create_artifact_binding(
    body: ArtifactBindingCreate, session: AsyncSession = Depends(get_db_session)
):
    try:
        return await repositories.create_artifact_binding(
            session,
            artifact_name=body.artifact_name,
            artifact_version=body.artifact_version,
            environment_name=body.environment_name,
            kind=body.kind,
            uri=body.uri,
        )
    except repositories.AlreadyExistsError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
    except repositories.NotFoundError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e


@router.get("", response_model=list[ArtifactBindingOut])
async def list_artifact_bindings(
    artifact_name: str | None = Query(default=None),
    session: AsyncSession = Depends(get_db_session),
):
    return await repositories.list_artifact_bindings(session, artifact_name=artifact_name)


@router.get("/{artifact_name}/{artifact_version}/{environment_name}", response_model=ArtifactBindingOut)
async def get_artifact_binding(
    artifact_name: str,
    artifact_version: str,
    environment_name: str,
    session: AsyncSession = Depends(get_db_session),
):
    try:
        return await repositories.get_artifact_binding(
            session, artifact_name, artifact_version, environment_name
        )
    except repositories.NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
