from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from api.auth import ROLE_OPERATOR, ROLE_VIEWER, Identity, require_role
from api.schemas import DatasetBindingCreate, DatasetBindingOut, DatasetBindingUpdate
from control_plane import audit, repositories
from control_plane.db import get_db_session

router = APIRouter(prefix="/v1/datasets", tags=["datasets"])


def _resource(dataset_name: str, environment_name: str) -> str:
    return f"{dataset_name}/{environment_name}"


@router.post("", response_model=DatasetBindingOut, status_code=201)
async def create_dataset_binding(
    body: DatasetBindingCreate,
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_OPERATOR)),
):
    resource = _resource(body.dataset_name, body.environment_name)
    try:
        binding = await repositories.create_dataset_binding(
            session,
            dataset_name=body.dataset_name,
            environment_name=body.environment_name,
            kind=body.kind,
            uri=body.uri,
        )
    except repositories.AlreadyExistsError as e:
        await audit.record_audit_event(
            session,
            identity=identity.email or identity.subject,
            action="DATASET_BINDING_CREATE",
            resource=resource,
            environment_name=body.environment_name,
            result=audit.RESULT_FAILURE,
            source=identity.source,
        )
        raise HTTPException(status_code=409, detail=str(e)) from e
    except repositories.NotFoundError as e:
        await audit.record_audit_event(
            session,
            identity=identity.email or identity.subject,
            action="DATASET_BINDING_CREATE",
            resource=resource,
            environment_name=body.environment_name,
            result=audit.RESULT_FAILURE,
            source=identity.source,
        )
        raise HTTPException(status_code=422, detail=str(e)) from e

    await audit.record_audit_event(
        session,
        identity=identity.email or identity.subject,
        action="DATASET_BINDING_CREATE",
        resource=resource,
        environment_name=body.environment_name,
        result=audit.RESULT_SUCCESS,
        source=identity.source,
    )
    await session.refresh(binding)
    return binding


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
    resource = _resource(dataset_name, environment_name)
    try:
        binding = await repositories.update_dataset_binding(
            session, dataset_name, environment_name, kind=body.kind, uri=body.uri
        )
    except repositories.NotFoundError as e:
        await audit.record_audit_event(
            session,
            identity=identity.email or identity.subject,
            action="DATASET_BINDING_UPDATE",
            resource=resource,
            environment_name=environment_name,
            result=audit.RESULT_FAILURE,
            source=identity.source,
        )
        raise HTTPException(status_code=404, detail=str(e)) from e

    await audit.record_audit_event(
        session,
        identity=identity.email or identity.subject,
        action="DATASET_BINDING_UPDATE",
        resource=resource,
        environment_name=environment_name,
        result=audit.RESULT_SUCCESS,
        source=identity.source,
    )
    await session.refresh(binding)
    return binding


@router.delete("/{dataset_name}/{environment_name}", status_code=204)
async def delete_dataset_binding(
    dataset_name: str,
    environment_name: str,
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_OPERATOR)),
):
    resource = _resource(dataset_name, environment_name)
    try:
        await repositories.delete_dataset_binding(session, dataset_name, environment_name)
    except repositories.NotFoundError as e:
        await audit.record_audit_event(
            session,
            identity=identity.email or identity.subject,
            action="DATASET_BINDING_DELETE",
            resource=resource,
            environment_name=environment_name,
            result=audit.RESULT_FAILURE,
            source=identity.source,
        )
        raise HTTPException(status_code=404, detail=str(e)) from e

    await audit.record_audit_event(
        session,
        identity=identity.email or identity.subject,
        action="DATASET_BINDING_DELETE",
        resource=resource,
        environment_name=environment_name,
        result=audit.RESULT_SUCCESS,
        source=identity.source,
    )
