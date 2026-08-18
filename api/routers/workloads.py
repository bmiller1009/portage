from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from api.auth import ROLE_DEVELOPER, ROLE_VIEWER, Identity, require_role
from api.schemas import WorkloadDefinitionOut
from control_plane import audit, repositories
from control_plane.db import get_db_session
from spec.workload.v1alpha1 import SparkWorkload

router = APIRouter(prefix="/v1/workloads", tags=["workloads"])


@router.post("", response_model=WorkloadDefinitionOut, status_code=201)
async def create_workload(
    body: SparkWorkload,
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_DEVELOPER)),
):
    """The request body is a portable workload definition (spec §7) — FastAPI
    validates it against SparkWorkload before this handler ever runs."""
    resource = f"{body.metadata.name}/{body.metadata.version}"
    try:
        workload = await repositories.create_workload_definition(
            session,
            name=body.metadata.name,
            version=body.metadata.version,
            definition=body.model_dump(),
        )
    except repositories.AlreadyExistsError as e:
        await audit.record_audit_event(
            session,
            identity=identity.email or identity.subject,
            action="WORKLOAD_REGISTER",
            resource=resource,
            environment_name=None,
            result=audit.RESULT_FAILURE,
            source=identity.source,
        )
        raise HTTPException(status_code=409, detail=str(e)) from e

    await audit.record_audit_event(
        session,
        identity=identity.email or identity.subject,
        action="WORKLOAD_REGISTER",
        resource=resource,
        environment_name=None,
        result=audit.RESULT_SUCCESS,
        source=identity.source,
    )
    # record_audit_event() commits, which expires `workload` — see
    # api/routers/runs.py's create_run() for the full explanation.
    await session.refresh(workload)
    return workload


@router.get("", response_model=list[WorkloadDefinitionOut])
async def list_workloads(
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_VIEWER)),
):
    return await repositories.list_workload_definitions(session)


@router.get("/{name}", response_model=WorkloadDefinitionOut)
async def get_workload(
    name: str,
    version: str | None = None,
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_VIEWER)),
):
    """Returns the latest version unless ?version= is given."""
    try:
        return await repositories.get_workload_definition(session, name, version=version)
    except repositories.NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
