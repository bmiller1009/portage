import uuid

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response
from sqlalchemy.ext.asyncio import AsyncSession

from api.auth import ROLE_DEVELOPER, ROLE_OPERATOR, ROLE_VIEWER, Identity, require_role
from api.schemas import RunCreate, RunEventOut, RunLogsOut, RunOut
from control_plane import repositories, run_service
from control_plane.db import get_db_session

router = APIRouter(prefix="/v1/runs", tags=["runs"])


@router.post("", response_model=RunOut)
async def create_run(
    body: RunCreate,
    response: Response,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_DEVELOPER)),
):
    """Persists ACCEPTED and returns immediately — spec §24. No synchronous
    provider call here; the reconciler (reconciler/service.py) picks this
    run up asynchronously. Replaying the same Idempotency-Key returns the
    existing run (200) rather than creating a duplicate (spec §25); a new
    run is a fresh submission (202)."""
    try:
        run, created = await run_service.create_run(
            session,
            workload_name=body.workload_name,
            workload_version=body.workload_version,
            environment_name=body.environment_name,
            idempotency_key=idempotency_key,
        )
    except repositories.NotFoundError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e

    response.status_code = 202 if created else 200
    return run


@router.get("", response_model=list[RunOut])
async def list_runs(
    environment_name: str | None = Query(default=None),
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_VIEWER)),
):
    return await run_service.list_runs(session, environment_name=environment_name)


@router.get("/{run_id}", response_model=RunOut)
async def get_run(
    run_id: uuid.UUID,
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_VIEWER)),
):
    try:
        return await run_service.get_run(session, run_id)
    except repositories.NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.get("/{run_id}/events", response_model=list[RunEventOut])
async def list_run_events(
    run_id: uuid.UUID,
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_VIEWER)),
):
    return await run_service.list_run_events(session, run_id)


@router.delete("/{run_id}", response_model=RunOut)
async def cancel_run(
    run_id: uuid.UUID,
    response: Response,
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_OPERATOR)),
):
    """Records cancellation intent — reconciler/service.py's cancel_runs()
    is what actually calls the provider (see run_service.cancel_run)."""
    try:
        run, pending = await run_service.cancel_run(session, run_id)
    except repositories.NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except run_service.InvalidRunStateError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e

    response.status_code = 202 if pending else 200
    return run


@router.get("/{run_id}/logs", response_model=RunLogsOut)
async def get_run_logs(
    run_id: uuid.UUID,
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_VIEWER)),
):
    try:
        return await run_service.get_run_logs(session, run_id)
    except repositories.NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e
    except run_service.RunNotSubmittedError as e:
        raise HTTPException(status_code=409, detail=str(e)) from e
