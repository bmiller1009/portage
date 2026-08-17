import uuid

from fastapi import APIRouter, Depends, Header, HTTPException, Response
from sqlalchemy.ext.asyncio import AsyncSession

from api.schemas import RunCreate, RunEventOut, RunOut
from control_plane import repositories, run_service
from control_plane.db import get_db_session

router = APIRouter(prefix="/v1/runs", tags=["runs"])


@router.post("", response_model=RunOut)
async def create_run(
    body: RunCreate,
    response: Response,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    session: AsyncSession = Depends(get_db_session),
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


@router.get("/{run_id}", response_model=RunOut)
async def get_run(run_id: uuid.UUID, session: AsyncSession = Depends(get_db_session)):
    try:
        return await run_service.get_run(session, run_id)
    except repositories.NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e)) from e


@router.get("/{run_id}/events", response_model=list[RunEventOut])
async def list_run_events(run_id: uuid.UUID, session: AsyncSession = Depends(get_db_session)):
    return await run_service.list_run_events(session, run_id)
