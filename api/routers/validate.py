from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from api.schemas import ValidateRequest, ValidateResponseOut
from control_plane import repositories, run_service
from control_plane.db import get_db_session

router = APIRouter(prefix="/v1/validate", tags=["validate"])


@router.post("", response_model=ValidateResponseOut)
async def validate_workload(body: ValidateRequest, session: AsyncSession = Depends(get_db_session)):
    """Static, pre-submission capability check (spec §20-21) against an
    inline workload definition — doesn't require the workload to already be
    registered via POST /v1/workloads, matching `plane validate workload.yaml`'s
    draft-checking use case."""
    try:
        result = await run_service.validate_workload(session, body.workload, body.environment_name)
    except repositories.NotFoundError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    return ValidateResponseOut(valid=result.valid, errors=result.errors)
