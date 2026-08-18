from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from api.auth import ROLE_ANALYST, Identity, require_role
from api.schemas import CertificationReportOut, ConformanceCompareRequest, ConformanceReportOut
from control_plane import certification, conformance
from control_plane.db import get_db_session

router = APIRouter(prefix="/v1/conformance", tags=["conformance"])


@router.post("/compare", response_model=ConformanceReportOut)
async def compare_runs(
    body: ConformanceCompareRequest,
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_ANALYST)),
):
    """Dynamic conformance comparison (spec §21) — resolves each run's
    output location + storage credentials server-side and compares them
    semantically (conformance/comparison.py). Read/verify-only, like
    POST /v1/validate, so Analyst+ rather than Operator+."""
    report = await conformance.compare_runs(session, body.run_ids, output_name=body.output_name)
    return report


@router.get("/report", response_model=CertificationReportOut)
async def certification_report(
    session: AsyncSession = Depends(get_db_session),
    identity: Identity = Depends(require_role(ROLE_ANALYST)),
):
    """Provider certification report (spec §78) — every registered
    execution+storage combination, PASS/FAIL/BLOCKED. Read-only, same
    role as the rest of this router."""
    report = await certification.generate_certification_report(session)
    return report
