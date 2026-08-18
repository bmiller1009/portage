from dataclasses import asdict

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from api.schemas import ProviderCapabilitiesOut, ProviderOut
from control_plane import provider_factory, repositories
from control_plane.db import get_db_session

router = APIRouter(prefix="/v1/providers", tags=["providers"])


@router.get("", response_model=list[ProviderOut])
async def list_providers(session: AsyncSession = Depends(get_db_session)):
    """Every registered ExecutionProfile/StorageProfile is a "provider"
    instance in the UI sense (spec §32's Providers page) — there's no
    separate provider-registry table, this just relabels the two profile
    tables under one combined view."""
    execution_profiles = await repositories.list_execution_profiles(session)
    storage_profiles = await repositories.list_storage_profiles(session)
    return [
        *(ProviderOut(name=p.name, kind="execution", provider=p.provider) for p in execution_profiles),
        *(ProviderOut(name=p.name, kind="storage", provider=p.provider) for p in storage_profiles),
    ]


@router.get("/{name}/capabilities", response_model=ProviderCapabilitiesOut)
async def get_provider_capabilities(name: str, session: AsyncSession = Depends(get_db_session)):
    """Read-only against the provider (a live capabilities() call), so —
    like GET /v1/runs/{id}/logs and POST /v1/validate — answered
    synchronously rather than deferred to the reconciler."""
    execution_profile = await repositories.get_execution_profile(session, name, required=False)
    if execution_profile is not None:
        provider = provider_factory.build_execution_provider(execution_profile)
        caps = await provider.capabilities()
        return ProviderCapabilitiesOut(
            name=name, kind="execution", provider=execution_profile.provider, capabilities=asdict(caps)
        )

    storage_profile = await repositories.get_storage_profile(session, name, required=False)
    if storage_profile is not None:
        storage_provider = provider_factory.build_storage_provider(storage_profile)
        storage_caps = storage_provider.capabilities()
        return ProviderCapabilitiesOut(
            name=name, kind="storage", provider=storage_profile.provider, capabilities=asdict(storage_caps)
        )

    raise HTTPException(status_code=404, detail=f"no execution or storage profile named '{name}'")
