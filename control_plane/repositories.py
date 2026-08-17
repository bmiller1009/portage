"""CRUD functions for the persistence layer, used by the API routers and
(later) the reconciler. Deliberately thin — no business logic beyond
"does this already exist" duplicate checks; validation against the
portable spec schemas happens in the API layer, not here.
"""

from typing import Literal, overload

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from control_plane.models import (
    DatasetBinding,
    Environment,
    ExecutionProfile,
    StorageProfile,
    WorkloadDefinition,
)


class AlreadyExistsError(Exception):
    pass


class NotFoundError(Exception):
    pass


# --- ExecutionProfile ---------------------------------------------------


async def create_execution_profile(
    session: AsyncSession, *, name: str, provider: str, config: dict
) -> ExecutionProfile:
    if await get_execution_profile(session, name, required=False) is not None:
        raise AlreadyExistsError(f"execution profile '{name}' already exists")
    profile = ExecutionProfile(name=name, provider=provider, config=config)
    session.add(profile)
    await session.commit()
    await session.refresh(profile)
    return profile


@overload
async def get_execution_profile(
    session: AsyncSession, name: str, *, required: Literal[True] = True
) -> ExecutionProfile: ...
@overload
async def get_execution_profile(
    session: AsyncSession, name: str, *, required: Literal[False]
) -> ExecutionProfile | None: ...
async def get_execution_profile(
    session: AsyncSession, name: str, *, required: bool = True
) -> ExecutionProfile | None:
    result = await session.execute(select(ExecutionProfile).where(ExecutionProfile.name == name))
    profile = result.scalar_one_or_none()
    if profile is None and required:
        raise NotFoundError(f"execution profile '{name}' not found")
    return profile


async def list_execution_profiles(session: AsyncSession) -> list[ExecutionProfile]:
    result = await session.execute(select(ExecutionProfile).order_by(ExecutionProfile.name))
    return list(result.scalars().all())


# --- StorageProfile ------------------------------------------------------


async def create_storage_profile(
    session: AsyncSession,
    *,
    name: str,
    provider: str,
    config: dict,
    credential_reference: dict,
) -> StorageProfile:
    if await get_storage_profile(session, name, required=False) is not None:
        raise AlreadyExistsError(f"storage profile '{name}' already exists")
    profile = StorageProfile(
        name=name, provider=provider, config=config, credential_reference=credential_reference
    )
    session.add(profile)
    await session.commit()
    await session.refresh(profile)
    return profile


@overload
async def get_storage_profile(
    session: AsyncSession, name: str, *, required: Literal[True] = True
) -> StorageProfile: ...
@overload
async def get_storage_profile(
    session: AsyncSession, name: str, *, required: Literal[False]
) -> StorageProfile | None: ...
async def get_storage_profile(
    session: AsyncSession, name: str, *, required: bool = True
) -> StorageProfile | None:
    result = await session.execute(select(StorageProfile).where(StorageProfile.name == name))
    profile = result.scalar_one_or_none()
    if profile is None and required:
        raise NotFoundError(f"storage profile '{name}' not found")
    return profile


async def list_storage_profiles(session: AsyncSession) -> list[StorageProfile]:
    result = await session.execute(select(StorageProfile).order_by(StorageProfile.name))
    return list(result.scalars().all())


# --- Environment -----------------------------------------------------------


async def create_environment(
    session: AsyncSession,
    *,
    name: str,
    execution_provider: str,
    execution_profile_name: str,
    storage_provider: str,
    storage_profile_name: str,
) -> Environment:
    if await get_environment(session, name, required=False) is not None:
        raise AlreadyExistsError(f"environment '{name}' already exists")
    # Referential integrity beyond the DB FK — surface a clear error rather
    # than a raw IntegrityError from a dangling profile reference.
    await get_execution_profile(session, execution_profile_name)
    await get_storage_profile(session, storage_profile_name)
    environment = Environment(
        name=name,
        execution_provider=execution_provider,
        execution_profile_name=execution_profile_name,
        storage_provider=storage_provider,
        storage_profile_name=storage_profile_name,
    )
    session.add(environment)
    await session.commit()
    await session.refresh(environment)
    return environment


@overload
async def get_environment(
    session: AsyncSession, name: str, *, required: Literal[True] = True
) -> Environment: ...
@overload
async def get_environment(
    session: AsyncSession, name: str, *, required: Literal[False]
) -> Environment | None: ...
async def get_environment(
    session: AsyncSession, name: str, *, required: bool = True
) -> Environment | None:
    result = await session.execute(select(Environment).where(Environment.name == name))
    environment = result.scalar_one_or_none()
    if environment is None and required:
        raise NotFoundError(f"environment '{name}' not found")
    return environment


async def list_environments(session: AsyncSession) -> list[Environment]:
    result = await session.execute(select(Environment).order_by(Environment.name))
    return list(result.scalars().all())


# --- DatasetBinding --------------------------------------------------------


async def create_dataset_binding(
    session: AsyncSession,
    *,
    dataset_name: str,
    environment_name: str,
    kind: str,
    uri: str,
) -> DatasetBinding:
    await get_environment(session, environment_name)
    existing = await get_dataset_binding(session, dataset_name, environment_name, required=False)
    if existing is not None:
        raise AlreadyExistsError(
            f"dataset '{dataset_name}' already has a binding for environment '{environment_name}'"
        )
    binding = DatasetBinding(
        dataset_name=dataset_name, environment_name=environment_name, kind=kind, uri=uri
    )
    session.add(binding)
    await session.commit()
    await session.refresh(binding)
    return binding


@overload
async def get_dataset_binding(
    session: AsyncSession, dataset_name: str, environment_name: str, *, required: Literal[True] = True
) -> DatasetBinding: ...
@overload
async def get_dataset_binding(
    session: AsyncSession, dataset_name: str, environment_name: str, *, required: Literal[False]
) -> DatasetBinding | None: ...
async def get_dataset_binding(
    session: AsyncSession, dataset_name: str, environment_name: str, *, required: bool = True
) -> DatasetBinding | None:
    result = await session.execute(
        select(DatasetBinding).where(
            DatasetBinding.dataset_name == dataset_name,
            DatasetBinding.environment_name == environment_name,
        )
    )
    binding = result.scalar_one_or_none()
    if binding is None and required:
        raise NotFoundError(
            f"no binding for dataset '{dataset_name}' in environment '{environment_name}'"
        )
    return binding


async def list_dataset_bindings(
    session: AsyncSession, *, dataset_name: str | None = None
) -> list[DatasetBinding]:
    query = select(DatasetBinding)
    if dataset_name is not None:
        query = query.where(DatasetBinding.dataset_name == dataset_name)
    result = await session.execute(query.order_by(DatasetBinding.dataset_name, DatasetBinding.environment_name))
    return list(result.scalars().all())


# --- WorkloadDefinition ------------------------------------------------


async def create_workload_definition(
    session: AsyncSession, *, name: str, version: str, definition: dict
) -> WorkloadDefinition:
    result = await session.execute(
        select(WorkloadDefinition).where(
            WorkloadDefinition.name == name, WorkloadDefinition.version == version
        )
    )
    if result.scalar_one_or_none() is not None:
        raise AlreadyExistsError(f"workload '{name}' version '{version}' already exists")
    workload = WorkloadDefinition(name=name, version=version, definition=definition)
    session.add(workload)
    await session.commit()
    await session.refresh(workload)
    return workload


@overload
async def get_workload_definition(
    session: AsyncSession,
    name: str,
    *,
    version: str | None = None,
    required: Literal[True] = True,
) -> WorkloadDefinition: ...
@overload
async def get_workload_definition(
    session: AsyncSession,
    name: str,
    *,
    version: str | None = None,
    required: Literal[False],
) -> WorkloadDefinition | None: ...
async def get_workload_definition(
    session: AsyncSession, name: str, *, version: str | None = None, required: bool = True
) -> WorkloadDefinition | None:
    query = select(WorkloadDefinition).where(WorkloadDefinition.name == name)
    if version is not None:
        query = query.where(WorkloadDefinition.version == version)
    query = query.order_by(WorkloadDefinition.created_at.desc())
    result = await session.execute(query)
    workload = result.scalars().first()
    if workload is None and required:
        raise NotFoundError(f"workload '{name}' not found")
    return workload


async def list_workload_definitions(session: AsyncSession) -> list[WorkloadDefinition]:
    result = await session.execute(
        select(WorkloadDefinition).order_by(WorkloadDefinition.name, WorkloadDefinition.version)
    )
    return list(result.scalars().all())
