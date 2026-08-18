"""CRUD functions for the persistence layer, used by the API routers and
(later) the reconciler. Deliberately thin — no business logic beyond
"does this already exist" duplicate checks; validation against the
portable spec schemas happens in the API layer, not here.
"""

import uuid
from datetime import UTC, datetime, timedelta
from typing import Literal, overload

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from control_plane.models import (
    ArtifactBinding,
    AuditEvent,
    DatasetBinding,
    Environment,
    ExecutionProfile,
    IdempotencyKey,
    ProviderRun,
    Run,
    RunEvent,
    StorageProfile,
    WorkloadDefinition,
)
from control_plane.run_state import RunState


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


# --- ArtifactBinding ---------------------------------------------------


async def create_artifact_binding(
    session: AsyncSession,
    *,
    artifact_name: str,
    artifact_version: str,
    environment_name: str,
    kind: str,
    uri: str,
) -> ArtifactBinding:
    await get_environment(session, environment_name)
    existing = await get_artifact_binding(
        session, artifact_name, artifact_version, environment_name, required=False
    )
    if existing is not None:
        raise AlreadyExistsError(
            f"artifact '{artifact_name}/{artifact_version}' already has a binding for "
            f"environment '{environment_name}'"
        )
    binding = ArtifactBinding(
        artifact_name=artifact_name,
        artifact_version=artifact_version,
        environment_name=environment_name,
        kind=kind,
        uri=uri,
    )
    session.add(binding)
    await session.commit()
    await session.refresh(binding)
    return binding


@overload
async def get_artifact_binding(
    session: AsyncSession,
    artifact_name: str,
    artifact_version: str,
    environment_name: str,
    *,
    required: Literal[True] = True,
) -> ArtifactBinding: ...
@overload
async def get_artifact_binding(
    session: AsyncSession,
    artifact_name: str,
    artifact_version: str,
    environment_name: str,
    *,
    required: Literal[False],
) -> ArtifactBinding | None: ...
async def get_artifact_binding(
    session: AsyncSession,
    artifact_name: str,
    artifact_version: str,
    environment_name: str,
    *,
    required: bool = True,
) -> ArtifactBinding | None:
    result = await session.execute(
        select(ArtifactBinding).where(
            ArtifactBinding.artifact_name == artifact_name,
            ArtifactBinding.artifact_version == artifact_version,
            ArtifactBinding.environment_name == environment_name,
        )
    )
    binding = result.scalar_one_or_none()
    if binding is None and required:
        raise NotFoundError(
            f"no binding for artifact '{artifact_name}/{artifact_version}' in "
            f"environment '{environment_name}'"
        )
    return binding


async def list_artifact_bindings(
    session: AsyncSession, *, artifact_name: str | None = None
) -> list[ArtifactBinding]:
    query = select(ArtifactBinding)
    if artifact_name is not None:
        query = query.where(ArtifactBinding.artifact_name == artifact_name)
    result = await session.execute(
        query.order_by(
            ArtifactBinding.artifact_name, ArtifactBinding.artifact_version, ArtifactBinding.environment_name
        )
    )
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


# --- Run lifecycle (spec §23-25) -------------------------------------------


async def create_run(
    session: AsyncSession, *, workload_name: str, workload_version: str, environment_name: str, state: str
) -> Run:
    run = Run(
        workload_name=workload_name,
        workload_version=workload_version,
        environment_name=environment_name,
        state=state,
    )
    session.add(run)
    await session.commit()
    await session.refresh(run)
    return run


async def get_run(session: AsyncSession, run_id: uuid.UUID) -> Run:
    result = await session.execute(select(Run).where(Run.id == run_id))
    run = result.scalar_one_or_none()
    if run is None:
        raise NotFoundError(f"run '{run_id}' not found")
    return run


async def list_runs_by_state(session: AsyncSession, states: list[str]) -> list[Run]:
    result = await session.execute(select(Run).where(Run.state.in_(states)).order_by(Run.created_at))
    return list(result.scalars().all())


# A run stuck in SUBMITTING longer than this was almost certainly abandoned
# by a crashed reconciler process mid-submission (spec §57's Control Plane
# Recovery Test) rather than one that's merely slow right now — comfortably
# longer than one reconcile tick at the default 5s interval.
_STUCK_SUBMITTING_GRACE_SECONDS = 30


async def claim_runs_for_submission(session: AsyncSession) -> list[Run]:
    """Atomically claims runs to (re-)submit: fresh ACCEPTED runs, plus any
    run stuck in SUBMITTING past the grace period (a prior submission
    attempt crashed between provider.submit() succeeding and this
    transition being persisted — spec §57). `SELECT ... FOR UPDATE SKIP
    LOCKED` lets concurrent reconciler replicas (HA deployment, spec §67)
    each claim disjoint rows instead of racing on the same one.

    Every claimed run is transitioned to SUBMITTING and committed exactly
    ONCE, as a batch, in this same function — deliberately not via
    transition_run_state()/update_run_state(), which each commit
    individually and would release the FOR UPDATE lock on the
    not-yet-processed rows after the very first commit, reopening the
    exact race this function exists to close."""
    cutoff = datetime.now(UTC) - timedelta(seconds=_STUCK_SUBMITTING_GRACE_SECONDS)
    result = await session.execute(
        select(Run)
        .where(
            or_(
                Run.state == RunState.ACCEPTED.value,
                (Run.state == RunState.SUBMITTING.value) & (Run.updated_at < cutoff),
            )
        )
        .order_by(Run.created_at)
        .with_for_update(skip_locked=True)
    )
    runs = list(result.scalars().all())
    for run in runs:
        from_state = run.state
        run.state = RunState.SUBMITTING.value
        session.add(
            RunEvent(
                run_id=run.id,
                from_state=from_state,
                to_state=RunState.SUBMITTING.value,
                message="claimed for submission",
            )
        )
    await session.commit()
    for run in runs:
        await session.refresh(run)
    return runs


async def increment_submission_attempts(session: AsyncSession, run: Run) -> int:
    run.submission_attempts += 1
    await session.commit()
    await session.refresh(run)
    return run.submission_attempts


async def list_runs(
    session: AsyncSession, *, environment_name: str | None = None, limit: int = 100
) -> list[Run]:
    """Most-recent-first (spec §32's Runs page lists recent activity, not
    a full history browser) — unlike list_runs_by_state, which orders
    oldest-first since the reconciler processes runs in submission order."""
    query = select(Run)
    if environment_name is not None:
        query = query.where(Run.environment_name == environment_name)
    result = await session.execute(query.order_by(Run.created_at.desc()).limit(limit))
    return list(result.scalars().all())


async def update_run_state(session: AsyncSession, run: Run, new_state: str) -> None:
    run.state = new_state
    await session.commit()


async def create_provider_run(
    session: AsyncSession, *, run_id: uuid.UUID, provider_run_id: str, provider: str, raw: dict
) -> ProviderRun:
    provider_run = ProviderRun(run_id=run_id, provider_run_id=provider_run_id, provider=provider, raw=raw)
    session.add(provider_run)
    await session.commit()
    await session.refresh(provider_run)
    return provider_run


async def get_latest_provider_run(session: AsyncSession, run_id: uuid.UUID) -> ProviderRun | None:
    result = await session.execute(
        select(ProviderRun)
        .where(ProviderRun.run_id == run_id)
        .order_by(ProviderRun.created_at.desc())
    )
    return result.scalars().first()


async def create_run_event(
    session: AsyncSession,
    *,
    run_id: uuid.UUID,
    from_state: str | None,
    to_state: str,
    message: str | None = None,
) -> RunEvent:
    event = RunEvent(run_id=run_id, from_state=from_state, to_state=to_state, message=message)
    session.add(event)
    await session.commit()
    await session.refresh(event)
    return event


async def list_run_events(session: AsyncSession, run_id: uuid.UUID) -> list[RunEvent]:
    result = await session.execute(
        select(RunEvent).where(RunEvent.run_id == run_id).order_by(RunEvent.created_at)
    )
    return list(result.scalars().all())


async def get_idempotency_key(session: AsyncSession, key: str) -> IdempotencyKey | None:
    result = await session.execute(select(IdempotencyKey).where(IdempotencyKey.key == key))
    return result.scalar_one_or_none()


async def create_idempotency_key(session: AsyncSession, *, key: str, run_id: uuid.UUID) -> IdempotencyKey:
    record = IdempotencyKey(key=key, run_id=run_id)
    session.add(record)
    await session.commit()
    await session.refresh(record)
    return record


async def create_audit_event(
    session: AsyncSession,
    *,
    identity: str,
    action: str,
    resource: str,
    environment_name: str | None,
    result: str,
    source: str,
    correlation_id: str,
) -> AuditEvent:
    event = AuditEvent(
        identity=identity,
        action=action,
        resource=resource,
        environment_name=environment_name,
        result=result,
        source=source,
        correlation_id=correlation_id,
    )
    session.add(event)
    await session.commit()
    await session.refresh(event)
    return event


async def list_audit_events(
    session: AsyncSession,
    *,
    resource: str | None = None,
    environment_name: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    limit: int = 100,
) -> list[AuditEvent]:
    query = select(AuditEvent)
    if resource is not None:
        query = query.where(AuditEvent.resource == resource)
    if environment_name is not None:
        query = query.where(AuditEvent.environment_name == environment_name)
    if since is not None:
        query = query.where(AuditEvent.created_at >= since)
    if until is not None:
        query = query.where(AuditEvent.created_at <= until)
    result = await session.execute(query.order_by(AuditEvent.created_at.desc()).limit(limit))
    return list(result.scalars().all())
