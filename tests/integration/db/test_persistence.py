"""Real-Postgres round-trip tests for the persistence layer (docs/
architecture/spec.md §27). Every test uses a unique random suffix in its
names so the suite is safely re-runnable against a persistent database,
not just a fresh per-CI-run container."""

import uuid

import pytest

from control_plane import repositories


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest.mark.asyncio
async def test_execution_profile_round_trip(session):
    name = _unique("k8s-profile")
    created = await repositories.create_execution_profile(
        session, name=name, provider="kubernetes", config={"namespace": "default"}
    )
    assert created.id is not None

    fetched = await repositories.get_execution_profile(session, name)
    assert fetched.provider == "kubernetes"
    assert fetched.config == {"namespace": "default"}


@pytest.mark.asyncio
async def test_create_execution_profile_duplicate_raises(session):
    name = _unique("dup-profile")
    await repositories.create_execution_profile(session, name=name, provider="kubernetes", config={})

    with pytest.raises(repositories.AlreadyExistsError):
        await repositories.create_execution_profile(session, name=name, provider="kubernetes", config={})


@pytest.mark.asyncio
async def test_get_missing_execution_profile_raises(session):
    with pytest.raises(repositories.NotFoundError):
        await repositories.get_execution_profile(session, _unique("nonexistent"))


@pytest.mark.asyncio
async def test_environment_requires_existing_profiles(session):
    with pytest.raises(repositories.NotFoundError):
        await repositories.create_environment(
            session,
            name=_unique("env"),
            execution_provider="kubernetes",
            execution_profile_name=_unique("ghost-exec"),
            storage_provider="s3",
            storage_profile_name=_unique("ghost-storage"),
        )


@pytest.mark.asyncio
async def test_environment_and_dataset_binding_round_trip(session):
    exec_profile = await repositories.create_execution_profile(
        session, name=_unique("exec"), provider="kubernetes", config={"namespace": "default"}
    )
    storage_profile = await repositories.create_storage_profile(
        session,
        name=_unique("storage"),
        provider="s3",
        config={"endpoint_url": "http://minio.portage-storage.svc.cluster.local:9000"},
        credential_reference={"provider": "env", "reference": "PORTAGE_MINIO_CREDENTIALS"},
    )
    env_name = _unique("k8s-remote")
    environment = await repositories.create_environment(
        session,
        name=env_name,
        execution_provider="kubernetes",
        execution_profile_name=exec_profile.name,
        storage_provider="s3",
        storage_profile_name=storage_profile.name,
    )
    assert environment.name == env_name

    dataset_name = _unique("wordcount.raw")
    binding = await repositories.create_dataset_binding(
        session,
        dataset_name=dataset_name,
        environment_name=env_name,
        kind="path",
        uri="s3a://portage-phase0/wordcount/input.txt",
    )
    assert binding.uri == "s3a://portage-phase0/wordcount/input.txt"

    fetched = await repositories.get_dataset_binding(session, dataset_name, env_name)
    assert fetched.kind == "path"

    bindings_for_dataset = await repositories.list_dataset_bindings(session, dataset_name=dataset_name)
    assert len(bindings_for_dataset) == 1


@pytest.mark.asyncio
async def test_dataset_binding_requires_existing_environment(session):
    with pytest.raises(repositories.NotFoundError):
        await repositories.create_dataset_binding(
            session,
            dataset_name=_unique("orphan.dataset"),
            environment_name=_unique("ghost-env"),
            kind="path",
            uri="s3a://bucket/key",
        )


@pytest.mark.asyncio
async def test_workload_definition_round_trip_and_latest_version(session):
    name = _unique("wordcount")
    await repositories.create_workload_definition(
        session, name=name, version="0.1.0", definition={"metadata": {"name": name, "version": "0.1.0"}}
    )
    await repositories.create_workload_definition(
        session, name=name, version="0.2.0", definition={"metadata": {"name": name, "version": "0.2.0"}}
    )

    latest = await repositories.get_workload_definition(session, name)
    assert latest.version == "0.2.0"

    specific = await repositories.get_workload_definition(session, name, version="0.1.0")
    assert specific.version == "0.1.0"


@pytest.mark.asyncio
async def test_create_duplicate_workload_version_raises(session):
    name = _unique("dup-workload")
    await repositories.create_workload_definition(
        session, name=name, version="1.0.0", definition={"metadata": {"name": name}}
    )

    with pytest.raises(repositories.AlreadyExistsError):
        await repositories.create_workload_definition(
            session, name=name, version="1.0.0", definition={"metadata": {"name": name}}
        )


# --- CRUD completeness (v0.6.2) -- update/delete for every config resource,
# including the real IntegrityError -> InUseError translation, which only a
# real Postgres FK constraint can actually exercise. -----------------------


@pytest.mark.asyncio
async def test_update_execution_profile(session):
    name = _unique("exec")
    await repositories.create_execution_profile(session, name=name, provider="kubernetes", config={"a": 1})

    updated = await repositories.update_execution_profile(
        session, name, provider="databricks", config={"b": 2}
    )

    assert updated.provider == "databricks"
    assert updated.config == {"b": 2}


@pytest.mark.asyncio
async def test_delete_execution_profile(session):
    name = _unique("exec")
    await repositories.create_execution_profile(session, name=name, provider="kubernetes", config={})

    await repositories.delete_execution_profile(session, name)

    with pytest.raises(repositories.NotFoundError):
        await repositories.get_execution_profile(session, name)


@pytest.mark.asyncio
async def test_delete_execution_profile_in_use_raises(session):
    exec_profile = await repositories.create_execution_profile(
        session, name=_unique("exec"), provider="kubernetes", config={}
    )
    storage_profile = await repositories.create_storage_profile(
        session, name=_unique("storage"), provider="s3", config={}, credential_reference={}
    )
    await repositories.create_environment(
        session,
        name=_unique("env"),
        execution_provider="kubernetes",
        execution_profile_name=exec_profile.name,
        storage_provider="s3",
        storage_profile_name=storage_profile.name,
    )

    with pytest.raises(repositories.InUseError):
        await repositories.delete_execution_profile(session, exec_profile.name)


@pytest.mark.asyncio
async def test_update_storage_profile(session):
    name = _unique("storage")
    await repositories.create_storage_profile(
        session, name=name, provider="s3", config={"a": 1}, credential_reference={"provider": "env"}
    )

    updated = await repositories.update_storage_profile(
        session, name, provider="adls", config={"b": 2}, credential_reference={"provider": "workload-identity"}
    )

    assert updated.provider == "adls"
    assert updated.config == {"b": 2}
    assert updated.credential_reference == {"provider": "workload-identity"}


@pytest.mark.asyncio
async def test_delete_storage_profile_in_use_raises(session):
    exec_profile = await repositories.create_execution_profile(
        session, name=_unique("exec"), provider="kubernetes", config={}
    )
    storage_profile = await repositories.create_storage_profile(
        session, name=_unique("storage"), provider="s3", config={}, credential_reference={}
    )
    await repositories.create_environment(
        session,
        name=_unique("env"),
        execution_provider="kubernetes",
        execution_profile_name=exec_profile.name,
        storage_provider="s3",
        storage_profile_name=storage_profile.name,
    )

    with pytest.raises(repositories.InUseError):
        await repositories.delete_storage_profile(session, storage_profile.name)


@pytest.mark.asyncio
async def test_update_environment(session):
    exec_profile_1 = await repositories.create_execution_profile(
        session, name=_unique("exec"), provider="kubernetes", config={}
    )
    exec_profile_2 = await repositories.create_execution_profile(
        session, name=_unique("exec"), provider="kubernetes", config={}
    )
    storage_profile = await repositories.create_storage_profile(
        session, name=_unique("storage"), provider="s3", config={}, credential_reference={}
    )
    env_name = _unique("env")
    await repositories.create_environment(
        session,
        name=env_name,
        execution_provider="kubernetes",
        execution_profile_name=exec_profile_1.name,
        storage_provider="s3",
        storage_profile_name=storage_profile.name,
    )

    updated = await repositories.update_environment(
        session,
        env_name,
        execution_provider="kubernetes",
        execution_profile_name=exec_profile_2.name,
        storage_provider="s3",
        storage_profile_name=storage_profile.name,
    )

    assert updated.execution_profile_name == exec_profile_2.name


@pytest.mark.asyncio
async def test_delete_environment(session):
    exec_profile = await repositories.create_execution_profile(
        session, name=_unique("exec"), provider="kubernetes", config={}
    )
    storage_profile = await repositories.create_storage_profile(
        session, name=_unique("storage"), provider="s3", config={}, credential_reference={}
    )
    env_name = _unique("env")
    await repositories.create_environment(
        session,
        name=env_name,
        execution_provider="kubernetes",
        execution_profile_name=exec_profile.name,
        storage_provider="s3",
        storage_profile_name=storage_profile.name,
    )

    await repositories.delete_environment(session, env_name)

    with pytest.raises(repositories.NotFoundError):
        await repositories.get_environment(session, env_name)


@pytest.mark.asyncio
async def test_update_dataset_binding(session):
    exec_profile = await repositories.create_execution_profile(
        session, name=_unique("exec"), provider="kubernetes", config={}
    )
    storage_profile = await repositories.create_storage_profile(
        session, name=_unique("storage"), provider="s3", config={}, credential_reference={}
    )
    env_name = _unique("env")
    await repositories.create_environment(
        session,
        name=env_name,
        execution_provider="kubernetes",
        execution_profile_name=exec_profile.name,
        storage_provider="s3",
        storage_profile_name=storage_profile.name,
    )
    dataset_name = _unique("dataset")
    await repositories.create_dataset_binding(
        session, dataset_name=dataset_name, environment_name=env_name, kind="path", uri="s3a://bucket/old"
    )

    updated = await repositories.update_dataset_binding(
        session, dataset_name, env_name, kind="table", uri="analytics.new"
    )

    assert updated.kind == "table"
    assert updated.uri == "analytics.new"


@pytest.mark.asyncio
async def test_delete_dataset_binding(session):
    exec_profile = await repositories.create_execution_profile(
        session, name=_unique("exec"), provider="kubernetes", config={}
    )
    storage_profile = await repositories.create_storage_profile(
        session, name=_unique("storage"), provider="s3", config={}, credential_reference={}
    )
    env_name = _unique("env")
    await repositories.create_environment(
        session,
        name=env_name,
        execution_provider="kubernetes",
        execution_profile_name=exec_profile.name,
        storage_provider="s3",
        storage_profile_name=storage_profile.name,
    )
    dataset_name = _unique("dataset")
    await repositories.create_dataset_binding(
        session, dataset_name=dataset_name, environment_name=env_name, kind="path", uri="s3a://bucket/x"
    )

    await repositories.delete_dataset_binding(session, dataset_name, env_name)

    with pytest.raises(repositories.NotFoundError):
        await repositories.get_dataset_binding(session, dataset_name, env_name)


@pytest.mark.asyncio
async def test_update_workload_definition(session):
    name = _unique("wordcount")
    await repositories.create_workload_definition(
        session, name=name, version="0.1.0", definition={"metadata": {"name": name}, "revision": 1}
    )

    updated = await repositories.update_workload_definition(
        session, name, "0.1.0", definition={"metadata": {"name": name}, "revision": 2}
    )

    assert updated.definition["revision"] == 2


@pytest.mark.asyncio
async def test_delete_workload_definition(session):
    name = _unique("wordcount")
    await repositories.create_workload_definition(
        session, name=name, version="0.1.0", definition={"metadata": {"name": name}}
    )

    await repositories.delete_workload_definition(session, name, "0.1.0")

    with pytest.raises(repositories.NotFoundError):
        await repositories.get_workload_definition(session, name, version="0.1.0")
