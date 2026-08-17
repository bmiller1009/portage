"""Fixtures for real-Postgres persistence tests. Requires DATABASE_URL and
an already-migrated schema (`alembic upgrade head`) — see
.github/workflows/ci.yml's persistence-tests job, or run a local Postgres
(e.g. on the remote dev box) and migrate it by hand.
"""

import os
import uuid
from pathlib import Path

import pytest
import pytest_asyncio

from control_plane import repositories
from control_plane.db import make_engine, make_session_maker
from spec.workload.v1alpha1 import parse_workload

pytestmark = pytest.mark.skipif(
    "DATABASE_URL" not in os.environ, reason="requires a live Postgres (DATABASE_URL env var)"
)

EXAMPLES_DIR = Path(__file__).resolve().parents[3] / "examples"


def unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


@pytest_asyncio.fixture
async def session():
    engine = make_engine()
    session_maker = make_session_maker(engine)
    async with session_maker() as s:
        yield s
    await engine.dispose()


@pytest_asyncio.fixture
async def environment_name(session) -> str:
    """A fully-seeded Environment (+ its ExecutionProfile/StorageProfile),
    ready to reference from a run — used by both run-persistence and
    reconciler tests."""
    exec_profile = await repositories.create_execution_profile(
        session,
        name=unique("exec"),
        provider="kubernetes",
        config={"namespace": "default", "service_account": "spark", "image": "portage/wordcount:0.1.0"},
    )
    storage_profile = await repositories.create_storage_profile(
        session,
        name=unique("storage"),
        provider="s3",
        config={"endpoint_url": "http://minio.local:9000"},
        credential_reference={"provider": "env", "reference": "PORTAGE_TEST"},
    )
    name = unique("k8s-remote")
    await repositories.create_environment(
        session,
        name=name,
        execution_provider="kubernetes",
        execution_profile_name=exec_profile.name,
        storage_provider="s3",
        storage_profile_name=storage_profile.name,
    )
    return name


@pytest_asyncio.fixture
async def workload_ref(session) -> tuple[str, str]:
    """A persisted WorkloadDefinition built from the real wordcount fixture
    (not a hand-rolled dict — reuses the same file the live Kubernetes run
    used). Returns (name, version)."""
    workload = parse_workload(EXAMPLES_DIR / "wordcount.yaml")
    name = unique("wordcount")
    await repositories.create_workload_definition(
        session, name=name, version=workload.metadata.version, definition=workload.model_dump()
    )
    return name, workload.metadata.version
