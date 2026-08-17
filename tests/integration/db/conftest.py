"""Fixtures for real-Postgres persistence tests. Requires DATABASE_URL and
an already-migrated schema (`alembic upgrade head`) — see
.github/workflows/ci.yml's persistence-tests job, or run a local Postgres
(e.g. on the remote dev box) and migrate it by hand.
"""

import os

import pytest
import pytest_asyncio

from control_plane.db import make_engine, make_session_maker

pytestmark = pytest.mark.skipif(
    "DATABASE_URL" not in os.environ, reason="requires a live Postgres (DATABASE_URL env var)"
)


@pytest_asyncio.fixture
async def session():
    engine = make_engine()
    session_maker = make_session_maker(engine)
    async with session_maker() as s:
        yield s
    await engine.dispose()
