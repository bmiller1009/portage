"""Async SQLAlchemy engine/session setup (docs/architecture/spec.md §27,
ADR 0008 — PostgreSQL is the only persistence technology in v0.x).

DATABASE_URL must be a postgresql+asyncpg:// URL, e.g.
postgresql+asyncpg://portage:portage@localhost:5432/portage
"""

import os
from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase


class Base(DeclarativeBase):
    """Shared declarative base for control_plane.models."""


def get_database_url() -> str:
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError(
            "DATABASE_URL is not set (expected a postgresql+asyncpg:// URL)"
        )
    return url


def make_engine(database_url: str | None = None) -> AsyncEngine:
    return create_async_engine(database_url or get_database_url(), pool_pre_ping=True)


def make_session_maker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


# Module-level engine/session-maker, built lazily so importing this module
# (e.g. for Alembic autogenerate) doesn't require DATABASE_URL to be set.
_engine: AsyncEngine | None = None
_session_maker: async_sessionmaker[AsyncSession] | None = None


def get_session_maker() -> async_sessionmaker[AsyncSession]:
    global _engine, _session_maker
    if _session_maker is None:
        _engine = make_engine()
        _session_maker = make_session_maker(_engine)
    return _session_maker


async def get_db_session() -> AsyncGenerator[AsyncSession]:
    """FastAPI dependency — yields a session, one per request."""
    session_maker = get_session_maker()
    async with session_maker() as session:
        yield session
