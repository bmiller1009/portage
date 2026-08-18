"""Shared test infrastructure for router-layer tests (test_api_*.py).

api.main.app is a single, module-wide FastAPI app object — every router
test file's `app.dependency_overrides[get_db_session] = ...` assignment
mutates the SAME dict, so whichever file's module-level assignment runs
last during pytest collection silently wins for every other file too.
Centralizing the fake session here means every file's assignment is
equivalent, so import order no longer matters.
"""


class FakeSession:
    """No real DB — just enough surface for the post-audit
    session.refresh(obj) call every mutating router makes (see
    api/routers/runs.py's create_run() for why that's needed: recording
    an audit event commits, which expires every other object still
    attached to the session under SQLAlchemy's default expire_on_commit)."""

    async def refresh(self, obj):
        pass


async def fake_session():
    yield FakeSession()
