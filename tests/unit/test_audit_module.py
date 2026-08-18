"""Unit test for control_plane/audit.py's thin wrapper — confirms it
forwards to repositories.create_audit_event with a real generated
correlation_id, and doesn't require any of api/auth.py (control_plane/
must never import from api/)."""

from typing import cast
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from control_plane import audit, repositories


@pytest.mark.asyncio
async def test_record_audit_event_forwards_to_repository(monkeypatch):
    mock_create = AsyncMock()
    monkeypatch.setattr(repositories, "create_audit_event", mock_create)

    await audit.record_audit_event(
        session=cast(AsyncSession, object()),
        identity="brad@example.com",
        action="RUN_CANCEL",
        resource="claims-normalization/8fa22",
        environment_name="onprem-prod",
        result=audit.RESULT_SUCCESS,
        source="oidc",
    )

    mock_create.assert_awaited_once()
    _args, kwargs = mock_create.call_args
    assert kwargs["identity"] == "brad@example.com"
    assert kwargs["action"] == "RUN_CANCEL"
    assert kwargs["resource"] == "claims-normalization/8fa22"
    assert kwargs["environment_name"] == "onprem-prod"
    assert kwargs["result"] == "SUCCESS"
    assert kwargs["source"] == "oidc"
    # A real, unique correlation id per call — not hardcoded or reused.
    assert kwargs["correlation_id"]


@pytest.mark.asyncio
async def test_record_audit_event_generates_distinct_correlation_ids(monkeypatch):
    mock_create = AsyncMock()
    monkeypatch.setattr(repositories, "create_audit_event", mock_create)

    for _ in range(2):
        await audit.record_audit_event(
            session=cast(AsyncSession, object()),
            identity="x",
            action="RUN_SUBMIT",
            resource="y",
            environment_name=None,
            result=audit.RESULT_SUCCESS,
            source="unauthenticated",
        )

    ids = [call.kwargs["correlation_id"] for call in mock_create.call_args_list]
    assert ids[0] != ids[1]


def test_control_plane_audit_does_not_import_api():
    """control_plane/ never depends on api/ — the reverse is the
    established direction everywhere else in this codebase."""
    import ast
    from pathlib import Path

    source = Path(audit.__file__).read_text()
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            assert not node.module.startswith("api"), f"control_plane/audit.py imports {node.module}"
        if isinstance(node, ast.Import):
            for alias in node.names:
                assert not alias.name.startswith("api"), f"control_plane/audit.py imports {alias.name}"
