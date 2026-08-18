"""Router-layer tests for GET /v1/audit — same pattern as
test_api_routers.py: repositories is monkeypatched, no database."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from api.main import app
from control_plane import repositories
from control_plane.db import get_db_session
from control_plane.models import AuditEvent
from tests.unit.conftest import fake_session

app.dependency_overrides[get_db_session] = fake_session
client = TestClient(app)


def test_list_audit_events_returns_events(monkeypatch):
    mock_list = AsyncMock(
        return_value=[
            AuditEvent(
                identity="brad@example.com",
                action="RUN_CANCEL",
                resource="claims-normalization/8fa22",
                environment_name="onprem-prod",
                result="SUCCESS",
                source="oidc",
                correlation_id="c0rrelation-id",
                created_at=datetime(2026, 8, 15, 16, 24, 18, tzinfo=UTC),
            )
        ]
    )
    monkeypatch.setattr(repositories, "list_audit_events", mock_list)

    resp = client.get("/v1/audit")

    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    assert body[0]["identity"] == "brad@example.com"
    assert body[0]["action"] == "RUN_CANCEL"
    assert body[0]["result"] == "SUCCESS"


def test_list_audit_events_passes_filters_through(monkeypatch):
    mock_list = AsyncMock(return_value=[])
    monkeypatch.setattr(repositories, "list_audit_events", mock_list)

    resp = client.get(
        "/v1/audit", params={"resource": "wordcount/0.1.0", "environment_name": "k8s-remote"}
    )

    assert resp.status_code == 200
    _args, kwargs = mock_list.call_args
    assert kwargs["resource"] == "wordcount/0.1.0"
    assert kwargs["environment_name"] == "k8s-remote"
