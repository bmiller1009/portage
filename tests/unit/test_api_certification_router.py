"""Router-layer test for GET /v1/conformance/report —
control_plane.certification.generate_certification_report is
monkeypatched, no database."""

from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from api.main import app
from control_plane import certification
from control_plane.certification import CertificationReport, CertificationRow
from control_plane.db import get_db_session
from tests.unit.conftest import fake_session

app.dependency_overrides[get_db_session] = fake_session
client = TestClient(app)


def test_report_returns_rows(monkeypatch):
    mock_report = AsyncMock(
        return_value=CertificationReport(
            rows=[
                CertificationRow("kubernetes", "s3", "PASS"),
                CertificationRow("databricks", "s3", "BLOCKED", "translation-layer only"),
            ]
        )
    )
    monkeypatch.setattr(certification, "generate_certification_report", mock_report)

    resp = client.get("/v1/conformance/report")

    assert resp.status_code == 200
    rows = resp.json()["rows"]
    assert rows[0]["status"] == "PASS"
    assert rows[1]["status"] == "BLOCKED"
    assert rows[1]["detail"] == "translation-layer only"
