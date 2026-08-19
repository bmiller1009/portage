"""Router-layer tests for POST /v1/validate — run_service.validate_workload
is monkeypatched, no database. The real capability-matching logic is
exercised for real in tests/integration/db/test_validate.py."""

from pathlib import Path
from unittest.mock import AsyncMock

import yaml
from fastapi.testclient import TestClient

from api.main import app
from control_plane import repositories, run_service
from control_plane.db import get_db_session
from control_plane.execution_provider import ValidationResult
from tests.unit.conftest import fake_session

EXAMPLES_DIR = Path(__file__).resolve().parents[2] / "examples"


app.dependency_overrides[get_db_session] = fake_session
client = TestClient(app)


def _workload_dict() -> dict:
    return yaml.safe_load((EXAMPLES_DIR / "wordcount.yaml").read_text())


def test_validate_returns_valid_true_for_compatible_workload(monkeypatch):
    monkeypatch.setattr(
        run_service, "validate_workload", AsyncMock(return_value=ValidationResult(valid=True))
    )

    resp = client.post(
        "/v1/validate", json={"workload": _workload_dict(), "environment_name": "k8s-remote"}
    )

    assert resp.status_code == 200
    assert resp.json() == {
        "valid": True,
        "errors": [],
        "portability_status": "PORTABLE",
        "provider_overrides": {},
    }


def test_validate_returns_valid_false_with_errors(monkeypatch):
    monkeypatch.setattr(
        run_service,
        "validate_workload",
        AsyncMock(return_value=ValidationResult(valid=False, errors=["workload requires GPU, provider does not support it"])),
    )

    resp = client.post(
        "/v1/validate", json={"workload": _workload_dict(), "environment_name": "k8s-remote"}
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["valid"] is False
    assert "GPU" in body["errors"][0]


def test_validate_unknown_environment_returns_422(monkeypatch):
    monkeypatch.setattr(
        run_service,
        "validate_workload",
        AsyncMock(side_effect=repositories.NotFoundError("environment 'ghost' not found")),
    )

    resp = client.post(
        "/v1/validate", json={"workload": _workload_dict(), "environment_name": "ghost"}
    )

    assert resp.status_code == 422


def test_validate_passes_through_portability_status(monkeypatch):
    monkeypatch.setattr(
        run_service,
        "validate_workload",
        AsyncMock(
            return_value=ValidationResult(
                valid=True,
                portability_status="PORTABLE_WITH_OVERRIDES",
                provider_overrides={"kubernetes": 2, "databricks": 0},
            )
        ),
    )

    resp = client.post(
        "/v1/validate", json={"workload": _workload_dict(), "environment_name": "k8s-remote"}
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["portability_status"] == "PORTABLE_WITH_OVERRIDES"
    assert body["provider_overrides"] == {"kubernetes": 2, "databricks": 0}
