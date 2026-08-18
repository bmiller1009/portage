"""Router-layer test for POST /v1/conformance/compare —
control_plane.conformance.compare_runs is monkeypatched, no database."""

import uuid
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from api.main import app
from control_plane import conformance
from control_plane.conformance import ConformancePairResult, ConformanceReport
from control_plane.db import get_db_session
from tests.unit.conftest import fake_session

app.dependency_overrides[get_db_session] = fake_session
client = TestClient(app)


def test_compare_returns_the_report(monkeypatch):
    mock_compare = AsyncMock(
        return_value=ConformanceReport(
            results=[ConformancePairResult("k8s-remote", "k8s-vast", "PASS", [])]
        )
    )
    monkeypatch.setattr(conformance, "compare_runs", mock_compare)

    resp = client.post(
        "/v1/conformance/compare",
        json={"run_ids": [str(uuid.uuid4()), str(uuid.uuid4())], "output_name": "counts"},
    )

    assert resp.status_code == 200
    body = resp.json()
    assert body["results"][0]["status"] == "PASS"
    assert body["results"][0]["left_environment"] == "k8s-remote"
    mock_compare.assert_awaited_once()
    assert mock_compare.await_args is not None
    assert mock_compare.await_args.kwargs["output_name"] == "counts"


def test_compare_reports_blocked_and_fail_statuses(monkeypatch):
    monkeypatch.setattr(
        conformance,
        "compare_runs",
        AsyncMock(
            return_value=ConformanceReport(
                results=[
                    ConformancePairResult("a", "b", "FAIL", ["row count mismatch"]),
                    ConformancePairResult("a", "c", "BLOCKED", ["run not SUCCEEDED (state=RUNNING)"]),
                ]
            )
        ),
    )

    resp = client.post("/v1/conformance/compare", json={"run_ids": [str(uuid.uuid4())] * 3})

    assert resp.status_code == 200
    statuses = [r["status"] for r in resp.json()["results"]]
    assert statuses == ["FAIL", "BLOCKED"]
