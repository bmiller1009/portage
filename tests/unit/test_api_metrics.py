"""Tests for GET /metrics and the api_errors_total middleware (issue #22,
spec §29). No database — same dependency-override pattern as the other
router test files."""

import uuid
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient

from api.main import app
from control_plane import metrics, repositories
from control_plane.db import get_db_session


async def _fake_session():
    yield None


app.dependency_overrides[get_db_session] = _fake_session
client = TestClient(app)


def test_metrics_returns_prometheus_text():
    # OTel's Prometheus bridge only emits a metric once it has at least one
    # recorded data point — force one so this test isn't order-dependent on
    # some other test having already touched the same counter.
    metrics.runs_created_total.add(0)

    resp = client.get("/metrics")

    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/plain")
    assert "portage_runs_created_total" in resp.text


def test_metrics_not_in_openapi_schema():
    schema = client.get("/openapi.json").json()
    assert "/metrics" not in schema["paths"]


def test_middleware_counts_4xx_response(monkeypatch):
    monkeypatch.setattr(
        repositories, "get_run", AsyncMock(side_effect=repositories.NotFoundError("run 'x' not found"))
    )

    resp = client.get(f"/v1/runs/{uuid.uuid4()}")
    assert resp.status_code == 404

    metrics_text = client.get("/metrics").text
    matching = [
        line
        for line in metrics_text.splitlines()
        if line.startswith("portage_api_errors_total{") and 'status_code="404"' in line
    ]
    assert matching, metrics_text


def test_middleware_counts_unhandled_exception_as_500(monkeypatch):
    monkeypatch.setattr(repositories, "get_run", AsyncMock(side_effect=RuntimeError("boom")))

    try:
        client.get(f"/v1/runs/{uuid.uuid4()}")
    except RuntimeError:
        pass  # TestClient re-raises the app's unhandled exception - expected.

    metrics_text = client.get("/metrics").text
    matching = [
        line
        for line in metrics_text.splitlines()
        if line.startswith("portage_api_errors_total{") and 'status_code="500"' in line
    ]
    assert matching, metrics_text
