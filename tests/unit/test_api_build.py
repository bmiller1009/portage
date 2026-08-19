"""GET /v1/build (v1.0.0 release identity) — router-layer test, same pattern
as test_api_audit_router.py: no database involved, this endpoint doesn't
touch one."""

from fastapi.testclient import TestClient

from api.main import app

client = TestClient(app)


def test_build_endpoint_reports_version_and_workload_spec_version():
    resp = client.get("/v1/build")
    assert resp.status_code == 200
    body = resp.json()
    assert body["version"] == "1.0.0"
    assert body["workload_spec_version"] == "runtime/v1"
    assert "git_sha" in body
    assert "build_time" in body


def test_build_endpoint_reports_unknown_git_sha_outside_a_built_image(monkeypatch):
    monkeypatch.delenv("PORTAGE_GIT_SHA", raising=False)
    monkeypatch.delenv("PORTAGE_BUILD_TIME", raising=False)
    resp = client.get("/v1/build")
    body = resp.json()
    assert body["git_sha"] == "unknown"
    assert body["build_time"] == "unknown"
