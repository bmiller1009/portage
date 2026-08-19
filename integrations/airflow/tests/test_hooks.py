"""Unit tests for PortageHook -- httpx is monkeypatched, no real network,
no real Airflow metadata DB (get_connection() is expected to fail in
this bare test environment and fall back to $PORTAGE_API_URL, exercised
directly here)."""

from unittest.mock import MagicMock

import httpx
import pytest
from airflow_provider_portage.hooks.portage import PortageHook


class FakeResponse:
    def __init__(self, status_code: int, json_data=None):
        self.status_code = status_code
        self._json = json_data or {}

    def json(self):
        return self._json

    def raise_for_status(self):
        if self.status_code >= 400:
            request = httpx.Request("GET", "http://test")
            raise httpx.HTTPStatusError("error", request=request, response=self)


@pytest.fixture(autouse=True)
def _api_url(monkeypatch):
    monkeypatch.setenv("PORTAGE_API_URL", "http://test-portage-api")


def test_api_base_url_falls_back_to_env_var():
    hook = PortageHook()
    assert hook._api_base_url() == "http://test-portage-api"


def test_register_workload_posts_and_returns_body(monkeypatch):
    mock_post = MagicMock(return_value=FakeResponse(201, {"name": "wordcount", "version": "0.1.0"}))
    monkeypatch.setattr(httpx, "post", mock_post)

    hook = PortageHook()
    result = hook.register_workload({"metadata": {"name": "wordcount", "version": "0.1.0"}})

    mock_post.assert_called_once()
    assert mock_post.call_args.args[0] == "http://test-portage-api/v1/workloads"
    assert result == {"name": "wordcount", "version": "0.1.0"}


def test_register_workload_tolerates_409_already_registered(monkeypatch):
    monkeypatch.setattr(httpx, "post", MagicMock(return_value=FakeResponse(409)))

    hook = PortageHook()
    workload = {"metadata": {"name": "wordcount", "version": "0.1.0"}}
    result = hook.register_workload(workload)

    assert result == workload


def test_register_workload_raises_on_other_errors(monkeypatch):
    monkeypatch.setattr(httpx, "post", MagicMock(return_value=FakeResponse(500)))

    hook = PortageHook()
    with pytest.raises(httpx.HTTPStatusError):
        hook.register_workload({"metadata": {"name": "x", "version": "0.1.0"}})


def test_submit_run_sends_idempotency_key_header(monkeypatch):
    mock_post = MagicMock(return_value=FakeResponse(200, {"id": "run-123"}))
    monkeypatch.setattr(httpx, "post", mock_post)

    hook = PortageHook()
    result = hook.submit_run(
        workload_name="wordcount", workload_version="0.1.0", environment_name="k8s-remote", idempotency_key="abc"
    )

    assert result == {"id": "run-123"}
    assert mock_post.call_args.kwargs["headers"] == {"Idempotency-Key": "abc"}


def test_get_run(monkeypatch):
    monkeypatch.setattr(httpx, "get", MagicMock(return_value=FakeResponse(200, {"id": "run-123", "state": "RUNNING"})))

    hook = PortageHook()
    result = hook.get_run("run-123")

    assert result["state"] == "RUNNING"


def test_cancel_run(monkeypatch):
    mock_delete = MagicMock(return_value=FakeResponse(200, {"id": "run-123", "state": "CANCELING"}))
    monkeypatch.setattr(httpx, "delete", mock_delete)

    hook = PortageHook()
    result = hook.cancel_run("run-123")

    assert result["state"] == "CANCELING"
