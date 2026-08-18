"""Tests plane conformance report as a pure REST client (spec §31) —
httpx is monkeypatched so no real network or API server is involved."""

from typing import cast

import httpx
import pytest
import typer

from cli.main import conformance_report as plane_conformance_report


class FakeResponse:
    def __init__(self, status_code: int, json_data):
        self.status_code = status_code
        self._json = json_data

    def json(self):
        return self._json

    def raise_for_status(self):
        if self.status_code >= 400:
            request = httpx.Request("GET", "http://test")
            raise httpx.HTTPStatusError("error", request=request, response=cast(httpx.Response, self))


def test_conformance_report_prints_all_rows(monkeypatch, capsys):
    report = {
        "rows": [
            {"execution_provider": "kubernetes", "storage_protocol": "s3", "status": "PASS", "detail": None},
            {
                "execution_provider": "databricks",
                "storage_protocol": "s3",
                "status": "BLOCKED",
                "detail": "translation-layer only",
            },
        ]
    }
    monkeypatch.setattr(httpx, "get", lambda url, **kwargs: FakeResponse(200, report))

    plane_conformance_report()

    out = capsys.readouterr().out
    assert "kubernetes + s3" in out
    assert "PASS" in out
    assert "databricks + s3" in out
    assert "BLOCKED" in out


def test_conformance_report_exits_nonzero_on_fail(monkeypatch):
    report = {
        "rows": [
            {
                "execution_provider": "kubernetes",
                "storage_protocol": "s3",
                "status": "FAIL",
                "detail": "kubeconfig not found",
            }
        ]
    }
    monkeypatch.setattr(httpx, "get", lambda url, **kwargs: FakeResponse(200, report))

    with pytest.raises(typer.Exit) as exc_info:
        plane_conformance_report()
    assert exc_info.value.exit_code == 1


def test_conformance_report_blocked_does_not_exit_nonzero(monkeypatch):
    report = {
        "rows": [
            {
                "execution_provider": "databricks",
                "storage_protocol": "s3",
                "status": "BLOCKED",
                "detail": "translation-layer only",
            }
        ]
    }
    monkeypatch.setattr(httpx, "get", lambda url, **kwargs: FakeResponse(200, report))

    # Should not raise -- BLOCKED is an honest state, not a failure.
    plane_conformance_report()
