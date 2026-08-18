"""Tests plane conformance test as a pure REST client (spec §31) — httpx
is monkeypatched so no real network or API server is involved."""

from pathlib import Path
from typing import cast

import httpx
import pytest
import typer

from cli.main import conformance_test as plane_conformance_test

EXAMPLES_DIR = Path(__file__).resolve().parents[2] / "examples"


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


def _install_fakes(monkeypatch, *, compare_report: dict):
    run_ids = {"onprem": "11111111-1111-1111-1111-111111111111", "azure": "22222222-2222-2222-2222-222222222222"}
    calls: dict[str, list] = {"post": [], "get": []}

    def fake_post(url, json=None, headers=None, **kwargs):
        calls["post"].append((url, json, headers))
        if url.endswith("/v1/workloads"):
            return FakeResponse(201, {})
        if url.endswith("/v1/runs"):
            assert json is not None
            env = json["environment_name"]
            return FakeResponse(202, {"id": run_ids[env]})
        if url.endswith("/v1/conformance/compare"):
            return FakeResponse(200, compare_report)
        raise AssertionError(f"unexpected POST {url}")

    def fake_get(url, **kwargs):
        calls["get"].append(url)
        return FakeResponse(200, {"state": "SUCCEEDED"})

    monkeypatch.setattr(httpx, "post", fake_post)
    monkeypatch.setattr(httpx, "get", fake_get)
    return calls, run_ids


def test_conformance_test_submits_to_every_environment_and_compares(monkeypatch, capsys):
    calls, run_ids = _install_fakes(
        monkeypatch,
        compare_report={"results": [{"left_environment": "onprem", "right_environment": "azure", "status": "PASS", "mismatches": []}]},
    )

    plane_conformance_test(
        workload_file=str(EXAMPLES_DIR / "wordcount.yaml"),
        environment=["onprem", "azure"],
        output=None,
        timeout_seconds=5,
        poll_interval_seconds=0,
    )

    run_post_calls = [c for c in calls["post"] if c[0].endswith("/v1/runs")]
    assert len(run_post_calls) == 2
    submitted_envs = {c[1]["environment_name"] for c in run_post_calls}
    assert submitted_envs == {"onprem", "azure"}

    compare_calls = [c for c in calls["post"] if c[0].endswith("/v1/conformance/compare")]
    assert len(compare_calls) == 1
    assert set(compare_calls[0][1]["run_ids"]) == set(run_ids.values())

    out = capsys.readouterr().out
    assert "onprem <-> azure: PASS" in out


def test_conformance_test_exits_nonzero_on_fail(monkeypatch):
    _install_fakes(
        monkeypatch,
        compare_report={
            "results": [
                {
                    "left_environment": "onprem",
                    "right_environment": "azure",
                    "status": "FAIL",
                    "mismatches": ["row count mismatch"],
                }
            ]
        },
    )

    with pytest.raises(typer.Exit) as exc_info:
        plane_conformance_test(
            workload_file=str(EXAMPLES_DIR / "wordcount.yaml"),
            environment=["onprem", "azure"],
            output=None,
            timeout_seconds=5,
            poll_interval_seconds=0,
        )
    assert exc_info.value.exit_code == 1


def test_conformance_test_exits_zero_when_all_pass(monkeypatch):
    _install_fakes(
        monkeypatch,
        compare_report={
            "results": [
                {"left_environment": "onprem", "right_environment": "azure", "status": "PASS", "mismatches": []}
            ]
        },
    )

    # Should not raise.
    plane_conformance_test(
        workload_file=str(EXAMPLES_DIR / "wordcount.yaml"),
        environment=["onprem", "azure"],
        output=None,
        timeout_seconds=5,
        poll_interval_seconds=0,
    )


def test_conformance_test_passes_output_name_through(monkeypatch):
    calls, _ = _install_fakes(
        monkeypatch,
        compare_report={"results": []},
    )

    plane_conformance_test(
        workload_file=str(EXAMPLES_DIR / "wordcount.yaml"),
        environment=["onprem", "azure"],
        output="counts",
        timeout_seconds=5,
        poll_interval_seconds=0,
    )

    compare_call = next(c for c in calls["post"] if c[0].endswith("/v1/conformance/compare"))
    assert compare_call[1]["output_name"] == "counts"
