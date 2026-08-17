"""Tests plane run's wiring (workload -> dataset resolution -> provider
submit/poll loop) against a fake execution provider — no cluster, no
network. Exercises cli/main.py's run() by calling it directly rather than
through the Typer CLI runner, since the logic under test is the wiring,
not argument parsing."""

from pathlib import Path
from unittest.mock import patch

import pytest
import typer

from cli.main import run as plane_run
from control_plane.execution_provider import (
    CapabilitySet,
    LogReference,
    ProviderRun,
    ProviderStatus,
    RunRequest,
    ValidationResult,
)
from control_plane.run_state import RunState

EXAMPLES_DIR = Path(__file__).resolve().parents[2] / "examples"


class FakeExecutionProvider:
    def __init__(self, statuses: list[RunState]):
        self._statuses = list(statuses)
        self.submitted_request: RunRequest | None = None

    async def validate(self, workload):
        return ValidationResult(valid=True)

    async def submit(self, run):
        self.submitted_request = run
        return ProviderRun(provider_run_id="fake-run-1")

    async def status(self, provider_run_id):
        state = self._statuses.pop(0) if len(self._statuses) > 1 else self._statuses[0]
        return ProviderStatus(state=state, provider_native_status=state.value)

    async def cancel(self, provider_run_id):
        pass

    async def logs(self, provider_run_id):
        return LogReference(description="fake")

    async def capabilities(self):
        return CapabilitySet(
            spark_versions=["4.2"],
            languages=["python"],
            dynamic_allocation=False,
            gpu=False,
            streaming=False,
            local_disk=True,
            spark_connect=False,
        )


@pytest.fixture(autouse=True)
def storage_env(monkeypatch):
    monkeypatch.setenv("S3_ACCESS_KEY", "test")
    monkeypatch.setenv("S3_SECRET_KEY", "test")
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio.local:9000")


def _run_with_fake_provider(statuses: list[RunState]):
    fake = FakeExecutionProvider(statuses)
    with patch("cli.main.build_execution_provider", return_value=fake):
        plane_run(
            workload_file=str(EXAMPLES_DIR / "wordcount.yaml"),
            environment="k8s-remote",
            timeout_seconds=5,
            poll_interval_seconds=0,
        )
    return fake


def test_run_succeeds_and_resolves_dataset_config():
    fake = _run_with_fake_provider([RunState.SUBMITTING, RunState.RUNNING, RunState.SUCCEEDED])

    assert fake.submitted_request is not None
    resolved = fake.submitted_request.resolved
    assert resolved.dataset_config == {
        "portable.dataset.wordcount.raw.uri": "s3a://portage-phase0/wordcount/input.txt",
        "portable.dataset.wordcount.counts.uri": "s3a://portage-phase0/wordcount/output",
    }
    assert resolved.storage_config["spark.hadoop.fs.s3a.endpoint"] == "http://minio.local:9000"


def test_run_exits_nonzero_on_failure():
    with pytest.raises(typer.Exit) as exc_info:
        _run_with_fake_provider([RunState.RUNNING, RunState.FAILED])
    assert exc_info.value.exit_code == 1
