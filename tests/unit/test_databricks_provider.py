import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

from control_plane.execution_provider import ResolvedWorkload, RunRequest
from control_plane.run_state import RunState
from providers.execution.databricks.provider import (
    DatabricksExecutionProvider,
    DatabricksProfile,
    WorkspaceClientLike,
)
from spec.workload.v1alpha1 import parse_workload

EXAMPLES_DIR = Path(__file__).resolve().parents[2] / "examples"


class FakeJobsAPI:
    """Stands in for databricks.sdk.WorkspaceClient().jobs — no live
    workspace needed. Every Phase 0 Databricks test uses this (ADR: the
    Databricks provider is mocked-only until real workspace credentials
    exist)."""

    def __init__(self):
        self.submitted_tasks: list | None = None
        self.submitted_run_name: str | None = None
        self.canceled_run_id: int | None = None
        self.run_to_return: SimpleNamespace | None = None

    def submit(self, *, run_name, tasks):
        self.submitted_run_name = run_name
        self.submitted_tasks = tasks
        return SimpleNamespace(run_id=42)

    def get_run(self, *, run_id):
        return self.run_to_return

    def cancel_run(self, *, run_id):
        self.canceled_run_id = run_id


def _fake_run(life_cycle_state=None, result_state=None):
    return SimpleNamespace(
        state=SimpleNamespace(
            life_cycle_state=SimpleNamespace(value=life_cycle_state) if life_cycle_state else None,
            result_state=SimpleNamespace(value=result_state) if result_state else None,
        )
    )


@pytest.fixture
def profile() -> DatabricksProfile:
    return DatabricksProfile(host="https://example.databricks.com", cluster_node_type_id="i3.xlarge")


@pytest.fixture
def resolved_run() -> RunRequest:
    workload = parse_workload(EXAMPLES_DIR / "wordcount.yaml")
    resolved = ResolvedWorkload(
        workload=workload,
        dataset_config={
            "spark.portable.dataset.wordcount.raw.uri": "s3a://portage-phase0/wordcount/input.txt",
            "spark.portable.dataset.wordcount.counts.uri": "s3a://portage-phase0/wordcount/output",
        },
        environment_name="databricks-mock",
    )
    return RunRequest(run_id="abc123", resolved=resolved)


@pytest.fixture
def resolved_jar_run() -> RunRequest:
    workload = parse_workload(EXAMPLES_DIR / "wordcount-jar.yaml")
    resolved = ResolvedWorkload(workload=workload, dataset_config={}, environment_name="databricks-mock")
    return RunRequest(run_id="abc123", resolved=resolved)


def test_build_run_submission_applies_runtime_profile_node_type(resolved_run):
    profile = DatabricksProfile(
        host="https://example.databricks.com",
        cluster_node_type_id="i3.xlarge",
        runtime_profiles={"high-memory": {"node_type_id": "r5.4xlarge"}},
    )
    resolved_run.resolved.workload.runtime.profile = "high-memory"

    task = DatabricksExecutionProvider(profile).build_run_submission(resolved_run)

    assert task.new_cluster is not None
    assert task.new_cluster.node_type_id == "r5.4xlarge"


def test_build_run_submission_unknown_runtime_profile_falls_back_to_default(profile, resolved_run):
    resolved_run.resolved.workload.runtime.profile = "gpu"  # not in this environment's config

    task = DatabricksExecutionProvider(profile).build_run_submission(resolved_run)

    assert task.new_cluster is not None
    assert task.new_cluster.node_type_id == "i3.xlarge"


def test_build_run_submission_splits_entry_point_into_package_and_entry(profile, resolved_run):
    provider = DatabricksExecutionProvider(profile)
    task = provider.build_run_submission(resolved_run)

    assert task.task_key == "main"
    assert task.python_wheel_task is not None
    assert task.python_wheel_task.package_name == "wordcount"
    assert task.python_wheel_task.entry_point == "count"
    assert task.new_cluster is not None
    assert task.new_cluster.node_type_id == "i3.xlarge"
    assert task.new_cluster.spark_conf is not None
    assert task.new_cluster.spark_conf["spark.portable.dataset.wordcount.raw.uri"] == (
        "s3a://portage-phase0/wordcount/input.txt"
    )
    assert task.libraries is not None
    assert task.libraries[0].whl == "artifact://wordcount/0.1.0"


def test_build_run_submission_jar_shape(profile, resolved_jar_run):
    provider = DatabricksExecutionProvider(profile)
    task = provider.build_run_submission(resolved_jar_run)

    assert task.spark_jar_task is not None
    assert task.spark_jar_task.main_class_name == "org.apache.spark.examples.SparkPi"
    assert task.spark_jar_task.parameters == ["2"]
    assert task.python_wheel_task is None
    assert task.libraries is not None
    assert task.libraries[0].jar == "local:///opt/spark/examples/jars/spark-examples.jar"


def test_validate_accepts_jvm_jar(profile, resolved_jar_run):
    provider = DatabricksExecutionProvider(profile)

    result = asyncio.run(provider.validate(resolved_jar_run.resolved))

    assert result.valid is True


def test_submit_calls_jobs_submit_and_returns_provider_run(profile, resolved_run):
    fake_jobs = FakeJobsAPI()
    provider = DatabricksExecutionProvider(profile, client=cast(WorkspaceClientLike, SimpleNamespace(jobs=fake_jobs)))

    result = asyncio.run(provider.submit(resolved_run))

    assert fake_jobs.submitted_run_name == "wordcount-abc123"
    assert fake_jobs.submitted_tasks is not None
    assert len(fake_jobs.submitted_tasks) == 1
    assert result.provider_run_id == "42"


@pytest.mark.parametrize(
    "life_cycle_state,result_state,expected",
    [
        ("PENDING", None, RunState.QUEUED),
        ("RUNNING", None, RunState.RUNNING),
        ("TERMINATED", "SUCCESS", RunState.SUCCEEDED),
        ("TERMINATED", "FAILED", RunState.FAILED),
        ("TERMINATED", "CANCELED", RunState.CANCELED),
        ("INTERNAL_ERROR", None, RunState.FAILED),
    ],
)
def test_status_maps_databricks_states_to_canonical_run_state(
    profile, life_cycle_state, result_state, expected
):
    fake_jobs = FakeJobsAPI()
    fake_jobs.run_to_return = _fake_run(life_cycle_state, result_state)
    provider = DatabricksExecutionProvider(profile, client=cast(WorkspaceClientLike, SimpleNamespace(jobs=fake_jobs)))

    status = asyncio.run(provider.status("42"))

    assert status.state == expected


def test_cancel_calls_jobs_cancel_run(profile):
    fake_jobs = FakeJobsAPI()
    provider = DatabricksExecutionProvider(profile, client=cast(WorkspaceClientLike, SimpleNamespace(jobs=fake_jobs)))

    asyncio.run(provider.cancel("42"))

    assert fake_jobs.canceled_run_id == 42


def test_validate_rejects_unsupported_spark_version(profile, resolved_run):
    resolved_run.resolved.workload.runtime.spark = "3.5"
    provider = DatabricksExecutionProvider(profile)

    result = asyncio.run(provider.validate(resolved_run.resolved))

    assert result.valid is False


def test_validate_rejects_dynamic_allocation_requirement(profile, resolved_run):
    """Databricks capabilities() declares dynamic_allocation=False."""
    resolved_run.resolved.workload.requirements.dynamicAllocation = True
    provider = DatabricksExecutionProvider(profile)

    result = asyncio.run(provider.validate(resolved_run.resolved))

    assert result.valid is False
    assert any("dynamic allocation" in e for e in result.errors)


def test_capabilities_reports_no_dynamic_allocation(profile):
    provider = DatabricksExecutionProvider(profile)
    caps = asyncio.run(provider.capabilities())
    assert caps.dynamic_allocation is False
