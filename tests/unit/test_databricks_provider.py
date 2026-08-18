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
        self.submitted_idempotency_token: str | None = None
        self.canceled_run_id: int | None = None
        self.run_to_return: SimpleNamespace | None = None
        self.raise_on_submit: Exception | None = None

    def submit(self, *, run_name, tasks, idempotency_token=None):
        if self.raise_on_submit is not None:
            raise self.raise_on_submit
        self.submitted_run_name = run_name
        self.submitted_tasks = tasks
        self.submitted_idempotency_token = idempotency_token
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
    # examples/wordcount-jar.yaml is spark "4.2" (fine for its primary,
    # Kubernetes-side use — the K8s Spark Operator supports 4.0-4.2). No
    # Databricks Runtime ships Spark 4.2 yet (see _SUPPORTED_SPARK_VERSIONS),
    # so this Databricks-specific fixture overrides just the version rather
    # than changing the shared example file.
    workload = parse_workload(EXAMPLES_DIR / "wordcount-jar.yaml")
    workload = workload.model_copy(update={"runtime": workload.runtime.model_copy(update={"spark": "4.1"})})
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
    # Regression: validate() must never touch a client, so a profile with
    # no credential_reference configured is still fully usable for it.
    assert provider._client is None


def test_get_client_constructs_workspace_client_via_oauth_m2m(monkeypatch):
    import databricks.sdk as databricks_sdk_module

    captured = {}

    class FakeWorkspaceClient:
        def __init__(self, *, host, client_id, client_secret):
            captured["host"] = host
            captured["client_id"] = client_id
            captured["client_secret"] = client_secret

    monkeypatch.setattr(databricks_sdk_module, "WorkspaceClient", FakeWorkspaceClient)
    monkeypatch.setenv("PORTAGE_DBX_CLIENT_ID", "app-id-123")
    monkeypatch.setenv("PORTAGE_DBX_CLIENT_SECRET", "app-secret-456")

    dbx_profile = DatabricksProfile(
        host="https://example.databricks.com",
        cluster_node_type_id="i3.xlarge",
        credential_reference={"provider": "env", "reference": "PORTAGE_DBX"},
    )
    provider = DatabricksExecutionProvider(dbx_profile)

    client = provider._get_client()

    assert captured == {
        "host": "https://example.databricks.com",
        "client_id": "app-id-123",
        "client_secret": "app-secret-456",
    }
    assert provider._get_client() is client  # cached, constructed only once


def test_get_client_missing_credentials_raises():
    from control_plane.credentials import CredentialResolutionError

    dbx_profile = DatabricksProfile(
        host="https://example.databricks.com",
        cluster_node_type_id="i3.xlarge",
        credential_reference={"provider": "env", "reference": "PORTAGE_DBX_MISSING"},
    )
    provider = DatabricksExecutionProvider(dbx_profile)

    with pytest.raises(CredentialResolutionError):
        provider._get_client()


def test_submit_calls_jobs_submit_and_returns_provider_run(profile, resolved_run):
    fake_jobs = FakeJobsAPI()
    provider = DatabricksExecutionProvider(profile, client=cast(WorkspaceClientLike, SimpleNamespace(jobs=fake_jobs)))

    result = asyncio.run(provider.submit(resolved_run))

    assert fake_jobs.submitted_run_name == "wordcount-abc123"
    assert fake_jobs.submitted_tasks is not None
    assert len(fake_jobs.submitted_tasks) == 1
    assert result.provider_run_id == "42"


def test_submit_passes_run_id_as_idempotency_token(profile, resolved_run):
    """Regression test for the duplicate-submission fix (spec §26/§67): a
    retried submit() for the same run must carry the same token, so
    Databricks itself de-dupes instead of starting a second real job."""
    fake_jobs = FakeJobsAPI()
    provider = DatabricksExecutionProvider(profile, client=cast(WorkspaceClientLike, SimpleNamespace(jobs=fake_jobs)))

    asyncio.run(provider.submit(resolved_run))

    assert fake_jobs.submitted_idempotency_token == "abc123"


def test_submit_raises_retryable_on_transient_databricks_error(profile, resolved_run):
    from databricks.sdk import errors as dbx_errors

    from control_plane.execution_provider import RetryableProviderError

    fake_jobs = FakeJobsAPI()
    fake_jobs.raise_on_submit = dbx_errors.TooManyRequests("rate limited")
    provider = DatabricksExecutionProvider(profile, client=cast(WorkspaceClientLike, SimpleNamespace(jobs=fake_jobs)))

    with pytest.raises(RetryableProviderError):
        asyncio.run(provider.submit(resolved_run))


def test_submit_raises_terminal_on_permanent_databricks_error(profile, resolved_run):
    from databricks.sdk import errors as dbx_errors

    from control_plane.execution_provider import TerminalProviderError

    fake_jobs = FakeJobsAPI()
    fake_jobs.raise_on_submit = dbx_errors.PermissionDenied("nope")
    provider = DatabricksExecutionProvider(profile, client=cast(WorkspaceClientLike, SimpleNamespace(jobs=fake_jobs)))

    with pytest.raises(TerminalProviderError):
        asyncio.run(provider.submit(resolved_run))


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


def test_validate_rejects_spark_4_2(profile, resolved_run):
    """No Databricks Runtime ships Spark 4.2 yet (checked Aug 2026) —
    regression test for the runtime compatibility matrix: this must not
    silently PASS just because Kubernetes' examples/wordcount.yaml (which
    resolved_run is built from) happens to declare spark: "4.2"."""
    resolved_run.resolved.workload.runtime.spark = "4.2"
    provider = DatabricksExecutionProvider(profile)

    result = asyncio.run(provider.validate(resolved_run.resolved))

    assert result.valid is False
    assert "4.2" in result.errors[0]


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


def test_capabilities_reports_current_supported_spark_versions(profile):
    provider = DatabricksExecutionProvider(profile)
    caps = asyncio.run(provider.capabilities())
    assert caps.spark_versions == ["4.0", "4.1"]
