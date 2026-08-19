import asyncio
from pathlib import Path

import pytest

from control_plane.execution_provider import ResolvedWorkload, RunRequest
from control_plane.run_state import RunState
from providers.execution.kubernetes.provider import (
    KubernetesExecutionProvider,
    KubernetesProfile,
)
from spec.workload.v1alpha1 import parse_workload

EXAMPLES_DIR = Path(__file__).resolve().parents[2] / "examples"


class FakeCustomObjectsApi:
    """Stands in for kubernetes.client.CustomObjectsApi — no cluster needed."""

    def __init__(self):
        self.created: dict | None = None
        self.deleted_name: str | None = None
        self.status_to_return: dict = {}
        self.raise_on_create: Exception | None = None
        self.raise_on_get: Exception | None = None
        self.raise_on_delete: Exception | None = None
        self.get_call_count = 0

    def create_namespaced_custom_object(self, *, group, version, namespace, plural, body, _request_timeout=None):
        if self.raise_on_create is not None:
            raise self.raise_on_create
        self.created = body
        return body

    def get_namespaced_custom_object(self, *, group, version, namespace, plural, name, _request_timeout=None):
        if self.raise_on_get is not None:
            raise self.raise_on_get
        self.get_call_count += 1
        return {"metadata": {"name": name}, "status": self.status_to_return}

    def delete_namespaced_custom_object(self, *, group, version, namespace, plural, name, _request_timeout=None):
        if self.raise_on_delete is not None:
            raise self.raise_on_delete
        self.deleted_name = name


@pytest.fixture
def profile() -> KubernetesProfile:
    return KubernetesProfile(
        namespace="default", service_account="spark", image="portage/wordcount:0.1.0"
    )


@pytest.fixture
def resolved_run() -> RunRequest:
    workload = parse_workload(EXAMPLES_DIR / "wordcount.yaml")
    resolved = ResolvedWorkload(
        workload=workload,
        dataset_config={
            "spark.portable.dataset.wordcount.raw.uri": "s3a://portage-phase0/wordcount/input.txt",
            "spark.portable.dataset.wordcount.counts.uri": "s3a://portage-phase0/wordcount/output",
        },
        environment_name="k8s-remote",
    )
    return RunRequest(run_id="abc123", resolved=resolved)


@pytest.fixture
def resolved_jar_run() -> RunRequest:
    workload = parse_workload(EXAMPLES_DIR / "wordcount-jar.yaml")
    resolved = ResolvedWorkload(workload=workload, dataset_config={}, environment_name="k8s-remote")
    return RunRequest(run_id="abc123", resolved=resolved)


@pytest.fixture
def resolved_pipeline_run() -> RunRequest:
    workload = parse_workload(EXAMPLES_DIR / "hello-pipeline.yaml")
    resolved = ResolvedWorkload(workload=workload, dataset_config={}, environment_name="k8s-remote")
    return RunRequest(run_id="abc123", resolved=resolved)


def test_build_spark_application_shape(profile, resolved_run):
    provider = KubernetesExecutionProvider(profile, api_client=FakeCustomObjectsApi())
    manifest = provider.build_spark_application(resolved_run)

    assert manifest["apiVersion"] == "spark.apache.org/v1"
    assert manifest["kind"] == "SparkApplication"
    assert manifest["metadata"]["name"] == "wordcount-abc123"
    spec = manifest["spec"]
    assert spec["driverArgs"][0] == "wordcount.jobs.count"
    assert spec["runtimeVersions"]["sparkVersion"] == "4.2.0"
    assert spec["sparkConf"]["spark.driver.memory"] == "2g"
    assert spec["sparkConf"]["spark.executor.memory"] == "2g"
    assert "driverSpec" not in spec
    assert "executorSpec" not in spec


def test_build_spark_application_applies_runtime_profile_node_selector(resolved_run):
    profile = KubernetesProfile(
        namespace="default",
        service_account="spark",
        image="portage/wordcount:0.1.0",
        runtime_profiles={"high-memory": {"nodeSelector": {"workload-type": "memory-optimized"}}},
    )
    resolved_run.resolved.workload.runtime.profile = "high-memory"
    provider = KubernetesExecutionProvider(profile, api_client=FakeCustomObjectsApi())

    spec = provider.build_spark_application(resolved_run)["spec"]

    assert spec["driverSpec"]["podTemplateSpec"]["spec"]["nodeSelector"] == {
        "workload-type": "memory-optimized"
    }
    assert spec["executorSpec"]["podTemplateSpec"]["spec"]["nodeSelector"] == {
        "workload-type": "memory-optimized"
    }
    assert "containers" not in spec["driverSpec"]["podTemplateSpec"]["spec"]


def test_build_spark_application_unknown_runtime_profile_is_a_noop(profile, resolved_run):
    resolved_run.resolved.workload.runtime.profile = "gpu"  # not in this environment's config

    spec = KubernetesExecutionProvider(profile, api_client=FakeCustomObjectsApi()).build_spark_application(
        resolved_run
    )["spec"]

    assert "driverSpec" not in spec
    assert "executorSpec" not in spec


def test_build_spark_application_adds_volume_mounts_when_present(profile, resolved_run):
    resolved_run.resolved.volume_mounts = [
        {
            "name": "vast-nfs-data",
            "volume": {"nfs": {"server": "vast.example.com", "path": "/export/portage"}},
            "mount_path": "/vast",
        }
    ]
    provider = KubernetesExecutionProvider(profile, api_client=FakeCustomObjectsApi())

    spec = provider.build_spark_application(resolved_run)["spec"]

    for role, container_name in [("driverSpec", "spark-kubernetes-driver"), ("executorSpec", "spark-kubernetes-executor")]:
        pod_spec = spec[role]["podTemplateSpec"]["spec"]
        assert pod_spec["volumes"] == [
            {"name": "vast-nfs-data", "nfs": {"server": "vast.example.com", "path": "/export/portage"}}
        ]
        container = pod_spec["containers"][0]
        assert container["name"] == container_name
        assert container["volumeMounts"] == [{"name": "vast-nfs-data", "mountPath": "/vast"}]
    assert (
        spec["sparkConf"]["spark.portable.dataset.wordcount.raw.uri"]
        == "s3a://portage-phase0/wordcount/input.txt"
    )


def test_build_spark_application_jar_shape(profile, resolved_jar_run):
    provider = KubernetesExecutionProvider(profile, api_client=FakeCustomObjectsApi())
    manifest = provider.build_spark_application(resolved_jar_run)

    spec = manifest["spec"]
    assert spec["mainClass"] == "org.apache.spark.examples.SparkPi"
    assert spec["jars"] == "local:///opt/spark/examples/jars/spark-examples.jar"
    assert spec["driverArgs"] == ["2"]
    assert "pyFiles" not in spec


def test_build_spark_application_declarative_pipeline_shape(profile, resolved_pipeline_run):
    provider = KubernetesExecutionProvider(profile, api_client=FakeCustomObjectsApi())
    manifest = provider.build_spark_application(resolved_pipeline_run)

    spec = manifest["spec"]
    assert spec["mainClass"] == "org.apache.spark.deploy.SparkPipelines"
    assert spec["driverArgs"] == [
        "/opt/spark/python/pyspark/pipelines/cli.py",
        "run",
        "--spec",
        "local:///opt/portage/pipelines/hello/spark-pipeline.yml",
    ]
    assert "jars" not in spec
    assert "pyFiles" not in spec


def test_validate_accepts_jvm_jar(profile, resolved_jar_run):
    provider = KubernetesExecutionProvider(profile, api_client=FakeCustomObjectsApi())

    result = asyncio.run(provider.validate(resolved_jar_run.resolved))

    assert result.valid is True


def test_submit_creates_custom_object_and_returns_provider_run(profile, resolved_run):
    fake_api = FakeCustomObjectsApi()
    provider = KubernetesExecutionProvider(profile, api_client=fake_api)

    result = asyncio.run(provider.submit(resolved_run))

    assert fake_api.created is not None
    assert result.provider_run_id == "wordcount-abc123"


def test_submit_recovers_from_already_exists_conflict(profile, resolved_run):
    """Regression test for the duplicate-submission fix (spec §26/§57/§67):
    a 409 on create means a prior submission attempt for this exact run
    already succeeded (crash recovery or a raced HA replica) — submit()
    must recover by reading the existing resource back, not fail a run
    that's actually fine."""
    from kubernetes.client.exceptions import ApiException

    fake_api = FakeCustomObjectsApi()
    fake_api.raise_on_create = ApiException(status=409, reason="Conflict")
    provider = KubernetesExecutionProvider(profile, api_client=fake_api)

    result = asyncio.run(provider.submit(resolved_run))

    assert result.provider_run_id == "wordcount-abc123"
    assert fake_api.get_call_count == 1


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_submit_raises_retryable_on_transient_api_status(profile, resolved_run, status):
    from kubernetes.client.exceptions import ApiException

    from control_plane.execution_provider import RetryableProviderError

    fake_api = FakeCustomObjectsApi()
    fake_api.raise_on_create = ApiException(status=status, reason="transient")
    provider = KubernetesExecutionProvider(profile, api_client=fake_api)

    with pytest.raises(RetryableProviderError):
        asyncio.run(provider.submit(resolved_run))


def test_submit_raises_terminal_on_other_api_status(profile, resolved_run):
    from kubernetes.client.exceptions import ApiException

    from control_plane.execution_provider import TerminalProviderError

    fake_api = FakeCustomObjectsApi()
    fake_api.raise_on_create = ApiException(status=400, reason="bad request")
    provider = KubernetesExecutionProvider(profile, api_client=fake_api)

    with pytest.raises(TerminalProviderError):
        asyncio.run(provider.submit(resolved_run))


def test_submit_raises_retryable_when_api_server_unreachable(profile, resolved_run):
    """The API server being genuinely unreachable (DNS failure, connection
    refused/timed out) raises urllib3.exceptions.MaxRetryError directly —
    never an ApiException, since that class only wraps a response the
    server actually sent (confirmed live,
    tests/chaos/test_provider_outage_recovery.py). Must be retryable, not
    left to fall through to the reconciler's unclassified-exception
    fallback (which treats it as an immediate, permanent FAILED)."""
    from typing import cast

    from urllib3.connectionpool import ConnectionPool
    from urllib3.exceptions import MaxRetryError

    from control_plane.execution_provider import RetryableProviderError

    fake_api = FakeCustomObjectsApi()
    fake_api.raise_on_create = MaxRetryError(
        pool=cast(ConnectionPool, None), url="/apis/spark.apache.org/v1/..."
    )
    provider = KubernetesExecutionProvider(profile, api_client=fake_api)

    with pytest.raises(RetryableProviderError):
        asyncio.run(provider.submit(resolved_run))


def test_status_raises_retryable_on_transient_api_status(profile):
    """Spec §56's "network interruption after submission" — a transient
    blip while polling status must be retryable, not an immediate FAILED
    for a run that's actually fine."""
    from kubernetes.client.exceptions import ApiException

    from control_plane.execution_provider import RetryableProviderError

    fake_api = FakeCustomObjectsApi()
    fake_api.raise_on_get = ApiException(status=503, reason="service unavailable")
    provider = KubernetesExecutionProvider(profile, api_client=fake_api)

    with pytest.raises(RetryableProviderError):
        asyncio.run(provider.status("wordcount-abc123"))


def test_status_raises_terminal_on_other_api_status(profile):
    from kubernetes.client.exceptions import ApiException

    from control_plane.execution_provider import TerminalProviderError

    fake_api = FakeCustomObjectsApi()
    fake_api.raise_on_get = ApiException(status=404, reason="not found")
    provider = KubernetesExecutionProvider(profile, api_client=fake_api)

    with pytest.raises(TerminalProviderError):
        asyncio.run(provider.status("wordcount-abc123"))


def test_cancel_raises_retryable_on_transient_api_status(profile):
    from kubernetes.client.exceptions import ApiException

    from control_plane.execution_provider import RetryableProviderError

    fake_api = FakeCustomObjectsApi()
    fake_api.raise_on_delete = ApiException(status=429, reason="rate limited")
    provider = KubernetesExecutionProvider(profile, api_client=fake_api)

    with pytest.raises(RetryableProviderError):
        asyncio.run(provider.cancel("wordcount-abc123"))


def test_cancel_treats_already_gone_as_success(profile):
    """A 404 on delete means a prior cancel attempt already removed it
    (crash recovery/HA race) — cancel() ensures absence, so this is
    success, not an error."""
    from kubernetes.client.exceptions import ApiException

    fake_api = FakeCustomObjectsApi()
    fake_api.raise_on_delete = ApiException(status=404, reason="not found")
    provider = KubernetesExecutionProvider(profile, api_client=fake_api)

    asyncio.run(provider.cancel("wordcount-abc123"))  # no exception == pass


@pytest.mark.parametrize(
    "native_state,expected",
    [
        ("Submitted", RunState.SUBMITTING),
        ("RunningHealthy", RunState.RUNNING),
        ("Succeeded", RunState.SUCCEEDED),
        ("Failed", RunState.FAILED),
        ("SchedulingFailure", RunState.FAILED),
        ("SomeUnknownFutureState", RunState.UNKNOWN),
    ],
)
def test_status_maps_native_state_to_canonical_run_state(
    profile, native_state, expected
):
    fake_api = FakeCustomObjectsApi()
    fake_api.status_to_return = {"currentState": {"currentStateSummary": native_state}}
    provider = KubernetesExecutionProvider(profile, api_client=fake_api)

    status = asyncio.run(provider.status("wordcount-abc123"))

    assert status.state == expected
    assert status.provider_native_status == native_state


@pytest.mark.parametrize(
    "last_real_state,expected",
    [("Succeeded", RunState.SUCCEEDED), ("Failed", RunState.FAILED)],
)
def test_status_resolves_resource_released_via_history(profile, last_real_state, expected):
    """ResourceReleased is a post-terminal cleanup marker with no outcome of
    its own — confirmed live during Phase 0 (a real job that failed still
    settled on currentStateSummary=ResourceReleased). status() must look
    back through history rather than report UNKNOWN forever."""
    fake_api = FakeCustomObjectsApi()
    fake_api.status_to_return = {
        "currentState": {"currentStateSummary": "ResourceReleased"},
        "stateTransitionHistory": {
            "0": {"currentStateSummary": "Submitted"},
            "1": {"currentStateSummary": "RunningHealthy"},
            "2": {"currentStateSummary": last_real_state},
            "3": {"currentStateSummary": "ResourceReleased"},
        },
    }
    provider = KubernetesExecutionProvider(profile, api_client=fake_api)

    status = asyncio.run(provider.status("wordcount-abc123"))

    assert status.state == expected
    assert status.provider_native_status == last_real_state


def test_status_falls_back_to_unknown_when_history_has_no_terminal_outcome(profile):
    fake_api = FakeCustomObjectsApi()
    fake_api.status_to_return = {
        "currentState": {"currentStateSummary": "ResourceReleased"},
        "stateTransitionHistory": {"0": {"currentStateSummary": "Submitted"}},
    }
    provider = KubernetesExecutionProvider(profile, api_client=fake_api)

    status = asyncio.run(provider.status("wordcount-abc123"))

    assert status.state == RunState.UNKNOWN
    assert status.provider_native_status == "ResourceReleased"


def test_cancel_deletes_custom_object(profile):
    fake_api = FakeCustomObjectsApi()
    provider = KubernetesExecutionProvider(profile, api_client=fake_api)

    asyncio.run(provider.cancel("wordcount-abc123"))

    assert fake_api.deleted_name == "wordcount-abc123"


def test_logs_references_correct_pod_label(profile):
    """Confirmed live via `kubectl get pod --show-labels` against a real
    driver pod: the operator labels it spark.operator/spark-app-name, not
    spark.apache.org/app-name — using the wrong one silently returns no
    logs (a real bug, caught only by running against a live cluster)."""
    provider = KubernetesExecutionProvider(profile, api_client=FakeCustomObjectsApi())

    ref = asyncio.run(provider.logs("wordcount-abc123"))

    assert ref.uri == "kubectl logs -n default -l spark.operator/spark-app-name=wordcount-abc123"


def test_validate_rejects_unsupported_spark_version(profile, resolved_run):
    resolved_run.resolved.workload.runtime.spark = "3.5"
    provider = KubernetesExecutionProvider(profile, api_client=FakeCustomObjectsApi())

    result = asyncio.run(provider.validate(resolved_run.resolved))

    assert result.valid is False
    assert "unsupported Spark version" in result.errors[0]


def test_validate_rejects_gpu_requirement(profile, resolved_run):
    resolved_run.resolved.workload.requirements.gpu = True
    provider = KubernetesExecutionProvider(profile, api_client=FakeCustomObjectsApi())

    result = asyncio.run(provider.validate(resolved_run.resolved))

    assert result.valid is False
    assert any("GPU" in e for e in result.errors)


def test_capabilities_declares_supported_spark_versions(profile):
    provider = KubernetesExecutionProvider(profile, api_client=FakeCustomObjectsApi())
    caps = asyncio.run(provider.capabilities())
    assert caps.spark_versions == ["4.1", "4.2"]
    assert caps.gpu is False
