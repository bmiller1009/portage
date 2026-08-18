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

    def create_namespaced_custom_object(self, *, group, version, namespace, plural, body):
        self.created = body
        return body

    def get_namespaced_custom_object(self, *, group, version, namespace, plural, name):
        return {"metadata": {"name": name}, "status": self.status_to_return}

    def delete_namespaced_custom_object(self, *, group, version, namespace, plural, name):
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


def test_capabilities_declares_supported_spark_versions(profile):
    provider = KubernetesExecutionProvider(profile, api_client=FakeCustomObjectsApi())
    caps = asyncio.run(provider.capabilities())
    assert caps.spark_versions == ["4.1", "4.2"]
    assert caps.gpu is False
