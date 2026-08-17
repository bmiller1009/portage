"""Kubernetes execution provider (docs/architecture/spec.md §14, ADR 0004).

Translates a resolved portable workload into an Apache Spark Kubernetes
Operator SparkApplication (spark.apache.org/v1 — verified empirically
against operator chart 1.8.0 / app version 1.0.0 during Phase 0 bring-up,
see docs/providers/kubernetes.md) and submits it via the official
Kubernetes Python client. The operator owns all pod scheduling; this
provider only ever creates/reads/deletes one custom resource per run.
"""

import asyncio
from dataclasses import dataclass
from typing import Any, Protocol, cast

from kubernetes import client as k8s_client
from kubernetes import config as k8s_config

from control_plane.execution_provider import (
    CapabilitySet,
    LogReference,
    ProviderRun,
    ProviderStatus,
    RunRequest,
    ValidationResult,
)
from control_plane.run_state import RunState

SPARK_APPLICATION_GROUP = "spark.apache.org"
SPARK_APPLICATION_VERSION = "v1"
SPARK_APPLICATION_PLURAL = "sparkapplications"

# Baked into the framework-managed Spark base image (spec §17): a fixed,
# generic launcher that imports "<module.path>.<callable>" (a workload's
# entryPoint) and calls it. Providers never generate per-job launcher code.
LAUNCHER_PATH = "local:///opt/portage/launcher.py"

# Native SparkApplication currentStateSummary -> canonical RunState (spec §23).
# Enum values confirmed against the live CRD (kubectl get crd
# sparkapplications.spark.apache.org -o json, .status.currentState).
_STATE_MAP: dict[str, RunState] = {
    "Submitted": RunState.SUBMITTING,
    "DriverRequested": RunState.QUEUED,
    "DriverStarted": RunState.QUEUED,
    "DriverReady": RunState.RUNNING,
    "InitializedBelowThresholdExecutors": RunState.RUNNING,
    "RunningWithBelowThresholdExecutors": RunState.RUNNING,
    "RunningWithPartialCapacity": RunState.RUNNING,
    "RunningHealthy": RunState.RUNNING,
    "Succeeded": RunState.SUCCEEDED,
    "Failed": RunState.FAILED,
    "DriverEvicted": RunState.FAILED,
    "DriverReadyTimedOut": RunState.FAILED,
    "DriverStartTimedOut": RunState.FAILED,
    "ExecutorsStartTimedOut": RunState.FAILED,
    "SchedulingFailure": RunState.FAILED,
    "TerminatedWithoutReleaseResources": RunState.FAILED,
    "ScheduledToRestart": RunState.SUBMITTING,
    "ResourceReleased": RunState.UNKNOWN,
}

_SUPPORTED_SPARK_VERSIONS = {"4.1", "4.2"}


class CustomObjectsApiLike(Protocol):
    """The subset of kubernetes.client.CustomObjectsApi this provider uses —
    a Protocol so unit tests can inject a fake without subclassing the real
    (heavily code-generated) SDK client."""

    def create_namespaced_custom_object(
        self, *, group: str, version: str, namespace: str, plural: str, body: dict
    ) -> dict: ...

    def get_namespaced_custom_object(
        self, *, group: str, version: str, namespace: str, plural: str, name: str
    ) -> dict: ...

    def delete_namespaced_custom_object(
        self, *, group: str, version: str, namespace: str, plural: str, name: str
    ) -> Any: ...


@dataclass
class KubernetesProfile:
    namespace: str
    service_account: str
    image: str
    kubeconfig_path: str | None = None
    context: str | None = None


def _to_spark_memory(k8s_quantity: str) -> str:
    """Convert a Kubernetes-style memory quantity ('8Gi', '512Mi') into the
    string Spark's memory config properties expect ('8g', '512m')."""
    if k8s_quantity.endswith("Gi"):
        return f"{k8s_quantity[:-2]}g"
    if k8s_quantity.endswith("Mi"):
        return f"{k8s_quantity[:-2]}m"
    return k8s_quantity


class KubernetesExecutionProvider:
    def __init__(
        self,
        profile: KubernetesProfile,
        api_client: CustomObjectsApiLike | None = None,
    ):
        self.profile = profile
        self._api: CustomObjectsApiLike
        if api_client is not None:
            # Testing seam — unit tests inject a fake CustomObjectsApi so no
            # real kubeconfig/cluster is needed to exercise translation logic.
            self._api = api_client
        else:
            k8s_config.load_kube_config(
                config_file=profile.kubeconfig_path, context=profile.context
            )
            # The generated k8s client's stubs type these methods far more
            # loosely than their actual runtime behavior (they return dicts
            # for custom resources) — cast to the narrower Protocol we
            # actually rely on rather than fight the generated union type.
            self._api = cast(CustomObjectsApiLike, k8s_client.CustomObjectsApi())

    async def validate(self, workload) -> ValidationResult:
        errors = []
        if workload.workload.runtime.spark not in _SUPPORTED_SPARK_VERSIONS:
            errors.append(f"unsupported Spark version: {workload.workload.runtime.spark}")
        if workload.workload.application.type != "python-wheel":
            errors.append(
                "Kubernetes provider prototype only supports python-wheel artifacts, "
                f"got {workload.workload.application.type}"
            )
        return ValidationResult(valid=not errors, errors=errors)

    def build_spark_application(self, run: RunRequest) -> dict:
        """Pure translation function — resolved workload -> SparkApplication
        manifest. No I/O, so it's directly unit-testable without a cluster."""
        workload = run.resolved.workload
        run_name = f"{workload.metadata.name}-{run.run_id}".lower()[:63].rstrip("-")

        spark_conf = {
            "spark.kubernetes.authenticate.driver.serviceAccountName": self.profile.service_account,
            "spark.kubernetes.container.image": self.profile.image,
            "spark.driver.cores": str(workload.resources.driver.cores),
            "spark.driver.memory": _to_spark_memory(workload.resources.driver.memory),
            "spark.executor.cores": str(workload.resources.executor.cores),
            "spark.executor.memory": _to_spark_memory(workload.resources.executor.memory),
            "spark.executor.instances": str(workload.resources.scaling.minExecutors),
            "spark.dynamicAllocation.enabled": "true",
            "spark.dynamicAllocation.minExecutors": str(workload.resources.scaling.minExecutors),
            "spark.dynamicAllocation.maxExecutors": str(workload.resources.scaling.maxExecutors),
            "spark.dynamicAllocation.shuffleTracking.enabled": "true",
            **run.resolved.storage_config,
            **run.resolved.dataset_config,
        }

        return {
            "apiVersion": f"{SPARK_APPLICATION_GROUP}/{SPARK_APPLICATION_VERSION}",
            "kind": "SparkApplication",
            "metadata": {"name": run_name, "namespace": self.profile.namespace},
            "spec": {
                "pyFiles": LAUNCHER_PATH,
                "driverArgs": [workload.application.entryPoint, *workload.arguments],
                "sparkConf": spark_conf,
                "runtimeVersions": {"sparkVersion": f"{workload.runtime.spark}.0"},
            },
        }

    async def submit(self, run: RunRequest) -> ProviderRun:
        manifest = self.build_spark_application(run)
        result = await asyncio.to_thread(
            self._api.create_namespaced_custom_object,
            group=SPARK_APPLICATION_GROUP,
            version=SPARK_APPLICATION_VERSION,
            namespace=self.profile.namespace,
            plural=SPARK_APPLICATION_PLURAL,
            body=manifest,
        )
        return ProviderRun(provider_run_id=result["metadata"]["name"], raw=result)

    async def status(self, provider_run_id: str) -> ProviderStatus:
        obj = await asyncio.to_thread(
            self._api.get_namespaced_custom_object,
            group=SPARK_APPLICATION_GROUP,
            version=SPARK_APPLICATION_VERSION,
            namespace=self.profile.namespace,
            plural=SPARK_APPLICATION_PLURAL,
            name=provider_run_id,
        )
        native_state = obj.get("status", {}).get("currentState", {}).get("currentStateSummary")
        if native_state is None:
            return ProviderStatus(state=RunState.SUBMITTING, provider_native_status="Unreported")
        return ProviderStatus(
            state=_STATE_MAP.get(native_state, RunState.UNKNOWN),
            provider_native_status=native_state,
        )

    async def cancel(self, provider_run_id: str) -> None:
        await asyncio.to_thread(
            self._api.delete_namespaced_custom_object,
            group=SPARK_APPLICATION_GROUP,
            version=SPARK_APPLICATION_VERSION,
            namespace=self.profile.namespace,
            plural=SPARK_APPLICATION_PLURAL,
            name=provider_run_id,
        )

    async def logs(self, provider_run_id: str) -> LogReference:
        return LogReference(
            description="driver pod logs via kubectl",
            uri=(
                f"kubectl logs -n {self.profile.namespace} "
                f"-l spark.apache.org/app-name={provider_run_id}"
            ),
        )

    async def capabilities(self) -> CapabilitySet:
        return CapabilitySet(
            spark_versions=sorted(_SUPPORTED_SPARK_VERSIONS),
            languages=["python", "jvm"],
            dynamic_allocation=True,
            gpu=False,
            streaming=False,
            local_disk=True,
            spark_connect=False,
        )
