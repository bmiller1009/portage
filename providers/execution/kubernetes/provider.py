"""Kubernetes execution provider (docs/architecture/spec.md §14, ADR 0004).

Translates a resolved portable workload into an Apache Spark Kubernetes
Operator SparkApplication (spark.apache.org/v1 — verified empirically
against operator chart 1.8.0 / app version 1.0.0 during Phase 0 bring-up,
see docs/providers/kubernetes.md) and submits it via the official
Kubernetes Python client. The operator owns all pod scheduling; this
provider only ever creates/reads/deletes one custom resource per run.

python-wheel goes through pyFiles + the generic launcher (see LAUNCHER_PATH
below); jvm-jar goes through mainClass + jars directly — both confirmed
present on the live CRD (`kubectl get crd sparkapplications.spark.apache.org
-o json`), unlike mainApplicationFile, which isn't. JVM doesn't need the
launcher indirection Python needs, since spark-submit's --class fixes the
entry point at JVM startup rather than requiring a dynamic import trick.
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
    match_capabilities,
)
from control_plane.run_state import TERMINAL_STATES, RunState

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
}

# "ResourceReleased" is a post-terminal cleanup marker (confirmed live: it's
# what currentStateSummary settles on after Succeeded/Failed once the
# operator releases driver/executor resources) — it carries no outcome of
# its own, so status() looks back through stateTransitionHistory for the
# terminal state it followed rather than reporting RunState.UNKNOWN forever.
_NON_INFORMATIVE_STATES = {"ResourceReleased"}

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


def _last_terminal_outcome(state_transition_history: dict) -> str | None:
    """Walk a SparkApplication's stateTransitionHistory (numeric-string keys,
    chronological) backwards for the most recent entry that maps to an
    actually-terminal RunState (SUCCEEDED/FAILED/CANCELED/LOST) — not just
    any known state, since e.g. "Submitted" is known but not terminal."""
    try:
        ordered_keys = sorted(state_transition_history, key=int, reverse=True)
    except (TypeError, ValueError):
        return None
    for key in ordered_keys:
        summary = state_transition_history[key].get("currentStateSummary")
        if summary and _STATE_MAP.get(summary) in TERMINAL_STATES:
            return summary
    return None


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
        errors = match_capabilities(workload.workload, await self.capabilities())
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

        # jvm-jar dispatches straight through mainClass/jars (both confirmed
        # present on the live spark.apache.org/v1 CRD, unlike
        # mainApplicationFile which isn't) — no generic-launcher indirection
        # needed, since --class fixes the entry point at JVM startup, unlike
        # Python's dynamic-import launcher trick.
        if workload.application.type == "jvm-jar":
            artifact_spec = {
                "mainClass": workload.application.entryPoint,
                "jars": workload.application.artifact,
                "driverArgs": list(workload.arguments),
            }
        else:
            artifact_spec = {
                "pyFiles": LAUNCHER_PATH,
                "driverArgs": [workload.application.entryPoint, *workload.arguments],
            }

        spec: dict = {
            **artifact_spec,
            "sparkConf": spark_conf,
            "runtimeVersions": {"sparkVersion": f"{workload.runtime.spark}.0"},
        }

        # VAST NFS mode (spec §48) — the one storage mode that isn't
        # expressible as sparkConf at all. driverSpec/executorSpec only
        # expose a podTemplateSpec field on this CRD generation (confirmed
        # live: `kubectl get crd sparkapplications.spark.apache.org -o
        # json`, no top-level volumes/volumeMounts fields exist) — a full
        # Kubernetes PodTemplateSpec merged onto the operator's own
        # generated pod, targeting its container by name (also confirmed
        # live: spark-kubernetes-driver / spark-kubernetes-executor — this
        # is Spark's own upstream Kubernetes-backend naming, not specific
        # to this operator).
        if run.resolved.volume_mounts:
            volumes = [{"name": vm["name"], **vm["volume"]} for vm in run.resolved.volume_mounts]
            volume_mounts = [
                {"name": vm["name"], "mountPath": vm["mount_path"]} for vm in run.resolved.volume_mounts
            ]
            spec["driverSpec"] = {
                "podTemplateSpec": {
                    "spec": {
                        "volumes": volumes,
                        "containers": [{"name": "spark-kubernetes-driver", "volumeMounts": volume_mounts}],
                    }
                }
            }
            spec["executorSpec"] = {
                "podTemplateSpec": {
                    "spec": {
                        "volumes": volumes,
                        "containers": [
                            {"name": "spark-kubernetes-executor", "volumeMounts": volume_mounts}
                        ],
                    }
                }
            }

        return {
            "apiVersion": f"{SPARK_APPLICATION_GROUP}/{SPARK_APPLICATION_VERSION}",
            "kind": "SparkApplication",
            "metadata": {"name": run_name, "namespace": self.profile.namespace},
            "spec": spec,
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
        status_obj = obj.get("status") or {}
        native_state = status_obj.get("currentState", {}).get("currentStateSummary")
        if native_state is None:
            return ProviderStatus(state=RunState.SUBMITTING, provider_native_status="Unreported")

        if native_state in _NON_INFORMATIVE_STATES:
            resolved = _last_terminal_outcome(status_obj.get("stateTransitionHistory", {}))
            if resolved is not None:
                native_state = resolved

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
        # The label is spark.operator/spark-app-name (this operator's own
        # namespace), not spark.apache.org/app-name (the CRD's own API
        # group) — confirmed live via `kubectl get pod --show-labels` against
        # a running driver pod; the two are easy to conflate but only one
        # actually appears on the pod.
        return LogReference(
            description="driver pod logs via kubectl",
            uri=(
                f"kubectl logs -n {self.profile.namespace} "
                f"-l spark.operator/spark-app-name={provider_run_id}"
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
