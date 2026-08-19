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

spark-declarative-pipeline (spec §39, v0.6.5) reuses the mainClass shape
(mainClass=SPARK_PIPELINES_MAIN_CLASS, no jars) -- confirmed live that this
correctly bootstraps SparkPipelines and reaches dataflow-graph creation.
**Known limitation, confirmed live, not yet worked around**: `spark-pipelines
run` requires a Spark Connect session (pyspark/pipelines/cli.py calls
spark.client.execute_command, which only exists under Connect), and
bootstrapping an embedded local Connect session inside a plain spark-submit
process hits a driver-plugin classloader wall
(NoClassDefFoundError: org/apache/spark/sql/classic/SparkSession) even once
the spark-connect_2.13 Maven artifact is pulled in via --packages -- the
class is verified present in spark-sql_2.13-4.2.0.jar on the same image, but
isn't visible from the classloader SparkContext uses to load driver plugins.
This isn't a missing-dependency gap closeable with more --packages/--jars;
`spark-pipelines` fundamentally wants either a separately-deployed, already
-running Spark Connect *server* (reached via --remote sc://host:port) or an
in-process one that this per-run, single-JVM SparkApplication CRD model has
no clean way to stand up. A real fix needs a persistent Connect server
deployed as its own long-lived resource, with this provider's pipeline
branch submitting a lightweight client run against it instead -- a
genuinely different shape from every other workload type here, and out of
this issue's scope.
"""

import asyncio
from dataclasses import dataclass, field
from typing import Any, NoReturn, Protocol, cast

from kubernetes import client as k8s_client
from kubernetes import config as k8s_config
from kubernetes.client.exceptions import ApiException
from urllib3.exceptions import MaxRetryError

from control_plane.execution_provider import (
    CapabilitySet,
    LogReference,
    ProviderRun,
    ProviderStatus,
    RetryableProviderError,
    RunRequest,
    TerminalProviderError,
    ValidationResult,
    compute_portability_status,
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

# Confirmed live (v0.6.5) against the apache/spark:4.2.0-python3 base image:
# bin/spark-pipelines itself resolves to exactly this path
# (Path(os.path.dirname(pyspark.__file__)) / "pipelines" / "cli.py") and
# execs `spark-class org.apache.spark.deploy.SparkPipelines <this path> "$@"`
# -- SparkPipelines is a normal Spark-core JVM main class (like
# org.apache.spark.repl.Main for spark-shell), so it needs no "jars" entry,
# just mainClass + this file as the first driverArgs element, mirroring how
# spark-shell invokes org.apache.spark.repl.Main with no primary resource.
PIPELINES_CLI_PATH = "/opt/spark/python/pyspark/pipelines/cli.py"
SPARK_PIPELINES_MAIN_CLASS = "org.apache.spark.deploy.SparkPipelines"

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

# Kubernetes API status codes spec §26 says are safe to retry (timeout,
# rate limit, transient server errors) — anything else on submit() is
# terminal. 409 (already exists) isn't in here at all: submit() handles it
# as a recovery signal, not an error (see submit()).
#
# 401 is included too, and this needs its own justification since "401
# means retry won't help" is the normal, correct assumption almost
# everywhere else: confirmed live (tests/chaos/test_ha_deployment.py,
# reproduced across ~13 real runs on 2026-08-19) that under heavy
# reconciler/API pod churn, a status()/cancel() call can get a genuine,
# real (correctly-signed, well-formed) 401 from the live Kubernetes API
# server against a resource whose *surrounding* calls — the submit()
# that created it, the status() polls immediately before and after —
# succeed with the exact same unmodified kubeconfig and token. A
# persistently wrong or revoked credential fails *every* call, not one
# call sandwiched between successful ones using the identical
# credential chain; that shape is far more consistent with a transient
# API-server-side hiccup under load than an actual authentication
# problem worth failing a run over. The exact server-side mechanism is
# still unconfirmed (tracked as #57) — this is a live-evidence-driven,
# bounded accommodation (submit() still gives up after
# MAX_SUBMISSION_ATTEMPTS, same as every other retryable code here), not
# a general "401s are safe to ignore" policy.
_RETRYABLE_API_STATUS_CODES = {401, 429, 500, 502, 503, 504}

# The generated Kubernetes client has no default request timeout at all —
# confirmed live (tests/chaos/test_provider_outage_recovery.py) that
# without one, a call against a genuinely unreachable API server just
# hangs on the underlying TCP connect for however long the OS/network
# path takes (which can be minutes, or indefinite), leaving a run stuck
# in SUBMITTING far longer than any reasonable retry budget rather than
# promptly raising MaxRetryError so _raise_unreachable() can classify it.
_API_REQUEST_TIMEOUT_SECONDS = 30


def _safe_api_exception_message(e: ApiException) -> str:
    """`str(ApiException)` includes the *raw* HTTP response headers and
    body (confirmed via kubernetes.client.exceptions.ApiException.__str__)
    — content from the API server this code doesn't control, which flows
    straight into RunEvent.message and is exposed via
    GET /v1/runs/{id}/events to any Viewer-role user. Uses only the
    structured status/reason fields instead — enough to classify and
    debug the failure without repeating whatever the server's response
    body happened to contain (a logging-hygiene finding from the v1.0.0
    release-hardening pass, not a known live incident)."""
    return f"Kubernetes API error {e.status}: {e.reason}"


def _raise_classified(e: ApiException) -> NoReturn:
    """Shared by status()/cancel() as well as submit() (spec §56's "network
    interruption after submission" scenario — a transient API blip while
    polling or canceling deserves the same retry treatment as one during
    submission, not an immediate FAILED for a run that's actually fine)."""
    message = _safe_api_exception_message(e)
    if e.status in _RETRYABLE_API_STATUS_CODES:
        raise RetryableProviderError(message) from e
    raise TerminalProviderError(message) from e


def _raise_unreachable(e: MaxRetryError) -> NoReturn:
    """The API server itself couldn't be reached at all (DNS failure,
    connection refused/timed out) — urllib3 raises MaxRetryError for this
    directly, never an ApiException (that class only wraps a response the
    server actually sent), so submit()/status()/cancel() need a second
    except clause to classify it. Always retryable: a genuine outage
    (spec §71's "provider outage recovery") deserves the same treatment
    as a transient 503 confirmed live — treating it as terminal instead
    (the prior behavior, caught only by the reconciler's unclassified-
    exception fallback) turned a temporary network partition into a
    permanently FAILED run."""
    raise RetryableProviderError(str(e)) from e


class CustomObjectsApiLike(Protocol):
    """The subset of kubernetes.client.CustomObjectsApi this provider uses —
    a Protocol so unit tests can inject a fake without subclassing the real
    (heavily code-generated) SDK client."""

    def create_namespaced_custom_object(
        self, *, group: str, version: str, namespace: str, plural: str, body: dict, _request_timeout: int
    ) -> dict: ...

    def get_namespaced_custom_object(
        self, *, group: str, version: str, namespace: str, plural: str, name: str, _request_timeout: int
    ) -> dict: ...

    def delete_namespaced_custom_object(
        self, *, group: str, version: str, namespace: str, plural: str, name: str, _request_timeout: int
    ) -> Any: ...


@dataclass
class KubernetesProfile:
    namespace: str
    service_account: str
    image: str
    kubeconfig_path: str | None = None
    context: str | None = None
    # Runtime profile name -> provider-specific hint dict (spec §18),
    # environment-scoped rather than a global registry — e.g.
    # {"high-memory": {"nodeSelector": {"workload-type": "memory-optimized"}}}.
    runtime_profiles: dict[str, dict] = field(default_factory=dict)


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


def _pod_spec(volume_mounts: list[dict] | None, node_selector: dict | None, container_name: str) -> dict:
    """Builds the podTemplateSpec.spec block for one role (driver or
    executor), merging whichever of volume mounts (spec §48) / a runtime
    profile's nodeSelector (spec §18) actually apply — empty dict (falsy)
    when neither does, so the caller can skip driverSpec/executorSpec
    entirely rather than emitting an empty podTemplateSpec."""
    pod_spec: dict = {}
    if node_selector:
        pod_spec["nodeSelector"] = node_selector
    if volume_mounts:
        pod_spec["volumes"] = [{"name": vm["name"], **vm["volume"]} for vm in volume_mounts]
        pod_spec["containers"] = [
            {
                "name": container_name,
                "volumeMounts": [{"name": vm["name"], "mountPath": vm["mount_path"]} for vm in volume_mounts],
            }
        ]
    return pod_spec


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
        portability = compute_portability_status(workload.workload)
        return ValidationResult(
            valid=not errors,
            errors=errors,
            portability_status=portability.status,
            provider_overrides=portability.overrides_by_provider,
        )

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
        elif workload.application.type == "spark-declarative-pipeline":
            artifact_spec = {
                "mainClass": SPARK_PIPELINES_MAIN_CLASS,
                "driverArgs": [
                    PIPELINES_CLI_PATH,
                    "run",
                    "--spec",
                    workload.application.pipelineSpec,
                    *workload.arguments,
                ],
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

        # driverSpec/executorSpec only expose a podTemplateSpec field on
        # this CRD generation (confirmed live: `kubectl get crd
        # sparkapplications.spark.apache.org -o json`, no top-level
        # volumes/volumeMounts/nodeSelector fields exist directly) — a
        # full Kubernetes PodTemplateSpec merged onto the operator's own
        # generated pod, targeting its container by name (also confirmed
        # live: spark-kubernetes-driver / spark-kubernetes-executor — this
        # is Spark's own upstream Kubernetes-backend naming, not specific
        # to this operator). Two independent things feed into it: VAST NFS
        # volume mounts (spec §48, not sparkConf-expressible at all) and a
        # runtime profile's nodeSelector hint (spec §18) — merged into one
        # podTemplateSpec per role rather than two competing ones.
        node_selector = None
        if workload.runtime.profile:
            node_selector = self.profile.runtime_profiles.get(workload.runtime.profile, {}).get(
                "nodeSelector"
            )

        driver_pod_spec = _pod_spec(run.resolved.volume_mounts, node_selector, "spark-kubernetes-driver")
        executor_pod_spec = _pod_spec(run.resolved.volume_mounts, node_selector, "spark-kubernetes-executor")
        if driver_pod_spec:
            spec["driverSpec"] = {"podTemplateSpec": {"spec": driver_pod_spec}}
        if executor_pod_spec:
            spec["executorSpec"] = {"podTemplateSpec": {"spec": executor_pod_spec}}

        return {
            "apiVersion": f"{SPARK_APPLICATION_GROUP}/{SPARK_APPLICATION_VERSION}",
            "kind": "SparkApplication",
            "metadata": {"name": run_name, "namespace": self.profile.namespace},
            "spec": spec,
        }

    async def submit(self, run: RunRequest) -> ProviderRun:
        manifest = self.build_spark_application(run)
        try:
            result = await asyncio.to_thread(
                self._api.create_namespaced_custom_object,
                group=SPARK_APPLICATION_GROUP,
                version=SPARK_APPLICATION_VERSION,
                namespace=self.profile.namespace,
                plural=SPARK_APPLICATION_PLURAL,
                body=manifest,
                _request_timeout=_API_REQUEST_TIMEOUT_SECONDS,
            )
            return ProviderRun(provider_run_id=result["metadata"]["name"], raw=result)
        except ApiException as e:
            if e.status == 409:
                # Already exists — the CR name is deterministic
                # (workload+run_id), so this means a prior submission
                # attempt for this exact run already succeeded (crash
                # recovery or a raced HA replica, spec §57/§67), not a real
                # error. Recover by reading back the existing resource
                # instead of failing a run that's actually fine.
                existing = await asyncio.to_thread(
                    self._api.get_namespaced_custom_object,
                    group=SPARK_APPLICATION_GROUP,
                    version=SPARK_APPLICATION_VERSION,
                    namespace=self.profile.namespace,
                    plural=SPARK_APPLICATION_PLURAL,
                    name=manifest["metadata"]["name"],
                    _request_timeout=_API_REQUEST_TIMEOUT_SECONDS,
                )
                return ProviderRun(provider_run_id=existing["metadata"]["name"], raw=existing)
            _raise_classified(e)
        except MaxRetryError as e:
            _raise_unreachable(e)

    async def status(self, provider_run_id: str) -> ProviderStatus:
        try:
            obj = await asyncio.to_thread(
                self._api.get_namespaced_custom_object,
                group=SPARK_APPLICATION_GROUP,
                version=SPARK_APPLICATION_VERSION,
                namespace=self.profile.namespace,
                plural=SPARK_APPLICATION_PLURAL,
                name=provider_run_id,
                _request_timeout=_API_REQUEST_TIMEOUT_SECONDS,
            )
        except ApiException as e:
            _raise_classified(e)
        except MaxRetryError as e:
            _raise_unreachable(e)
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
        try:
            await asyncio.to_thread(
                self._api.delete_namespaced_custom_object,
                group=SPARK_APPLICATION_GROUP,
                version=SPARK_APPLICATION_VERSION,
                namespace=self.profile.namespace,
                plural=SPARK_APPLICATION_PLURAL,
                name=provider_run_id,
                _request_timeout=_API_REQUEST_TIMEOUT_SECONDS,
            )
        except ApiException as e:
            if e.status == 404:
                # Already gone (e.g. a prior cancel attempt already
                # deleted it before a crash/retry) — cancel is meant to
                # ensure absence, so this is success, not an error.
                return
            _raise_classified(e)
        except MaxRetryError as e:
            _raise_unreachable(e)

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
