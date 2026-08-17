"""Databricks execution provider (docs/architecture/spec.md §16, ADR 0004).

Translation-layer prototype only — no live Databricks workspace was
available during Phase 0, so this is tested against a fake WorkspaceClient
(see tests/unit/test_databricks_provider.py), never a real one. Live
submission is future work once workspace credentials exist.

Translates a resolved workload into a Jobs API 2.2 one-time-run submission
(`jobs.submit`) using a python_wheel_task, per spec §16's guidance to prefer
wheel/JAR tasks over notebooks. The wheel task's `package_name`/`entry_point`
are derived from the workload's dotted entryPoint ("wordcount.jobs.count" ->
package "wordcount", entry point "count") — the example wheel under
examples/wordcount_app registers a matching console_scripts entry point so
the same artifact is invocable by both providers (see examples/wordcount_app/
pyproject.toml and providers/execution/kubernetes/provider.py's launcher).
"""

from dataclasses import dataclass
from typing import Any, Protocol

from databricks.sdk.service import compute as dbx_compute
from databricks.sdk.service import jobs as dbx_jobs

from control_plane.execution_provider import (
    CapabilitySet,
    LogReference,
    ProviderRun,
    ProviderStatus,
    RunRequest,
    ValidationResult,
)
from control_plane.run_state import RunState

_SUPPORTED_SPARK_VERSIONS = {"4.1", "4.2"}

# Databricks Runtime <-> Apache Spark version compatibility (spec §46):
# Runtime 19 = Spark 4.2.0, Runtime 18 LTS = Spark 4.1.0. The exact Jobs API
# `spark_version` cluster identifier string (e.g. "16.4.x-scala2.12"-shaped)
# is only available from a live workspace via w.clusters.spark_versions() —
# these are documented placeholders to be confirmed against a real
# workspace before any live submission is attempted.
_SPARK_TO_DBR_CLUSTER_VERSION = {
    "4.2": "PLACEHOLDER-runtime-19.x-confirm-via-clusters.spark_versions",
    "4.1": "PLACEHOLDER-runtime-18.x-lts-confirm-via-clusters.spark_versions",
}

# Native Databricks RunLifeCycleState / RunResultState -> canonical RunState
# (spec §23), verified against databricks.sdk.service.jobs.RunLifeCycleState
# / RunResultState rather than assumed.
_LIFECYCLE_STATE_MAP: dict[str, RunState] = {
    "PENDING": RunState.QUEUED,
    "QUEUED": RunState.QUEUED,
    "BLOCKED": RunState.QUEUED,
    "WAITING_FOR_RETRY": RunState.QUEUED,
    "RUNNING": RunState.RUNNING,
    "TERMINATING": RunState.RUNNING,
    "INTERNAL_ERROR": RunState.FAILED,
    "SKIPPED": RunState.CANCELED,
}
_RESULT_STATE_MAP: dict[str, RunState] = {
    "SUCCESS": RunState.SUCCEEDED,
    "SUCCESS_WITH_FAILURES": RunState.FAILED,
    "FAILED": RunState.FAILED,
    "TIMEDOUT": RunState.FAILED,
    "UPSTREAM_FAILED": RunState.FAILED,
    "EXCLUDED": RunState.FAILED,
    "DISABLED": RunState.FAILED,
    "MAXIMUM_CONCURRENT_RUNS_REACHED": RunState.FAILED,
    "CANCELED": RunState.CANCELED,
    "UPSTREAM_CANCELED": RunState.CANCELED,
}


class JobsAPILike(Protocol):
    """The subset of databricks.sdk.WorkspaceClient().jobs this provider
    uses — a Protocol so unit tests can inject a fake without subclassing
    the real SDK client."""

    def submit(self, *, run_name: str, tasks: list[dbx_jobs.SubmitTask]) -> Any: ...
    def get_run(self, *, run_id: int) -> Any: ...
    def cancel_run(self, *, run_id: int) -> Any: ...


class WorkspaceClientLike(Protocol):
    jobs: JobsAPILike


@dataclass
class DatabricksProfile:
    host: str
    cluster_node_type_id: str
    num_workers: int = 1


def _split_entry_point(entry_point: str) -> tuple[str, str]:
    """"wordcount.jobs.count" -> ("wordcount", "count") — package name is the
    first path segment, entry point name is the last (spec §17's artifact
    convention, extended for Databricks' console_scripts requirement)."""
    package_name = entry_point.split(".")[0]
    entry_point_name = entry_point.rsplit(".", 1)[-1]
    return package_name, entry_point_name


class DatabricksExecutionProvider:
    def __init__(self, profile: DatabricksProfile, client: WorkspaceClientLike | None = None):
        self.profile = profile
        # Testing seam — unit tests inject a fake client so no live
        # workspace/network call is ever made (ADR: mocked-only for Phase 0).
        # None is only valid for translation-only use (build_run_submission);
        # submit/status/cancel require a real client and assert accordingly.
        self._client = client

    async def validate(self, workload) -> ValidationResult:
        errors = []
        if workload.workload.runtime.spark not in _SUPPORTED_SPARK_VERSIONS:
            errors.append(f"unsupported Spark version: {workload.workload.runtime.spark}")
        if workload.workload.application.type != "python-wheel":
            errors.append(
                "Databricks provider prototype only supports python-wheel artifacts, "
                f"got {workload.workload.application.type}"
            )
        return ValidationResult(valid=not errors, errors=errors)

    def build_run_submission(self, run: RunRequest) -> dbx_jobs.SubmitTask:
        """Pure translation function — resolved workload -> Jobs API 2.2
        one-time-run submission payload. No I/O, directly unit-testable."""
        workload = run.resolved.workload
        package_name, entry_point_name = _split_entry_point(workload.application.entryPoint)
        cluster_spark_version = _SPARK_TO_DBR_CLUSTER_VERSION.get(
            workload.runtime.spark, workload.runtime.spark
        )

        spark_conf = {**run.resolved.storage_config, **run.resolved.dataset_config}

        return dbx_jobs.SubmitTask(
            task_key="main",
            python_wheel_task=dbx_jobs.PythonWheelTask(
                package_name=package_name,
                entry_point=entry_point_name,
                parameters=list(workload.arguments),
            ),
            libraries=[dbx_compute.Library(whl=workload.application.artifact)],
            new_cluster=dbx_compute.ClusterSpec(
                spark_version=cluster_spark_version,
                node_type_id=self.profile.cluster_node_type_id,
                num_workers=self.profile.num_workers,
                spark_conf=spark_conf,
            ),
        )

    async def submit(self, run: RunRequest) -> ProviderRun:
        assert self._client is not None, "DatabricksExecutionProvider needs a client to submit"
        task = self.build_run_submission(run)
        result = self._client.jobs.submit(
            run_name=f"{run.resolved.workload.metadata.name}-{run.run_id}", tasks=[task]
        )
        return ProviderRun(provider_run_id=str(result.run_id))

    async def status(self, provider_run_id: str) -> ProviderStatus:
        assert self._client is not None, "DatabricksExecutionProvider needs a client to poll status"
        run = self._client.jobs.get_run(run_id=int(provider_run_id))
        state = run.state
        if state is None:
            return ProviderStatus(state=RunState.UNKNOWN, provider_native_status="UNREPORTED")

        lifecycle = state.life_cycle_state.value if state.life_cycle_state else None
        result_state = state.result_state.value if state.result_state else None

        if lifecycle == "TERMINATED" and result_state:
            canonical = _RESULT_STATE_MAP.get(result_state, RunState.UNKNOWN)
            native = f"{lifecycle}/{result_state}"
        else:
            canonical = _LIFECYCLE_STATE_MAP.get(lifecycle or "", RunState.UNKNOWN)
            native = lifecycle or "UNKNOWN"

        return ProviderStatus(state=canonical, provider_native_status=native)

    async def cancel(self, provider_run_id: str) -> None:
        assert self._client is not None, "DatabricksExecutionProvider needs a client to cancel"
        self._client.jobs.cancel_run(run_id=int(provider_run_id))

    async def logs(self, provider_run_id: str) -> LogReference:
        return LogReference(
            description="Databricks run output (provider-managed)",
            uri=f"{self.profile.host}/#job/run/{provider_run_id}",
        )

    async def capabilities(self) -> CapabilitySet:
        return CapabilitySet(
            spark_versions=sorted(_SUPPORTED_SPARK_VERSIONS),
            languages=["python", "jvm"],
            dynamic_allocation=False,
            gpu=False,
            streaming=False,
            local_disk=True,
            spark_connect=False,
        )
