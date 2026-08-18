"""Databricks execution provider (docs/architecture/spec.md §16, ADR 0004).

Translation-layer prototype only — no live Databricks workspace was
available during Phase 0, so this is tested against a fake WorkspaceClient
(see tests/unit/test_databricks_provider.py), never a real one. Live
submission is future work once workspace credentials exist.

Translates a resolved workload into a Jobs API 2.2 one-time-run submission
(`jobs.submit`) using a python_wheel_task or spark_jar_task depending on
`application.type` (spec §16's guidance to prefer wheel/JAR tasks over
notebooks). The wheel task's `package_name`/`entry_point` are derived from
the workload's dotted entryPoint ("wordcount.jobs.count" -> package
"wordcount", entry point "count") — the example wheel under
examples/wordcount_app registers a matching console_scripts entry point so
the same artifact is invocable by both providers (see examples/wordcount_app/
pyproject.toml and providers/execution/kubernetes/provider.py's launcher).
The jar task's entryPoint is used directly as `main_class_name` — no
splitting needed, since a JVM main class isn't a console_scripts entry point.

Client construction is lazy (see _get_client()): validate() and
build_run_submission() are pure translation and never touch a live client,
so a Databricks-provider environment can be statically validated with zero
Databricks credentials configured anywhere. Only submit()/status()/cancel()
construct a real WorkspaceClient, via OAuth M2M (spec §66) using
credentials resolved through control_plane.credentials at that point, not
at provider-construction time.
"""

from dataclasses import dataclass, field
from typing import Any, Protocol, cast

from databricks.sdk import errors as dbx_errors
from databricks.sdk.errors.base import DatabricksError
from databricks.sdk.service import compute as dbx_compute
from databricks.sdk.service import jobs as dbx_jobs

from control_plane.credentials import resolve_databricks_credentials
from control_plane.execution_provider import (
    CapabilitySet,
    LogReference,
    ProviderRun,
    ProviderStatus,
    RetryableProviderError,
    RunRequest,
    TerminalProviderError,
    ValidationResult,
    match_capabilities,
)
from control_plane.run_state import RunState

# Databricks SDK error types spec §26 says are safe to retry (rate limit,
# transient unavailability, transient server error, timeout) — everything
# else DatabricksError-derived (bad request, permission denied, not found,
# unauthenticated, ...) is terminal. Confirmed real via SDK introspection
# (databricks.sdk.errors.STATUS_CODE_MAPPING), not guessed.
_RETRYABLE_DATABRICKS_ERRORS = (
    dbx_errors.TooManyRequests,
    dbx_errors.RequestLimitExceeded,
    dbx_errors.TemporarilyUnavailable,
    dbx_errors.InternalError,
    dbx_errors.DeadlineExceeded,
    dbx_errors.OperationTimeout,
)

_SUPPORTED_SPARK_VERSIONS = {"4.0", "4.1"}

# Databricks Runtime <-> Apache Spark version compatibility (spec §46),
# verified against current public docs (docs.databricks.com/aws/en/release-
# notes/runtime/, checked Aug 2026): Databricks Runtime 17.3 LTS ships
# Spark 4.0.0; the 18.x series (latest 18.2) ships Spark 4.1.0. No
# Databricks Runtime ships Spark 4.2 yet, so it's deliberately excluded
# from _SUPPORTED_SPARK_VERSIONS above — claiming support for it would
# make validate() return a false PASS for a workload that cannot actually
# run on any Databricks cluster today. The exact Jobs API `spark_version`
# cluster identifier string (e.g. "17.3.x-scala2.13"-shaped) is only
# available from a live workspace via w.clusters.spark_versions() — these
# are best-effort values from public docs, to be confirmed against a real
# workspace before any live submission is attempted.
_SPARK_TO_DBR_CLUSTER_VERSION = {
    "4.1": "PLACEHOLDER-runtime-18.x-confirm-via-clusters.spark_versions",
    "4.0": "PLACEHOLDER-runtime-17.3-lts-confirm-via-clusters.spark_versions",
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

    def submit(
        self, *, run_name: str, tasks: list[dbx_jobs.SubmitTask], idempotency_token: str | None = None
    ) -> Any: ...
    def get_run(self, *, run_id: int) -> Any: ...
    def cancel_run(self, *, run_id: int) -> Any: ...


class WorkspaceClientLike(Protocol):
    jobs: JobsAPILike


@dataclass
class DatabricksProfile:
    host: str
    cluster_node_type_id: str
    num_workers: int = 1
    # Runtime profile name -> provider-specific hint dict (spec §18),
    # environment-scoped rather than a global registry — e.g.
    # {"high-memory": {"node_type_id": "r5.4xlarge"}}.
    runtime_profiles: dict[str, dict] = field(default_factory=dict)
    # {"provider": "env", "reference": "PORTAGE_DATABRICKS"} (spec §35) —
    # resolved lazily by _get_client(), never at profile-construction time,
    # so a profile with no real credentials configured anywhere is still
    # valid for translation-only use (validate(), build_run_submission()).
    credential_reference: dict = field(default_factory=dict)


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
        # None means "construct a real one lazily, on first actual need" —
        # see _get_client(). validate()/build_run_submission() never call
        # it, so translation-only use never requires credentials at all.
        self._client = client

    def _get_client(self) -> WorkspaceClientLike:
        if self._client is not None:
            return self._client
        from databricks.sdk import WorkspaceClient

        credentials = resolve_databricks_credentials(self.profile.credential_reference)
        self._client = cast(
            WorkspaceClientLike,
            WorkspaceClient(
                host=self.profile.host,
                client_id=credentials.client_id,
                client_secret=credentials.client_secret,
            ),
        )
        return self._client

    async def validate(self, workload) -> ValidationResult:
        errors = match_capabilities(workload.workload, await self.capabilities())
        return ValidationResult(valid=not errors, errors=errors)

    def build_run_submission(self, run: RunRequest) -> dbx_jobs.SubmitTask:
        """Pure translation function — resolved workload -> Jobs API 2.2
        one-time-run submission payload. No I/O, directly unit-testable."""
        workload = run.resolved.workload
        cluster_spark_version = _SPARK_TO_DBR_CLUSTER_VERSION.get(
            workload.runtime.spark, workload.runtime.spark
        )

        spark_conf = {**run.resolved.storage_config, **run.resolved.dataset_config}

        if workload.application.type == "jvm-jar":
            task_kwargs = {
                "spark_jar_task": dbx_jobs.SparkJarTask(
                    main_class_name=workload.application.entryPoint,
                    parameters=list(workload.arguments),
                ),
                "libraries": [dbx_compute.Library(jar=workload.application.artifact)],
            }
        else:
            package_name, entry_point_name = _split_entry_point(workload.application.entryPoint)
            task_kwargs = {
                "python_wheel_task": dbx_jobs.PythonWheelTask(
                    package_name=package_name,
                    entry_point=entry_point_name,
                    parameters=list(workload.arguments),
                ),
                "libraries": [dbx_compute.Library(whl=workload.application.artifact)],
            }

        node_type_id = self.profile.cluster_node_type_id
        if workload.runtime.profile:
            # Not found in this environment's config is a no-op, not an
            # error (spec §18: "provider translation can determine the
            # actual infrastructure") — falls back to the profile default.
            node_type_id = self.profile.runtime_profiles.get(workload.runtime.profile, {}).get(
                "node_type_id", node_type_id
            )

        return dbx_jobs.SubmitTask(
            task_key="main",
            new_cluster=dbx_compute.ClusterSpec(
                spark_version=cluster_spark_version,
                node_type_id=node_type_id,
                num_workers=self.profile.num_workers,
                spark_conf=spark_conf,
            ),
            **task_kwargs,
        )

    async def submit(self, run: RunRequest) -> ProviderRun:
        task = self.build_run_submission(run)
        try:
            result = self._get_client().jobs.submit(
                run_name=f"{run.resolved.workload.metadata.name}-{run.run_id}",
                tasks=[task],
                # run.run_id is the run's own stable UUID (spec §26/§67) —
                # a retried submit() with the same token returns the
                # existing run instead of creating a second real
                # execution, confirmed real via SDK introspection
                # (JobsAPI.submit(..., idempotency_token: Optional[str])).
                idempotency_token=run.run_id,
            )
            return ProviderRun(provider_run_id=str(result.run_id))
        except _RETRYABLE_DATABRICKS_ERRORS as e:
            raise RetryableProviderError(str(e)) from e
        except DatabricksError as e:
            raise TerminalProviderError(str(e)) from e

    async def status(self, provider_run_id: str) -> ProviderStatus:
        try:
            run = self._get_client().jobs.get_run(run_id=int(provider_run_id))
        except _RETRYABLE_DATABRICKS_ERRORS as e:
            # spec §56's "network interruption after submission" — a
            # transient API blip while polling deserves the same retry
            # treatment as one during submission, not an immediate FAILED
            # for a run that's actually fine.
            raise RetryableProviderError(str(e)) from e
        except DatabricksError as e:
            raise TerminalProviderError(str(e)) from e

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
        try:
            self._get_client().jobs.cancel_run(run_id=int(provider_run_id))
        except dbx_errors.NotFound:
            # Already gone (e.g. a prior cancel attempt already canceled
            # it before a crash/retry) — cancel is meant to ensure a
            # non-running state, so this is success, not an error.
            return
        except _RETRYABLE_DATABRICKS_ERRORS as e:
            raise RetryableProviderError(str(e)) from e
        except DatabricksError as e:
            raise TerminalProviderError(str(e)) from e

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
