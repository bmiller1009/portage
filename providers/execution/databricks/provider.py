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

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Protocol, cast

import requests
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
    compute_portability_status,
    match_capabilities,
)
from control_plane.run_state import RunState
from providers.execution.databricks import compatibility

logger = logging.getLogger(__name__)

# Databricks SDK error types spec §26 says are safe to retry (rate limit,
# transient unavailability, transient server error, timeout) — everything
# else DatabricksError-derived (bad request, permission denied, not found,
# unauthenticated, ...) is terminal. Confirmed real via SDK introspection
# (databricks.sdk.errors.STATUS_CODE_MAPPING), not guessed.
#
# requests.exceptions.ConnectionError/Timeout cover the workspace being
# genuinely unreachable (DNS failure, connection refused/timed out) —
# distinct from every DatabricksError above, which only wraps a response
# the workspace actually sent. The SDK's HTTP layer is requests-based and
# raises these directly, uncaught by any DatabricksError clause. Applied
# by the same reasoning as the identical, live-confirmed gap in
# providers/execution/kubernetes/provider.py's MaxRetryError handling
# (tests/chaos/test_provider_outage_recovery.py) — not independently
# live-verified against a real unreachable Databricks workspace, since
# doing so would require disrupting the only live-verified workspace this
# project has.
_RETRYABLE_DATABRICKS_ERRORS = (
    dbx_errors.TooManyRequests,
    dbx_errors.RequestLimitExceeded,
    dbx_errors.TemporarilyUnavailable,
    dbx_errors.InternalError,
    dbx_errors.DeadlineExceeded,
    dbx_errors.OperationTimeout,
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
)

# Databricks Runtime <-> Apache Spark version compatibility now lives in
# compatibility.py, along with the reasoning for why it's a curated table
# rather than a fully discovery-driven one.
_DBR_CLUSTER_VERSION_PATTERN = re.compile(r"^\d+(\.\d+)?\.x-[\w.\-]+$")

# Confirmed live (v0.3): environment_version "2" does not support wheel
# dependency loading for serverless Python-wheel tasks; "4" does.
_SERVERLESS_ENVIRONMENT_KEY = "portage-serverless"
_SERVERLESS_ENVIRONMENT_VERSION = "4"

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
        self,
        *,
        run_name: str,
        tasks: list[dbx_jobs.SubmitTask],
        environments: list[dbx_jobs.JobEnvironment] | None = None,
        idempotency_token: str | None = None,
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
    # Confirmed live (v0.3): some workspaces administratively forbid
    # classic job clusters ("Only serverless compute is supported in the
    # workspace") -- jobs.submit() then must omit new_cluster entirely
    # and reference a serverless JobEnvironment instead (see
    # build_run_submission()/build_job_environments() below). Opt-in,
    # defaulting False, so every workspace that *does* allow classic
    # clusters (the common case, and what every existing test assumes)
    # is unaffected.
    serverless: bool = False
    # Runtime profile name -> provider-specific hint dict (spec §18),
    # environment-scoped rather than a global registry — e.g.
    # {"high-memory": {"node_type_id": "r5.4xlarge"}}.
    runtime_profiles: dict[str, dict] = field(default_factory=dict)
    # {"provider": "env", "reference": "PORTAGE_DATABRICKS"} (spec §35) —
    # resolved lazily by _get_client(), never at profile-construction time,
    # so a profile with no real credentials configured anywhere is still
    # valid for translation-only use (validate(), build_run_submission()).
    credential_reference: dict = field(default_factory=dict)
    # Explicit-override escape hatch (req: "explicit compatible override ->
    # accepted", "invalid runtime override -> rejected") — {portable spark
    # version: DBR cluster-version key}, e.g. {"4.3": "20.x-scala2.13"},
    # for a workspace running a Databricks Runtime ahead of
    # compatibility.py's curated table. Only consulted for spark versions
    # not already in that curated table (see
    # compatibility.resolve_cluster_version), so it can never silently
    # shadow a known-good mapping. Validated eagerly in __post_init__
    # (fails at profile construction, not at first use) since a malformed
    # override is a configuration mistake, not a runtime condition.
    dbr_cluster_version_overrides: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for spark_version, cluster_version in self.dbr_cluster_version_overrides.items():
            if not _DBR_CLUSTER_VERSION_PATTERN.match(cluster_version):
                raise ValueError(
                    f"invalid dbr_cluster_version_overrides entry for Spark {spark_version!r}: "
                    f"{cluster_version!r} doesn't look like a real Databricks Runtime cluster "
                    "version key (expected a shape like '19.x-scala2.13' or "
                    "'17.3.x-scala2.13')"
                )


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
        # (spark_version, cluster_version) pairs already cross-checked this
        # process's lifetime — compatibility.cross_check_against_workspace()
        # is a real API call, so this keeps it to once per pair per
        # long-lived provider instance rather than once per submission.
        self._cross_checked_versions: set[tuple[str, str]] = set()

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

    def _cross_check_compatibility_once(self, client: WorkspaceClientLike, spark_version: str) -> None:
        cluster_version = compatibility.resolve_cluster_version(
            spark_version, override=self.profile.dbr_cluster_version_overrides.get(spark_version)
        )
        if cluster_version is None:
            return
        cache_key = (spark_version, cluster_version)
        if cache_key in self._cross_checked_versions:
            return
        self._cross_checked_versions.add(cache_key)
        warning = compatibility.cross_check_against_workspace(client, spark_version, cluster_version)
        if warning:
            logger.warning(warning)

    async def validate(self, workload) -> ValidationResult:
        errors = match_capabilities(workload.workload, await self.capabilities())
        portability = compute_portability_status(workload.workload)
        return ValidationResult(
            valid=not errors,
            errors=errors,
            portability_status=portability.status,
            provider_overrides=portability.overrides_by_provider,
        )

    def build_run_submission(self, run: RunRequest) -> dbx_jobs.SubmitTask:
        """Pure translation function — resolved workload -> Jobs API 2.2
        one-time-run submission payload. No I/O, directly unit-testable.

        **Confirmed live (v0.3)**: when `self.profile.serverless` is set,
        the returned task has no `new_cluster` to attach `spark_conf` to,
        and Databricks Serverless compute has no per-task or
        per-environment equivalent (confirmed against the SDK's own
        `PythonWheelTask`/`compute.Environment` dataclasses — neither
        exposes a spark_conf-shaped field). Worse: even a *classic*
        cluster's `spark_conf` wouldn't help here, because Serverless's
        `SparkSession.builder.getOrCreate()` is backed by Spark Connect,
        and Spark Connect's own config-read RPC rejects any non-Spark-
        builtin key outright ([CONFIG_NOT_AVAILABLE.WITHOUT_SUGGESTION]),
        independent of how it was set. The Jobs API's own
        `environment_variables_key` (SubmitTask -> JobEnvironmentVariables)
        looked like the fix but confirmed live *not* to actually reach a
        serverless python_wheel_task's process environment (tested via a
        raw SDK submission with no Portage code involved at all). What
        does work, confirmed live end-to-end: `PythonWheelTask.parameters`
        as plain `--<key>=<value>` strings, surfaced as ordinary
        `sys.argv` entries ahead of the wheel's console_scripts entry
        point. So `run.resolved.storage_config`/`dataset_config` (ADR
        0006's `spark.portable.dataset.*.uri` contract every provider
        otherwise relies on) are appended to `parameters` that way for
        serverless specifically — `examples/wordcount_app`'s own job code
        (`_portable_config()`) already falls back to parsing them from
        `sys.argv` when `spark.conf.get()` is rejected, so this needs no
        per-application special-casing beyond that one shared helper."""
        workload = run.resolved.workload
        cluster_spark_version = compatibility.resolve_cluster_version(
            workload.runtime.spark,
            override=self.profile.dbr_cluster_version_overrides.get(workload.runtime.spark),
        )
        if cluster_spark_version is None:
            # validate() (called before submission in both the API's
            # POST /v1/validate path and the reconciler's own pre-submit
            # check) should already have caught this via capabilities() —
            # reaching here means something submitted without validating
            # first, so fail loudly rather than guess at a cluster version
            # key that was never confirmed to exist.
            raise TerminalProviderError(
                f"no known or overridden Databricks Runtime for Spark {workload.runtime.spark!r}"
            )

        spark_conf = {**run.resolved.storage_config, **run.resolved.dataset_config}

        if workload.application.type == "spark-declarative-pipeline":
            # Not yet supported here (spec §39's Declarative Pipelines work is
            # scoped to the Kubernetes provider for v0.6.5 -- see
            # providers/execution/kubernetes/provider.py's own docstring for
            # the confirmed-live Spark Connect blocker even there). Reject
            # explicitly rather than silently falling into the python-wheel
            # branch below, where a None entryPoint would crash instead.
            raise TerminalProviderError(
                "Databricks provider does not yet support spark-declarative-pipeline workloads"
            )
        if workload.application.type == "jvm-jar":
            if self.profile.serverless:
                # Confirmed against Databricks' own Jobs API docs: Serverless
                # compute supports notebook/Python-wheel/Python-script/dbt
                # task types, not spark_jar_task -- a real cluster is the
                # only way to run a JVM main class today.
                raise TerminalProviderError(
                    "Databricks serverless compute does not support jvm-jar (spark_jar_task) workloads"
                )
            task_kwargs = {
                "spark_jar_task": dbx_jobs.SparkJarTask(
                    main_class_name=workload.application.entryPoint,
                    parameters=list(workload.arguments),
                ),
                "libraries": [dbx_compute.Library(jar=workload.application.artifact)],
            }
        else:
            # ApplicationSpec's own validator (spec/workload/v1alpha1.py)
            # guarantees entryPoint is set for every type but
            # spark-declarative-pipeline, already handled above.
            assert workload.application.entryPoint is not None
            package_name, entry_point_name = _split_entry_point(workload.application.entryPoint)
            if self.profile.serverless:
                # Serverless resolves dependencies from the task's
                # environment_key (see build_job_environments()), not a
                # `libraries` list on the task itself -- confirmed live,
                # `libraries` is simply ignored there. spark_conf reaches
                # the job as extra --key=value parameters (see this
                # method's own docstring for why, and _portable_config()
                # in examples/wordcount_app for the matching read side).
                task_kwargs = {
                    "python_wheel_task": dbx_jobs.PythonWheelTask(
                        package_name=package_name,
                        entry_point=entry_point_name,
                        parameters=[
                            *workload.arguments,
                            *(f"--{k}={v}" for k, v in spark_conf.items()),
                        ],
                    ),
                    "environment_key": _SERVERLESS_ENVIRONMENT_KEY,
                }
            else:
                task_kwargs = {
                    "python_wheel_task": dbx_jobs.PythonWheelTask(
                        package_name=package_name,
                        entry_point=entry_point_name,
                        parameters=list(workload.arguments),
                    ),
                    "libraries": [dbx_compute.Library(whl=workload.application.artifact)],
                }

        if self.profile.serverless:
            return dbx_jobs.SubmitTask(task_key="main", **task_kwargs)

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

    def build_job_environments(self, run: RunRequest) -> list[dbx_jobs.JobEnvironment]:
        """Pure translation function, paired with build_run_submission()'s
        `environment_key` -- serverless Python-wheel tasks resolve their
        wheel dependency from this job-level `environments` list rather
        than the task's own `libraries` (see build_run_submission()'s
        docstring for the confirmed-live spark_conf gap this doesn't
        solve). Empty for non-serverless profiles and non-python-wheel
        types, since jobs.submit(environments=[]) is a harmless no-op."""
        if not self.profile.serverless:
            return []
        workload = run.resolved.workload
        if workload.application.type != "python-wheel":
            return []
        # ApplicationSpec's validator guarantees artifact is set for
        # python-wheel.
        assert workload.application.artifact is not None
        return [
            dbx_jobs.JobEnvironment(
                environment_key=_SERVERLESS_ENVIRONMENT_KEY,
                spec=dbx_compute.Environment(
                    environment_version=_SERVERLESS_ENVIRONMENT_VERSION,
                    dependencies=[workload.application.artifact],
                ),
            )
        ]

    async def submit(self, run: RunRequest) -> ProviderRun:
        task = self.build_run_submission(run)
        environments = self.build_job_environments(run)
        client = self._get_client()
        self._cross_check_compatibility_once(client, run.resolved.workload.runtime.spark)
        try:
            result = client.jobs.submit(
                run_name=f"{run.resolved.workload.metadata.name}-{run.run_id}",
                tasks=[task],
                environments=environments or None,
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
        # Curated table plus any operator-configured overrides (spec: "an
        # explicit compatible override -> accepted") — a workload
        # targeting an override-only Spark version must pass
        # match_capabilities() the same way one targeting a curated-table
        # version does, or the override would be accepted at submission
        # but rejected at validate(), an inconsistency no operator should
        # have to work around.
        spark_versions = compatibility.SUPPORTED_SPARK_VERSIONS | set(
            self.profile.dbr_cluster_version_overrides
        )
        return CapabilitySet(
            spark_versions=sorted(spark_versions),
            languages=["python", "jvm"],
            dynamic_allocation=False,
            gpu=False,
            streaming=False,
            local_disk=True,
            spark_connect=False,
            # Real live runs since v0.3 (OAuth M2M against a real
            # workspace) — not a translation-layer prototype today, even
            # though this module's own header docstring predates that and
            # hasn't caught up (tracked separately, doc-audit scope).
            verification="live_verified",
        )
