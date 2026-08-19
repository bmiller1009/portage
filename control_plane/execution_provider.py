"""The ExecutionProvider protocol every execution provider implements
(docs/architecture/spec.md §13, ADR 0005). Capability-driven, not
if-provider-branching — see providers/execution/kubernetes and
providers/execution/databricks for the two Phase 0 implementations.
"""

from dataclasses import dataclass, field
from typing import Literal, Protocol

from control_plane.run_state import RunState
from spec.workload.v1alpha1 import SparkWorkload


@dataclass
class ResolvedWorkload:
    """A workload plus its environment resolution: dataset config
    (portable.dataset.<name>.uri -> value, per ADR 0006) and storage
    provider config (credentials, endpoint, required jars — spec §49) kept
    as separate fields since they come from different resolution steps.

    volume_mounts is None for every storage provider except VAST NFS mode
    (spec §48) — NFS access is Kubernetes-CSI-backed, not expressible as
    Spark config at all, so it needs actual pod volume mounts instead. See
    KubernetesExecutionProvider.build_spark_application()."""

    workload: SparkWorkload
    dataset_config: dict[str, str]
    environment_name: str
    storage_config: dict[str, str] = field(default_factory=dict)
    volume_mounts: list[dict] | None = None


@dataclass
class RunRequest:
    run_id: str
    resolved: ResolvedWorkload


@dataclass
class ProviderRun:
    provider_run_id: str
    raw: dict | None = None


@dataclass
class ProviderStatus:
    state: RunState
    provider_native_status: str
    message: str = ""


@dataclass
class LogReference:
    description: str
    uri: str | None = None


@dataclass
class PortabilityStatus:
    """The result of compute_portability_status() below — a property of
    the workload definition itself (its own providerOverrides field), not
    of any particular provider's capabilities."""

    status: Literal["PORTABLE", "PORTABLE_WITH_OVERRIDES"]
    overrides_by_provider: dict[str, int]


def compute_portability_status(workload: SparkWorkload) -> PortabilityStatus:
    """ADR 0010 / spec §19: a workload with any providerOverrides entries
    is still valid and schedulable, but its portability claim must be
    reported honestly rather than as a plain PORTABLE — this is a pure
    function of the workload's own providerOverrides field, computable
    offline with no provider or environment involved (spec §19's own
    example output has no per-provider capability check in it at all,
    just a count per namespace)."""
    overrides_by_provider = {
        provider: len(overrides) for provider, overrides in workload.providerOverrides.items()
    }
    status: Literal["PORTABLE", "PORTABLE_WITH_OVERRIDES"] = (
        "PORTABLE_WITH_OVERRIDES" if any(overrides_by_provider.values()) else "PORTABLE"
    )
    return PortabilityStatus(status=status, overrides_by_provider=overrides_by_provider)


@dataclass
class ValidationResult:
    valid: bool
    errors: list[str] = field(default_factory=list)
    portability_status: Literal["PORTABLE", "PORTABLE_WITH_OVERRIDES"] = "PORTABLE"
    provider_overrides: dict[str, int] = field(default_factory=dict)


@dataclass
class CapabilitySet:
    """Provider capability declaration (spec §20).

    `verification` operationalizes a distinction this project has always
    made in prose (provider READMEs' "translation-layer prototype, never
    run against live infrastructure" caveats) but never represented in
    data: "live_verified" means this provider has actually executed a
    real workload against real infrastructure (Kubernetes, Databricks —
    both live-verified since v0.1/v0.3 respectively); "translation_layer_
    only" means it's tested against fakes/mocks only, no real
    infrastructure available to this project. Not a claim about whether
    the *capability values themselves* (spark_versions etc.) are
    trustworthy — both tiers are still the provider's own honest
    self-report, matched the same way by match_capabilities() below. Only
    execution providers are live_verified/translation_layer_only in
    exactly the same two tiers as storage providers
    (StorageCapabilitySet, control_plane/storage_provider.py) by design,
    not because every provider actually falls into only two buckets in
    general — a third tier isn't backed by real evidence anywhere in this
    project yet, so adding one now would overclaim precision the
    underlying data doesn't have.
    """

    spark_versions: list[str]
    languages: list[str]
    dynamic_allocation: bool
    gpu: bool
    streaming: bool
    local_disk: bool
    spark_connect: bool
    verification: Literal["live_verified", "translation_layer_only"] = "live_verified"


class RetryableProviderError(Exception):
    """A provider submission/status/cancel call failed in a way spec §26
    says is safe to retry (timeout, 429, transient 5xx, transient API
    blip) — the reconciler requeues the run rather than failing it.
    Providers raise this themselves, since only they know which of their
    own error shapes are transient."""


class TerminalProviderError(Exception):
    """A provider call failed in a way that must not be retried (spec
    §26: "must not silently rerun an application after confirmed
    execution failure") — the reconciler fails the run immediately, same
    as an unclassified exception, but explicitly rather than by default."""


class ExecutionProvider(Protocol):
    async def validate(self, workload: ResolvedWorkload) -> ValidationResult: ...
    async def submit(self, run: RunRequest) -> ProviderRun: ...
    async def status(self, provider_run_id: str) -> ProviderStatus: ...
    async def cancel(self, provider_run_id: str) -> None: ...
    async def logs(self, provider_run_id: str) -> LogReference: ...
    async def capabilities(self) -> CapabilitySet: ...


def match_capabilities(workload: SparkWorkload, capabilities: CapabilitySet) -> list[str]:
    """Fail-fast capability matching (spec §20-21) — shared by every
    provider's validate() so the check happens exactly once, the same way,
    whether it's driven by the reconciler before submission or by
    POST /v1/validate before a run is ever created."""
    errors: list[str] = []
    if workload.runtime.spark not in capabilities.spark_versions:
        errors.append(f"unsupported Spark version: {workload.runtime.spark}")

    language = "jvm" if workload.application.type == "jvm-jar" else "python"
    if language not in capabilities.languages:
        errors.append(f"unsupported language: {language}")

    for required, supported, label in [
        (workload.requirements.dynamicAllocation, capabilities.dynamic_allocation, "dynamic allocation"),
        (workload.requirements.gpu, capabilities.gpu, "GPU"),
        (workload.requirements.streaming, capabilities.streaming, "streaming"),
        (workload.requirements.localDisk, capabilities.local_disk, "local disk"),
        (workload.requirements.sparkConnect, capabilities.spark_connect, "Spark Connect"),
    ]:
        if required and not supported:
            errors.append(f"workload requires {label}, provider does not support it")

    return errors
