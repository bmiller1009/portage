"""The ExecutionProvider protocol every execution provider implements
(docs/architecture/spec.md §13, ADR 0005). Capability-driven, not
if-provider-branching — see providers/execution/kubernetes and
providers/execution/databricks for the two Phase 0 implementations.
"""

from dataclasses import dataclass, field
from typing import Protocol

from control_plane.run_state import RunState
from spec.workload.v1alpha1 import SparkWorkload


@dataclass
class ResolvedWorkload:
    """A workload plus its environment resolution: dataset config
    (portable.dataset.<name>.uri -> value, per ADR 0006) and storage
    provider config (credentials, endpoint, required jars — spec §49) kept
    as separate fields since they come from different resolution steps."""

    workload: SparkWorkload
    dataset_config: dict[str, str]
    environment_name: str
    storage_config: dict[str, str] = field(default_factory=dict)


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
class ValidationResult:
    valid: bool
    errors: list[str] = field(default_factory=list)


@dataclass
class CapabilitySet:
    """Provider capability declaration (spec §20)."""

    spark_versions: list[str]
    languages: list[str]
    dynamic_allocation: bool
    gpu: bool
    streaming: bool
    local_disk: bool
    spark_connect: bool


class ExecutionProvider(Protocol):
    async def validate(self, workload: ResolvedWorkload) -> ValidationResult: ...
    async def submit(self, run: RunRequest) -> ProviderRun: ...
    async def status(self, provider_run_id: str) -> ProviderStatus: ...
    async def cancel(self, provider_run_id: str) -> None: ...
    async def logs(self, provider_run_id: str) -> LogReference: ...
    async def capabilities(self) -> CapabilitySet: ...
