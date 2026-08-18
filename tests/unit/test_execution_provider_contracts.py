"""Provider contract tests (spec §52.2): "Every ExecutionProvider runs the
same suite." The automated, always-on form of "provider certification"
(spec §78/§67) — a new execution provider added later has to satisfy this
exact suite too, not just its own hand-picked tests. Reuses each
provider's existing fake client (FakeCustomObjectsApi, FakeJobsAPI) rather
than duplicating them.

Every test function calls _cases() itself, so each test gets its own
fresh provider + fake-client instances — no state leaks between tests
sharing the same parametrize id.
"""

import asyncio
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from control_plane.execution_provider import (
    ExecutionProvider,
    ResolvedWorkload,
    RetryableProviderError,
    RunRequest,
    TerminalProviderError,
)
from control_plane.run_state import RunState
from providers.execution.databricks.provider import (
    DatabricksExecutionProvider,
    DatabricksProfile,
    WorkspaceClientLike,
)
from providers.execution.kubernetes.provider import KubernetesExecutionProvider, KubernetesProfile
from spec.workload.v1alpha1 import parse_workload
from tests.unit.test_databricks_provider import FakeJobsAPI, _fake_run
from tests.unit.test_kubernetes_provider import FakeCustomObjectsApi

EXAMPLES_DIR = Path(__file__).resolve().parents[2] / "examples"


@dataclass
class ExecutionProviderCase:
    name: str
    provider: ExecutionProvider
    valid_run: RunRequest
    invalid_run: RunRequest  # unsupported spark version -> validate() rejects
    inject_error: Any  # (case, exception) -> None, makes the next submit() raise it
    make_retryable_error: Any
    make_terminal_error: Any
    set_status_succeeded: Any  # configures the fake so status() reports SUCCEEDED


def _kubernetes_case() -> ExecutionProviderCase:
    from kubernetes.client.exceptions import ApiException

    profile = KubernetesProfile(namespace="default", service_account="spark", image="portage/wordcount:0.1.0")
    fake_api = FakeCustomObjectsApi()
    provider = KubernetesExecutionProvider(profile, api_client=fake_api)

    workload = parse_workload(EXAMPLES_DIR / "wordcount.yaml")
    resolved = ResolvedWorkload(workload=workload, dataset_config={}, environment_name="contract-test")
    valid_run = RunRequest(run_id="contract-test-1", resolved=resolved)

    unsupported = workload.model_copy(update={"runtime": workload.runtime.model_copy(update={"spark": "3.5"})})
    invalid_run = RunRequest(
        run_id="contract-test-2",
        resolved=ResolvedWorkload(workload=unsupported, dataset_config={}, environment_name="contract-test"),
    )

    return ExecutionProviderCase(
        name="kubernetes",
        provider=provider,
        valid_run=valid_run,
        invalid_run=invalid_run,
        inject_error=lambda error: setattr(fake_api, "raise_on_create", error),
        make_retryable_error=lambda: ApiException(status=429, reason="rate limited"),
        make_terminal_error=lambda: ApiException(status=400, reason="bad request"),
        set_status_succeeded=lambda: setattr(
            fake_api, "status_to_return", {"currentState": {"currentStateSummary": "Succeeded"}}
        ),
    )


def _databricks_case() -> ExecutionProviderCase:
    from databricks.sdk import errors as dbx_errors

    profile = DatabricksProfile(host="https://example.databricks.com", cluster_node_type_id="i3.xlarge")
    fake_jobs = FakeJobsAPI()
    provider = DatabricksExecutionProvider(
        profile, client=cast(WorkspaceClientLike, SimpleNamespace(jobs=fake_jobs))
    )

    workload = parse_workload(EXAMPLES_DIR / "wordcount.yaml")
    workload = workload.model_copy(update={"runtime": workload.runtime.model_copy(update={"spark": "4.1"})})
    resolved = ResolvedWorkload(workload=workload, dataset_config={}, environment_name="contract-test")
    valid_run = RunRequest(run_id="contract-test-1", resolved=resolved)

    unsupported = workload.model_copy(update={"runtime": workload.runtime.model_copy(update={"spark": "3.5"})})
    invalid_run = RunRequest(
        run_id="contract-test-2",
        resolved=ResolvedWorkload(workload=unsupported, dataset_config={}, environment_name="contract-test"),
    )

    return ExecutionProviderCase(
        name="databricks",
        provider=provider,
        valid_run=valid_run,
        invalid_run=invalid_run,
        inject_error=lambda error: setattr(fake_jobs, "raise_on_submit", error),
        make_retryable_error=lambda: dbx_errors.TooManyRequests("rate limited"),
        make_terminal_error=lambda: dbx_errors.PermissionDenied("nope"),
        set_status_succeeded=lambda: setattr(fake_jobs, "run_to_return", _fake_run("TERMINATED", "SUCCESS")),
    )


def _cases() -> list[ExecutionProviderCase]:
    return [_kubernetes_case(), _databricks_case()]


_CASE_IDS = [c.name for c in _cases()]


@pytest.mark.parametrize("case", _cases(), ids=_CASE_IDS)
def test_capabilities_are_well_formed(case: ExecutionProviderCase):
    caps = asyncio.run(case.provider.capabilities())
    assert caps.spark_versions
    assert caps.languages


@pytest.mark.parametrize("case", _cases(), ids=_CASE_IDS)
def test_validate_accepts_a_valid_workload(case: ExecutionProviderCase):
    result = asyncio.run(case.provider.validate(case.valid_run.resolved))
    assert result.valid is True
    assert result.errors == []


@pytest.mark.parametrize("case", _cases(), ids=_CASE_IDS)
def test_validate_rejects_an_invalid_workload(case: ExecutionProviderCase):
    result = asyncio.run(case.provider.validate(case.invalid_run.resolved))
    assert result.valid is False
    assert result.errors


@pytest.mark.parametrize("case", _cases(), ids=_CASE_IDS)
def test_submit_returns_a_provider_run_with_an_id(case: ExecutionProviderCase):
    result = asyncio.run(case.provider.submit(case.valid_run))
    assert result.provider_run_id


@pytest.mark.parametrize("case", _cases(), ids=_CASE_IDS)
def test_submit_classifies_a_retryable_provider_error(case: ExecutionProviderCase):
    case.inject_error(case.make_retryable_error())
    with pytest.raises(RetryableProviderError):
        asyncio.run(case.provider.submit(case.valid_run))


@pytest.mark.parametrize("case", _cases(), ids=_CASE_IDS)
def test_submit_classifies_a_terminal_provider_error(case: ExecutionProviderCase):
    case.inject_error(case.make_terminal_error())
    with pytest.raises(TerminalProviderError):
        asyncio.run(case.provider.submit(case.valid_run))


@pytest.mark.parametrize("case", _cases(), ids=_CASE_IDS)
def test_status_maps_a_terminal_native_state_to_succeeded(case: ExecutionProviderCase):
    provider_run = asyncio.run(case.provider.submit(case.valid_run))
    case.set_status_succeeded()
    status = asyncio.run(case.provider.status(provider_run.provider_run_id))
    assert status.state == RunState.SUCCEEDED


@pytest.mark.parametrize("case", _cases(), ids=_CASE_IDS)
def test_cancel_does_not_raise(case: ExecutionProviderCase):
    provider_run = asyncio.run(case.provider.submit(case.valid_run))
    asyncio.run(case.provider.cancel(provider_run.provider_run_id))  # no exception == pass


@pytest.mark.parametrize("case", _cases(), ids=_CASE_IDS)
def test_logs_returns_a_description(case: ExecutionProviderCase):
    provider_run = asyncio.run(case.provider.submit(case.valid_run))
    ref = asyncio.run(case.provider.logs(provider_run.provider_run_id))
    assert ref.description
