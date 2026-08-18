"""Real-Postgres tests for POST /v1/validate's underlying service function,
run_service.validate_workload (spec §20-21). Uses a fake ExecutionProvider
(not real Kubernetes) so this stays CI-fast and infra-free, same pattern as
test_reconciler.py — the environment/execution-profile resolution from the
database is real, only the provider construction is faked."""

import pytest

from control_plane import provider_factory, repositories, run_service
from control_plane.execution_provider import CapabilitySet, ValidationResult
from spec.workload.v1alpha1 import parse_workload

EXAMPLES_DIR = __import__("pathlib").Path(__file__).resolve().parents[3] / "examples"


class FakeExecutionProvider:
    def __init__(self, capabilities: CapabilitySet):
        self._capabilities = capabilities

    async def capabilities(self) -> CapabilitySet:
        return self._capabilities

    async def validate(self, workload) -> ValidationResult:
        from control_plane.execution_provider import match_capabilities

        errors = match_capabilities(workload.workload, self._capabilities)
        return ValidationResult(valid=not errors, errors=errors)


_FULL_CAPS = CapabilitySet(
    spark_versions=["4.1", "4.2"],
    languages=["python", "jvm"],
    dynamic_allocation=True,
    gpu=True,
    streaming=True,
    local_disk=True,
    spark_connect=True,
)


@pytest.mark.asyncio
async def test_validate_workload_passes_for_compatible_workload(session, environment_name, monkeypatch):
    monkeypatch.setattr(
        provider_factory, "build_execution_provider", lambda execution_profile: FakeExecutionProvider(_FULL_CAPS)
    )
    workload = parse_workload(EXAMPLES_DIR / "wordcount.yaml")

    result = await run_service.validate_workload(session, workload, environment_name)

    assert result.valid is True


@pytest.mark.asyncio
async def test_validate_workload_fails_for_gpu_requirement(session, environment_name, monkeypatch):
    no_gpu_caps = CapabilitySet(
        spark_versions=["4.1", "4.2"], languages=["python", "jvm"], dynamic_allocation=True,
        gpu=False, streaming=True, local_disk=True, spark_connect=True,
    )
    monkeypatch.setattr(
        provider_factory, "build_execution_provider", lambda execution_profile: FakeExecutionProvider(no_gpu_caps)
    )
    workload = parse_workload(EXAMPLES_DIR / "wordcount.yaml")
    workload.requirements.gpu = True

    result = await run_service.validate_workload(session, workload, environment_name)

    assert result.valid is False
    assert any("GPU" in e for e in result.errors)


@pytest.mark.asyncio
async def test_validate_workload_requires_existing_environment(session, monkeypatch):
    monkeypatch.setattr(
        provider_factory, "build_execution_provider", lambda execution_profile: FakeExecutionProvider(_FULL_CAPS)
    )
    workload = parse_workload(EXAMPLES_DIR / "wordcount.yaml")

    with pytest.raises(repositories.NotFoundError):
        await run_service.validate_workload(session, workload, "ghost-environment")
