"""Unit tests for reconciler/service.py's _resolve_artifact() — pure logic
against a mocked repositories.get_artifact_binding, no database. The
end-to-end resolve/missing-binding paths through reconcile_once() are
exercised for real in tests/integration/db/test_artifacts.py."""

from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from control_plane import repositories
from control_plane.models import ArtifactBinding
from reconciler.service import _resolve_artifact
from spec.artifact.v1alpha1 import ArtifactResolutionError
from spec.workload.v1alpha1 import parse_workload

EXAMPLES_DIR = Path(__file__).resolve().parents[2] / "examples"
_SESSION = cast(AsyncSession, None)


@pytest.mark.asyncio
async def test_passes_through_non_artifact_reference_unchanged(monkeypatch):
    # examples/wordcount-jar.yaml's artifact is a local:// path baked into
    # the image, not an artifact:// reference — resolution should be a
    # complete no-op, not even touching the database.
    get_binding = AsyncMock()
    monkeypatch.setattr(repositories, "get_artifact_binding", get_binding)
    workload = parse_workload(EXAMPLES_DIR / "wordcount-jar.yaml")

    resolved = await _resolve_artifact(_SESSION, workload, "k8s-remote")

    assert resolved.application.artifact == workload.application.artifact
    get_binding.assert_not_awaited()


@pytest.mark.asyncio
async def test_resolves_artifact_reference_to_bound_uri(monkeypatch):
    monkeypatch.setattr(
        repositories,
        "get_artifact_binding",
        AsyncMock(
            return_value=ArtifactBinding(
                artifact_name="wordcount",
                artifact_version="0.1.0",
                environment_name="k8s-remote",
                kind="path",
                uri="s3a://portage-phase0/artifacts/wordcount-0.1.0.whl",
            )
        ),
    )
    workload = parse_workload(EXAMPLES_DIR / "wordcount.yaml")
    assert workload.application.artifact == "artifact://wordcount/0.1.0"

    resolved = await _resolve_artifact(_SESSION, workload, "k8s-remote")

    assert resolved.application.artifact == "s3a://portage-phase0/artifacts/wordcount-0.1.0.whl"
    # The original workload object is untouched — providers reading a
    # stale reference elsewhere would be a real bug.
    assert workload.application.artifact == "artifact://wordcount/0.1.0"


@pytest.mark.asyncio
async def test_raises_when_no_binding_for_environment(monkeypatch):
    monkeypatch.setattr(repositories, "get_artifact_binding", AsyncMock(return_value=None))
    workload = parse_workload(EXAMPLES_DIR / "wordcount.yaml")

    with pytest.raises(ArtifactResolutionError, match="no binding for environment"):
        await _resolve_artifact(_SESSION, workload, "k8s-remote")
