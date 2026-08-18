import pytest

from spec.artifact.v1alpha1 import (
    Artifact,
    ArtifactResolutionError,
    parse_artifact_reference,
    resolve_artifact_uri,
)


def _artifact(name: str, version: str, **bindings: str) -> Artifact:
    return Artifact.model_validate(
        {
            "apiVersion": "runtime/v1alpha1",
            "kind": "Artifact",
            "metadata": {"name": name, "version": version},
            "bindings": {env: {"kind": "path", "uri": uri} for env, uri in bindings.items()},
        }
    )


def test_parse_artifact_reference():
    assert parse_artifact_reference("artifact://wordcount/0.1.0") == ("wordcount", "0.1.0")


def test_parse_artifact_reference_rejects_non_artifact_scheme():
    with pytest.raises(ArtifactResolutionError, match="not an artifact:// reference"):
        parse_artifact_reference("s3a://bucket/wordcount.whl")


@pytest.mark.parametrize("reference", ["artifact://wordcount", "artifact://wordcount/", "artifact:///0.1.0"])
def test_parse_artifact_reference_rejects_malformed_reference(reference):
    with pytest.raises(ArtifactResolutionError, match="malformed artifact reference"):
        parse_artifact_reference(reference)


def test_resolve_artifact_uri_for_environment():
    artifact = _artifact("wordcount", "0.1.0", k8s_remote="s3a://portage-phase0/artifacts/wordcount-0.1.0.whl")

    uri = resolve_artifact_uri("artifact://wordcount/0.1.0", artifact, "k8s_remote")

    assert uri == "s3a://portage-phase0/artifacts/wordcount-0.1.0.whl"


def test_resolve_artifact_uri_raises_when_no_binding_for_environment():
    artifact = _artifact("wordcount", "0.1.0", other_env="s3a://bucket/wordcount-0.1.0.whl")

    with pytest.raises(ArtifactResolutionError, match="no binding for environment 'k8s_remote'"):
        resolve_artifact_uri("artifact://wordcount/0.1.0", artifact, "k8s_remote")
