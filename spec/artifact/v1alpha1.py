"""Artifact repository schema, v1alpha1 (docs/architecture/spec.md §51).
Deliberately mirrors spec/dataset/v1alpha1.py's Dataset/PathBinding/
resolve_dataset_config() — same shape of problem (a logical identifier
resolving to a per-environment physical location), just for application
artifacts instead of datasets: "artifact://claims-normalization/4.18.2"
resolves to an S3 object, an ADLS object, or an internal artifact
repository, depending on environment — "this prevents workload
definitions from embedding deployment infrastructure" (§51). Path
bindings only, matching the dataset model's own current scope.
"""

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel


class ArtifactMetadata(BaseModel):
    name: str
    version: str


class ArtifactPathBinding(BaseModel):
    kind: Literal["path"] = "path"
    uri: str


class Artifact(BaseModel):
    apiVersion: Literal["runtime/v1alpha1"]
    kind: Literal["Artifact"]
    metadata: ArtifactMetadata
    bindings: dict[str, ArtifactPathBinding]


def parse_artifact(path: str | Path) -> Artifact:
    """Load and validate an Artifact binding definition from a YAML file."""
    with open(path) as f:
        raw = yaml.safe_load(f)
    return Artifact.model_validate(raw)


class ArtifactResolutionError(Exception):
    """Raised for an unparseable artifact:// reference, or one with no
    binding for the target environment (spec §51's resolver)."""


def parse_artifact_reference(reference: str) -> tuple[str, str]:
    """"artifact://wordcount/0.1.0" -> ("wordcount", "0.1.0")."""
    if not reference.startswith("artifact://"):
        raise ArtifactResolutionError(f"not an artifact:// reference: {reference}")
    rest = reference.removeprefix("artifact://")
    name, _, version = rest.partition("/")
    if not name or not version:
        raise ArtifactResolutionError(
            f"malformed artifact reference (expected artifact://<name>/<version>): {reference}"
        )
    return name, version


def resolve_artifact_uri(reference: str, artifact: Artifact, environment: str) -> str:
    """Resolves an artifact:// reference to its environment-specific URI.
    Unlike resolve_dataset_config(), which resolves a whole workload's
    dataset set at once, this takes a single reference — a workload has
    exactly one application artifact (spec §17), not a dict of them."""
    binding = artifact.bindings.get(environment)
    if binding is None:
        name, version = parse_artifact_reference(reference)
        raise ArtifactResolutionError(
            f"artifact '{name}/{version}' has no binding for environment '{environment}'"
        )
    return binding.uri
