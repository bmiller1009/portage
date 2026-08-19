"""Environment schema (docs/architecture/spec.md §8). Stable as of v1.0
(docs/architecture/STABILITY.md) — apiVersion "runtime/v1"; the
"runtime/v1alpha1" name still parses but is deprecated.

Intentionally minimal: `execution.profile`/`data.profile` name an
`ExecutionProfile`/`StorageProfile` row (connection details, credential
references, catalog config) resolved by the API against the database, not
by this model — this schema itself only says which provider+profile an
environment name points at.
"""

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, model_validator

from spec.stability import warn_if_deprecated


class EnvironmentMetadata(BaseModel):
    name: str


class ExecutionRef(BaseModel):
    provider: Literal["kubernetes", "databricks"]
    profile: str


class DataRef(BaseModel):
    provider: Literal["s3", "vast", "adls"]
    profile: str


class Environment(BaseModel):
    apiVersion: Literal["runtime/v1", "runtime/v1alpha1"]
    kind: Literal["Environment"]
    metadata: EnvironmentMetadata
    execution: ExecutionRef
    data: DataRef

    @model_validator(mode="after")
    def _warn_deprecated_api_version(self) -> "Environment":
        warn_if_deprecated(self.apiVersion, "Environment")
        return self


def parse_environment(path: str | Path) -> Environment:
    with open(path) as f:
        raw = yaml.safe_load(f)
    return Environment.model_validate(raw)
