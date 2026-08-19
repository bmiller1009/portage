"""Portable Workload schema (docs/architecture/spec.md §7.1). Stable as of
v1.0 (docs/architecture/STABILITY.md) — apiVersion "runtime/v1"; the
"runtime/v1alpha1" name still parses but is deprecated."""

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, model_validator

from spec.stability import warn_if_deprecated


class WorkloadMetadata(BaseModel):
    name: str
    version: str


class RuntimeSpec(BaseModel):
    spark: str
    # Named runtime profile (spec §18 — "standard"/"high-memory"/"gpu"),
    # e.g. Kubernetes nodeSelector or a Databricks node-type SKU. Optional
    # and environment-scoped (ExecutionProfile.config["runtimeProfiles"]),
    # not a global registry — a name absent from the target environment's
    # config is not an error (§18: "provider translation can determine
    # the actual infrastructure"), just a no-op.
    profile: str | None = None


class ApplicationSpec(BaseModel):
    """type discriminates which fields are required (spec §39):
    python-wheel/jvm-jar carry a build artifact + a function/class entry
    point; spark-declarative-pipeline carries a pipelineSpec reference
    (a path to a spark-pipeline.yml, resolved through the same
    artifact:// binding mechanism as `artifact`) instead — there is no
    separate build artifact or single entry point for a declarative
    pipeline, just its spec file and the transformation sources it
    points at."""

    type: Literal["python-wheel", "jvm-jar", "spark-declarative-pipeline"]
    artifact: str | None = None
    entryPoint: str | None = None
    pipelineSpec: str | None = None

    @model_validator(mode="after")
    def _check_type_specific_fields(self) -> "ApplicationSpec":
        if self.type == "spark-declarative-pipeline":
            if not self.pipelineSpec:
                raise ValueError("pipelineSpec is required when type is 'spark-declarative-pipeline'")
        elif not self.artifact or not self.entryPoint:
            raise ValueError(f"artifact and entryPoint are required when type is '{self.type}'")
        return self


class DatasetRef(BaseModel):
    dataset: str


class DatasetsSpec(BaseModel):
    inputs: dict[str, DatasetRef] = Field(default_factory=dict)
    outputs: dict[str, DatasetRef] = Field(default_factory=dict)


class DriverResources(BaseModel):
    cores: int
    memory: str


class ExecutorResources(BaseModel):
    cores: int
    memory: str


class ScalingSpec(BaseModel):
    minExecutors: int
    maxExecutors: int


class ResourcesSpec(BaseModel):
    driver: DriverResources
    executor: ExecutorResources
    scaling: ScalingSpec


class ExecutionPolicy(BaseModel):
    timeout: str
    retries: int = 0


class RequirementsSpec(BaseModel):
    """Explicit capability requirements (spec §20-21) a provider must
    support for this workload to be schedulable there — matched against a
    provider's CapabilitySet by control_plane/execution_provider.py's
    match_capabilities(). All default False: most workloads need none of
    these, and existing workload definitions parse unchanged."""

    dynamicAllocation: bool = False
    gpu: bool = False
    streaming: bool = False
    localDisk: bool = False
    sparkConnect: bool = False


class SparkWorkload(BaseModel):
    apiVersion: Literal["runtime/v1", "runtime/v1alpha1"]
    kind: Literal["SparkWorkload"]
    metadata: WorkloadMetadata
    runtime: RuntimeSpec
    application: ApplicationSpec
    arguments: list[str] = Field(default_factory=list)
    datasets: DatasetsSpec
    resources: ResourcesSpec
    execution: ExecutionPolicy
    requirements: RequirementsSpec = Field(default_factory=RequirementsSpec)

    @model_validator(mode="after")
    def _warn_deprecated_api_version(self) -> "SparkWorkload":
        warn_if_deprecated(self.apiVersion, "SparkWorkload")
        return self


def parse_workload(path: str | Path) -> SparkWorkload:
    """Load and validate a portable workload definition from a YAML file."""
    with open(path) as f:
        raw = yaml.safe_load(f)
    return SparkWorkload.model_validate(raw)


def json_schema() -> dict:
    """Export the v1alpha1 JSON Schema, per the spec's own recommendation
    that the portable workload schema be publicly documented (§43)."""
    return SparkWorkload.model_json_schema()
