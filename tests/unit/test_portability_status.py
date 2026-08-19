"""control_plane/execution_provider.py's compute_portability_status()
(spec §19, ADR 0010) — pure function of a workload's own providerOverrides
field, no provider or environment involved."""

from control_plane.execution_provider import compute_portability_status
from spec.workload.v1alpha1 import (
    ApplicationSpec,
    DatasetsSpec,
    DriverResources,
    ExecutionPolicy,
    ExecutorResources,
    ResourcesSpec,
    RuntimeSpec,
    ScalingSpec,
    SparkWorkload,
    WorkloadMetadata,
)


def _workload(provider_overrides: dict | None = None) -> SparkWorkload:
    return SparkWorkload(
        apiVersion="runtime/v1",
        kind="SparkWorkload",
        metadata=WorkloadMetadata(name="w", version="0.1.0"),
        runtime=RuntimeSpec(spark="4.2"),
        application=ApplicationSpec(type="python-wheel", artifact="artifact://w/0.1.0", entryPoint="w.main"),
        datasets=DatasetsSpec(),
        resources=ResourcesSpec(
            driver=DriverResources(cores=1, memory="2Gi"),
            executor=ExecutorResources(cores=1, memory="2Gi"),
            scaling=ScalingSpec(minExecutors=1, maxExecutors=2),
        ),
        execution=ExecutionPolicy(timeout="30m"),
        providerOverrides=provider_overrides or {},
    )


def test_no_overrides_is_plain_portable():
    result = compute_portability_status(_workload())
    assert result.status == "PORTABLE"
    assert result.overrides_by_provider == {}


def test_overrides_on_one_provider_degrades_status():
    result = compute_portability_status(
        _workload({"kubernetes": {"nodeSelector": {}, "tolerations": []}})
    )
    assert result.status == "PORTABLE_WITH_OVERRIDES"
    assert result.overrides_by_provider == {"kubernetes": 2}


def test_matches_spec_19_example_shape():
    """spec.md §19's own worked example: kubernetes 2, databricks 0 ->
    PORTABLE WITH PROVIDER-SPECIFIC OVERRIDES."""
    result = compute_portability_status(
        _workload({"kubernetes": {"a": 1, "b": 2}, "databricks": {}})
    )
    assert result.overrides_by_provider == {"kubernetes": 2, "databricks": 0}
    assert result.status == "PORTABLE_WITH_OVERRIDES"


def test_empty_provider_namespace_alone_does_not_degrade_status():
    result = compute_portability_status(_workload({"databricks": {}}))
    assert result.status == "PORTABLE"
    assert result.overrides_by_provider == {"databricks": 0}
