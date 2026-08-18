"""Unit tests for control_plane/execution_provider.py's match_capabilities()
(spec §20-21) — pure function, table-driven, no provider/DB needed."""

from typing import Literal

from control_plane.execution_provider import CapabilitySet, match_capabilities
from spec.workload.v1alpha1 import (
    ApplicationSpec,
    DatasetsSpec,
    DriverResources,
    ExecutionPolicy,
    ExecutorResources,
    RequirementsSpec,
    ResourcesSpec,
    RuntimeSpec,
    ScalingSpec,
    SparkWorkload,
    WorkloadMetadata,
)

FULL_CAPABILITIES = CapabilitySet(
    spark_versions=["4.1", "4.2"],
    languages=["python", "jvm"],
    dynamic_allocation=True,
    gpu=True,
    streaming=True,
    local_disk=True,
    spark_connect=True,
)


def _workload(
    spark: str = "4.2",
    application_type: Literal["python-wheel", "jvm-jar"] = "python-wheel",
    requirements: RequirementsSpec | None = None,
) -> SparkWorkload:
    return SparkWorkload(
        apiVersion="runtime/v1alpha1",
        kind="SparkWorkload",
        metadata=WorkloadMetadata(name="w", version="0.1.0"),
        runtime=RuntimeSpec(spark=spark),
        application=ApplicationSpec(type=application_type, artifact="artifact://w/0.1.0", entryPoint="w.main"),
        datasets=DatasetsSpec(),
        resources=ResourcesSpec(
            driver=DriverResources(cores=1, memory="2Gi"),
            executor=ExecutorResources(cores=1, memory="2Gi"),
            scaling=ScalingSpec(minExecutors=1, maxExecutors=2),
        ),
        execution=ExecutionPolicy(timeout="30m"),
        requirements=requirements or RequirementsSpec(),
    )


def test_all_compatible_returns_no_errors():
    errors = match_capabilities(_workload(), FULL_CAPABILITIES)
    assert errors == []


def test_unsupported_spark_version():
    errors = match_capabilities(_workload(spark="3.5"), FULL_CAPABILITIES)
    assert any("unsupported Spark version" in e for e in errors)


def test_unsupported_language():
    caps = CapabilitySet(
        spark_versions=["4.2"],
        languages=["python"],
        dynamic_allocation=True,
        gpu=True,
        streaming=True,
        local_disk=True,
        spark_connect=True,
    )
    errors = match_capabilities(_workload(application_type="jvm-jar"), caps)
    assert any("unsupported language: jvm" in e for e in errors)


def test_dynamic_allocation_required_but_unsupported():
    caps = CapabilitySet(
        spark_versions=["4.2"], languages=["python"], dynamic_allocation=False,
        gpu=True, streaming=True, local_disk=True, spark_connect=True,
    )
    errors = match_capabilities(
        _workload(requirements=RequirementsSpec(dynamicAllocation=True)), caps
    )
    assert any("dynamic allocation" in e for e in errors)


def test_gpu_required_but_unsupported():
    caps = CapabilitySet(
        spark_versions=["4.2"], languages=["python"], dynamic_allocation=True,
        gpu=False, streaming=True, local_disk=True, spark_connect=True,
    )
    errors = match_capabilities(_workload(requirements=RequirementsSpec(gpu=True)), caps)
    assert any("GPU" in e for e in errors)


def test_streaming_required_but_unsupported():
    caps = CapabilitySet(
        spark_versions=["4.2"], languages=["python"], dynamic_allocation=True,
        gpu=True, streaming=False, local_disk=True, spark_connect=True,
    )
    errors = match_capabilities(_workload(requirements=RequirementsSpec(streaming=True)), caps)
    assert any("streaming" in e for e in errors)


def test_local_disk_required_but_unsupported():
    caps = CapabilitySet(
        spark_versions=["4.2"], languages=["python"], dynamic_allocation=True,
        gpu=True, streaming=True, local_disk=False, spark_connect=True,
    )
    errors = match_capabilities(_workload(requirements=RequirementsSpec(localDisk=True)), caps)
    assert any("local disk" in e for e in errors)


def test_spark_connect_required_but_unsupported():
    caps = CapabilitySet(
        spark_versions=["4.2"], languages=["python"], dynamic_allocation=True,
        gpu=True, streaming=True, local_disk=True, spark_connect=False,
    )
    errors = match_capabilities(_workload(requirements=RequirementsSpec(sparkConnect=True)), caps)
    assert any("Spark Connect" in e for e in errors)


def test_multiple_mismatches_all_reported():
    caps = CapabilitySet(
        spark_versions=["4.1"], languages=["jvm"], dynamic_allocation=False,
        gpu=False, streaming=False, local_disk=False, spark_connect=False,
    )
    errors = match_capabilities(
        _workload(
            spark="4.2",
            requirements=RequirementsSpec(gpu=True, streaming=True),
        ),
        caps,
    )
    assert len(errors) == 4  # spark version, language, gpu, streaming
