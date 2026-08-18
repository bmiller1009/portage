"""Dynamic conformance comparison (spec §21, §67): "the same workload
executes across multiple providers... compare semantic results." Given a
set of runs already submitted for the same workload against different
environments, resolves each run's output dataset location and storage
config server-side — the CLI is a pure REST client (spec §31), it never
touches provider credentials directly — and runs
conformance/comparison.py's semantic comparator pairwise.

A leg that isn't SUCCEEDED, or whose workload/environment can't resolve
an output location, is BLOCKED rather than compared — an honest third
state (spec §66's pattern for the still-missing Databricks credentials),
not a fabricated FAIL.
"""

import asyncio
import uuid
from dataclasses import dataclass, field

from sqlalchemy.ext.asyncio import AsyncSession

from conformance.comparison import compare_semantic
from control_plane import provider_factory, repositories
from control_plane.run_state import RunState
from spec.workload.v1alpha1 import SparkWorkload

STATUS_PASS = "PASS"
STATUS_FAIL = "FAIL"
STATUS_BLOCKED = "BLOCKED"


@dataclass
class ConformancePairResult:
    left_environment: str
    right_environment: str
    status: str
    mismatches: list[str] = field(default_factory=list)


@dataclass
class ConformanceReport:
    results: list[ConformancePairResult] = field(default_factory=list)


@dataclass
class _ResolvedLeg:
    environment_name: str
    uri: str
    storage_config: dict[str, str]


@dataclass
class _BlockedLeg:
    environment_name: str
    reason: str


async def _resolve_leg(session: AsyncSession, run_id: uuid.UUID, output_name: str | None):
    try:
        run = await repositories.get_run(session, run_id)
    except repositories.NotFoundError as e:
        return _BlockedLeg(environment_name=str(run_id), reason=str(e))

    if run.state != RunState.SUCCEEDED.value:
        return _BlockedLeg(environment_name=run.environment_name, reason=f"run not SUCCEEDED (state={run.state})")

    workload_row = await repositories.get_workload_definition(
        session, run.workload_name, version=run.workload_version
    )
    workload = SparkWorkload.model_validate(workload_row.definition)
    output_refs = workload.datasets.outputs
    if not output_refs:
        return _BlockedLeg(run.environment_name, "workload declares no output datasets")

    if output_name is not None:
        if output_name not in output_refs:
            return _BlockedLeg(run.environment_name, f"workload has no output named '{output_name}'")
        dataset_name = output_refs[output_name].dataset
    else:
        # Deterministic (dict insertion order, spec-defined workload
        # field), not arbitrary — first declared output when the caller
        # doesn't name one.
        dataset_name = next(iter(output_refs.values())).dataset

    binding = await repositories.get_dataset_binding(
        session, dataset_name, run.environment_name, required=False
    )
    if binding is None:
        return _BlockedLeg(
            run.environment_name, f"no dataset binding for '{dataset_name}' in '{run.environment_name}'"
        )

    environment = await repositories.get_environment(session, run.environment_name)
    storage_profile = await repositories.get_storage_profile(session, environment.storage_profile_name)
    storage_config = provider_factory.build_storage_config(storage_profile)
    return _ResolvedLeg(environment_name=run.environment_name, uri=binding.uri, storage_config=storage_config)


async def compare_runs(
    session: AsyncSession, run_ids: list[uuid.UUID], *, output_name: str | None = None
) -> ConformanceReport:
    legs = [await _resolve_leg(session, run_id, output_name) for run_id in run_ids]

    results: list[ConformancePairResult] = []
    for i in range(len(legs)):
        for j in range(i + 1, len(legs)):
            left, right = legs[i], legs[j]
            if isinstance(left, _BlockedLeg):
                results.append(
                    ConformancePairResult(
                        left.environment_name, right.environment_name, STATUS_BLOCKED, [left.reason]
                    )
                )
                continue
            if isinstance(right, _BlockedLeg):
                results.append(
                    ConformancePairResult(
                        left.environment_name, right.environment_name, STATUS_BLOCKED, [right.reason]
                    )
                )
                continue

            # compare_semantic() does blocking Parquet I/O (pyarrow) —
            # off the event loop, same asyncio.to_thread() pattern the
            # Kubernetes provider uses for its own blocking SDK calls.
            comparison = await asyncio.to_thread(
                compare_semantic,
                left.uri,
                right.uri,
                left_storage_config=left.storage_config,
                right_storage_config=right.storage_config,
            )
            results.append(
                ConformancePairResult(
                    left.environment_name,
                    right.environment_name,
                    STATUS_PASS if comparison.matched else STATUS_FAIL,
                    comparison.mismatches,
                )
            )

    return ConformanceReport(results=results)
