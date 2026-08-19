"""Databricks Runtime <-> Apache Spark version compatibility (spec §46).

Curated, not (yet) fully discovery-driven — Databricks doesn't expose a
"what Spark version does this workspace's Jobs API accept" endpoint
independent of actually creating a cluster, so a static table stays the
primary source, refreshed against real release notes / a real workspace's
`w.clusters.spark_versions()` as new Databricks Runtime versions ship. The
selection criterion for each entry is the same one used since v0.3: the
plain (non-ML, non-Photon, non-GPU, non-aarch64) scala2.13 cluster-version
key for that runtime's LTS or latest release.

Confirmed live (v1.0.0 release-hardening, 2026-08-19) against a real
workspace's `w.clusters.spark_versions()`:
  17.3.x-scala2.13 -> "17.3 LTS (includes Apache Spark 4.0.0, Scala 2.13)"
  18.2.x-scala2.13 -> "18.2 (includes Apache Spark 4.1.0, Scala 2.13)"
  19.x-scala2.13   -> "19 (includes Apache Spark 4.2.0, Scala 2.13)"
Databricks Runtime 19 reached general availability 2026-07-23
(docs.databricks.com/aws/en/release-notes/runtime/19) — Spark 4.2 is a
real, currently-supported combination as of this file's own last check,
not a future/aspirational one.
"""

SPARK_TO_DBR_CLUSTER_VERSION: dict[str, str] = {
    "4.0": "17.3.x-scala2.13",
    "4.1": "18.2.x-scala2.13",
    "4.2": "19.x-scala2.13",
}

SUPPORTED_SPARK_VERSIONS: frozenset[str] = frozenset(SPARK_TO_DBR_CLUSTER_VERSION)


def resolve_cluster_version(spark_version: str, override: str | None = None) -> str | None:
    """The DBR cluster-version key to submit for a given portable
    `runtime.spark` value, or None if `spark_version` isn't in the curated
    table and no override was given — callers must not submit a job with
    a `spark_version` string that came from nowhere validated.

    `override` is `DatabricksProfile.dbr_cluster_version_override` (spec
    §46's "explicit compatible override" escape hatch) — lets an operator
    on a workspace running a newer Databricks Runtime than this table
    currently knows about submit anyway, on their own authority, without
    Portage itself asserting the mapping is correct. Only used when
    `spark_version` isn't already in the curated table, so it can never
    silently shadow a known-good mapping.
    """
    if spark_version in SPARK_TO_DBR_CLUSTER_VERSION:
        return SPARK_TO_DBR_CLUSTER_VERSION[spark_version]
    return override


def cross_check_against_workspace(client, spark_version: str, cluster_version: str) -> str | None:
    """Optional, bounded live cross-check — not a discovery-based
    replacement for the curated table above (Databricks has no endpoint
    for "is this Spark version supported" independent of actually
    creating a cluster, so there's nothing to fully discover from). When a
    real client is available, confirms `cluster_version` is still a key
    the workspace's own `w.clusters.spark_versions()` recognizes and that
    its name actually mentions the expected Spark version — catching the
    table going stale (a workspace deprecating a runtime, or Databricks
    changing a key's shape) without requiring a live call on every
    validate(). Returns a warning string to log, or None if everything
    matches; never raises — a workspace being unreachable here shouldn't
    turn an otherwise-valid static-table match into a hard failure.
    """
    try:
        versions = client.clusters.spark_versions()
    except Exception:  # noqa: BLE001 - best-effort cross-check, never load-bearing
        return None
    matches = [v for v in (versions.versions or []) if v.key == cluster_version]
    if not matches:
        return (
            f"Databricks Runtime cluster version '{cluster_version}' (curated for Spark "
            f"{spark_version}) was not found in this workspace's own spark_versions() list — "
            "the curated compatibility table may be stale for this workspace."
        )
    name = matches[0].name or ""
    if spark_version not in name:
        return (
            f"Databricks Runtime cluster version '{cluster_version}' is recognized by this "
            f"workspace, but its name ('{name}') doesn't mention Spark {spark_version} — the "
            "curated compatibility table may no longer match this workspace's actual runtime."
        )
    return None
