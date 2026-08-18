"""Semantic result comparison (spec §22): cross-provider tests must
compare semantic output, not physical files — file layout, partition
sizing, compression, and metadata may legitimately differ between
providers (ADR 0011). Never expect `Parquet file A == Parquet file B`.

Compares: schema (column name/type set, order-independent), row count,
per-column null distribution, key numeric aggregates (sum/min/max), and
deterministic row hashes (an order-independent, duplicate-aware multiset
comparison of every row's content — the strongest data-equality check
here, not just count/schema). Partition coverage is compared only when
`partition_columns` is given, since it's meaningless for an unpartitioned
dataset. "Business invariants" (spec §22) are intentionally out of scope
here — they're workload-specific and not automatable without a
per-workload spec of what invariant to check; #40's conformance runner
is where a caller could layer workload-specific checks on top of this.

Reads Parquet directly via pyarrow's S3 filesystem — no PySpark/JVM
needed for a read-only comparison (pyspark is intentionally not a
portage dependency, see examples/wordcount_app's own pyproject.toml).

_read_table() (I/O) and _compare_tables() (pure logic) are deliberately
split — the comparison logic is what actually needs thorough test
coverage, and pyarrow's S3FileSystem uses AWS's own C++ SDK rather than
botocore, so moto (used everywhere else in this codebase to fake S3)
can't intercept it. Unit tests exercise _compare_tables() directly
against synthetic in-memory tables; #39's live verification exercises
the real I/O path against the real remote MinIO.
"""

import hashlib
from collections import Counter
from dataclasses import dataclass, field
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.fs as pafs


class UnsupportedUriSchemeError(Exception):
    pass


@dataclass
class ComparisonResult:
    matched: bool
    mismatches: list[str] = field(default_factory=list)


def _filesystem_and_path(uri: str, storage_config: dict[str, str]) -> tuple[pafs.FileSystem, str]:
    """s3a://bucket/path (the same URI shape dataset bindings already use,
    spec §11) -> a pyarrow S3FileSystem built from the resolved storage
    config's spark.hadoop.fs.s3a.* keys (the same config the Spark job
    itself used to write the data) plus the bucket-relative path
    pyarrow's filesystem API expects (no scheme)."""
    parsed = urlparse(uri)
    if parsed.scheme not in ("s3a", "s3"):
        raise UnsupportedUriSchemeError(f"comparison.py only reads s3a:///s3:// URIs, got: {uri}")

    kwargs: dict[str, str] = {}
    access_key = storage_config.get("spark.hadoop.fs.s3a.access.key")
    secret_key = storage_config.get("spark.hadoop.fs.s3a.secret.key")
    endpoint = storage_config.get("spark.hadoop.fs.s3a.endpoint")
    if access_key:
        kwargs["access_key"] = access_key
    if secret_key:
        kwargs["secret_key"] = secret_key
    if endpoint:
        kwargs["endpoint_override"] = endpoint
        kwargs["scheme"] = "https" if endpoint.startswith("https") else "http"

    filesystem = pafs.S3FileSystem(**kwargs)
    path = f"{parsed.netloc}{parsed.path}"
    return filesystem, path


def _read_table(uri: str, storage_config: dict[str, str]) -> pa.Table:
    filesystem, path = _filesystem_and_path(uri, storage_config)
    return ds.dataset(path, filesystem=filesystem, format="parquet").to_table()


def _row_hash(row: dict) -> str:
    # Sorted by key so column order (which may legitimately differ across
    # providers, per this module's docstring) never affects the hash.
    canonical = "|".join(f"{k}={row[k]!r}" for k in sorted(row))
    return hashlib.sha256(canonical.encode()).hexdigest()


def _distinct_partition_values(table: pa.Table, partition_columns: list[str]) -> set[tuple]:
    rows = table.select(partition_columns).to_pylist()
    return {tuple(row[c] for c in partition_columns) for row in rows}


def _compare_tables(
    left: pa.Table, right: pa.Table, *, partition_columns: list[str] | None = None
) -> ComparisonResult:
    mismatches: list[str] = []

    left_schema = {f.name: str(f.type) for f in left.schema}
    right_schema = {f.name: str(f.type) for f in right.schema}
    if left_schema != right_schema:
        mismatches.append(f"schema mismatch: left={left_schema} right={right_schema}")

    if left.num_rows != right.num_rows:
        mismatches.append(f"row count mismatch: left={left.num_rows} right={right.num_rows}")

    common_columns = sorted(set(left_schema) & set(right_schema))
    for column in common_columns:
        left_nulls = left.column(column).null_count
        right_nulls = right.column(column).null_count
        if left_nulls != right_nulls:
            mismatches.append(f"null count mismatch on '{column}': left={left_nulls} right={right_nulls}")

    def _is_numeric(field_type: pa.DataType) -> bool:
        return pa.types.is_integer(field_type) or pa.types.is_floating(field_type)

    for column in common_columns:
        # A type mismatch on this column is already reported by the
        # schema check above — skip the aggregate comparison rather than
        # calling a numeric-only kernel on a non-numeric column, which
        # raises instead of returning a clean mismatch.
        if not (
            _is_numeric(left.schema.field(column).type) and _is_numeric(right.schema.field(column).type)
        ):
            continue
        left_col, right_col = left.column(column), right.column(column)
        # pyarrow.compute's kernel functions (sum/min/max) are generated
        # dynamically at import time from a kernel registry, not declared
        # as static module attributes — real and correct at runtime
        # (confirmed: tests/unit/test_comparison.py exercises all three),
        # just invisible to pyright's static analysis.
        aggregates = (
            ("sum", pc.sum),  # pyright: ignore[reportAttributeAccessIssue]
            ("min", pc.min),  # pyright: ignore[reportAttributeAccessIssue]
            ("max", pc.max),  # pyright: ignore[reportAttributeAccessIssue]
        )
        for label, fn in aggregates:
            left_value, right_value = fn(left_col).as_py(), fn(right_col).as_py()
            if left_value != right_value:
                mismatches.append(f"{label}('{column}') mismatch: left={left_value} right={right_value}")

    if partition_columns:
        left_partitions = _distinct_partition_values(left, partition_columns)
        right_partitions = _distinct_partition_values(right, partition_columns)
        if left_partitions != right_partitions:
            mismatches.append(
                f"partition coverage mismatch: left={left_partitions} right={right_partitions}"
            )

    if left_schema == right_schema:
        left_hashes = Counter(_row_hash(row) for row in left.to_pylist())
        right_hashes = Counter(_row_hash(row) for row in right.to_pylist())
        if left_hashes != right_hashes:
            mismatches.append("row content mismatch: deterministic row hashes differ")

    return ComparisonResult(matched=not mismatches, mismatches=mismatches)


def compare_semantic(
    left_uri: str,
    right_uri: str,
    *,
    left_storage_config: dict[str, str],
    right_storage_config: dict[str, str],
    partition_columns: list[str] | None = None,
) -> ComparisonResult:
    left_table = _read_table(left_uri, left_storage_config)
    right_table = _read_table(right_uri, right_storage_config)
    return _compare_tables(left_table, right_table, partition_columns=partition_columns)
