"""Unit tests for conformance/comparison.py's semantic comparator (spec
§22). _compare_tables() is pure and operates on in-memory pyarrow Tables,
so these run fast with synthetic fixtures — no real S3/MinIO needed. The
real I/O path (_read_table(), against real Parquet in real MinIO) is
live-verified separately, not here — pyarrow's S3FileSystem uses AWS's
own C++ SDK rather than botocore, so moto can't fake it the way it fakes
every other S3 interaction in this codebase.
"""

import pyarrow as pa
import pytest

from conformance.comparison import (
    UnsupportedUriSchemeError,
    _compare_tables,
    _filesystem_and_path,
)


def _table(**columns) -> pa.Table:
    return pa.table(columns)


def test_identical_tables_match():
    left = _table(word=["a", "b", "c"], count=[1, 2, 3])
    right = _table(word=["a", "b", "c"], count=[1, 2, 3])

    result = _compare_tables(left, right)

    assert result.matched is True
    assert result.mismatches == []


def test_identical_tables_match_regardless_of_row_order():
    """Deterministic row hashing must be order-independent — different
    providers may legitimately return rows in a different physical order."""
    left = _table(word=["a", "b", "c"], count=[1, 2, 3])
    right = _table(word=["c", "a", "b"], count=[3, 1, 2])

    result = _compare_tables(left, right)

    assert result.matched is True


def test_identical_tables_match_regardless_of_column_order():
    left = _table(word=["a", "b"], count=[1, 2])
    right = _table(count=[1, 2], word=["a", "b"])

    result = _compare_tables(left, right)

    assert result.matched is True


def test_schema_mismatch_detected():
    left = _table(word=["a"], count=[1])
    right = _table(word=["a"], count=["1"])  # count is a string here, not int

    result = _compare_tables(left, right)

    assert result.matched is False
    assert any("schema mismatch" in m for m in result.mismatches)


def test_row_count_mismatch_detected():
    left = _table(word=["a", "b"])
    right = _table(word=["a", "b", "c"])

    result = _compare_tables(left, right)

    assert result.matched is False
    assert any("row count mismatch" in m for m in result.mismatches)


def test_null_distribution_mismatch_detected():
    left = _table(word=["a", None, "c"])
    right = _table(word=["a", "b", "c"])

    result = _compare_tables(left, right)

    assert result.matched is False
    assert any("null count mismatch" in m for m in result.mismatches)


def test_key_aggregate_mismatch_detected():
    left = _table(count=[1, 2, 3])
    right = _table(count=[1, 2, 4])  # same row count, different sum/max

    result = _compare_tables(left, right)

    assert result.matched is False
    assert any("sum('count')" in m for m in result.mismatches)
    assert any("max('count')" in m for m in result.mismatches)


def test_row_content_mismatch_detected_despite_matching_count_and_aggregates():
    """Two datasets can have the same row count, schema, and even the
    same column sum while containing genuinely different rows — only the
    row-hash comparison catches this."""
    left = _table(count=[1, 5])
    right = _table(count=[3, 3])  # same sum (6), different actual rows

    result = _compare_tables(left, right)

    assert result.matched is False
    assert any("row content mismatch" in m for m in result.mismatches)


def test_duplicate_rows_are_not_collapsed():
    """A multiset comparison, not a set comparison — losing a duplicate
    row is a real data bug the comparator must catch."""
    left = _table(word=["a", "a", "b"])
    right = _table(word=["a", "b", "b"])

    result = _compare_tables(left, right)

    assert result.matched is False
    assert any("row content mismatch" in m for m in result.mismatches)


def test_partition_coverage_mismatch_detected_when_requested():
    left = _table(region=["us", "eu"], count=[1, 2])
    right = _table(region=["us", "apac"], count=[1, 2])

    result = _compare_tables(left, right, partition_columns=["region"])

    assert result.matched is False
    assert any("partition coverage mismatch" in m for m in result.mismatches)


def test_partition_coverage_not_checked_unless_requested():
    left = _table(region=["us", "eu"], count=[1, 2])
    right = _table(region=["us", "apac"], count=[2, 1])

    result = _compare_tables(left, right)  # no partition_columns given

    # Different partition values, but row content also happens to differ
    # in a way that's already caught — the point here is just that no
    # "partition coverage mismatch" message appears when not requested.
    assert not any("partition coverage" in m for m in result.mismatches)


def test_filesystem_and_path_rejects_non_s3_scheme():
    with pytest.raises(UnsupportedUriSchemeError):
        _filesystem_and_path("abfss://container@account.dfs.core.windows.net/path", {})


def test_filesystem_and_path_splits_bucket_and_key():
    _filesystem, path = _filesystem_and_path(
        "s3a://portage-phase0/wordcount/output",
        {
            "spark.hadoop.fs.s3a.access.key": "AKIA123",
            "spark.hadoop.fs.s3a.secret.key": "secret",
            "spark.hadoop.fs.s3a.endpoint": "http://minio.local:9000",
        },
    )
    assert path == "portage-phase0/wordcount/output"
