"""Local unit test for the pure transform (no cluster, no I/O) — run via
this package's own dev environment: `pip install -e ".[dev]"` then
`pytest tests/` from examples/claims_app/. Not part of the top-level
`pytest tests/unit` suite: pyspark is deliberately not a portage
dependency (see pyproject.toml), matching examples/wordcount_app's own
precedent of keeping PySpark-based checks in the example package's own
test environment."""

import pytest
from claims.jobs import join_and_normalize
from pyspark.sql import Row, SparkSession


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("test-claims-normalization").getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def claims(spark):
    return spark.createDataFrame(
        [
            Row(claim_id="C1", provider_id="P1", amount="1500.00", service_date="2026-01-05"),
            Row(claim_id="C2", provider_id="P2", amount="250.00", service_date="2026-01-06"),
            Row(claim_id="C3", provider_id="P1", amount="50.00", service_date="2026-01-07"),
            # No matching provider row for P9 — a real, reportable
            # unmatched reference, not a row that should silently vanish.
            Row(claim_id="C4", provider_id="P9", amount="900.00", service_date="2026-01-08"),
        ]
    )


@pytest.fixture
def reference(spark):
    return spark.createDataFrame(
        [
            Row(provider_id="P1", provider_name="  dr. jane smith  ", specialty="Cardiology"),
            Row(provider_id="P2", provider_name="dr. john doe", specialty="Dermatology"),
        ]
    )


def test_join_matches_claims_to_provider_reference(spark, claims, reference):
    result = {row.claim_id: row for row in join_and_normalize(claims, reference).collect()}

    assert result["C1"].provider_name == "DR. JANE SMITH"
    assert result["C1"].specialty == "Cardiology"
    assert result["C2"].provider_name == "DR. JOHN DOE"


def test_amount_band_derivation(spark, claims, reference):
    result = {row.claim_id: row for row in join_and_normalize(claims, reference).collect()}

    assert result["C1"].amount_band == "high"  # 1500
    assert result["C2"].amount_band == "medium"  # 250
    assert result["C3"].amount_band == "low"  # 50


def test_unmatched_provider_is_kept_not_dropped(spark, claims, reference):
    result = {row.claim_id: row for row in join_and_normalize(claims, reference).collect()}

    assert result["C4"].provider_matched is False
    assert result["C4"].provider_name is None
    # Still a real row with its own data intact, not silently discarded
    # by an inner join.
    assert result["C4"].amount == 900.0


def test_output_row_count_equals_input_claim_count(claims, reference):
    assert join_and_normalize(claims, reference).count() == claims.count()
