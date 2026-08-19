"""Real PySpark claims-normalization ETL — the v1.0 "nontrivial workload"
portability fixture (docs/architecture/spec.md §66's original hero-demo
intent: "same workload/artifact runs on (Kubernetes+VAST) and
(Databricks+ADLS) with no application changes"). Reads its input/output
locations from the portable spark.portable.dataset.*.uri contract (ADR
0006), same as examples/wordcount_app/wordcount/jobs.py — no Portage SDK
import.

Unlike wordcount's single-input map/reduce, this does a genuine two-input
join: raw claims joined against a provider reference table, with derived
normalization (a cleaned provider name, a match-validity flag, an amount
band) — the multi-dataset-join complexity claims-normalization.yaml was
always meant to demonstrate, never actually built until now.

_portable_config()'s spark.conf.get() -> os.environ -> sys.argv fallback
chain is copied from wordcount_app/wordcount/jobs.py, confirmed live
there against Databricks Serverless (v0.3) — duplicated rather than
shared, since example application code deliberately has no dependency on
Portage or on sibling example packages (spec §17: application code stays
infrastructure-agnostic)."""

import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, trim, upper, when


def _portable_config(spark, key: str) -> str:
    try:
        return spark.conf.get(key)
    except Exception:
        env_key = key.upper().replace(".", "_")
        value = os.environ.get(env_key)
        if value is not None:
            return value
        prefix = f"--{key}="
        for arg in sys.argv:
            if arg.startswith(prefix):
                return arg[len(prefix) :]
        raise


def join_and_normalize(claims: DataFrame, reference: DataFrame) -> DataFrame:
    """The actual transformation — a pure function of two DataFrames, no
    I/O, so it's directly unit-testable against small in-memory fixtures
    (examples/claims_app/tests/test_jobs.py) without a cluster.

    A left join (not inner): a claim with no matching provider is real,
    reportable data (an unmatched/invalid reference), not something to
    silently drop — provider_matched surfaces that instead of losing the
    row."""
    joined = claims.join(reference, on="provider_id", how="left")
    return joined.select(
        col("claim_id"),
        col("provider_id"),
        upper(trim(col("provider_name"))).alias("provider_name"),
        col("specialty"),
        col("amount").cast("double").alias("amount"),
        col("service_date"),
        col("provider_name").isNotNull().alias("provider_matched"),
        when(col("amount").cast("double") >= 1000, "high")
        .when(col("amount").cast("double") >= 100, "medium")
        .otherwise("low")
        .alias("amount_band"),
    )


def normalize() -> None:
    spark = SparkSession.builder.appName("portage-claims-normalization").getOrCreate()

    claims_uri = _portable_config(spark, "spark.portable.dataset.claims.raw.uri")
    reference_uri = _portable_config(spark, "spark.portable.dataset.provider.reference.uri")
    output_uri = _portable_config(spark, "spark.portable.dataset.claims.normalized.uri")

    claims = spark.read.option("header", True).csv(claims_uri)
    reference = spark.read.option("header", True).csv(reference_uri)

    normalized = join_and_normalize(claims, reference)

    normalized.write.mode("overwrite").parquet(output_uri)
    spark.stop()


if __name__ == "__main__":
    normalize()
