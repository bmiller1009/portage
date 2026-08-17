"""Real PySpark word-count job — the Phase 0 portability fixture
(docs/architecture/spec.md §63). Reads its input/output locations
exclusively from the portable.dataset.*.uri Spark configuration contract
(ADR 0006) — no Portage SDK import, no hardcoded infrastructure path."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lower, split


def count() -> None:
    spark = SparkSession.builder.appName("portage-wordcount").getOrCreate()

    input_uri = spark.conf.get("spark.portable.dataset.wordcount.raw.uri")
    output_uri = spark.conf.get("spark.portable.dataset.wordcount.counts.uri")

    text = spark.read.text(input_uri)
    words = text.select(explode(split(lower(col("value")), r"\s+")).alias("word"))
    words = words.filter(col("word") != "")
    counts = words.groupBy("word").count().orderBy(col("count").desc())

    counts.write.mode("overwrite").parquet(output_uri)
    spark.stop()


if __name__ == "__main__":
    count()
