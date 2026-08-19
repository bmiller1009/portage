"""Real PySpark word-count job — the Phase 0 portability fixture
(docs/architecture/spec.md §63). Reads its input/output locations from the
portable.dataset.*.uri Spark configuration contract (ADR 0006) — no Portage
SDK import, no hardcoded infrastructure path.

_portable_config() extends that contract with two fallbacks (v0.3),
confirmed live against a real Databricks Serverless workspace:
- Serverless's Spark Connect session rejects spark.conf.get() for any
  non-Spark-builtin key ([CONFIG_NOT_AVAILABLE.WITHOUT_SUGGESTION] -- a
  Spark Connect read-allowlist restriction, not a "value missing" error,
  and there's no ClusterSpec to attach spark_conf to under serverless at
  all), so a plain os.environ fallback is tried next.
- Databricks Jobs API's `environment_variables_key` (SubmitTask ->
  JobEnvironment) turned out not to actually reach a serverless
  python_wheel_task's process environment either -- confirmed live via a
  raw SDK submission with no Portage code involved at all, so this isn't
  a bug in this provider's translation. `PythonWheelTask.parameters`
  (surfaced as plain sys.argv strings ahead of the wheel's console_scripts
  entry point, confirmed via the real Databricks stack trace) is the
  fallback that actually works, so it's the last resort here.
Every other provider/environment still resolves the value via
spark.conf.get() exactly as before; the extra fallbacks only matter where
that call is rejected outright."""

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lower, split


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


def count() -> None:
    spark = SparkSession.builder.appName("portage-wordcount").getOrCreate()

    input_uri = _portable_config(spark, "spark.portable.dataset.wordcount.raw.uri")
    output_uri = _portable_config(spark, "spark.portable.dataset.wordcount.counts.uri")

    text = spark.read.text(input_uri)
    words = text.select(explode(split(lower(col("value")), r"\s+")).alias("word"))
    words = words.filter(col("word") != "")
    counts = words.groupBy("word").count().orderBy(col("count").desc())

    counts.write.mode("overwrite").parquet(output_uri)
    spark.stop()


if __name__ == "__main__":
    count()
