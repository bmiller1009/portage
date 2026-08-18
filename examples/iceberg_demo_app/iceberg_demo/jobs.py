"""v0.5 table-dataset-binding live-verification fixture
(docs/architecture/spec.md §11). Two jobs, run as two separate Portage
workloads against the same logical "demo.table" dataset: write_demo_table
creates a real Iceberg table via the fully-qualified identifier Portage
resolves into spark.portable.dataset.demo.table.identifier;
read_demo_table reads it back through the same logical name in a
completely separate Spark session/run, and writes the rows to a plain
path dataset so the round trip is inspectable without a SQL client."""

from pyspark.sql import SparkSession


def write_demo_table() -> None:
    spark = SparkSession.builder.appName("portage-iceberg-demo-write").getOrCreate()

    identifier = spark.conf.get("spark.portable.dataset.demo.table.identifier")
    df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "label"])
    df.writeTo(identifier).create()
    spark.stop()


def read_demo_table() -> None:
    spark = SparkSession.builder.appName("portage-iceberg-demo-read").getOrCreate()

    identifier = spark.conf.get("spark.portable.dataset.demo.table.identifier")
    output_uri = spark.conf.get("spark.portable.dataset.demo.readback.uri")

    df = spark.table(identifier)
    df.write.mode("overwrite").json(output_uri)
    spark.stop()


if __name__ == "__main__":
    write_demo_table()
