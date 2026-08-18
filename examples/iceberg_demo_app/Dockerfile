# Application image for the v0.5 table-dataset-binding live-verification
# fixture. Build the wheel first (python -m build --wheel), then:
#   docker build -t portage/iceberg-demo:0.1.0 -f examples/iceberg_demo_app/Dockerfile examples/iceberg_demo_app
#
# Built on apache/spark:4.1.3-python3 rather than the project's shared
# portage/spark-base:phase0 (pinned to Spark 4.2.0) — confirmed live that
# the currently-published org.apache.iceberg:iceberg-spark-runtime-4.0
# artifact (no dedicated 4.2 build exists yet, per Iceberg's own
# multi-engine support matrix) throws java.lang.IncompatibleClassChangeError
# against Spark 4.2.0's org.apache.spark.sql.connector.catalog.View, for
# both the 1.10.1 and 1.11.0 releases and both createOrReplace() and
# create(). Spark 4.1 is already a supported version on the Kubernetes
# execution provider (_SUPPORTED_SPARK_VERSIONS in
# providers/execution/kubernetes/provider.py), so this is a legitimate,
# already-declared-compatible target, not a one-off workaround version.
# The multi-stage COPY --from pulls the framework launcher out of the
# already-built shared base image rather than duplicating it.
FROM portage/spark-base:phase0 AS launcher
FROM apache/spark:4.1.3-python3
COPY --from=launcher /opt/portage/launcher.py /opt/portage/launcher.py

# The base image's non-root user has HOME=/nonexistent (deliberately
# unwritable); give pip somewhere to write its cache.
ENV HOME=/tmp

# Iceberg's Spark runtime jar — not bundled with the official Spark image.
# Fetched at build time rather than resolved per-run via
# spark.jars.packages, so a job never depends on Maven Central
# reachability from inside the cluster. Note: spark.sql.extensions is NOT
# set via a baked-in spark-defaults.conf here — the Kubernetes Spark
# Operator mounts its own generated ConfigMap over /opt/spark/conf at
# runtime, silently shadowing any static file an image bakes in at that
# path (confirmed live). It's supplied as real Spark config instead, by
# spec/dataset/v1alpha1.py's resolve_iceberg_catalog_config().
ADD --chown=spark:spark https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.11.0/iceberg-spark-runtime-4.0_2.13-1.11.0.jar /opt/spark/jars/

# iceberg-aws-bundle — confirmed live to be required on the Spark client
# too, not just the REST catalog server: reading a table (SparkScan ->
# SparkReadConf.localityEnabled() -> Util.usesHadoopFileIO() ->
# ResolvingFileIO.ioClass()) probes for
# org.apache.iceberg.aws.s3.S3FileIO via Class.forName() even when the
# actual data path goes through Hadoop's S3A instead. Without this jar,
# that probe throws a hard NoClassDefFoundError (not a caught
# ClassNotFoundException) and aborts the read entirely.
ADD --chown=spark:spark https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.11.0/iceberg-aws-bundle-1.11.0.jar /opt/spark/jars/

COPY dist/iceberg_demo-0.1.0-py3-none-any.whl /tmp/
# --no-deps: pyspark is already provided by the base Spark image
# (importable via its PYTHONPATH).
RUN pip install --no-cache-dir --no-deps /tmp/iceberg_demo-0.1.0-py3-none-any.whl
