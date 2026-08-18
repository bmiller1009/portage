# Apache Kyuubi image (docs/architecture/spec.md §38/§68 — Kyuubi is
# explicitly optional there; built and deployed anyway per project
# decision, not skipped). No official prebuilt Kyuubi+Spark image exists
# — confirmed via Kyuubi's own docs, which only document
# bin/docker-image-tool.sh (build-your-own from the binary distribution).
# Same pattern this project already uses for its own Spark base image
# (providers/execution/kubernetes/image/Dockerfile): layer framework
# glue onto an official upstream image.
#
# Built on apache/spark:4.1.3-python3, the same Spark version
# examples/iceberg_demo_app uses — confirmed live there that Iceberg's
# currently-published iceberg-spark-runtime-4.0 artifact is incompatible
# with Spark 4.2.0. JAVA_HOME and SPARK_HOME are already set as image
# env vars by the base image, so no separate kyuubi-env.sh is needed —
# Kyuubi inherits them directly.
#
#   docker build -t portage/kyuubi:0.1.0 -f providers/query/kyuubi/image/Dockerfile providers/query/kyuubi/image
FROM apache/spark:4.1.3-python3

USER root

ENV KYUUBI_VERSION=1.11.0
ENV KYUUBI_HOME=/opt/kyuubi

RUN curl -fsSL "https://archive.apache.org/dist/kyuubi/kyuubi-${KYUUBI_VERSION}/apache-kyuubi-${KYUUBI_VERSION}-bin.tgz" -o /tmp/kyuubi.tgz \
    && tar -xzf /tmp/kyuubi.tgz -C /opt \
    && mv "/opt/apache-kyuubi-${KYUUBI_VERSION}-bin" "$KYUUBI_HOME" \
    && rm /tmp/kyuubi.tgz \
    && chown -R spark:spark "$KYUUBI_HOME"

# Iceberg's Spark runtime + AWS bundle — same pinned versions and same
# reasons as examples/iceberg_demo_app/Dockerfile (iceberg-aws-bundle is
# needed even for reads that never touch Hadoop's S3A path directly).
ADD --chown=spark:spark https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.11.0/iceberg-spark-runtime-4.0_2.13-1.11.0.jar /opt/spark/jars/
ADD --chown=spark:spark https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.11.0/iceberg-aws-bundle-1.11.0.jar /opt/spark/jars/

COPY --chown=spark:spark kyuubi-defaults.conf "$KYUUBI_HOME/conf/kyuubi-defaults.conf"

USER spark

EXPOSE 10009

ENTRYPOINT ["/opt/kyuubi/bin/kyuubi", "run"]
