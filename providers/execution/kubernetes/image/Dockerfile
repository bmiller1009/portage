# Framework-managed Spark base image (docs/architecture/spec.md §17).
# Layers the Portage generic entry-point launcher onto the official Apache
# Spark image. Application artifacts (e.g. examples/wordcount_app's wheel)
# are layered on top of *this* image per workload, not this one directly.
#
# Exact base tag confirmed against Docker Hub during Phase 0 bring-up —
# apache/spark:4.2.0-python3 matches the spec's Spark 4.2 target exactly
# (docs/providers/kubernetes.md).
FROM apache/spark:4.2.0-python3

COPY launcher.py /opt/portage/launcher.py
