# Application image for the v1.0 claims-normalization fixture: layers the
# wheel artifact onto the framework-managed Spark base image (spec §17),
# same pattern as examples/wordcount_app/Dockerfile. Build the wheel first
# (python -m build --wheel), then:
#   docker build -t portage/claims:4.18.2 -f examples/claims_app/Dockerfile examples/claims_app
FROM portage/spark-base:phase0

# The base image's non-root user has HOME=/nonexistent (deliberately
# unwritable); give pip somewhere to write its cache.
ENV HOME=/tmp

COPY dist/claims-4.18.2-py3-none-any.whl /tmp/
# --no-deps: pyspark is already provided by the base Spark image
# (importable via its PYTHONPATH) — reinstalling it from sdist here is
# both wasteful (~450MB from-source build) and unnecessary.
RUN pip install --no-cache-dir --no-deps /tmp/claims-4.18.2-py3-none-any.whl
