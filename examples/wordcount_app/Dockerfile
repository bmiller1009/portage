# Application image for the Phase 0 wordcount fixture: layers the wheel
# artifact onto the framework-managed Spark base image (spec §17). Build
# the wheel first (python -m build --wheel), then:
#   docker build -t portage/wordcount:0.1.0 -f examples/wordcount_app/Dockerfile examples/wordcount_app
FROM portage/spark-base:phase0

# The base image's non-root user has HOME=/nonexistent (deliberately
# unwritable); give pip somewhere to write its cache.
ENV HOME=/tmp

COPY dist/wordcount-0.1.0-py3-none-any.whl /tmp/
# --no-deps: pyspark is already provided by the base Spark image
# (importable via its PYTHONPATH) — reinstalling it from sdist here is
# both wasteful (~450MB from-source build) and unnecessary.
RUN pip install --no-cache-dir --no-deps /tmp/wordcount-0.1.0-py3-none-any.whl
