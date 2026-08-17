# Application image for the Phase 0 wordcount fixture: layers the wheel
# artifact onto the framework-managed Spark base image (spec §17). Build
# the wheel first (python -m build --wheel), then:
#   docker build -t portage/wordcount:0.1.0 -f examples/wordcount_app/Dockerfile examples/wordcount_app
FROM portage/spark-base:phase0

COPY dist/wordcount-0.1.0-py3-none-any.whl /tmp/
RUN pip install --no-cache-dir /tmp/wordcount-0.1.0-py3-none-any.whl
