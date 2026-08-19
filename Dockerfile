# Portage's own control plane (API + reconciler) — not to be confused
# with providers/execution/kubernetes/image/Dockerfile, which builds the
# Spark *workload execution* base image. This image runs the API and the
# reconciler; charts/portage's Deployments select which process to run via
# `command`/`args`, since both share the exact same installed package (see
# pyproject.toml's [tool.hatch.build.targets.wheel] packages list).
FROM python:3.12-slim

# Surfaced at runtime via GET /v1/build (control_plane/version.py) — not
# knowable from installed package metadata, so they only exist when a build
# actually passes them; a source checkout or editable install reports
# "unknown" for both instead of guessing.
ARG GIT_SHA=unknown
ARG BUILD_TIME=unknown
ENV PORTAGE_GIT_SHA=${GIT_SHA}
ENV PORTAGE_BUILD_TIME=${BUILD_TIME}

WORKDIR /app

# alembic.ini + alembic/ are needed at runtime (the migration Job runs
# `alembic upgrade head` from this same image), not just at build time.
COPY pyproject.toml alembic.ini README.md ./
COPY alembic ./alembic
COPY api ./api
COPY control_plane ./control_plane
COPY reconciler ./reconciler
COPY providers ./providers
COPY spec ./spec
COPY conformance ./conformance
COPY cli ./cli

RUN pip install --no-cache-dir .

RUN useradd --create-home --uid 1000 portage
USER portage

EXPOSE 8000 9091
