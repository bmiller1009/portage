#!/usr/bin/env bash
# `make release VERSION=1.0.0` (v1.0.0 release-hardening — req #42): runs
# every verification gate a release needs *before* a human creates and
# pushes the `vX.Y.Z` tag that actually triggers
# .github/workflows/release.yml. This script never pushes, tags, or
# publishes anything itself — it only proves the tree is ready to.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [ -z "${VERSION:-}" ]; then
  echo "Usage: make release VERSION=1.0.0" >&2
  exit 1
fi

echo "==> Checking working tree is clean"
if [ -n "$(git status --porcelain)" ]; then
  echo "error: working tree is not clean — commit or stash changes first" >&2
  git status --short >&2
  exit 1
fi

echo "==> Checking version consistency (expecting ${VERSION})"
check_version() {
  local file="$1" pattern="$2" label="$3"
  if ! grep -qE "$pattern" "$file"; then
    echo "error: ${label} in ${file} does not match ${VERSION}" >&2
    exit 1
  fi
}
check_version pyproject.toml "^version = \"${VERSION}\"$" "package version"
check_version charts/portage/Chart.yaml "^version: ${VERSION}$" "chart version"
check_version charts/portage/Chart.yaml "^appVersion: \"${VERSION}\"$" "chart appVersion"
check_version charts/portage/values.yaml "^  tag: \"${VERSION}\"$" "default image tag"

echo "==> Lint + type check"
ruff check .
pyright

echo "==> Unit tests"
pytest tests/unit -v

echo "==> OpenAPI schema drift check"
python scripts/export_openapi.py
git diff --exit-code openapi.json || {
  echo "error: openapi.json is out of date — run scripts/export_openapi.py and commit the result" >&2
  exit 1
}

echo "==> Package build"
rm -rf dist build
python -m build
python -m venv /tmp/portage-release-check-venv
/tmp/portage-release-check-venv/bin/pip install -q dist/*.whl
/tmp/portage-release-check-venv/bin/plane --version
rm -rf /tmp/portage-release-check-venv

echo "==> Container build"
docker build -t "portage/control-plane:${VERSION}" \
  --build-arg GIT_SHA="$(git rev-parse HEAD)" \
  --build-arg BUILD_TIME="$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  .

echo "==> Helm chart"
helm lint charts/portage
helm template charts/portage --set database.existingSecretName=release-check-placeholder >/dev/null

echo
echo "All release gates passed for ${VERSION}."
echo "Next step (manual, not run by this script): git tag v${VERSION} && git push origin v${VERSION}"
