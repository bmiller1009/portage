#!/usr/bin/env bash
# Tears down everything scripts/dev-up.sh created — kept separate from
# `make dev` itself so re-running dev-up.sh after a partial failure doesn't
# require a manual cleanup pass first (dev-up.sh's own idempotency handles
# that); this is for a genuine "start over" or "reclaim the machine".
#
# Only stops processes by the PID dev-up.sh itself recorded (.dev-*.pid) —
# never pattern-matches on command line, which could otherwise mistake an
# unrelated hand-started api/reconciler process on the same machine for
# one of this script's own.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CLUSTER_NAME="${PORTAGE_DEV_CLUSTER_NAME:-portage-dev}"
POSTGRES_CONTAINER="${PORTAGE_DEV_POSTGRES_CONTAINER:-portage-dev-postgres}"

log() { printf '\n==> %s\n' "$1"; }

stop_pidfile() {
  local pidfile="$1"
  if [ -f "$pidfile" ]; then
    kill "$(cat "$pidfile")" 2>/dev/null || true
    rm -f "$pidfile"
  fi
}

log "Stopping control plane processes"
stop_pidfile "${ROOT_DIR}/.dev-api.pid"
stop_pidfile "${ROOT_DIR}/.dev-reconciler.pid"

log "Removing PostgreSQL container ($POSTGRES_CONTAINER)"
docker rm -f "$POSTGRES_CONTAINER" >/dev/null 2>&1 || true

log "Deleting kind cluster ($CLUSTER_NAME)"
kind delete cluster --name "$CLUSTER_NAME" 2>/dev/null || true

echo "done"
