#!/usr/bin/env bash
# make dev (docs/architecture/spec.md §45): stands up kind + PostgreSQL +
# MinIO + Spark Operator + the control plane, then seeds a "local"
# environment so `plane run examples/wordcount.yaml --environment local`
# works with zero cloud accounts. Every step checks-before-acting so a
# re-run after a partial failure doesn't error out or duplicate resources.
#
# Every command/manifest here was first run by hand against a real cluster
# during Phase 0/v0.1 development (docs/providers/kubernetes.md,
# docs/providers/s3.md) — this script is that same sequence, scripted.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

CLUSTER_NAME="${PORTAGE_DEV_CLUSTER_NAME:-portage-dev}"
KUBE_CONTEXT="kind-${CLUSTER_NAME}"
POSTGRES_CONTAINER="${PORTAGE_DEV_POSTGRES_CONTAINER:-portage-dev-postgres}"
POSTGRES_PORT="${PORTAGE_DEV_POSTGRES_PORT:-15433}"
API_PORT="${PORTAGE_DEV_API_PORT:-8124}"
RECONCILER_METRICS_PORT="${PORTAGE_DEV_RECONCILER_METRICS_PORT:-9092}"
MINIO_ACCESS_KEY="portage-dev"
MINIO_SECRET_KEY="portage-dev-secret"
DATABASE_URL="postgresql+asyncpg://portage:portage@localhost:${POSTGRES_PORT}/portage"
API_URL="http://127.0.0.1:${API_PORT}"

log() { printf '\n==> %s\n' "$1"; }

# --- 0. Preflight: inotify limits (Linux only) -------------------------------
# kind's own documented gotcha (https://kind.sigs.k8s.io/docs/user/known-issues/
# #pod-errors-due-to-too-many-open-files): the Debian default
# fs.inotify.max_user_instances=128 is easy to exhaust once more than one
# cluster's worth of node containers exist on the same host, and the
# failure mode is a confusing "could not find a log line that matches
# ...Multi-User System..." from `kind create cluster`, not an inotify error
# — found and fixed empirically while first bringing this script up.
if [ -r /proc/sys/fs/inotify/max_user_instances ]; then
  current_instances=$(cat /proc/sys/fs/inotify/max_user_instances)
  if [ "$current_instances" -lt 512 ]; then
    echo "WARNING: fs.inotify.max_user_instances=$current_instances is too low for kind" >&2
    echo "  fix: sudo sysctl fs.inotify.max_user_instances=512 fs.inotify.max_user_watches=524288" >&2
  fi
fi

# --- 1. Python package ------------------------------------------------------
log "Installing portage in editable mode"
pip install -e ".[dev]"

# --- 2. kind cluster ---------------------------------------------------------
log "kind cluster ($CLUSTER_NAME)"
if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  echo "already exists"
else
  kind create cluster --name "$CLUSTER_NAME" --config deploy/dev/kind-config.yaml
fi

# --- 3. Spark Kubernetes Operator -------------------------------------------
log "Spark Kubernetes Operator"
helm repo add spark-kubernetes-operator https://apache.github.io/spark-kubernetes-operator >/dev/null 2>&1 || true
helm repo update spark-kubernetes-operator >/dev/null

if helm status spark-kubernetes-operator -n spark-operator --kube-context "$KUBE_CONTEXT" >/dev/null 2>&1; then
  echo "already installed"
else
  helm install spark-kubernetes-operator spark-kubernetes-operator/spark-kubernetes-operator \
    -n spark-operator --create-namespace --kube-context "$KUBE_CONTEXT" \
    --set workloadResources.namespaces.create=false \
    --set workloadResources.namespaces.data="{default}"
  # A fresh install picks up the namespace config immediately; only an
  # upgrade to an already-running operator needs the explicit restart
  # (docs/providers/kubernetes.md's RBAC gotcha #1).
  kubectl --context "$KUBE_CONTEXT" -n spark-operator rollout status deployment/spark-kubernetes-operator --timeout=120s
fi

log "deletecollection RBAC supplement"
kubectl --context "$KUBE_CONTEXT" apply -f deploy/dev/rbac-deletecollection.yaml

# --- 4. MinIO -----------------------------------------------------------------
log "MinIO"
kubectl --context "$KUBE_CONTEXT" apply -f deploy/dev/minio.yaml
kubectl --context "$KUBE_CONTEXT" -n portage-storage rollout status deployment/minio --timeout=120s

log "MinIO bucket + seed input"
kubectl --context "$KUBE_CONTEXT" -n portage-storage exec deploy/minio -- \
  mc alias set local http://localhost:9000 "$MINIO_ACCESS_KEY" "$MINIO_SECRET_KEY" >/dev/null
kubectl --context "$KUBE_CONTEXT" -n portage-storage exec deploy/minio -- \
  mc mb --ignore-existing local/portage-local >/dev/null
if kubectl --context "$KUBE_CONTEXT" -n portage-storage exec deploy/minio -- \
    mc stat local/portage-local/wordcount/input.txt >/dev/null 2>&1; then
  echo "input already seeded"
else
  SEED_TEXT="the quick fox and the lazy dog watch the fox run while the dog sleeps near a big brown tree"
  for _ in $(seq 1 50); do printf '%s\n' "$SEED_TEXT"; done \
    | kubectl --context "$KUBE_CONTEXT" -n portage-storage exec -i deploy/minio -- \
        sh -c 'cat > /tmp/input.txt && mc cp /tmp/input.txt local/portage-local/wordcount/input.txt' >/dev/null
fi

# --- 4b. Iceberg REST Catalog --------------------------------------------------
log "Iceberg REST Catalog"
kubectl --context "$KUBE_CONTEXT" apply -f deploy/dev/iceberg-rest.yaml
kubectl --context "$KUBE_CONTEXT" -n portage-storage rollout status deployment/iceberg-rest --timeout=120s

# --- 4c. Trino ------------------------------------------------------------------
log "Trino"
kubectl --context "$KUBE_CONTEXT" apply -f deploy/dev/trino.yaml
kubectl --context "$KUBE_CONTEXT" -n portage-query rollout status deployment/trino --timeout=120s

# --- 4d. Kyuubi -----------------------------------------------------------------
log "Kyuubi image + deployment"
docker build -t portage/kyuubi:0.1.0 -f providers/query/kyuubi/image/Dockerfile providers/query/kyuubi/image
kind load docker-image portage/kyuubi:0.1.0 --name "$CLUSTER_NAME"
kubectl --context "$KUBE_CONTEXT" apply -f deploy/dev/kyuubi.yaml
kubectl --context "$KUBE_CONTEXT" -n portage-query rollout status deployment/kyuubi --timeout=180s

# --- 5. PostgreSQL --------------------------------------------------------------
log "PostgreSQL ($POSTGRES_CONTAINER on :$POSTGRES_PORT)"
if docker ps --format '{{.Names}}' | grep -qx "$POSTGRES_CONTAINER"; then
  echo "already running"
elif docker ps -a --format '{{.Names}}' | grep -qx "$POSTGRES_CONTAINER"; then
  docker start "$POSTGRES_CONTAINER" >/dev/null
else
  docker run -d --name "$POSTGRES_CONTAINER" \
    -e POSTGRES_USER=portage -e POSTGRES_PASSWORD=portage -e POSTGRES_DB=portage \
    -p "${POSTGRES_PORT}:5432" postgres:16 >/dev/null
fi
until docker exec "$POSTGRES_CONTAINER" pg_isready -U portage >/dev/null 2>&1; do sleep 1; done

log "Migrations"
DATABASE_URL="$DATABASE_URL" alembic upgrade head

# --- 6. Application image ----------------------------------------------------
log "Application image (portage/spark-base:phase0, portage/wordcount:0.1.0)"
docker build -t portage/spark-base:phase0 -f providers/execution/kubernetes/image/Dockerfile providers/execution/kubernetes/image
python -m build --wheel --outdir examples/wordcount_app/dist examples/wordcount_app >/dev/null
docker build -t portage/wordcount:0.1.0 -f examples/wordcount_app/Dockerfile examples/wordcount_app
kind load docker-image portage/wordcount:0.1.0 --name "$CLUSTER_NAME"

# --- 7. Control plane (API + reconciler) -------------------------------------
# PIDs are tracked in .dev-*.pid (not just pattern-matched at teardown time)
# so dev-down.sh can never mistake an unrelated process — e.g. a
# hand-started reconciler for a different environment on the same
# machine — for one of this script's own.
log "Control plane processes"
if ! curl -sf "${API_URL}/health" >/dev/null 2>&1; then
  DATABASE_URL="$DATABASE_URL" \
    nohup uvicorn api.main:app --host 127.0.0.1 --port "$API_PORT" \
    > "${ROOT_DIR}/.dev-api.log" 2>&1 < /dev/null &
  echo $! > "${ROOT_DIR}/.dev-api.pid"
  for _ in $(seq 1 30); do curl -sf "${API_URL}/health" >/dev/null 2>&1 && break; sleep 1; done
else
  echo "API already running on :$API_PORT"
fi

if ! curl -sf "http://127.0.0.1:${RECONCILER_METRICS_PORT}/metrics" >/dev/null 2>&1; then
  DATABASE_URL="$DATABASE_URL" \
    PORTAGE_LOCAL_ACCESS_KEY="$MINIO_ACCESS_KEY" PORTAGE_LOCAL_SECRET_KEY="$MINIO_SECRET_KEY" \
    PORTAGE_RECONCILE_INTERVAL_SECONDS=5 PORTAGE_RECONCILER_METRICS_PORT="$RECONCILER_METRICS_PORT" \
    nohup python -m reconciler.main \
    > "${ROOT_DIR}/.dev-reconciler.log" 2>&1 < /dev/null &
  echo $! > "${ROOT_DIR}/.dev-reconciler.pid"
else
  echo "reconciler already running (metrics on :$RECONCILER_METRICS_PORT)"
fi

# --- 8. Seed the "local" environment ------------------------------------------
log "Seeding local environment"
post() { curl -sf -X POST "${API_URL}$1" -H 'Content-Type: application/json' -d "$2" >/dev/null 2>&1 || true; }

post /v1/execution-profiles '{"name":"local-kind","provider":"kubernetes","config":{"namespace":"default","service_account":"spark","image":"portage/wordcount:0.1.0","context":"'"$KUBE_CONTEXT"'"}}'
post /v1/storage-profiles '{"name":"local-minio","provider":"s3","config":{"endpoint_url":"http://minio.portage-storage.svc.cluster.local:9000","path_style_access":true},"credential_reference":{"provider":"env","reference":"PORTAGE_LOCAL"}}'
post /v1/environments '{"name":"local","execution_provider":"kubernetes","execution_profile_name":"local-kind","storage_provider":"s3","storage_profile_name":"local-minio"}'
post /v1/datasets '{"dataset_name":"wordcount.raw","environment_name":"local","kind":"path","uri":"s3a://portage-local/wordcount/input.txt"}'
post /v1/datasets '{"dataset_name":"wordcount.counts","environment_name":"local","kind":"path","uri":"s3a://portage-local/wordcount/output"}'

log "Done"
cat <<EOF

  PORTAGE_API_URL=${API_URL} plane run examples/wordcount.yaml --environment local

EOF
