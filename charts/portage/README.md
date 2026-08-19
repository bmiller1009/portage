# charts/portage

Helm chart for Portage's own control plane — the API and reconciler
(docs/architecture/spec.md §46). Not the Spark workload execution image
(`providers/execution/kubernetes/image/`) or the third-party Spark
Kubernetes Operator (`scripts/dev-up.sh` installs that separately).

Live-verified (v1.0): `helm install` onto a real Kubernetes cluster with
2 API replicas + 2 reconciler replicas, both fully `Ready`, a real
`alembic upgrade head` migration run as a pre-install Helm hook against a
real PostgreSQL instance, and a real TLS 1.3 handshake terminated at an
nginx `Ingress` routing through to a live API pod's `/health` endpoint.

## What this chart does not do

- **Does not bundle PostgreSQL.** Spec §46 calls for "external/HA
  PostgreSQL" — bring your own, referenced via `database.existingSecretName`
  (a Secret you create, holding a `postgresql+asyncpg://` URL under
  `database.existingSecretKey`, default key name `DATABASE_URL`).
- **Does not terminate TLS in-process.** TLS terminates at the `Ingress`
  (`ingress.tlsSecretName` — a TLS keypair Secret, e.g. issued by
  cert-manager when `ingress.certManagerIssuer` is set), matching spec
  §46's separate listing of "Ingress/load balancer" and "TLS" as distinct
  recommended production items.
- **Does not enumerate provider credentials.** `credentials.existingSecretName`
  (optional) is exposed via `envFrom` to both the API and reconciler pods
  — its keys become whatever `PORTAGE_*_ACCESS_KEY` /
  `PORTAGE_DATABRICKS_CLIENT_SECRET` / etc. env vars your
  `ExecutionProfile`/`StorageProfile` rows' `credential_reference` fields
  expect (`control_plane/credentials.py`). The chart doesn't know these
  names in advance since they're profile-defined, not fixed.

## Minimal install

```bash
kubectl create secret generic portage-db \
  --from-literal=DATABASE_URL='postgresql+asyncpg://user:pass@host:5432/portage'

helm install portage charts/portage \
  --set database.existingSecretName=portage-db
```

## With ingress + TLS

```bash
kubectl create secret tls portage-tls --cert=tls.crt --key=tls.key

helm install portage charts/portage \
  --set database.existingSecretName=portage-db \
  --set ingress.enabled=true \
  --set ingress.host=portage.example.com \
  --set ingress.tlsSecretName=portage-tls
```

See `values.yaml` for the full set of options (replica counts, auth/OIDC
config, image repository/tag).

## Building the image

```bash
docker build -t portage/control-plane:1.0.0 -f Dockerfile .
```

One image, two roles — `charts/portage`'s API and reconciler Deployments
select which process runs via `command`/`args` (`uvicorn api.main:app ...`
vs. `python -m reconciler.main`), since both share the exact same
installed `portage` package.
