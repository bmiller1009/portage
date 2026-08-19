.PHONY: dev dev-down test lint release

# Stands up kind + PostgreSQL + MinIO + Spark Operator + the control plane
# (docs/architecture/spec.md §45) — see scripts/dev-up.sh. Idempotent: safe
# to re-run after a partial failure.
dev:
	scripts/dev-up.sh

# Tears down everything `make dev` created.
dev-down:
	scripts/dev-down.sh

test:
	pytest tests/unit -v

lint:
	ruff check .
	pyright
	cd ui && npm run lint && npm run build

# Verification gates only (scripts/release-check.sh) — never pushes, tags,
# or publishes. Once it passes: git tag vX.Y.Z && git push origin vX.Y.Z,
# which triggers .github/workflows/release.yml.
release:
	VERSION=$(VERSION) scripts/release-check.sh
