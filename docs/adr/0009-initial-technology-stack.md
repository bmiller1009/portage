# 0009. Initial technology stack

## Status

Accepted

## Context

The control plane's workload is dominated by async I/O — orchestrating Kubernetes and Databricks API calls, resolving configuration, persisting/reading PostgreSQL state — not CPU-bound computation (Spark itself does the heavy lifting). Provider SDKs for Kubernetes and Databricks, and cloud SDKs generally, are strongest in Python. OpenAPI generation and rapid provider development also favor a dynamic, well-tooled ecosystem (spec §41).

## Decision

Control plane: Python 3.12+, FastAPI, Pydantic, SQLAlchemy, asyncpg, Alembic. Database: PostgreSQL 16+ (see ADR 0008). Kubernetes integration: official Kubernetes Python client, Apache Spark Kubernetes Operator, Helm 3. Databricks integration: official Databricks SDK, Jobs API 2.2, OAuth M2M/federation. CLI: Python, Typer, HTTP client against the public REST API only — no direct database or Kubernetes access from the CLI. UI: React, TypeScript, Vite, TanStack Query, kept dependency-light. Telemetry: OpenTelemetry, Prometheus-compatible metrics, structured JSON logging. Test stack: pytest, pytest-asyncio, pyright/mypy, ruff, kind, Docker, Helm.

## Consequences

A future rewrite into Go or another language is explicitly deferred until operational evidence justifies it — this is a starting choice, not a permanent constraint (spec §41). All provider SDK choices should default to the vendor's official client library rather than a third-party wrapper, matching the upstream-first principle (ADR 0004).
