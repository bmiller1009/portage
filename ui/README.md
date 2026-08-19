# ui

Minimal operational UI (React, TypeScript, Vite, TanStack Query, `react-router-dom`) — a pure client of the public REST API, with no privileged UI-only backend (`docs/architecture/spec.md` §4.3, §32). No component library — plain CSS, matching the spec's own framing: "The UI is operational, not an authoring studio."

Explicitly not built here: drag/drop DAG editors, a notebook editor, a large SQL IDE, BI visualization, or a workflow designer.

## Pages

- **Runs** — list (`GET /v1/runs`, auto-refreshing) and detail (status, environment, execution/storage provider, duration, event history, a Logs reference, a Cancel button) — the detail view is the spec §32 mockup's real fields; fields with no real data source yet (driver health, live executor counts) are left out rather than fabricated.
- **Workloads** — `GET /v1/workloads`.
- **Environments** — `GET /v1/environments`.
- **Datasets** — dataset bindings (`GET /v1/datasets`) and artifact bindings (`GET /v1/artifacts`, spec §51) as two sections on one page.
- **Providers** — every registered execution/storage profile (`GET /v1/providers`); click a row to fetch its live capability set (`GET /v1/providers/{name}/capabilities`).
- **Conformance** — an honest "not yet implemented" placeholder for *this page specifically*; the underlying capability (`plane conformance test`/`report`, `GET /v1/conformance/*`) is real and live-verified — this UI page hasn't been built to surface it yet.
- **System** — API/ready health, a recent-run-state summary, and a link to `GET /metrics` (raw Prometheus text isn't rendered inline — spec §29: "the runtime should export rather than become a dashboard product").

## Development

```
npm install
npm run dev     # Vite dev server, defaults to http://localhost:8123 for the API
npm run build    # tsc -b && vite build — also what `make lint` runs from the repo root
npm run lint     # oxlint
```

Point at a different API with `VITE_API_URL` (e.g. `VITE_API_URL=http://127.0.0.1:8123 npm run dev`). CORS is wide open on the API in dev (`api/main.py`), so no proxy config is needed.
