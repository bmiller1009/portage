# Provider SDK

v1.0 (spec.md §71, "Portable Contract" exit criterion) requires the
provider SDK to be stable. There is no separate installable SDK package,
and **no dynamic or hot-loadable plugin mechanism exists** — Portage
provides stable provider interfaces and contract tests for implementing
additional providers, not a plugin system you install without touching
core code. The SDK *is* the two Python `Protocol` classes below,
structurally implemented (no base-class inheritance required) by
`providers/execution/kubernetes/provider.py` and
`providers/execution/databricks/provider.py` for execution, and
`providers/storage/s3/provider.py`, `providers/storage/vast/provider.py`,
and `providers/storage/adls/provider.py` for storage. Writing a new
provider today means implementing one of these two Protocols and adding
it to `control_plane/provider_factory.py`'s dispatch (see "Wiring a
provider in" below) — a core-code change and PR, not an installable
plugin. Dynamic provider loading (discoverable third-party packages,
entry-point registration) remains roadmap work, not a current
capability — this document will say so explicitly if that ever changes.

## ExecutionProvider

Defined in `control_plane/execution_provider.py`.

```python
class ExecutionProvider(Protocol):
    async def validate(self, workload: ResolvedWorkload) -> ValidationResult: ...
    async def submit(self, run: RunRequest) -> ProviderRun: ...
    async def status(self, provider_run_id: str) -> ProviderStatus: ...
    async def cancel(self, provider_run_id: str) -> None: ...
    async def logs(self, provider_run_id: str) -> LogReference: ...
    async def capabilities(self) -> CapabilitySet: ...
```

- **`validate`** — fail-fast capability checking before a run is ever
  submitted (spec §20-21). Every real implementation delegates to the
  shared `match_capabilities(workload, capabilities)` helper in the same
  module rather than reimplementing the check, so a new provider should
  do the same: call `capabilities()`, pass the result and the workload
  into `match_capabilities()`, and wrap any returned error strings in a
  `ValidationResult`.
- **`submit`** — translates a `ResolvedWorkload` (the workload plus its
  resolved dataset/storage config — see below) into whatever the
  underlying platform's job-submission call is (a `SparkApplication` CRD
  create for Kubernetes, a `jobs.submit()` call for Databricks) and
  returns a `ProviderRun` carrying that platform's own run identifier.
  Submission must be idempotent under retry — the reconciler may call
  `submit` again after a crash before it observes the first call's
  result, so a provider needs some form of a stable idempotency key (the
  Kubernetes provider uses the run id as the CRD's own resource name,
  which is naturally idempotent via a 409-on-duplicate-create; the
  Databricks provider uses the Jobs API's `idempotency_token`).
- **`status`** — polls the platform for the run's current state and maps
  it onto the shared `RunState` enum (`control_plane/run_state.py`) — the
  one place a provider's own status vocabulary (Kubernetes pod phases,
  Databricks `RunLifeCycleState`/`RunResultState`) gets translated into
  Portage's provider-agnostic terminal/non-terminal states.
- **`cancel`** — best-effort cancellation of an in-flight run.
- **`logs`** — returns a `LogReference` (a description plus an optional
  URI) rather than log content itself — spec §26 treats log retrieval as
  a pointer the caller follows (`kubectl logs`, the Databricks run UI),
  not a payload this API proxies.
- **`capabilities`** — a static `CapabilitySet` declaration (supported
  Spark versions, languages, and the boolean feature flags
  `match_capabilities()` checks workload requirements against:
  `dynamic_allocation`, `gpu`, `streaming`, `local_disk`,
  `spark_connect`), plus `verification` (`"live_verified"` or
  `"translation_layer_only"`, v1.0.0) — an honest, self-reported signal
  distinguishing "this provider has actually executed a real workload
  against real infrastructure" from "this provider is implemented and
  unit-tested against fakes, but no real infrastructure has been
  reachable to test it against." A new provider should default to
  `"translation_layer_only"` until it has a real live run behind it.

**Errors**: raise `RetryableProviderError` for anything the reconciler
should safely requeue (timeouts, 429s, transient 5xx) and
`TerminalProviderError` for anything that must not be retried (spec §26:
"must not silently rerun an application after confirmed execution
failure"). Only the provider knows which of its own error shapes are
which — the reconciler doesn't guess.

**`ResolvedWorkload`** (the input to `validate`/`submit`, via `RunRequest`):
the `SparkWorkload` plus everything the provider needs to actually run it —
`dataset_config` (the `spark.portable.dataset.<name>.uri`/`.identifier`
entries from ADR 0006), `storage_config` (credentials/endpoint/jars from
the environment's storage provider), and `volume_mounts` (`None` for every
provider except Kubernetes with VAST NFS mode, where storage access isn't
expressible as Spark config at all and needs real pod volume mounts).

## StorageProvider

Defined in `control_plane/storage_provider.py`.

```python
class StorageProvider(Protocol):
    def resolve_uri(self, binding_uri: str) -> str: ...
    def spark_config(self) -> dict[str, str]: ...
    def health_check(self) -> bool: ...
    def capabilities(self) -> StorageCapabilitySet: ...
    def volume_mounts(self) -> list[dict] | None: ...
```

- **`resolve_uri`** — normalizes a dataset binding's raw URI into
  whatever form the provider's Spark connector expects (e.g. VAST's
  S3-compatible mode delegates straight to `S3StorageProvider`'s own
  `resolve_uri`; ADLS maps `abfss://...` paths through unchanged).
- **`spark_config`** — the `spark.hadoop.fs.*`-style config entries
  (credentials, endpoint, path-style-access flags) needed for Spark to
  actually reach the storage backend. Emits workload-identity/OAuth
  config instead of static keys when no credential value is configured
  (see `AdlsStorageProvider`'s `spark_config()` for the precedent —
  "missing credential" means "use ambient identity," not an error).
- **`health_check`** — a cheap, synchronous reachability check (spec §47).
- **`capabilities`** — protocol name plus whether path bindings, table
  bindings, or both are supported (spec §11's dataset binding kinds), plus
  `verification` (same `"live_verified"`/`"translation_layer_only"` tier
  as `ExecutionProvider.capabilities`, v1.0.0).
- **`volume_mounts`** — `None` for every provider except VAST NFS mode,
  which returns the actual Kubernetes volume/volumeMount manifest
  fragments the execution provider merges into the pod spec (spec §48).
  Every concrete provider implements this itself, trivially returning
  `None`, since Protocol conformance here is structural, not inherited.

## Wiring a provider in

Providers are constructed from persisted `ExecutionProfile`/
`StorageProfile` database rows by `control_plane/provider_factory.py`'s
`build_execution_provider()`/`build_storage_provider()` — a small
dispatch on the profile's `provider` string (`"kubernetes"`,
`"databricks"`; `"s3"`, `"vast"`, `"adls"`) to the matching concrete
class, passing the profile's `config` dict and resolved
`credential_reference` through to the provider's constructor. This is the
single, obvious dispatch point — no scattered `if provider == "x"`
conditionals exist elsewhere in the codebase (`api/`, `reconciler/`,
`cli/` all call through this factory rather than branching on provider
name themselves). Adding a new provider means:

1. Implement the Protocol (`ExecutionProvider` or `StorageProvider`).
2. Add one dispatch branch in `control_plane/provider_factory.py`.
3. Add the new provider name to the `Literal` unions in
   `spec/environment/v1alpha1.py`'s `ExecutionRef`/`DataRef`.
4. Pass the shared provider-contract test suite
   (`tests/unit/test_execution_provider_contracts.py` or
   `test_storage_provider_contracts.py`) — the same behavioral checks
   every existing provider runs, not a new bespoke test suite per
   provider.

No other file needs to change, by design (ADR 0005) — but this is still
a core-code PR, not an installable plugin (see this document's opening).

## Related

Schema (not provider) stability is documented separately in
[STABILITY.md](STABILITY.md).
