# sdk/python

An optional convenience SDK (e.g. `datasets.uri("claims.raw")`) layered over the stable, public `portable.dataset.*` Spark configuration contract (`docs/architecture/spec.md` §10). Application code must never be *required* to import this — the raw configuration contract is what stays stable and prevents lock-in to the runtime itself.

Not yet implemented — post-Phase-0.
