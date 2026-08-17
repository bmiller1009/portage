# spec/environment

The `Environment` schema (`docs/architecture/spec.md` §8): a named profile (`onprem-prod`, `azure-prod`) that resolves to a specific execution provider, storage provider, catalog provider, and identity provider. Application developers should normally only ever choose a workload + an environment name — never the underlying infrastructure objects.

Not yet implemented — Phase 0 currently hardcodes environment resolution in the CLI/execution providers; a real `Environment` model and resolver is v0.1 milestone scope.
