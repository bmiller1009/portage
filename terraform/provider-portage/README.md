# terraform-provider-portage

A Terraform provider for Portage (docs/architecture/spec.md §69) — manages
`portage_environment`, `portage_execution_profile`, `portage_storage_profile`,
and `portage_workload` as infrastructure-as-code, wrapping the public REST
API the way every other Portage integration does (spec §31): no local
state beyond what Terraform itself tracks, no direct database access.

`config`/`credential_reference`/`definition` attributes are JSON-encoded
strings (`jsonencode(...)`), not a native Terraform re-modeling of every
nested field — the same pragmatic choice most providers make for
complex, provider-specific nested config.

Note: the Terraform-facing attribute is `provider_type`, not `provider`
— `provider` is a reserved Terraform resource-block meta-argument, and a
schema attribute with that exact name gets misparsed as a reference to
an unrelated provider (confirmed live; see `execution_profile_resource.go`).

## Build

```
go build -o terraform-provider-portage .
```

## Local testing (dev overrides)

```
cat > ~/.terraformrc <<EOF
provider_installation {
  dev_overrides {
    "bmiller1009/portage" = "$(pwd)"
  }
  direct {}
}
EOF
cd examples
PORTAGE_API_URL=http://localhost:8000 terraform init
PORTAGE_API_URL=http://localhost:8000 terraform apply
```

## Acceptance tests

Real HTTP against a fake `httptest.Server`, not the real control plane —
standard `terraform-plugin-testing` pattern. Not run in CI by default
(needs a Go toolchain and a real `terraform` binary on `PATH`, same
opt-in-local-check status as `tests/integration/` needing a live
Postgres):

```
TF_ACC=1 go test ./internal/provider/... -v
```
