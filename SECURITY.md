# Security Policy

## Reporting a vulnerability

Please **do not** open a public GitHub issue for a suspected security vulnerability.

Report it privately using [GitHub's private vulnerability reporting](https://github.com/bmiller1009/portage/security/advisories/new) for this repository (Security tab → "Report a vulnerability"). Include:

- A description of the vulnerability and its potential impact.
- Steps to reproduce it, or a proof of concept if you have one.
- The version/commit you tested against.

You should receive an acknowledgment within a reasonable time. This is a small, actively-maintained open-source project with a single lead maintainer (see the README's Governance section) — there is no formal SLA on response time, but reports are taken seriously and triaged promptly.

## Scope

In scope: the control plane (`api/`, `control_plane/`, `reconciler/`), the execution/storage providers (`providers/`), the CLI (`cli/`), the operational UI (`ui/`), and the Helm chart (`charts/`).

Out of scope: vulnerabilities in third-party dependencies (report those upstream — though we'd appreciate a heads-up too, since we ship them), and vulnerabilities that require an attacker to already have `PlatformAdmin`-level RBAC access to your own deployment (that's an operational trust boundary, not a Portage bug).

## Supported versions

Security fixes are made against the latest released version. There is no long-term-support branch policy yet — see [`CHANGELOG.md`](CHANGELOG.md) for the current release.
