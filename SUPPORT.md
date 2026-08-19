# Support

## Community support

- **Bugs and feature requests**: [GitHub Issues](https://github.com/bmiller1009/portage/issues).
- **Questions and general discussion**: GitHub Issues is also fine for now — open one and tag it as a question.
- **Security vulnerabilities**: see [`SECURITY.md`](SECURITY.md) — do not file these as public issues.

Response times are best-effort. Portage is maintained by a single lead maintainer (see the README's Governance section) — issues and PRs are triaged and reviewed as time allows, not on a fixed schedule.

## What this project does not provide

**There is no production support SLA.** Portage is open-source software distributed under the Apache License 2.0, which includes the standard "AS IS," no-warranty terms (see [`LICENSE`](LICENSE)). Running it in production is your own operational responsibility — the reliability work described in [`docs/verification/v1.0.0.md`](docs/verification/v1.0.0.md) (chaos testing, HA deployment, live-verified providers) is real engineering rigor, not a guarantee of uptime or support response times.

There is currently no commercial support offering. If that changes, it will be announced here rather than implied.

## Before opening an issue

Check [`docs/verification/v1.0.0.md`](docs/verification/v1.0.0.md) and the [compatibility matrix](README.md#compatibility-matrix) first — some environment/version combinations are explicitly marked `EXPERIMENTAL` or untested, and that's expected, not a bug report waiting to happen.
