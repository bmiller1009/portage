# 0001. Record architecture decisions

## Status

Accepted

## Context

Portage begins from an unusually complete architecture spec (`docs/architecture/spec.md`), but that document is a point-in-time draft, not a living record. As the project moves through Phase 0 and beyond, decisions will be revisited, challenged, and occasionally reversed by people who were not present for the original reasoning. Without a durable log, the project either relitigates settled questions repeatedly or loses track of why a constraint exists and quietly erodes it.

## Decision

We will keep an Architecture Decision Record log in `docs/adr/`, one Markdown file per decision, numbered sequentially, using Status/Context/Decision/Consequences sections. ADRs are immutable once accepted — a changed decision gets a new ADR that supersedes the old one, not an edit.

## Consequences

Every non-obvious architectural constraint in the codebase should be traceable to an ADR. Reviewers can push back on a PR that violates one, and can also propose a new ADR to change it deliberately rather than by drift.
