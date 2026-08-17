# reconciler

The async worker that owns the submit/poll/converge loop described in `docs/architecture/spec.md` §24: it validates a `RunRequest`, submits it to the appropriate `ExecutionProvider`, polls or watches provider status, and updates the canonical run state in PostgreSQL.

This separation is what makes control-plane failure non-fatal to running Spark jobs (§4.5, ADR 0007): once a workload is submitted, the execution provider owns it, and the reconciler just rediscovers and converges state on restart.

Not yet implemented — v0.1 milestone.
