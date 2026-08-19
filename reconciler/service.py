"""The async submit/poll/converge worker (docs/architecture/spec.md §24).
Two testable stages, called in sequence by reconcile_once(): submit_new_runs
picks up ACCEPTED runs and submits them to a real execution provider;
poll_active_runs advances already-submitted runs toward a terminal state.

Every per-run exception is caught and turned into a FAILED transition
rather than crashing the loop — control-plane resilience (ADR 0007) applies
to the reconciler itself, not just to the API staying up during an outage.
"""

import json
from datetime import UTC, datetime

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from control_plane import metrics, provider_factory, repositories, run_service, webhooks
from control_plane.execution_provider import (
    ResolvedWorkload,
    RetryableProviderError,
    RunRequest,
    TerminalProviderError,
)
from control_plane.models import StorageProfile
from control_plane.run_state import TERMINAL_STATES, RunState
from spec.artifact.v1alpha1 import (
    Artifact,
    ArtifactMetadata,
    ArtifactPathBinding,
    parse_artifact_reference,
    resolve_artifact_uri,
)
from spec.dataset.v1alpha1 import (
    Dataset,
    DatasetMetadata,
    PathBinding,
    TableBinding,
    resolve_dataset_config,
    resolve_iceberg_catalog_config,
)
from spec.workload.v1alpha1 import SparkWorkload

_ACTIVE_STATES = [RunState.SUBMITTING.value, RunState.QUEUED.value, RunState.RUNNING.value]


async def _event_timestamp(session: AsyncSession, run_id, to_state: str) -> datetime | None:
    """Latest event that transitioned into to_state, or None if it never
    has — used to compute latency histograms from RunEvent's own timestamps
    rather than tracking duration state separately."""
    events = await repositories.list_run_events(session, run_id)
    for event in reversed(events):
        if event.to_state == to_state:
            return event.created_at
    return None


async def _resolve_dataset_config(
    session: AsyncSession,
    workload: SparkWorkload,
    environment_name: str,
    storage_profile: StorageProfile,
    storage_config: dict[str, str],
) -> dict[str, str]:
    """Adapts persisted DatasetBinding rows into the shape
    spec/dataset/v1alpha1.py's resolve_dataset_config() already expects,
    reusing that function rather than duplicating its logic. A row's
    `kind` selects PathBinding vs TableBinding (§11) — for the latter, the
    `uri` column holds the table's fully-qualified identifier rather than
    a URI, the same column repurposed rather than adding a new one.

    storage_config is the caller's already-resolved
    provider_factory.build_storage_config(storage_profile) output — reused
    here (not re-resolved) to source Iceberg's native S3 client config
    (spark.sql.catalog.<name>.s3.*) from the same credentials the
    spark.hadoop.fs.s3a.* keys already carry, since both configure access
    to the same underlying bucket."""
    refs = list(workload.datasets.inputs.values()) + list(workload.datasets.outputs.values())
    datasets: dict[str, Dataset] = {}
    for ref in refs:
        binding_row = await repositories.get_dataset_binding(
            session, ref.dataset, environment_name, required=False
        )
        if binding_row is not None:
            binding = (
                TableBinding(identifier=binding_row.uri)
                if binding_row.kind == "table"
                else PathBinding(uri=binding_row.uri)
            )
            datasets[ref.dataset] = Dataset(
                apiVersion="runtime/v1",
                kind="Dataset",
                metadata=DatasetMetadata(name=ref.dataset),
                bindings={environment_name: binding},
            )
    config = resolve_dataset_config(workload, datasets, environment_name)
    config.update(
        resolve_iceberg_catalog_config(
            workload,
            datasets,
            environment_name,
            iceberg_catalog_uri=storage_profile.config.get("iceberg_catalog_uri"),
            iceberg_warehouse=storage_profile.config.get("iceberg_warehouse"),
            s3_endpoint=storage_config.get("spark.hadoop.fs.s3a.endpoint"),
            s3_access_key=storage_config.get("spark.hadoop.fs.s3a.access.key"),
            s3_secret_key=storage_config.get("spark.hadoop.fs.s3a.secret.key"),
            s3_path_style_access=storage_config.get("spark.hadoop.fs.s3a.path.style.access"),
        )
    )
    return config


async def _resolve_artifact_reference(session: AsyncSession, reference: str, environment_name: str) -> str:
    """Resolves a single artifact:// reference (spec §51) through the
    persisted ArtifactBinding table to its environment-specific URI. A
    non-artifact:// value (e.g. a local:// path baked into an image, like
    examples/wordcount-jar.yaml's) passes through unchanged — the
    abstraction is opt-in, not mandatory."""
    if not reference.startswith("artifact://"):
        return reference

    name, version = parse_artifact_reference(reference)
    binding_row = await repositories.get_artifact_binding(
        session, name, version, environment_name, required=False
    )
    artifact = Artifact(
        apiVersion="runtime/v1",
        kind="Artifact",
        metadata=ArtifactMetadata(name=name, version=version),
        bindings=(
            {environment_name: ArtifactPathBinding(uri=binding_row.uri)} if binding_row is not None else {}
        ),
    )
    return resolve_artifact_uri(reference, artifact, environment_name)


async def _resolve_artifact(
    session: AsyncSession, workload: SparkWorkload, environment_name: str
) -> SparkWorkload:
    """Resolves both of ApplicationSpec's possible artifact:// fields —
    `artifact` (python-wheel/jvm-jar) and `pipelineSpec`
    (spark-declarative-pipeline, spec §39) — through the same
    ArtifactBinding-backed mechanism, returning a copy of the workload
    with whichever field is set replaced by its resolved URI. Providers
    keep reading these fields exactly as before, they just now get a
    real URI instead of a logical reference."""
    updates: dict[str, str] = {}
    if workload.application.artifact is not None:
        updates["artifact"] = await _resolve_artifact_reference(
            session, workload.application.artifact, environment_name
        )
    if workload.application.pipelineSpec is not None:
        updates["pipelineSpec"] = await _resolve_artifact_reference(
            session, workload.application.pipelineSpec, environment_name
        )
    if not updates:
        return workload
    return workload.model_copy(update={"application": workload.application.model_copy(update=updates)})


MAX_SUBMISSION_ATTEMPTS = 5


async def submit_new_runs(session: AsyncSession) -> None:
    # Transitions every claimed run to SUBMITTING and commits before this
    # loop calls provider.submit() (spec §57/§67) — a real, persisted claim,
    # unlike the old ACCEPTED-until-submit-returns window a crash could
    # land in. Also picks up runs stuck in SUBMITTING from a prior crash.
    for run in await repositories.claim_runs_for_submission(session):
        try:
            workload_row = await repositories.get_workload_definition(
                session, run.workload_name, version=run.workload_version
            )
            workload = SparkWorkload.model_validate(workload_row.definition)
            workload = await _resolve_artifact(session, workload, run.environment_name)
            environment = await repositories.get_environment(session, run.environment_name)
            execution_profile = await repositories.get_execution_profile(
                session, environment.execution_profile_name
            )
            storage_profile = await repositories.get_storage_profile(
                session, environment.storage_profile_name
            )
            storage_config = provider_factory.build_storage_config(storage_profile)

            resolved = ResolvedWorkload(
                workload=workload,
                dataset_config=await _resolve_dataset_config(
                    session, workload, run.environment_name, storage_profile, storage_config
                ),
                storage_config=storage_config,
                environment_name=run.environment_name,
                volume_mounts=provider_factory.build_storage_volume_mounts(storage_profile),
            )
            provider = provider_factory.build_execution_provider(execution_profile)

            validation = await provider.validate(resolved)
            if not validation.valid:
                await run_service.transition_run_state(
                    session, run, RunState.FAILED, message="; ".join(validation.errors)
                )
                continue

            try:
                provider_run = await provider.submit(RunRequest(run_id=str(run.id), resolved=resolved))
            except RetryableProviderError as e:
                metrics.provider_errors_total.add(1)
                attempts = await repositories.increment_submission_attempts(session, run)
                if attempts >= MAX_SUBMISSION_ATTEMPTS:
                    await run_service.transition_run_state(
                        session,
                        run,
                        RunState.FAILED,
                        message=f"exceeded max submission attempts ({attempts}): {e}",
                    )
                else:
                    await run_service.transition_run_state(
                        session,
                        run,
                        RunState.ACCEPTED,
                        message=f"retryable submission error (attempt {attempts}): {e}",
                    )
                continue
            except TerminalProviderError as e:
                metrics.provider_errors_total.add(1)
                await run_service.transition_run_state(session, run, RunState.FAILED, message=str(e))
                continue

            await repositories.create_provider_run(
                session,
                run_id=run.id,
                provider_run_id=provider_run.provider_run_id,
                provider=execution_profile.provider,
                raw=provider_run.raw or {},
            )
            accepted_at = await _event_timestamp(session, run.id, RunState.ACCEPTED.value)
            if accepted_at is not None:
                metrics.submission_latency_seconds.record(
                    (datetime.now(UTC) - accepted_at).total_seconds()
                )
            await run_service.transition_run_state(
                session, run, RunState.QUEUED, message=f"submitted as {provider_run.provider_run_id}"
            )
        except Exception as e:  # noqa: BLE001 - deliberate: one run's failure must not stop the loop
            # Unclassified — a provider bug or something genuinely unknown,
            # not a RetryableProviderError/TerminalProviderError the
            # provider deliberately raised. Kept terminal (same as before
            # this issue) rather than silently retried, but labeled so
            # it's visibly different from a provider's own classification.
            metrics.provider_errors_total.add(1)
            await run_service.transition_run_state(
                session, run, RunState.FAILED, message=f"unclassified error: {e}"
            )


async def poll_active_runs(session: AsyncSession) -> None:
    for run in await repositories.list_runs_by_state(session, _ACTIVE_STATES):
        try:
            provider_run = await repositories.get_latest_provider_run(session, run.id)
            if provider_run is None:
                continue

            environment = await repositories.get_environment(session, run.environment_name)
            execution_profile = await repositories.get_execution_profile(
                session, environment.execution_profile_name
            )
            provider = provider_factory.build_execution_provider(execution_profile)

            try:
                status = await provider.status(provider_run.provider_run_id)
            except RetryableProviderError as e:
                # spec §56's "network interruption after submission" — a
                # transient status() blip must not fail a run that's
                # actually fine. Skip this tick; the run stays in its
                # current active state and gets polled again next tick.
                metrics.provider_errors_total.add(1)
                await run_service.transition_run_state(
                    session, run, RunState(run.state), message=f"transient poll error, retrying: {e}"
                )
                continue

            if status.state.value != run.state:
                if status.state == RunState.RUNNING:
                    queued_at = await _event_timestamp(session, run.id, RunState.QUEUED.value)
                    if queued_at is not None:
                        metrics.queue_latency_seconds.record(
                            (datetime.now(UTC) - queued_at).total_seconds()
                        )
                elif status.state in TERMINAL_STATES:
                    running_at = await _event_timestamp(session, run.id, RunState.RUNNING.value)
                    if running_at is not None:
                        metrics.execution_duration_seconds.record(
                            (datetime.now(UTC) - running_at).total_seconds()
                        )
                await run_service.transition_run_state(
                    session, run, status.state, message=status.provider_native_status
                )
        except Exception as e:  # noqa: BLE001 - same rationale as submit_new_runs
            metrics.provider_errors_total.add(1)
            await run_service.transition_run_state(
                session, run, RunState.FAILED, message=f"unclassified error: {e}"
            )


async def cancel_runs(session: AsyncSession) -> None:
    for run in await repositories.list_runs_by_state(session, [RunState.CANCELING.value]):
        try:
            provider_run = await repositories.get_latest_provider_run(session, run.id)
            if provider_run is None:
                # Never submitted — nothing at a provider to cancel.
                await run_service.transition_run_state(session, run, RunState.CANCELED, message="canceled")
                continue

            environment = await repositories.get_environment(session, run.environment_name)
            execution_profile = await repositories.get_execution_profile(
                session, environment.execution_profile_name
            )
            provider = provider_factory.build_execution_provider(execution_profile)

            try:
                await provider.cancel(provider_run.provider_run_id)
            except RetryableProviderError as e:
                # Same reasoning as poll_active_runs() — a transient
                # cancel() blip must not fail a cancel that would
                # otherwise succeed; stays CANCELING, retried next tick.
                metrics.provider_errors_total.add(1)
                await run_service.transition_run_state(
                    session, run, RunState.CANCELING, message=f"transient cancel error, retrying: {e}"
                )
                continue

            await run_service.transition_run_state(session, run, RunState.CANCELED, message="canceled")
        except Exception as e:  # noqa: BLE001 - same rationale as submit_new_runs
            metrics.provider_errors_total.add(1)
            await run_service.transition_run_state(
                session, run, RunState.FAILED, message=f"unclassified error: {e}"
            )


MAX_WEBHOOK_DELIVERY_ATTEMPTS = 5


async def deliver_webhooks(session: AsyncSession) -> None:
    """Sends every pending WebhookDelivery (spec §39/§69) — the only tick
    function here that does outbound HTTP to a caller-controlled URL, so
    a slow or dead endpoint must never block or fail anything else in
    this loop. Each delivery is independent: one failing never affects
    another, and a delivery that keeps failing is marked "failed" past
    MAX_WEBHOOK_DELIVERY_ATTEMPTS rather than retried forever."""
    for delivery in await repositories.list_pending_webhook_deliveries(session):
        subscription = await repositories.get_webhook_subscription(session, delivery.subscription_id)
        if subscription is None or not subscription.enabled:
            await repositories.mark_webhook_delivery(session, delivery, status="failed", attempts=delivery.attempts)
            continue

        body = json.dumps(delivery.payload).encode()
        signature = webhooks.sign_payload(body, subscription.secret)
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.post(
                    subscription.url,
                    content=body,
                    headers={"Content-Type": "application/json", "X-Portage-Signature": signature},
                )
            response.raise_for_status()
            await repositories.mark_webhook_delivery(
                session, delivery, status="delivered", attempts=delivery.attempts + 1
            )
        except httpx.HTTPError:
            attempts = delivery.attempts + 1
            status = "failed" if attempts >= MAX_WEBHOOK_DELIVERY_ATTEMPTS else "pending"
            await repositories.mark_webhook_delivery(session, delivery, status=status, attempts=attempts)


async def reconcile_once(session: AsyncSession) -> None:
    await submit_new_runs(session)
    await poll_active_runs(session)
    await cancel_runs(session)
    await deliver_webhooks(session)
