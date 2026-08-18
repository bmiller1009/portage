import { useState } from "react";
import { useParams } from "react-router-dom";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api, ApiError } from "../api";
import StatusBadge from "../components/StatusBadge";

const TERMINAL_STATES = new Set(["SUCCEEDED", "FAILED", "CANCELED", "LOST"]);

function formatDuration(startIso: string | null, endIso: string | null): string {
  if (!startIso) return "—";
  const start = new Date(startIso).getTime();
  const end = endIso ? new Date(endIso).getTime() : Date.now();
  const seconds = Math.max(0, Math.round((end - start) / 1000));
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  return m > 0 ? `${m}m ${s}s` : `${s}s`;
}

export default function RunDetailPage() {
  const { runId } = useParams<{ runId: string }>();
  const queryClient = useQueryClient();
  const [logsError, setLogsError] = useState<string | null>(null);

  const runQuery = useQuery({
    queryKey: ["run", runId],
    queryFn: () => api.getRun(runId!),
    enabled: !!runId,
    // Stop polling once the run is done — nothing left to observe.
    refetchInterval: (q) => (q.state.data && TERMINAL_STATES.has(q.state.data.state) ? false : 5000),
  });

  const eventsQuery = useQuery({
    queryKey: ["run-events", runId],
    queryFn: () => api.listRunEvents(runId!),
    enabled: !!runId,
  });

  const environmentsQuery = useQuery({ queryKey: ["environments"], queryFn: api.listEnvironments });

  const logsQuery = useQuery({
    queryKey: ["run-logs", runId],
    queryFn: () => api.getRunLogs(runId!),
    enabled: false,
    retry: false,
  });

  const cancelMutation = useMutation({
    mutationFn: () => api.cancelRun(runId!),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["run", runId] });
      queryClient.invalidateQueries({ queryKey: ["run-events", runId] });
    },
  });

  if (!runId) return null;
  if (runQuery.isLoading) return <p>Loading run…</p>;
  if (runQuery.error) return <p className="error">Failed to load run: {(runQuery.error as Error).message}</p>;

  const run = runQuery.data!;
  const environment = environmentsQuery.data?.find((e) => e.name === run.environment_name);
  const isTerminal = TERMINAL_STATES.has(run.state);

  async function handleShowLogs() {
    setLogsError(null);
    try {
      await logsQuery.refetch({ throwOnError: true });
    } catch (e) {
      setLogsError(e instanceof ApiError ? e.detail : "failed to fetch logs");
    }
  }

  return (
    <div>
      <h1>
        {run.workload_name}
        <span className="muted">@{run.workload_version}</span>
      </h1>

      <dl className="detail-grid">
        <dt>Status</dt>
        <dd>
          <StatusBadge state={run.state} />
        </dd>

        <dt>Environment</dt>
        <dd>{run.environment_name}</dd>

        <dt>Execution</dt>
        <dd>{environment?.execution_provider ?? "—"}</dd>

        <dt>Storage</dt>
        <dd>{environment?.storage_provider ?? "—"}</dd>

        <dt>Started</dt>
        <dd>{run.created_at ? new Date(run.created_at).toLocaleString() : "—"}</dd>

        <dt>Duration</dt>
        <dd>{formatDuration(run.created_at, isTerminal ? run.updated_at : null)}</dd>
      </dl>

      <section>
        <h2>Events</h2>
        {eventsQuery.isLoading && <p>Loading events…</p>}
        {eventsQuery.data && (
          <ul className="event-list">
            {eventsQuery.data.map((event, i) => (
              <li key={i}>
                {event.from_state ?? "(none)"} → <strong>{event.to_state}</strong>
                {event.message ? `: ${event.message}` : ""}
              </li>
            ))}
          </ul>
        )}
      </section>

      <section className="actions">
        <button onClick={handleShowLogs} disabled={logsQuery.isFetching}>
          {logsQuery.isFetching ? "Fetching logs…" : "Logs"}
        </button>
        {logsQuery.data && (
          <p className="logs-ref">
            {logsQuery.data.description}
            {logsQuery.data.uri ? <code>{logsQuery.data.uri}</code> : null}
          </p>
        )}
        {logsError && <p className="error">{logsError}</p>}

        {!isTerminal && (
          <button
            className="danger"
            onClick={() => cancelMutation.mutate()}
            disabled={cancelMutation.isPending}
          >
            {cancelMutation.isPending ? "Canceling…" : "Cancel"}
          </button>
        )}
        {cancelMutation.isError && (
          <p className="error">
            {cancelMutation.error instanceof ApiError
              ? cancelMutation.error.detail
              : "failed to cancel run"}
          </p>
        )}
      </section>
    </div>
  );
}
