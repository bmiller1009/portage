import { useQuery } from "@tanstack/react-query";
import { api } from "../api";

const API_URL = import.meta.env.VITE_API_URL ?? "http://localhost:8123";

function statusLine(label: string, ok: boolean | undefined, error: unknown) {
  if (error) return `${label}: unreachable`;
  if (ok === undefined) return `${label}: checking…`;
  return `${label}: ${ok ? "ok" : "not ready"}`;
}

export default function SystemPage() {
  const health = useQuery({ queryKey: ["health"], queryFn: api.health, refetchInterval: 5000 });
  const ready = useQuery({ queryKey: ["ready"], queryFn: api.ready, refetchInterval: 5000 });
  const runs = useQuery({ queryKey: ["runs"], queryFn: () => api.listRuns() });

  const counts: Record<string, number> = {};
  for (const run of runs.data ?? []) {
    counts[run.state] = (counts[run.state] ?? 0) + 1;
  }

  return (
    <div>
      <h1>System</h1>
      <p className="muted">
        Operational status, not a dashboard product (spec §29) — the full metrics surface is exported as
        Prometheus text, not rendered here.
      </p>

      <h2>Control plane</h2>
      <ul className="event-list">
        <li>{statusLine("API", health.data?.status === "ok", health.error)}</li>
        <li>{statusLine("Ready", ready.data?.status === "ok", ready.error)}</li>
      </ul>

      <h2>Recent runs (last {runs.data?.length ?? 0})</h2>
      {Object.keys(counts).length === 0 && <p className="muted">No runs yet.</p>}
      {Object.keys(counts).length > 0 && (
        <table className="kv-table">
          <tbody>
            {Object.entries(counts).map(([state, count]) => (
              <tr key={state}>
                <td>{state}</td>
                <td>{count}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      <h2>Metrics</h2>
      <p>
        <a href={`${API_URL}/metrics`} target="_blank" rel="noreferrer">
          {API_URL}/metrics
        </a>{" "}
        — raw Prometheus text (spec §29).
      </p>
    </div>
  );
}
