import { useQuery } from "@tanstack/react-query";
import { Link } from "react-router-dom";
import { api } from "../api";
import StatusBadge from "../components/StatusBadge";

export default function RunsPage() {
  const { data, isLoading, error } = useQuery({ queryKey: ["runs"], queryFn: () => api.listRuns() });

  if (isLoading) return <p>Loading runs…</p>;
  if (error) return <p className="error">Failed to load runs: {(error as Error).message}</p>;

  return (
    <div>
      <h1>Runs</h1>
      {data && data.length === 0 && <p className="muted">No runs yet.</p>}
      {data && data.length > 0 && (
        <table className="data-table">
          <thead>
            <tr>
              <th>Workload</th>
              <th>Environment</th>
              <th>State</th>
              <th>Started</th>
            </tr>
          </thead>
          <tbody>
            {data.map((run) => (
              <tr key={run.id}>
                <td>
                  <Link to={`/runs/${run.id}`}>
                    {run.workload_name}@{run.workload_version}
                  </Link>
                </td>
                <td>{run.environment_name}</td>
                <td>
                  <StatusBadge state={run.state} />
                </td>
                <td>{run.created_at ? new Date(run.created_at).toLocaleString() : "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
