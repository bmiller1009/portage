import { useQuery } from "@tanstack/react-query";
import { api } from "../api";

export default function WorkloadsPage() {
  const { data, isLoading, error } = useQuery({ queryKey: ["workloads"], queryFn: api.listWorkloads });

  if (isLoading) return <p>Loading workloads…</p>;
  if (error) return <p className="error">Failed to load workloads: {(error as Error).message}</p>;

  return (
    <div>
      <h1>Workloads</h1>
      {data && data.length === 0 && <p className="muted">No workloads registered.</p>}
      {data && data.length > 0 && (
        <table className="data-table">
          <thead>
            <tr>
              <th>Name</th>
              <th>Version</th>
              <th>Spark</th>
              <th>Artifact type</th>
            </tr>
          </thead>
          <tbody>
            {data.map((w) => (
              <tr key={`${w.name}@${w.version}`}>
                <td>{w.name}</td>
                <td>{w.version}</td>
                <td>{(w.definition.runtime as { spark?: string } | undefined)?.spark ?? "—"}</td>
                <td>{(w.definition.application as { type?: string } | undefined)?.type ?? "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
