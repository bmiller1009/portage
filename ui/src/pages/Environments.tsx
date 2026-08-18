import { useQuery } from "@tanstack/react-query";
import { api } from "../api";

export default function EnvironmentsPage() {
  const { data, isLoading, error } = useQuery({
    queryKey: ["environments"],
    queryFn: api.listEnvironments,
  });

  if (isLoading) return <p>Loading environments…</p>;
  if (error) return <p className="error">Failed to load environments: {(error as Error).message}</p>;

  return (
    <div>
      <h1>Environments</h1>
      {data && data.length === 0 && <p className="muted">No environments registered.</p>}
      {data && data.length > 0 && (
        <table className="data-table">
          <thead>
            <tr>
              <th>Name</th>
              <th>Execution</th>
              <th>Storage</th>
            </tr>
          </thead>
          <tbody>
            {data.map((env) => (
              <tr key={env.name}>
                <td>{env.name}</td>
                <td>
                  {env.execution_provider} <span className="muted">({env.execution_profile_name})</span>
                </td>
                <td>
                  {env.storage_provider} <span className="muted">({env.storage_profile_name})</span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
