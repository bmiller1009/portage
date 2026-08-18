import { useQuery } from "@tanstack/react-query";
import { api } from "../api";

export default function DatasetsPage() {
  const datasets = useQuery({ queryKey: ["datasets"], queryFn: api.listDatasets });
  const artifacts = useQuery({ queryKey: ["artifacts"], queryFn: api.listArtifacts });

  return (
    <div>
      <h1>Datasets</h1>

      <h2>Dataset bindings</h2>
      {datasets.isLoading && <p>Loading…</p>}
      {datasets.error && <p className="error">{(datasets.error as Error).message}</p>}
      {datasets.data && datasets.data.length === 0 && <p className="muted">No dataset bindings.</p>}
      {datasets.data && datasets.data.length > 0 && (
        <table className="data-table">
          <thead>
            <tr>
              <th>Dataset</th>
              <th>Environment</th>
              <th>URI</th>
            </tr>
          </thead>
          <tbody>
            {datasets.data.map((d) => (
              <tr key={`${d.dataset_name}/${d.environment_name}`}>
                <td>{d.dataset_name}</td>
                <td>{d.environment_name}</td>
                <td>
                  <code>{d.uri}</code>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}

      <h2>Artifact bindings</h2>
      <p className="muted">
        The application artifact repository (spec §51) — a logical <code>artifact://name/version</code>{" "}
        reference resolves to one of these per environment.
      </p>
      {artifacts.isLoading && <p>Loading…</p>}
      {artifacts.error && <p className="error">{(artifacts.error as Error).message}</p>}
      {artifacts.data && artifacts.data.length === 0 && <p className="muted">No artifact bindings.</p>}
      {artifacts.data && artifacts.data.length > 0 && (
        <table className="data-table">
          <thead>
            <tr>
              <th>Artifact</th>
              <th>Environment</th>
              <th>URI</th>
            </tr>
          </thead>
          <tbody>
            {artifacts.data.map((a) => (
              <tr key={`${a.artifact_name}/${a.artifact_version}/${a.environment_name}`}>
                <td>
                  {a.artifact_name}@{a.artifact_version}
                </td>
                <td>{a.environment_name}</td>
                <td>
                  <code>{a.uri}</code>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
