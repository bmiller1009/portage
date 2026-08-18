import { Fragment, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "../api";

function CapabilitiesRow({ name }: { name: string }) {
  const { data, isLoading, error } = useQuery({
    queryKey: ["provider-capabilities", name],
    queryFn: () => api.getProviderCapabilities(name),
  });

  if (isLoading) return <p>Loading capabilities…</p>;
  if (error) return <p className="error">{(error as Error).message}</p>;

  return (
    <table className="kv-table">
      <tbody>
        {Object.entries(data!.capabilities).map(([key, value]) => (
          <tr key={key}>
            <td>{key}</td>
            <td>{Array.isArray(value) ? value.join(", ") : String(value)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export default function ProvidersPage() {
  const { data, isLoading, error } = useQuery({ queryKey: ["providers"], queryFn: api.listProviders });
  const [expanded, setExpanded] = useState<string | null>(null);

  if (isLoading) return <p>Loading providers…</p>;
  if (error) return <p className="error">Failed to load providers: {(error as Error).message}</p>;

  return (
    <div>
      <h1>Providers</h1>
      <p className="muted">
        Every registered execution/storage profile, each declaring real capabilities (spec §20, §47) —
        click a row to fetch its live capability set.
      </p>
      {data && data.length === 0 && <p className="muted">No providers registered.</p>}
      {data && data.length > 0 && (
        <table className="data-table">
          <thead>
            <tr>
              <th>Name</th>
              <th>Kind</th>
              <th>Provider</th>
            </tr>
          </thead>
          <tbody>
            {data.map((p) => (
              <Fragment key={p.name}>
                <tr className="clickable" onClick={() => setExpanded(expanded === p.name ? null : p.name)}>
                  <td>{p.name}</td>
                  <td>{p.kind}</td>
                  <td>{p.provider}</td>
                </tr>
                {expanded === p.name && (
                  <tr>
                    <td colSpan={3}>
                      <CapabilitiesRow name={p.name} />
                    </td>
                  </tr>
                )}
              </Fragment>
            ))}
          </tbody>
        </table>
      )}
    </div>
  );
}
