// Thin REST client for the control-plane API (docs/architecture/spec.md
// §30) — the UI is a pure client of the public API, no privileged
// UI-only backend (§4.3, §32).

const BASE_URL = import.meta.env.VITE_API_URL ?? "http://localhost:8123";

export class ApiError extends Error {
  status: number;
  detail: string;
  constructor(status: number, detail: string) {
    super(`${status}: ${detail}`);
    this.status = status;
    this.detail = detail;
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const resp = await fetch(`${BASE_URL}${path}`, {
    ...init,
    headers: { "Content-Type": "application/json", ...init?.headers },
  });
  if (!resp.ok) {
    let detail = resp.statusText;
    try {
      const body = await resp.json();
      detail = body.detail ?? detail;
    } catch {
      // non-JSON error body — fall back to statusText
    }
    throw new ApiError(resp.status, detail);
  }
  if (resp.status === 204) return undefined as T;
  return resp.json() as Promise<T>;
}

export interface RunOut {
  id: string;
  workload_name: string;
  workload_version: string;
  environment_name: string;
  state: string;
  created_at: string | null;
  updated_at: string | null;
}

export interface RunEventOut {
  from_state: string | null;
  to_state: string;
  message: string | null;
}

export interface RunLogsOut {
  description: string;
  uri: string | null;
}

export interface WorkloadDefinitionOut {
  name: string;
  version: string;
  definition: Record<string, unknown>;
}

export interface EnvironmentOut {
  name: string;
  execution_provider: string;
  execution_profile_name: string;
  storage_provider: string;
  storage_profile_name: string;
}

export interface DatasetBindingOut {
  dataset_name: string;
  environment_name: string;
  kind: string;
  uri: string;
}

export interface ArtifactBindingOut {
  artifact_name: string;
  artifact_version: string;
  environment_name: string;
  kind: string;
  uri: string;
}

export interface ProviderOut {
  name: string;
  kind: "execution" | "storage";
  provider: string;
}

export interface ProviderCapabilitiesOut {
  name: string;
  kind: "execution" | "storage";
  provider: string;
  capabilities: Record<string, unknown>;
}

export const api = {
  listRuns: (environmentName?: string) =>
    request<RunOut[]>(
      `/v1/runs${environmentName ? `?environment_name=${encodeURIComponent(environmentName)}` : ""}`,
    ),
  getRun: (id: string) => request<RunOut>(`/v1/runs/${id}`),
  listRunEvents: (id: string) => request<RunEventOut[]>(`/v1/runs/${id}/events`),
  getRunLogs: (id: string) => request<RunLogsOut>(`/v1/runs/${id}/logs`),
  cancelRun: (id: string) => request<RunOut>(`/v1/runs/${id}`, { method: "DELETE" }),

  listWorkloads: () => request<WorkloadDefinitionOut[]>("/v1/workloads"),
  listEnvironments: () => request<EnvironmentOut[]>("/v1/environments"),
  listDatasets: () => request<DatasetBindingOut[]>("/v1/datasets"),
  listArtifacts: () => request<ArtifactBindingOut[]>("/v1/artifacts"),

  listProviders: () => request<ProviderOut[]>("/v1/providers"),
  getProviderCapabilities: (name: string) =>
    request<ProviderCapabilitiesOut>(`/v1/providers/${encodeURIComponent(name)}/capabilities`),

  health: () => request<{ status: string }>("/health"),
  ready: () => request<{ status: string }>("/ready"),
};
