import { getControlPlaneUrl, getApiKey } from './config';

/**
 * HTTP client for SkyREPL control plane API.
 *
 * Includes API key authentication via Bearer token in all requests.
 * Uses Bun's built-in fetch() and WebSocket.
 */
export class ApiClient {
  private baseUrl: string;
  private apiKey: string | null;

  constructor(baseUrl?: string, apiKey?: string | null) {
    this.baseUrl = baseUrl ?? getControlPlaneUrl();
    this.apiKey = apiKey !== undefined ? apiKey : getApiKey();
  }

  private headers(extra?: Record<string, string>): Record<string, string> {
    const h: Record<string, string> = {};
    if (this.apiKey) h['Authorization'] = `Bearer ${this.apiKey}`;
    if (extra) Object.assign(h, extra);
    return h;
  }

  /**
   * Check control plane health.
   * GET /v1/health
   */
  async healthCheck(): Promise<boolean> {
    const response = await fetch(`${this.baseUrl}/v1/health`);
    return response.ok;
  }

  /**
   * Launch a run workflow.
   * POST /v1/workflows/launch-run
   */
  async launchRun(request: LaunchRunRequest): Promise<LaunchRunResponse> {
    const response = await fetch(`${this.baseUrl}/v1/workflows/launch-run`, {
      method: 'POST',
      headers: this.headers({ 'Content-Type': 'application/json' }),
      body: JSON.stringify(request),
    });

    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      const msg = (body as any)?.error?.message ?? `HTTP ${response.status}`;
      throw new Error(`launch-run failed: ${msg}`);
    }

    return (await response.json()) as LaunchRunResponse;
  }

  /**
   * Get workflow status.
   * GET /v1/workflows/:id/status
   */
  async getWorkflowStatus(id: string): Promise<WorkflowStatus> {
    const response = await fetch(`${this.baseUrl}/v1/workflows/${id}/status`, {
      headers: this.headers(),
    });

    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      const msg = (body as any)?.error?.message ?? `HTTP ${response.status}`;
      throw new Error(`workflow status failed: ${msg}`);
    }

    return (await response.json()) as WorkflowStatus;
  }

  /**
   * Stream run logs via WebSocket.
   * WS /v1/runs/:id/logs
   *
   * The server sends JSON messages:
   * - Log lines: { stream: "stdout"|"stderr", data: "...", timestamp: N }
   * - Status changes: { type: "status", status: "completed"|"failed"|"timeout", exit_code?, error? }
   *
   * @param runId Run ID to stream logs for
   * @param onLog Callback for each log line
   * @param onStatus Callback for status changes (run completed/failed)
   */
  streamLogs(
    runId: string,
    onLog: (stream: 'stdout' | 'stderr', data: string) => void,
    onStatus?: (status: string, exitCode?: number, error?: string) => void,
  ): Promise<void> {
    const wsUrl = this.baseUrl.replace(/^http/, 'ws');
    const url = `${wsUrl}/v1/runs/${runId}/logs`;

    return new Promise<void>((resolve, reject) => {
      const ws = new WebSocket(url);
      let settled = false;
      let lastError: Error | null = null;

      const settle = (fn: () => void) => {
        if (!settled) {
          settled = true;
          fn();
        }
      };

      ws.onopen = () => {
        // Connection established; server will push logs
      };

      ws.onmessage = (event) => {
        try {
          const msg = JSON.parse(typeof event.data === 'string' ? event.data : '');

          if (msg.type === 'status') {
            // Run status change (completed, failed, timeout)
            onStatus?.(msg.status, msg.exit_code, msg.error);
            if (msg.status === 'completed' || msg.status === 'failed' || msg.status === 'timeout') {
              ws.close();
              settle(() => resolve());
            }
          } else if (msg.stream && msg.data !== undefined) {
            // Log line
            onLog(msg.stream, msg.data);
          }
        } catch {
          // Non-JSON message; treat as stdout
          onLog('stdout', String(event.data));
        }
      };

      ws.onerror = (event) => {
        lastError = new Error(`WebSocket error: ${(event as any).message ?? 'connection failed'}`);
      };

      ws.onclose = (event) => {
        if ((event as any).code !== 1000 && lastError) {
          settle(() => reject(lastError));
        } else {
          settle(() => resolve());
        }
      };
    });
  }

  /**
   * List runs.
   * GET /v1/runs
   */
  async listRuns(filters?: { status?: string }): Promise<{ data: any[] }> {
    const params = new URLSearchParams();
    if (filters?.status) params.set('status', filters.status);
    const qs = params.toString();
    const response = await fetch(`${this.baseUrl}/v1/runs${qs ? '?' + qs : ''}`, {
      headers: this.headers(),
    });
    if (!response.ok) throw new Error(`list runs failed: HTTP ${response.status}`);
    return (await response.json()) as { data: any[] };
  }

  /**
   * Cancel a workflow.
   * POST /v1/workflows/:id/cancel
   */
  async cancelWorkflow(id: string, reason?: string): Promise<{ workflow_id: number; status: string; cancelled: boolean }> {
    const response = await fetch(`${this.baseUrl}/v1/workflows/${id}/cancel`, {
      method: 'POST',
      headers: this.headers({ 'Content-Type': 'application/json' }),
      body: JSON.stringify({ reason: reason ?? 'user_requested' }),
    });
    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      const msg = (body as any)?.error?.message ?? `HTTP ${response.status}`;
      throw new Error(`cancel workflow failed: ${msg}`);
    }
    return (await response.json()) as { workflow_id: number; status: string; cancelled: boolean };
  }

  /**
   * List instances.
   * GET /v1/instances
   */
  async listInstances(filters?: { status?: string; provider?: string }): Promise<{ data: any[] }> {
    const params = new URLSearchParams();
    if (filters?.status) params.set('status', filters.status);
    if (filters?.provider) params.set('provider', filters.provider);
    const qs = params.toString();
    const response = await fetch(`${this.baseUrl}/v1/instances${qs ? '?' + qs : ''}`, {
      headers: this.headers(),
    });
    if (!response.ok) throw new Error(`list instances failed: HTTP ${response.status}`);
    return (await response.json()) as { data: any[] };
  }

  /**
   * List allocations.
   * GET /v1/allocations
   */
  async listAllocations(filters?: { status?: string }): Promise<{ data: any[] }> {
    const params = new URLSearchParams();
    if (filters?.status) params.set('status', filters.status);
    const qs = params.toString();
    const response = await fetch(`${this.baseUrl}/v1/allocations${qs ? '?' + qs : ''}`, {
      headers: this.headers(),
    });
    if (!response.ok) throw new Error(`list allocations failed: HTTP ${response.status}`);
    return (await response.json()) as { data: any[] };
  }

  /**
   * List workflows.
   * GET /v1/workflows
   */
  async listWorkflows(): Promise<{ data: any[] }> {
    const response = await fetch(`${this.baseUrl}/v1/workflows`, {
      headers: this.headers(),
    });
    if (!response.ok) throw new Error(`list workflows failed: HTTP ${response.status}`);
    return (await response.json()) as { data: any[] };
  }

  /**
   * Terminate an instance.
   * POST /v1/workflows/terminate-instance
   */
  async terminateInstance(instanceId: number): Promise<{ workflow_id: number; instance_id: number; status: string }> {
    const response = await fetch(`${this.baseUrl}/v1/workflows/terminate-instance`, {
      method: 'POST',
      headers: this.headers({ 'Content-Type': 'application/json' }),
      body: JSON.stringify({ instanceId }),
    });

    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      const msg = (body as any)?.error?.message ?? `HTTP ${response.status}`;
      throw new Error(`terminate-instance failed: ${msg}`);
    }

    return (await response.json()) as { workflow_id: number; instance_id: number; status: string };
  }

  /**
   * Extend the debug hold on a COMPLETE allocation.
   * POST /v1/allocations/:id/extend
   */
  async extendAllocation(id: number, durationMs: number): Promise<{ data: any }> {
    const response = await fetch(`${this.baseUrl}/v1/allocations/${id}/extend`, {
      method: 'POST',
      headers: this.headers({ 'Content-Type': 'application/json' }),
      body: JSON.stringify({ duration_ms: durationMs }),
    });
    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      const msg = (body as any)?.error?.message ?? `HTTP ${response.status}`;
      throw new Error(`extend allocation failed: ${msg}`);
    }
    return (await response.json()) as { data: any };
  }

  /**
   * Release the debug hold on a COMPLETE allocation.
   * POST /v1/allocations/:id/release
   */
  async releaseAllocation(id: number): Promise<{ data: any }> {
    const response = await fetch(`${this.baseUrl}/v1/allocations/${id}/release`, {
      method: 'POST',
      headers: this.headers({ 'Content-Type': 'application/json' }),
      body: JSON.stringify({}),
    });
    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      const msg = (body as any)?.error?.message ?? `HTTP ${response.status}`;
      throw new Error(`release allocation failed: ${msg}`);
    }
    return (await response.json()) as { data: any };
  }

  /**
   * List artifacts for a run.
   * GET /v1/runs/:id/artifacts
   */
  async listArtifacts(runId: string): Promise<{ data: ArtifactEntry[] }> {
    const response = await fetch(`${this.baseUrl}/v1/runs/${runId}/artifacts`, {
      headers: this.headers(),
    });
    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      const msg = (body as any)?.error?.message ?? `HTTP ${response.status}`;
      throw new Error(`list artifacts failed: ${msg}`);
    }
    return (await response.json()) as { data: ArtifactEntry[] };
  }

  /**
   * Download an artifact.
   * GET /v1/artifacts/:id/download
   */
  async downloadArtifact(artifactId: string): Promise<{ data: Buffer; filename: string }> {
    // Use redirect: "manual" so we can follow 302 without forwarding the Bearer token.
    // Presigned URLs carry their own auth in the query string — forwarding our token
    // would corrupt the S3 request signature.
    const response = await fetch(`${this.baseUrl}/v1/artifacts/${artifactId}/download`, {
      headers: this.headers(),
      redirect: 'manual',
    });

    let finalResponse: Response;
    if (response.status === 302) {
      const location = response.headers.get('Location');
      if (!location) throw new Error('download artifact failed: 302 with no Location header');
      // Follow redirect WITHOUT auth header
      finalResponse = await fetch(location);
    } else {
      finalResponse = response;
    }

    if (!finalResponse.ok) {
      const body = await finalResponse.json().catch(() => ({}));
      const msg = (body as any)?.error?.message ?? `HTTP ${finalResponse.status}`;
      throw new Error(`download artifact failed: ${msg}`);
    }
    const disposition = finalResponse.headers.get('Content-Disposition') ?? '';
    const match = disposition.match(/filename="?([^"]+)"?/);
    const filename = match?.[1] ?? `artifact-${artifactId}`;
    const data = Buffer.from(await finalResponse.arrayBuffer());
    return { data, filename };
  }

  // ─── Key Management (AUTH-04) ────────────────────────────────────

  async createKey(name: string, role: string): Promise<{ id: number; name: string; role: string; raw_key: string; expires_at: number }> {
    const response = await fetch(`${this.baseUrl}/v1/keys`, {
      method: 'POST',
      headers: this.headers({ 'Content-Type': 'application/json' }),
      body: JSON.stringify({ name, role }),
    });
    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      const msg = (body as any)?.error?.message ?? `HTTP ${response.status}`;
      throw new Error(`create-key failed: ${msg}`);
    }
    const { data } = await response.json() as { data: any };
    return data;
  }

  async listKeys(): Promise<{ data: any[] }> {
    const response = await fetch(`${this.baseUrl}/v1/keys`, {
      headers: this.headers(),
    });
    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      const msg = (body as any)?.error?.message ?? `HTTP ${response.status}`;
      throw new Error(`list-keys failed: ${msg}`);
    }
    return response.json() as Promise<{ data: any[] }>;
  }

  async revokeKey(id: number): Promise<void> {
    const response = await fetch(`${this.baseUrl}/v1/keys/${id}`, {
      method: 'DELETE',
      headers: this.headers(),
    });
    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      const msg = (body as any)?.error?.message ?? `HTTP ${response.status}`;
      throw new Error(`revoke-key failed: ${msg}`);
    }
  }

  // ─── Team Management (TENANT-02) ─────────────────────────────────

  async createTenant(name: string, opts?: { seat_cap?: number; budget_usd?: number }): Promise<{ data: any }> {
    const response = await fetch(`${this.baseUrl}/v1/tenants`, {
      method: 'POST',
      headers: this.headers({ 'Content-Type': 'application/json' }),
      body: JSON.stringify({ name, ...opts }),
    });
    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      const msg = (body as any)?.error?.message ?? `HTTP ${response.status}`;
      throw new Error(`team create failed: ${msg}`);
    }
    return (await response.json()) as { data: any };
  }

  async getTenant(id: number): Promise<{ data: any }> {
    const response = await fetch(`${this.baseUrl}/v1/tenants/${id}`, {
      headers: this.headers(),
    });
    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      const msg = (body as any)?.error?.message ?? `HTTP ${response.status}`;
      throw new Error(`team info failed: ${msg}`);
    }
    return (await response.json()) as { data: any };
  }

  async updateTenant(id: number, updates: Record<string, unknown>): Promise<{ data: any }> {
    const response = await fetch(`${this.baseUrl}/v1/tenants/${id}`, {
      method: 'PATCH',
      headers: this.headers({ 'Content-Type': 'application/json' }),
      body: JSON.stringify(updates),
    });
    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      const msg = (body as any)?.error?.message ?? `HTTP ${response.status}`;
      throw new Error(`team update failed: ${msg}`);
    }
    return (await response.json()) as { data: any };
  }

  async listTenantUsers(tenantId: number): Promise<{ data: any[] }> {
    const response = await fetch(`${this.baseUrl}/v1/tenants/${tenantId}/users`, {
      headers: this.headers(),
    });
    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      const msg = (body as any)?.error?.message ?? `HTTP ${response.status}`;
      throw new Error(`team list failed: ${msg}`);
    }
    return (await response.json()) as { data: any[] };
  }

  async inviteUser(tenantId: number, email: string, role: string, opts?: { display_name?: string; budget_usd?: number }): Promise<{ data: any }> {
    const response = await fetch(`${this.baseUrl}/v1/tenants/${tenantId}/users`, {
      method: 'POST',
      headers: this.headers({ 'Content-Type': 'application/json' }),
      body: JSON.stringify({ email, role, ...opts }),
    });
    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      const msg = (body as any)?.error?.message ?? `HTTP ${response.status}`;
      throw new Error(`team invite failed: ${msg}`);
    }
    return (await response.json()) as { data: any };
  }

  async removeUser(userId: number): Promise<void> {
    const response = await fetch(`${this.baseUrl}/v1/users/${userId}`, {
      method: 'DELETE',
      headers: this.headers(),
    });
    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      const msg = (body as any)?.error?.message ?? `HTTP ${response.status}`;
      throw new Error(`team remove failed: ${msg}`);
    }
  }

  async updateUser(userId: number, updates: Record<string, unknown>): Promise<{ data: any }> {
    const response = await fetch(`${this.baseUrl}/v1/users/${userId}`, {
      method: 'PATCH',
      headers: this.headers({ 'Content-Type': 'application/json' }),
      body: JSON.stringify(updates),
    });
    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      const msg = (body as any)?.error?.message ?? `HTTP ${response.status}`;
      throw new Error(`user update failed: ${msg}`);
    }
    return (await response.json()) as { data: any };
  }

  async getUsage(): Promise<{ data: any }> {
    const response = await fetch(`${this.baseUrl}/v1/usage`, {
      headers: this.headers(),
    });
    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      const msg = (body as any)?.error?.message ?? `HTTP ${response.status}`;
      throw new Error(`usage failed: ${msg}`);
    }
    return (await response.json()) as { data: any };
  }

  /**
   * Run server-side preflight checks before a launch.
   * GET /v1/preflight
   */
  async getPreflight(params: {
    operation: 'launch-run' | 'terminate-instance' | 'create-snapshot';
    spec?: string;
    provider?: string;
    region?: string;
  }): Promise<PreflightResponse> {
    const qs = new URLSearchParams({ operation: params.operation });
    if (params.spec) qs.set('spec', params.spec);
    if (params.provider) qs.set('provider', params.provider);
    if (params.region) qs.set('region', params.region);

    const response = await fetch(`${this.baseUrl}/v1/preflight?${qs}`, {
      headers: this.headers(),
    });
    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      const msg = (body as any)?.error?.message ?? `HTTP ${response.status}`;
      throw new Error(`preflight failed: ${msg}`);
    }
    return (await response.json()) as PreflightResponse;
  }

  // Stubs for future slices (not needed for Slice 1)
  async checkBlobs(_checksums: string[]): Promise<CheckBlobsResponse> {
    throw new Error('checkBlobs not implemented');
  }

  async uploadBlob(_checksum: string, _data: Buffer): Promise<void> {
    throw new Error('uploadBlob not implemented');
  }

  /**
   * Stream workflow progress via SSE.
   * GET /v1/workflows/:id/stream
   *
   * The server sends named SSE events:
   *   node_started, node_completed, node_failed,
   *   workflow_completed, workflow_failed, heartbeat
   *
   * Resolves when the workflow reaches a terminal state (completed or failed).
   * Rejects on connection error.
   */
  streamWorkflowProgress(
    workflowId: string,
    onEvent: (event: WorkflowStreamEvent) => void,
  ): Promise<WorkflowStreamTerminalEvent> {
    const url = `${this.baseUrl}/v1/workflows/${workflowId}/stream`;

    return new Promise<WorkflowStreamTerminalEvent>((resolve, reject) => {
      const abortController = new AbortController();

      // Use fetch to open the SSE stream
      fetch(url, { headers: this.headers(), signal: abortController.signal })
        .then(async (response) => {
          if (!response.ok) {
            const body = await response.json().catch(() => ({}));
            const msg = (body as any)?.error?.message ?? `HTTP ${response.status}`;
            reject(new Error(`streamWorkflowProgress failed: ${msg}`));
            return;
          }

          if (!response.body) {
            reject(new Error('streamWorkflowProgress: no response body'));
            return;
          }

          const reader = response.body.getReader();
          const decoder = new TextDecoder();
          let buffer = '';
          let currentEvent = '';
          let terminated = false;

          const processLine = (line: string) => {
            if (line.startsWith('event:')) {
              currentEvent = line.slice(6).trim();
            } else if (line.startsWith('data:')) {
              const dataStr = line.slice(5).trim();
              if (!dataStr) return;
              try {
                const data = JSON.parse(dataStr);
                const event: WorkflowStreamEvent = { event: currentEvent, data };
                onEvent(event);
                if (currentEvent === 'workflow_completed' || currentEvent === 'workflow_failed') {
                  terminated = true;
                  resolve({ event: currentEvent, data } as WorkflowStreamTerminalEvent);
                  reader.cancel().catch(() => {});
                  abortController.abort();
                }
              } catch {
                // Ignore malformed data lines
              }
              currentEvent = '';
            }
          };

          try {
            while (!terminated) {
              const { done, value } = await reader.read();
              if (done) break;
              buffer += decoder.decode(value, { stream: true });
              const lines = buffer.split('\n');
              // Keep the last (possibly incomplete) line in the buffer
              buffer = lines.pop() ?? '';
              for (const line of lines) {
                processLine(line);
              }
            }
          } catch (err) {
            // Abort errors are expected when we cancel after a terminal event
            if (!terminated) {
              reject(err);
            }
          }
        })
        .catch((err) => {
          // Abort errors are expected when we cancel after a terminal event
          if (!abortController.signal.aborted) {
            reject(err);
          }
        });
    });
  }
}

// =============================================================================
// Type Definitions
// =============================================================================

// Wire-format types for CLI client. These match the API contract (snake_case).
// The shared package (@skyrepl/contracts/api/workflows) has canonical domain types
// with camelCase field names. These CLI types use snake_case to match the HTTP
// wire format, so they are defined locally rather than imported.
// See: contracts/src/api/workflows.ts for LaunchRunRequest, LaunchRunResponse, WorkflowStatusResponse

export interface LaunchRunRequest {
  command: string;
  workdir: string;
  spec: string;
  provider: string;
  region?: string;
  instance_type?: string;
  init_checksum?: string | null;
  use_tailscale?: boolean;
  max_duration_ms: number;
  hold_duration_ms: number;
  create_snapshot: boolean;
  files: FileManifestEntry[];
  artifact_patterns: string[];
  env?: Record<string, string>;
  spot?: boolean;
}

export interface FileManifestEntry {
  path: string;
  checksum: string;
  sizeBytes: number;
}

export interface LaunchRunResponse {
  workflow_id: number;
  run_id: number;
  status: string; // Wire format includes 'created' | 'deduplicated'
  status_url?: string; // CLI extension; not in shared canonical type
}

export interface CheckBlobsResponse {
  missing: string[];
  urls: Record<string, string>;
}

export interface PreflightWarning {
  type: string;
  message: string;
  severity: 'info' | 'warning' | 'critical';
  details?: Record<string, unknown>;
}

export interface PreflightError {
  code: string;
  message: string;
  details?: Record<string, unknown>;
}

export interface PreflightResponse {
  ok: boolean;
  warnings: PreflightWarning[];
  errors?: PreflightError[];
}

// WorkflowStatus uses snake_case fields matching the API wire format.
// The 'progress' field is a CLI-specific extension for display purposes.
export interface WorkflowStatus {
  workflow_id: number;
  type: string;
  status: string;
  current_node: string | null;
  nodes_total: number;
  nodes_completed: number;
  nodes_failed: number;
  started_at: number | null;
  finished_at: number | null;
  progress: {
    completed_nodes: number;
    total_nodes: number;
    percentage: number;
  };
  output: unknown;
  error: {
    code: string;
    message: string;
    category: string;
    node_id?: string;
    details?: unknown;
  } | null;
}

export interface WorkflowEvent {
  type: string;
  message?: string;
  details?: unknown;
}

export interface WorkflowStreamEvent {
  event: string;
  data: Record<string, unknown>;
}

export type WorkflowStreamTerminalEvent =
  | { event: 'workflow_completed'; data: { status: string; output: unknown; timestamp: number } }
  | { event: 'workflow_failed'; data: { error: string; node_id?: string; timestamp: number } };

export interface ArtifactEntry {
  id: number;
  run_id: number;
  path: string | null;
  checksum: string | null;
  size_bytes: number;
  created_at: number;
}

export interface WorkflowCompletionResult {
  run_id: string;
  instance_id: string;
  allocation_id: string;
  exit_code: number | null;
  duration_ms: number;
  cost_usd: number;
  hold_remaining_ms: number;
  spot_interrupted: boolean;
  has_artifacts: boolean;
}
