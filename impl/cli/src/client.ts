import { getControlPlaneUrl } from './config';

/**
 * HTTP client for SkyREPL control plane API.
 *
 * Slice 1: Simple fetch() calls with no auth, no retry logic.
 * Uses Bun's built-in fetch() and WebSocket.
 */
export class ApiClient {
  private baseUrl: string;

  constructor(baseUrl?: string) {
    this.baseUrl = baseUrl ?? getControlPlaneUrl();
  }

  /**
   * Launch a run workflow.
   * POST /v1/workflows/launch-run
   */
  async launchRun(request: LaunchRunRequest): Promise<LaunchRunResponse> {
    const response = await fetch(`${this.baseUrl}/v1/workflows/launch-run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
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
    const response = await fetch(`${this.baseUrl}/v1/workflows/${id}/status`);

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
              resolve();
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

      ws.onerror = () => {
        // WebSocket errors are followed by onclose, so resolve there
      };

      ws.onclose = () => {
        resolve();
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
    const response = await fetch(`${this.baseUrl}/v1/runs${qs ? '?' + qs : ''}`);
    if (!response.ok) throw new Error(`list runs failed: HTTP ${response.status}`);
    return (await response.json()) as { data: any[] };
  }

  /**
   * Cancel a workflow.
   * POST /v1/workflows/:id/cancel
   */
  async cancelWorkflow(id: string, reason?: string): Promise<{ workflowId: number; status: string; cancelled: boolean }> {
    const response = await fetch(`${this.baseUrl}/v1/workflows/${id}/cancel`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ reason: reason ?? 'user_requested' }),
    });
    if (!response.ok) {
      const body = await response.json().catch(() => ({}));
      const msg = (body as any)?.error?.message ?? `HTTP ${response.status}`;
      throw new Error(`cancel workflow failed: ${msg}`);
    }
    return (await response.json()) as { workflowId: number; status: string; cancelled: boolean };
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
    const response = await fetch(`${this.baseUrl}/v1/instances${qs ? '?' + qs : ''}`);
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
    const response = await fetch(`${this.baseUrl}/v1/allocations${qs ? '?' + qs : ''}`);
    if (!response.ok) throw new Error(`list allocations failed: HTTP ${response.status}`);
    return (await response.json()) as { data: any[] };
  }

  /**
   * List workflows.
   * GET /v1/workflows
   */
  async listWorkflows(): Promise<{ data: any[] }> {
    const response = await fetch(`${this.baseUrl}/v1/workflows`);
    if (!response.ok) throw new Error(`list workflows failed: HTTP ${response.status}`);
    return (await response.json()) as { data: any[] };
  }

  // Stubs for future slices (not needed for Slice 1)
  async checkBlobs(_checksums: string[]): Promise<CheckBlobsResponse> {
    throw new Error('checkBlobs not implemented');
  }

  async uploadBlob(_checksum: string, _data: Buffer): Promise<void> {
    throw new Error('uploadBlob not implemented');
  }

  async streamWorkflowProgress(
    _workflowId: string,
    _onEvent: (event: WorkflowEvent) => void
  ): Promise<WorkflowCompletionResult> {
    throw new Error('streamWorkflowProgress not implemented');
  }
}

// =============================================================================
// Type Definitions
// =============================================================================

// Wire-format types for CLI client. These match the API contract (snake_case).
// The shared package (@skyrepl/shared/api/workflows) has canonical domain types
// with camelCase field names. These CLI types use snake_case to match the HTTP
// wire format, so they are defined locally rather than imported.
// See: impl/shared/src/api/workflows.ts for LaunchRunRequest, LaunchRunResponse, WorkflowStatusResponse

export interface LaunchRunRequest {
  command: string;
  workdir: string;
  spec: string;
  provider: string;
  region?: string;
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

// WorkflowStatus uses camelCase fields matching shared WorkflowStatusResponse.
// The 'progress' field is a CLI-specific extension for display purposes.
export interface WorkflowStatus {
  workflowId: number;
  type: string;
  status: string;
  currentNode: string | null;
  nodesTotal: number;
  nodesCompleted: number;
  nodesFailed: number;
  startedAt: number | null;
  finishedAt: number | null;
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
    nodeId?: string;
    details?: unknown;
  } | null;
}

export interface WorkflowEvent {
  type: string;
  message?: string;
  details?: unknown;
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
