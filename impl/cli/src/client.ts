import { getControlPlaneUrl } from './config';

/**
 * HTTP client for SkyREPL control plane API.
 *
 * Responsibilities:
 * - HTTP request/response handling with auth
 * - Retry logic for transient failures
 * - SSE streaming for workflow progress
 * - WebSocket streaming for logs
 */
export class ApiClient {
  private baseUrl: string;
  private token: string | null = null;

  constructor() {
    this.baseUrl = getControlPlaneUrl();
  }

  /**
   * Launch a run workflow.
   * POST /v1/workflows/launch-run
   *
   * @param request Launch run request parameters
   * @returns Workflow submission result with workflow_id and run_id
   */
  async launchRun(request: LaunchRunRequest): Promise<LaunchRunResponse> {
    throw new Error('launchRun not implemented');
  }

  /**
   * Check which blobs already exist on the server.
   * POST /v1/blobs/check
   *
   * @param checksums Array of SHA-256 checksums to check
   * @returns Object with missing checksums and presigned upload URLs
   */
  async checkBlobs(checksums: string[]): Promise<CheckBlobsResponse> {
    throw new Error('checkBlobs not implemented');
  }

  /**
   * Upload a blob to the server.
   * PUT /v1/blobs/:checksum
   *
   * @param checksum SHA-256 checksum of the blob
   * @param data Blob data as Buffer
   */
  async uploadBlob(checksum: string, data: Buffer): Promise<void> {
    throw new Error('uploadBlob not implemented');
  }

  /**
   * Get workflow status.
   * GET /v1/workflows/:id/status
   *
   * @param id Workflow ID
   * @returns Workflow status information
   */
  async getWorkflowStatus(id: string): Promise<WorkflowStatus> {
    throw new Error('getWorkflowStatus not implemented');
  }

  /**
   * Stream workflow progress via SSE.
   * GET /v1/workflows/:id/stream
   *
   * @param workflowId Workflow ID to stream
   * @param onEvent Callback for each SSE event
   * @returns Promise that resolves with final workflow result
   */
  async streamWorkflowProgress(
    workflowId: string,
    onEvent: (event: WorkflowEvent) => void
  ): Promise<WorkflowCompletionResult> {
    throw new Error('streamWorkflowProgress not implemented');
  }
}

// Type definitions (stubbed)

export interface LaunchRunRequest {
  command: string;
  workdir: string;
  spec: string;
  provider: string;
  region?: string;
  init_checksum?: string | null;
  use_tailscale: boolean;
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
  workflow_id: string;
  run_id: string;
}

export interface CheckBlobsResponse {
  missing: string[];
  urls: Record<string, string>;
}

export interface WorkflowStatus {
  id: string;
  status: string;
  current_node?: string;
}

export interface WorkflowEvent {
  type: string;
  message?: string;
  details?: any;
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
