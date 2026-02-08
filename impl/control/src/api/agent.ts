// api/agent.ts - Agent-Facing Endpoints
// Stub: All function bodies throw "not implemented"

import { Elysia } from "elysia";

// =============================================================================
// Agent Request Types
// =============================================================================

export interface AgentHeartbeatRequest {
  instance_id: number;
  allocation_id?: number;
  run_id?: number;
  status: "idle" | "preparing" | "running" | "uploading";
  cpu_percent?: number;
  memory_percent?: number;
  disk_percent?: number;
  gpu_utilization?: number;
  gpu_memory_percent?: number;
  tailscale_ip?: string | null;
  tailscale_status?: "not_installed" | "installing" | "ready" | "failed";
  active_allocations: Array<{
    allocation_id: number;
    has_ssh_sessions: boolean;
  }>;
}

export interface AgentLogsRequest {
  run_id: number;
  stream: "stdout" | "stderr" | "sync_complete";
  data: string;
  timestamp: number;
  sequence?: number;
  phase?: "sync" | "execution";
  sync_success?: boolean;
}

export interface AgentStatusRequest {
  run_id: number;
  status: "started" | "completed" | "failed" | "timeout";
  exit_code?: number;
  error_message?: string;
}

export interface SpotInterruptStartRequest {
  run_id: number;
  time_remaining_ms: number;
}

export interface SpotInterruptCompleteRequest {
  run_id: number;
  checkpoint_exit_code?: number;
  artifacts_uploaded: number;
}

export interface HeartbeatPanicStartRequest {
  run_id: number;
  time_since_ack_ms: number;
}

export interface HeartbeatPanicCompleteRequest {
  run_id: number;
  checkpoint_exit_code?: number;
  artifacts_uploaded: number;
}

export interface PanicDiagnosticsRequest {
  instance_id: number;
  reason: string;
  last_state: {
    workflow_state: string;
    run_id: number | null;
    allocation_id: number | null;
    uptime_seconds: number;
    last_heartbeat_ack: number | null;
    sse_reconnect_count: number;
  };
  diagnostics: {
    cpu_percent: number;
    memory_percent: number;
    disk_percent: number;
    active_threads: number;
    log_buffer_size: number;
  };
  error_logs: string[];
  timestamp: number;
}

// =============================================================================
// Route Registration
// =============================================================================

export function registerAgentRoutes(app: Elysia<any>): void {
  throw new Error("not implemented");
}

// =============================================================================
// Handler Helpers
// =============================================================================

export function handleSyncComplete(
  runId: number,
  syncSuccess: boolean
): Promise<void> {
  throw new Error("not implemented");
}

export function mapStatusToWorkflowState(status: string): string {
  throw new Error("not implemented");
}

export function getControlPlaneId(): string {
  throw new Error("not implemented");
}
