// api/sse-protocol.ts - SSE/WebSocket Stream Management
// Stub: All function bodies throw "not implemented"

import { Elysia } from "elysia";

// =============================================================================
// Stream Message Types
// =============================================================================

export interface LogsQueryParams {
  stream?: "stdout" | "stderr" | "both";
  follow?: boolean;
  timestamps?: boolean;
}

export interface LogBroadcast {
  stream: "stdout" | "stderr";
  data: string;
  timestamp: number;
  sequence?: number;
}

export interface StatusBroadcast {
  type: "status";
  status: "running" | "completed" | "failed" | "timeout";
  exit_code?: number;
  error?: string;
}

export interface SyncBroadcast {
  type: "sync_complete";
  success: boolean;
}

export type WorkflowStreamEvent =
  | { event: "node_started"; data: { node_id: string; node_type: string; timestamp: number } }
  | { event: "node_completed"; data: { node_id: string; output: unknown; timestamp: number } }
  | { event: "node_failed"; data: { node_id: string; error: string; timestamp: number } }
  | { event: "pattern_applied"; data: { pattern: string; triggered_by: string; timestamp: number } }
  | { event: "progress"; data: { completed: number; total: number; percentage: number } }
  | { event: "subworkflow_started"; data: { subworkflow_id: string; type: string } }
  | { event: "subworkflow_completed"; data: { subworkflow_id: string; status: string } }
  | { event: "workflow_completed"; data: { status: string; output: unknown; timestamp: number } }
  | { event: "workflow_failed"; data: { error: string; node_id?: string; timestamp: number } }
  | { event: "heartbeat"; data: { timestamp: number } };

// =============================================================================
// SSE Manager
// =============================================================================

export class SSEManager {
  createCommandStream(
    instanceId: string,
    allocationId: string | undefined,
    request: Request
  ): Response {
    throw new Error("not implemented");
  }

  async sendCommand(instanceId: string, command: unknown): Promise<boolean> {
    throw new Error("not implemented");
  }

  async hasPendingCommands(instanceId: number): Promise<boolean> {
    throw new Error("not implemented");
  }

  getNextCommandId(): number {
    throw new Error("not implemented");
  }

  isAgentConnected(instanceId: string): boolean {
    throw new Error("not implemented");
  }

  subscribeToLogs(runId: string, ws: WebSocket): void {
    throw new Error("not implemented");
  }

  broadcastLog(runId: string, message: LogBroadcast): void {
    throw new Error("not implemented");
  }

  subscribeToWorkflow(workflowId: string, ws: WebSocket): void {
    throw new Error("not implemented");
  }

  broadcastWorkflowEvent(workflowId: string, event: WorkflowStreamEvent): void {
    throw new Error("not implemented");
  }

  closeRunSubscribers(runId: string, finalMessage: StatusBroadcast): void {
    throw new Error("not implemented");
  }

  closeWorkflowSubscribers(workflowId: string, finalEvent: WorkflowStreamEvent): void {
    throw new Error("not implemented");
  }
}

export const sseManager = new SSEManager();

// =============================================================================
// WebSocket Route Registration
// =============================================================================

export function registerWebSocketRoutes(app: Elysia<any>): void {
  throw new Error("not implemented");
}

// =============================================================================
// SSE Workflow Stream
// =============================================================================

export function registerSSEWorkflowStream(app: Elysia<any>): void {
  throw new Error("not implemented");
}
