// api/sse-protocol.ts - SSE/WebSocket Stream Management

import { Elysia } from "elysia";
import {
  queryOne,
  queryMany,
  type Workflow,
  type WorkflowNode,
} from "../material/db";
import type { ControlToAgentMessage } from "@skyrepl/shared";

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
// Helpers
// =============================================================================

function getControlPlaneId(): string {
  return process.env.CONTROL_PLANE_ID ?? "cp-default";
}

// =============================================================================
// SSE Manager
// =============================================================================

/**
 * Manages SSE connections from agents and WebSocket connections from CLI clients.
 *
 * Agent SSE: Long-lived, agent-initiated, unidirectional (control -> agent).
 * CLI WebSocket: Bidirectional for log streaming and workflow progress.
 */
export class SSEManager {
  /** Active agent SSE connections: instanceId -> SSE controller */
  private agentStreams = new Map<string, ReadableStreamDefaultController<Uint8Array>>();

  /** Pending commands waiting for agent connection: instanceId -> commands[] */
  private pendingCommands = new Map<string, ControlToAgentMessage[]>();

  /** Active CLI log subscribers: runId -> Set<WebSocket> */
  private logSubscribers = new Map<string, Set<WebSocket>>();

  /** Active CLI workflow subscribers: workflowId -> Set<WebSocket> */
  private workflowSubscribers = new Map<string, Set<WebSocket>>();

  /** Command ID counter for acknowledgment protocol */
  private nextCommandId = 1;

  // --- Agent SSE Connections ------------------------------------------------

  /** Create SSE command stream for an agent */
  createCommandStream(
    instanceId: string,
    allocationId: string | undefined,
    request: Request
  ): Response {
    const encoder = new TextEncoder();

    const stream = new ReadableStream<Uint8Array>({
      start: (controller) => {
        // Close existing connection for this instance (replace)
        if (this.agentStreams.has(instanceId)) {
          console.warn("[sse] Replacing existing SSE connection", { instanceId });
          try {
            this.agentStreams.get(instanceId)!.close();
          } catch {
            /* already closed */
          }
        }

        this.agentStreams.set(instanceId, controller);
        console.info("[sse] Agent SSE connected", { instanceId, allocationId });

        // Send any pending commands
        const pending = this.pendingCommands.get(instanceId) ?? [];
        for (const cmd of pending) {
          const data = `event: ${cmd.type}\ndata: ${JSON.stringify(cmd)}\n\n`;
          controller.enqueue(encoder.encode(data));
        }
        this.pendingCommands.delete(instanceId);

        // Send initial heartbeat_ack to confirm connection
        const ack = `event: heartbeat_ack\ndata: ${JSON.stringify({
          type: "heartbeat_ack",
          control_plane_id: getControlPlaneId(),
        })}\n\n`;
        controller.enqueue(encoder.encode(ack));
      },

      cancel: () => {
        this.agentStreams.delete(instanceId);
        console.info("[sse] Agent SSE disconnected", { instanceId });
      },
    });

    return new Response(stream, {
      headers: {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
        "X-Accel-Buffering": "no", // Disable nginx buffering
      },
    });
  }

  /** Send a command to an agent via SSE */
  async sendCommand(
    instanceId: string,
    command: ControlToAgentMessage
  ): Promise<boolean> {
    const controller = this.agentStreams.get(instanceId);

    if (!controller) {
      // Queue for when agent reconnects
      const pending = this.pendingCommands.get(instanceId) ?? [];
      pending.push(command);
      this.pendingCommands.set(instanceId, pending);
      console.warn("[sse] Agent not connected, command queued", {
        instanceId,
        type: command.type,
      });
      return false;
    }

    try {
      const encoder = new TextEncoder();
      const data = `event: ${command.type}\ndata: ${JSON.stringify(command)}\n\n`;
      controller.enqueue(encoder.encode(data));
      return true;
    } catch (error) {
      console.error("[sse] Failed to send SSE command", { instanceId, error });
      this.agentStreams.delete(instanceId);
      return false;
    }
  }

  /** Check if there are pending commands for an instance */
  hasPendingCommands(instanceId: string): boolean {
    const pending = this.pendingCommands.get(instanceId) ?? [];
    return pending.length > 0;
  }

  /** Get next command ID for acknowledgment protocol */
  getNextCommandId(): number {
    return this.nextCommandId++;
  }

  /** Check if agent is connected via SSE */
  isAgentConnected(instanceId: string): boolean {
    return this.agentStreams.has(instanceId);
  }

  // --- CLI WebSocket: Log Streaming -----------------------------------------

  /** Register WebSocket for run log streaming */
  subscribeToLogs(runId: string, ws: WebSocket): void {
    if (!this.logSubscribers.has(runId)) {
      this.logSubscribers.set(runId, new Set());
    }
    this.logSubscribers.get(runId)!.add(ws);

    ws.addEventListener("close", () => {
      this.logSubscribers.get(runId)?.delete(ws);
      if (this.logSubscribers.get(runId)?.size === 0) {
        this.logSubscribers.delete(runId);
      }
    });
  }

  /** Broadcast log message to all CLI subscribers for a run */
  broadcastLog(runId: string, message: LogBroadcast): void {
    const subscribers = this.logSubscribers.get(runId);
    if (!subscribers) return;

    const data = JSON.stringify(message);
    for (const ws of subscribers) {
      try {
        ws.send(data);
      } catch {
        subscribers.delete(ws);
      }
    }
  }

  // --- CLI WebSocket: Workflow Progress -------------------------------------

  /** Register WebSocket for workflow progress streaming */
  subscribeToWorkflow(workflowId: string, ws: WebSocket): void {
    if (!this.workflowSubscribers.has(workflowId)) {
      this.workflowSubscribers.set(workflowId, new Set());
    }
    this.workflowSubscribers.get(workflowId)!.add(ws);

    ws.addEventListener("close", () => {
      this.workflowSubscribers.get(workflowId)?.delete(ws);
      if (this.workflowSubscribers.get(workflowId)?.size === 0) {
        this.workflowSubscribers.delete(workflowId);
      }
    });
  }

  /** Broadcast workflow event to all CLI subscribers */
  broadcastWorkflowEvent(
    workflowId: string,
    event: WorkflowStreamEvent
  ): void {
    const subscribers = this.workflowSubscribers.get(workflowId);
    if (!subscribers) return;

    const data = JSON.stringify(event);
    for (const ws of subscribers) {
      try {
        ws.send(data);
      } catch {
        subscribers.delete(ws);
      }
    }
  }

  /** Close all subscribers for a run (when run completes) */
  closeRunSubscribers(runId: string, finalMessage: StatusBroadcast): void {
    const subscribers = this.logSubscribers.get(runId);
    if (!subscribers) return;

    const data = JSON.stringify(finalMessage);
    for (const ws of subscribers) {
      try {
        ws.send(data);
        ws.close(1000, "Run completed");
      } catch {
        /* ignore */
      }
    }
    this.logSubscribers.delete(runId);
  }

  /** Close all subscribers for a workflow (when workflow completes) */
  closeWorkflowSubscribers(
    workflowId: string,
    finalEvent: WorkflowStreamEvent
  ): void {
    const subscribers = this.workflowSubscribers.get(workflowId);
    if (!subscribers) return;

    const data = JSON.stringify(finalEvent);
    for (const ws of subscribers) {
      try {
        ws.send(data);
        ws.close(1000, "Workflow completed");
      } catch {
        /* ignore */
      }
    }
    this.workflowSubscribers.delete(workflowId);
  }
}

export const sseManager = new SSEManager();

// =============================================================================
// WebSocket Route Registration
// =============================================================================

export function registerWebSocketRoutes(app: Elysia<any>): void {
  // WS /v1/runs/:id/logs - Live log streaming for CLI
  app.ws("/v1/runs/:id/logs", {
    open(ws) {
      const runId = (ws.data as any).params.id;
      console.info("[sse] CLI log stream opened", { runId });

      // Subscribe to live updates (no historical replay in Slice 1)
      sseManager.subscribeToLogs(runId, ws.raw as unknown as WebSocket);
    },
    message(_ws, _message) {
      // No client -> server messages defined yet
    },
    close(ws) {
      console.debug("[sse] CLI log stream closed", {
        runId: (ws.data as any).params.id,
      });
    },
  });

  // WS /v1/workflows/:id/progress - Live workflow progress for CLI
  app.ws("/v1/workflows/:id/progress", {
    open(ws) {
      const workflowId = (ws.data as any).params.id;
      console.info("[sse] CLI workflow progress stream opened", { workflowId });

      // Subscribe to live updates (no historical replay in Slice 1)
      sseManager.subscribeToWorkflow(
        workflowId,
        ws.raw as unknown as WebSocket
      );
    },
    message(_ws, _message) {
      // No client -> server messages defined yet
    },
    close(ws) {
      console.debug("[sse] CLI workflow stream closed", {
        workflowId: (ws.data as any).params.id,
      });
    },
  });
}

// =============================================================================
// SSE Workflow Stream - GET /v1/workflows/:id/stream
// =============================================================================

export function registerSSEWorkflowStream(app: Elysia<any>): void {
  app.get("/v1/workflows/:id/stream", ({ params }) => {
    const workflowId = params.id;

    // Validate workflow exists
    const workflow = queryOne<Workflow>(
      "SELECT * FROM workflows WHERE id = ?",
      [workflowId]
    );
    if (!workflow) {
      return new Response(
        JSON.stringify({
          error: {
            code: "WORKFLOW_NOT_FOUND",
            message: "Workflow not found",
            category: "not_found",
          },
        }),
        { status: 404, headers: { "Content-Type": "application/json" } }
      );
    }

    const encoder = new TextEncoder();

    const stream = new ReadableStream<Uint8Array>({
      start(controller) {
        // Send historical events for already-completed nodes
        const nodes = queryMany<WorkflowNode>(
          "SELECT * FROM workflow_nodes WHERE workflow_id = ? ORDER BY started_at ASC",
          [workflowId]
        );

        for (const node of nodes) {
          if (node.started_at) {
            const event = `event: node_started\ndata: ${JSON.stringify({
              node_id: node.node_id,
              node_type: node.node_type,
              timestamp: node.started_at,
            })}\n\n`;
            controller.enqueue(encoder.encode(event));
          }
          if (node.status === "completed" && node.finished_at) {
            const event = `event: node_completed\ndata: ${JSON.stringify({
              node_id: node.node_id,
              output: node.output_json ? JSON.parse(node.output_json) : null,
              timestamp: node.finished_at,
            })}\n\n`;
            controller.enqueue(encoder.encode(event));
          }
          if (node.status === "failed") {
            const errorInfo = node.error_json
              ? JSON.parse(node.error_json)
              : null;
            const errorMessage =
              typeof errorInfo === "string"
                ? errorInfo
                : errorInfo?.message ?? "Unknown error";
            const event = `event: node_failed\ndata: ${JSON.stringify({
              node_id: node.node_id,
              error: errorMessage,
              timestamp: node.finished_at,
            })}\n\n`;
            controller.enqueue(encoder.encode(event));
          }
        }

        // If workflow already completed, send final event and close
        if (
          ["completed", "failed", "cancelled"].includes(workflow.status)
        ) {
          let finalEvent: string;
          if (workflow.status === "completed") {
            finalEvent = `event: workflow_completed\ndata: ${JSON.stringify({
              status: "completed",
              output: workflow.output_json
                ? JSON.parse(workflow.output_json)
                : null,
              timestamp: workflow.finished_at,
            })}\n\n`;
          } else {
            const errorInfo = workflow.error_json
              ? JSON.parse(workflow.error_json)
              : null;
            const errorMessage =
              typeof errorInfo === "string"
                ? errorInfo
                : errorInfo?.message ?? "Unknown error";
            finalEvent = `event: workflow_failed\ndata: ${JSON.stringify({
              error: errorMessage,
              timestamp: workflow.finished_at,
            })}\n\n`;
          }
          controller.enqueue(encoder.encode(finalEvent));
          controller.close();
          return;
        }

        // Subscribe to live workflow events via a WebSocket-like wrapper
        const listeners = new Map<string, Array<(event: any) => void>>();
        const fakeWs = {
          send(data: string) {
            try {
              const parsed = JSON.parse(data);
              const event = `event: ${parsed.event}\ndata: ${data}\n\n`;
              controller.enqueue(encoder.encode(event));
            } catch {
              /* ignore */
            }
          },
          close() {
            const closeFns = listeners.get("close") ?? [];
            for (const fn of closeFns) { try { fn({}); } catch { /* ignore */ } }
            controller.close();
          },
          addEventListener(type: string, fn: (event: any) => void) {
            if (!listeners.has(type)) listeners.set(type, []);
            listeners.get(type)!.push(fn);
          },
          removeEventListener(type: string, fn: (event: any) => void) {
            const fns = listeners.get(type);
            if (fns) {
              const idx = fns.indexOf(fn);
              if (idx >= 0) fns.splice(idx, 1);
            }
          },
        } as unknown as WebSocket;

        sseManager.subscribeToWorkflow(workflowId, fakeWs);

        // Heartbeat every 30s to keep connection alive
        const heartbeatInterval = setInterval(() => {
          try {
            const hb = `event: heartbeat\ndata: ${JSON.stringify({
              timestamp: Date.now(),
            })}\n\n`;
            controller.enqueue(encoder.encode(hb));
          } catch {
            clearInterval(heartbeatInterval);
          }
        }, 30_000);
      },
    });

    return new Response(stream, {
      headers: {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
        "X-Accel-Buffering": "no",
      },
    });
  });
}
