// api/sse-protocol.ts - SSE/WebSocket Stream Management

import { Elysia } from "elysia";
import {
  queryMany,
  getWorkflow,
  type Workflow,
  type WorkflowNode,
} from "../material/db"; // raw-db: boutique queries (workflow_nodes ORDER BY started_at), see WL-057
import type { ControlToAgentMessage } from "@skyrepl/contracts";
import { getControlPlaneId } from "../config";
import { workflowEvents } from "../workflow/events";
import { setCommandBus } from "../events/command-bus";

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
  /** Monotonic sequence ID assigned by SSEManager for gap detection / reconnect replay */
  seq?: number;
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
  | { event: "node_completed"; data: { node_id: string; node_type: string; output: unknown; timestamp: number } }
  | { event: "node_failed"; data: { node_id: string; node_type: string; error: string; timestamp: number } }
  | { event: "pattern_applied"; data: { pattern: string; triggered_by: string; timestamp: number } }
  | { event: "progress"; data: { completed: number; total: number; percentage: number } }
  | { event: "subworkflow_started"; data: { subworkflow_id: string; type: string } }
  | { event: "subworkflow_completed"; data: { subworkflow_id: string; status: string } }
  | { event: "workflow_completed"; data: { status: string; output: unknown; timestamp: number } }
  | { event: "workflow_failed"; data: { error: string; node_id?: string; timestamp: number } }
  | { event: "heartbeat"; data: { timestamp: number } }
  // Budget events (WL-061-3B §6)
  | { event: "budget_warning"; data: { tenant_id: number; remaining_cents: number; remaining_pct: number; total_cents: number; budget_cents: number; timestamp: number } }
  | { event: "budget_extension_offer"; data: { tenant_id: number; suggested_cents: number; extend_url: string; timestamp: number } };

// =============================================================================
// SSE Manager
// =============================================================================

/**
 * Manages SSE connections from agents and WebSocket connections from CLI clients.
 *
 * Agent SSE: Long-lived, agent-initiated, unidirectional (control -> agent).
 * CLI WebSocket: Bidirectional for log streaming and workflow progress.
 */
/** Maximum number of log messages to retain per run for reconnect replay */
const LOG_REPLAY_BUFFER_SIZE = 500;

// =============================================================================
// Ring Buffer
// =============================================================================

class RingBuffer<T> {
  private buffer: (T | undefined)[];
  private writeIdx = 0;
  private count = 0;
  constructor(private capacity: number) { this.buffer = new Array(capacity); }
  push(item: T) {
    this.buffer[this.writeIdx] = item;
    this.writeIdx = (this.writeIdx + 1) % this.capacity;
    if (this.count < this.capacity) this.count++;
  }
  toArray(): T[] {
    if (this.count < this.capacity) return this.buffer.slice(0, this.count) as T[];
    // Wrap: oldest is at writeIdx, newest is at writeIdx-1
    return [...this.buffer.slice(this.writeIdx), ...this.buffer.slice(0, this.writeIdx)] as T[];
  }
}

/** Maximum number of pending commands to retain per instance (FIFO eviction if exceeded) */
const MAX_PENDING_COMMANDS = 50;

/** Sustained backpressure duration before disconnecting a slow agent (5 min) */
const SSE_BACKPRESSURE_DISCONNECT_MS = 5 * 60 * 1000;

interface CommandDeliveryState {
  command: ControlToAgentMessage;
  sentAt: number;
  retryCount: number;
}

export class SSEManager {
  /** Active agent SSE connections: instanceId -> SSE controller */
  private agentStreams = new Map<string, ReadableStreamDefaultController<Uint8Array>>();

  /** Pending commands waiting for agent connection: instanceId -> commands[] */
  private pendingCommands = new Map<string, ControlToAgentMessage[]>();

  /** Active CLI log subscribers: runId -> Set<WebSocket> */
  private logSubscribers = new Map<string, Set<WebSocket>>();

  /** Active CLI workflow subscribers: workflowId -> Set<WebSocket> */
  private workflowSubscribers = new Map<string, Set<WebSocket>>();

  /** Command ID counter for acknowledgment protocol.
   *  Seeded from Date.now() to avoid collisions after control plane restarts
   *  (agents dedup by command_id, so restarting from 1 would cause them to
   *  silently skip commands whose IDs were already processed). */
  private nextCommandId = Date.now();

  /** Unacknowledged commands: instanceId -> Map<commandId, CommandDeliveryState> */
  private unackedCommands = new Map<string, Map<number, CommandDeliveryState>>();

  /** Backpressure tracking: instanceId -> timestamp when backpressure first detected */
  private backpressureSince = new Map<string, number>();

  /** Per-run monotonic sequence counter for log messages */
  private logSequenceCounters = new Map<string, number>();

  /** Per-run ring buffer of recent log messages for reconnect replay */
  private logReplayBuffers = new Map<string, RingBuffer<LogBroadcast>>();

  /** Get the next log sequence number for a run */
  getNextLogSequence(runId: string): number {
    const current = this.logSequenceCounters.get(runId) ?? 0;
    const next = current + 1;
    this.logSequenceCounters.set(runId, next);
    return next;
  }

  /** Replay buffered log messages from a given sequence number (exclusive) */
  replayLogsFrom(runId: string, afterSeq: number, ws: WebSocket): number {
    const ring = this.logReplayBuffers.get(runId);
    const buffer = ring ? ring.toArray() : [];
    let replayed = 0;
    for (const msg of buffer) {
      if (msg.seq !== undefined && msg.seq > afterSeq) {
        try {
          ws.send(JSON.stringify(msg));
          replayed++;
        } catch {
          break;
        }
      }
    }
    return replayed;
  }

  /** Clean up sequence tracking for a completed run */
  cleanupRunSequences(runId: string): void {
    this.logSequenceCounters.delete(runId);
    this.logReplayBuffers.delete(runId);
  }

  /**
   * Clean up all log-related state for a completed run.
   * Should be called from workflow finalization (e.g., engine.ts finalizeRun).
   * Removes entries from logSequenceCounters, logReplayBuffers, and logSubscribers.
   */
  cleanupRun(runId: string): void {
    this.logSequenceCounters.delete(runId);
    this.logReplayBuffers.delete(runId);
    this.logSubscribers.delete(runId);
  }

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

  /**
   * Enqueue data to an agent's SSE stream with backpressure handling.
   * Commands (isCommand=true) are never dropped.
   * Non-commands (heartbeat_ack) are dropped when the consumer is slow.
   * Connection is closed after sustained backpressure (5 min).
   */
  enqueueToAgent(
    instanceId: string,
    controller: ReadableStreamDefaultController<Uint8Array>,
    data: Uint8Array,
    isCommand: boolean
  ): boolean {
    const desiredSize = controller.desiredSize;
    const backpressured = desiredSize !== null && desiredSize <= 0;

    if (backpressured) {
      if (!this.backpressureSince.has(instanceId)) {
        this.backpressureSince.set(instanceId, Date.now());
        console.warn("[sse] Backpressure detected on agent SSE", { instanceId, desiredSize });
      }

      // Drop non-command messages (heartbeat_ack) under backpressure
      if (!isCommand) {
        return false;
      }

      // Disconnect after sustained overload
      const since = this.backpressureSince.get(instanceId)!;
      if (Date.now() - since >= SSE_BACKPRESSURE_DISCONNECT_MS) {
        console.warn("[sse] Disconnecting slow consumer after sustained backpressure", { instanceId });
        try { controller.close(); } catch { /* already closed */ }
        this.agentStreams.delete(instanceId);
        this.backpressureSince.delete(instanceId);
        return false;
      }
    } else if (this.backpressureSince.has(instanceId)) {
      console.info("[sse] Backpressure cleared on agent SSE", { instanceId });
      this.backpressureSince.delete(instanceId);
    }

    try {
      controller.enqueue(data);
      return true;
    } catch {
      this.agentStreams.delete(instanceId);
      this.backpressureSince.delete(instanceId);
      return false;
    }
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
      // FIFO eviction if pending commands exceed limit (SM-02)
      while (pending.length > MAX_PENDING_COMMANDS) {
        pending.shift();
      }
      this.pendingCommands.set(instanceId, pending);
      console.log("[sse] Agent not connected, command queued", {
        instanceId,
        type: command.type,
      });
      return false;
    }

    const encoder = new TextEncoder();
    const data = `event: ${command.type}\ndata: ${JSON.stringify(command)}\n\n`;
    const isCommand = command.type !== "heartbeat_ack";

    if (!this.enqueueToAgent(instanceId, controller, encoder.encode(data), isCommand)) {
      if (!isCommand) {
        // Heartbeat_ack dropped due to backpressure — not an error
        return true;
      }
      console.error("[sse] Failed to send SSE command", { instanceId, type: command.type });
      return false;
    }

    // After successful send, track for ack (only for commands with command_id)
    const commandId = (command as any).command_id;
    if (commandId != null) {
      if (!this.unackedCommands.has(instanceId)) {
        this.unackedCommands.set(instanceId, new Map());
      }
      this.unackedCommands.get(instanceId)!.set(commandId, {
        command,
        sentAt: Date.now(),
        retryCount: 0,
      });
    }

    return true;
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

  /** Process acknowledged command IDs from agent heartbeat */
  processCommandAcks(instanceId: string, ackedIds: number[]): void {
    const unacked = this.unackedCommands.get(instanceId);
    if (!unacked) return;
    for (const id of ackedIds) {
      unacked.delete(id);
    }
    if (unacked.size === 0) {
      this.unackedCommands.delete(instanceId);
    }
  }

  /** Retry unacknowledged commands that have timed out. Returns number of retries attempted. */
  retryUnackedCommands(instanceId: string, timeoutMs: number, maxRetries: number = 3): number {
    const unacked = this.unackedCommands.get(instanceId);
    if (!unacked) return 0;

    const now = Date.now();
    let retries = 0;
    const toRemove: number[] = [];

    for (const [commandId, state] of unacked) {
      if (now - state.sentAt < timeoutMs) continue;

      if (state.retryCount >= maxRetries) {
        console.warn("[sse] Command ack timeout, max retries exhausted", { instanceId, commandId, retryCount: state.retryCount });
        toRemove.push(commandId);
        continue;
      }

      // Retry: re-send the command (commands are always high-priority)
      const controller = this.agentStreams.get(instanceId);
      if (controller) {
        const encoder = new TextEncoder();
        const data = `event: ${state.command.type}\ndata: ${JSON.stringify(state.command)}\n\n`;
        if (this.enqueueToAgent(instanceId, controller, encoder.encode(data), true)) {
          state.sentAt = now;
          state.retryCount++;
          retries++;
          console.info("[sse] Retrying unacked command", { instanceId, commandId, retryCount: state.retryCount });
        } else {
          toRemove.push(commandId);
        }
      } else {
        // Agent disconnected — move command to pending queue for reconnect
        const pending = this.pendingCommands.get(instanceId) ?? [];
        pending.push(state.command);
        while (pending.length > MAX_PENDING_COMMANDS) {
          pending.shift();
        }
        this.pendingCommands.set(instanceId, pending);
        toRemove.push(commandId);
      }
    }

    for (const id of toRemove) {
      unacked.delete(id);
    }
    if (unacked.size === 0) {
      this.unackedCommands.delete(instanceId);
    }

    return retries;
  }

  /** Clean up command tracking when agent disconnects */
  cleanupInstance(instanceId: string): void {
    this.unackedCommands.delete(instanceId);
    this.backpressureSince.delete(instanceId);
  }

  // --- CLI WebSocket: Log Streaming -----------------------------------------

  /** Register WebSocket for run log streaming */
  subscribeToLogs(runId: string, ws: WebSocket): void {
    if (!this.logSubscribers.has(runId)) {
      this.logSubscribers.set(runId, new Set());
    }
    this.logSubscribers.get(runId)!.add(ws);
    // Cleanup is handled by unsubscribeFromLogs() called from the WS close handler
  }

  /** Remove WebSocket from run log subscribers */
  unsubscribeFromLogs(runId: string, ws: WebSocket): void {
    this.logSubscribers.get(runId)?.delete(ws);
    if (this.logSubscribers.get(runId)?.size === 0) {
      this.logSubscribers.delete(runId);
    }
  }

  /** Broadcast log message to all CLI subscribers for a run */
  broadcastLog(runId: string, message: LogBroadcast): void {
    // Assign monotonic sequence ID for gap detection and reconnect replay
    const seq = this.getNextLogSequence(runId);
    message.seq = seq;

    // Store in replay buffer (fixed-size ring buffer — FIFO eviction, no allocation on overflow)
    let ring = this.logReplayBuffers.get(runId);
    if (!ring) {
      ring = new RingBuffer<LogBroadcast>(LOG_REPLAY_BUFFER_SIZE);
      this.logReplayBuffers.set(runId, ring);
    }
    ring.push(message);

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
    // Cleanup is handled by unsubscribeFromWorkflow() called from the WS close handler
  }

  /** Remove WebSocket from workflow progress subscribers */
  unsubscribeFromWorkflow(workflowId: string, ws: WebSocket): void {
    this.workflowSubscribers.get(workflowId)?.delete(ws);
    if (this.workflowSubscribers.get(workflowId)?.size === 0) {
      this.workflowSubscribers.delete(workflowId);
    }
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
    // Clean up sequence tracking after a delay to allow late reconnections
    setTimeout(() => this.cleanupRunSequences(runId), 60_000);
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
setCommandBus(sseManager);

// =============================================================================
// WebSocket Route Registration
// =============================================================================

export function registerWebSocketRoutes(app: Elysia<any>): void {
  // WS /v1/runs/:id/logs - Live log streaming for CLI
  app.ws("/v1/runs/:id/logs", {
    open(ws) {
      const runId = (ws.data as any).params.id;
      const query = (ws.data as any).query ?? {};
      const lastSeq = query.last_seq ? parseInt(query.last_seq, 10) : 0;
      console.info("[sse] CLI log stream opened", { runId, lastSeq });

      // Subscribe to live updates
      sseManager.subscribeToLogs(runId, ws.raw as unknown as WebSocket);

      // Replay buffered messages from after lastSeq for reconnect gap fill
      if (lastSeq > 0) {
        const replayed = sseManager.replayLogsFrom(runId, lastSeq, ws.raw as unknown as WebSocket);
        if (replayed > 0) {
          console.info("[sse] Replayed log messages on reconnect", { runId, lastSeq, replayed });
        }
      }
    },
    message(_ws, _message) {
      // No client -> server messages defined yet
    },
    close(ws) {
      const runId = (ws.data as any).params.id;
      console.debug("[sse] CLI log stream closed", { runId });
      sseManager.unsubscribeFromLogs(runId, ws.raw as unknown as WebSocket);
    },
  });

  // WS /v1/workflows/:id/progress - Live workflow progress for CLI
  app.ws("/v1/workflows/:id/progress", {
    open(ws) {
      const workflowId = (ws.data as any).params.id;
      console.info("[sse] CLI workflow progress stream opened", { workflowId });

      // Subscribe to live updates (no historical replay yet)
      sseManager.subscribeToWorkflow(
        workflowId,
        ws.raw as unknown as WebSocket
      );
    },
    message(_ws, _message) {
      // No client -> server messages defined yet
    },
    close(ws) {
      const workflowId = (ws.data as any).params.id;
      console.debug("[sse] CLI workflow stream closed", { workflowId });
      sseManager.unsubscribeFromWorkflow(workflowId, ws.raw as unknown as WebSocket);
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
    const workflow = getWorkflow(Number(workflowId));
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

    let heartbeatInterval: ReturnType<typeof setInterval> | null = null;
    let fakeWs: WebSocket | null = null; // SM-26: Moved to outer scope for cancel() access
    let engineCleanup: (() => void) | null = null; // Engine listener cleanup ref for cancel()

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
              node_type: node.node_type,
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
              node_type: node.node_type,
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
        fakeWs = {
          send(data: string) {
            try {
              const parsed = JSON.parse(data);
              // Send inner data only (not the outer {event, data} wrapper)
              // to match the catch-up event format
              const innerData = parsed.data ? JSON.stringify(parsed.data) : data;
              const event = `event: ${parsed.event}\ndata: ${innerData}\n\n`;
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

        // Bridge engine events → SSE: subscribe to the engine emitter and
        // forward only events for this specific workflow to its SSE clients.
        const wfIdNum = parseInt(workflowId, 10);

        const onNodeStarted = (payload: import("../workflow/events").NodeStartedPayload) => {
          if (payload.workflowId !== wfIdNum) return;
          sseManager.broadcastWorkflowEvent(workflowId, {
            event: "node_started",
            data: { node_id: payload.nodeId, node_type: payload.nodeType, timestamp: payload.timestamp },
          });
        };
        const onNodeCompleted = (payload: import("../workflow/events").NodeCompletedPayload) => {
          if (payload.workflowId !== wfIdNum) return;
          sseManager.broadcastWorkflowEvent(workflowId, {
            event: "node_completed",
            data: { node_id: payload.nodeId, node_type: payload.nodeType, output: payload.output, timestamp: payload.timestamp },
          });
        };
        const onNodeFailed = (payload: import("../workflow/events").NodeFailedPayload) => {
          if (payload.workflowId !== wfIdNum) return;
          sseManager.broadcastWorkflowEvent(workflowId, {
            event: "node_failed",
            data: { node_id: payload.nodeId, node_type: payload.nodeType, error: payload.error, timestamp: payload.timestamp },
          });
        };
        const onWorkflowCompleted = (payload: import("../workflow/events").WorkflowCompletedPayload) => {
          if (payload.workflowId !== wfIdNum) return;
          sseManager.closeWorkflowSubscribers(workflowId, {
            event: "workflow_completed",
            data: { status: "completed", output: payload.output, timestamp: payload.timestamp },
          });
          cleanup();
        };
        const onWorkflowFailed = (payload: import("../workflow/events").WorkflowFailedPayload) => {
          if (payload.workflowId !== wfIdNum) return;
          sseManager.closeWorkflowSubscribers(workflowId, {
            event: "workflow_failed",
            data: { error: payload.error, node_id: payload.nodeId, timestamp: payload.timestamp },
          });
          cleanup();
        };

        const cleanup = () => {
          workflowEvents.off("node_started", onNodeStarted);
          workflowEvents.off("node_completed", onNodeCompleted);
          workflowEvents.off("node_failed", onNodeFailed);
          workflowEvents.off("workflow_completed", onWorkflowCompleted);
          workflowEvents.off("workflow_failed", onWorkflowFailed);
          if (heartbeatInterval) {
            clearInterval(heartbeatInterval);
            heartbeatInterval = null;
          }
          engineCleanup = null;
        };

        // Expose cleanup to outer cancel() scope
        engineCleanup = cleanup;

        workflowEvents.on("node_started", onNodeStarted);
        workflowEvents.on("node_completed", onNodeCompleted);
        workflowEvents.on("node_failed", onNodeFailed);
        workflowEvents.on("workflow_completed", onWorkflowCompleted);
        workflowEvents.on("workflow_failed", onWorkflowFailed);

        // Heartbeat every 30s to keep connection alive
        heartbeatInterval = setInterval(() => {
          try {
            const hb = `event: heartbeat\ndata: ${JSON.stringify({
              timestamp: Date.now(),
            })}\n\n`;
            controller.enqueue(encoder.encode(hb));
          } catch {
            clearInterval(heartbeatInterval!);
            heartbeatInterval = null;
          }
        }, 30_000);
      },

      cancel() {
        // Clean up engine event listeners and heartbeat interval
        if (engineCleanup) {
          engineCleanup();
        } else if (heartbeatInterval) {
          // Fallback if engineCleanup wasn't set (e.g., early-return for completed workflow)
          clearInterval(heartbeatInterval);
          heartbeatInterval = null;
        }
        // SM-26: Pass fakeWs instead of {} to properly unsubscribe
        if (fakeWs) {
          sseManager.unsubscribeFromWorkflow(workflowId, fakeWs);
        }
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
