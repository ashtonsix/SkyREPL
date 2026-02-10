// api/agent.ts - Agent-Facing Endpoints
// Handles HTTP endpoints that Python agents on compute instances call.
// Agent→control: HTTP POST. Control→agent: SSE (managed by SSEManager).

import { Elysia } from "elysia";
import {
  queryOne,
  execute,
  updateRun,
  createBlob,
  createObject,
  addObjectTag,
  addResourceToManifest,
  getInstance,
  type Allocation,
  type Run,
  type Manifest,
} from "../material/db";
import { activateAllocation, failAllocation } from "../workflow/state-transitions";
import { sseManager } from "./sse-protocol";
import type { AgentBridge } from "../workflow/nodes/start-run";
import type { StartRunMessage } from "@skyrepl/shared";
import { extractToken, verifyInstanceToken } from "./middleware/auth";

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
// Auth Helper
// =============================================================================

/**
 * Verify authentication for agent requests.
 * Returns an error object if auth fails, or null if auth passes.
 */
function requireAuth(
  body: { instance_id?: string | number },
  request: Request,
  query?: Record<string, string>
): { error: string; message: string } | null {
  const instanceId = body?.instance_id;
  if (!instanceId) {
    return { error: "unauthorized", message: "Missing instance_id" };
  }

  const token = extractToken(request, query);
  if (!token) {
    // No token provided: check if this instance requires auth
    // Instances without a token hash allow unauthenticated access (backward compat)
    const instance = getInstance(Number(instanceId));
    if (instance?.registration_token_hash) {
      return { error: "unauthorized", message: "Missing authentication token" };
    }
    return null; // no token required for this instance
  }

  if (!verifyInstanceToken(Number(instanceId), token)) {
    return { error: "unauthorized", message: "Invalid authentication token" };
  }

  return null; // auth passed
}

// =============================================================================
// Route Registration
// =============================================================================

export function registerAgentRoutes(app: Elysia<any>): void {
  // ─── Heartbeat ───────────────────────────────────────────────────────

  app.post("/v1/agent/heartbeat", async ({ body, request, set }) => {
    const hb = body as AgentHeartbeatRequest;

    // Auth check
    const authError = requireAuth(hb, request);
    if (authError) {
      set.status = 401;
      return authError;
    }

    // Update instance heartbeat timestamp
    execute("UPDATE instances SET last_heartbeat = ? WHERE id = ?", [
      Date.now(),
      hb.instance_id,
    ]);

    // Send heartbeat_ack via SSE (if SSE connection exists)
    const hasPendingCommands = sseManager.hasPendingCommands(
      String(hb.instance_id)
    );
    await sseManager.sendCommand(String(hb.instance_id), {
      type: "heartbeat_ack",
      control_plane_id: getControlPlaneId(),
    });

    return {
      ack: true,
      control_plane_id: getControlPlaneId(),
      server_time: Date.now(),
      commands_pending: hasPendingCommands,
    };
  });

  // ─── Logs ────────────────────────────────────────────────────────────

  app.post("/v1/agent/logs", async ({ body, request, set }) => {
    const log = body as AgentLogsRequest;

    // Auth check - need to get instance_id from run_id
    const run = queryOne<Run>("SELECT * FROM runs WHERE id = ?", [log.run_id]);
    if (!run) {
      set.status = 404;
      return { error: "not_found", message: "Run not found" };
    }
    const allocation = queryOne<Allocation>(
      "SELECT * FROM allocations WHERE run_id = ?",
      [log.run_id]
    );
    // If allocation exists, verify auth. If no allocation yet, skip auth (backward compat).
    if (allocation) {
      const authError = requireAuth({ instance_id: allocation.instance_id }, request);
      if (authError) {
        set.status = 401;
        return authError;
      }
    }

    // Append log data to the run's log stream (chunk buffering)
    const { appendLogData } = await import("../material/storage");

    // For Slice 1, we need a blob_id per run+stream. Get or create log object.
    // Look up existing log object for this run+stream
    const existingLogObj = queryOne<{ id: number; blob_id: number }>(
      `SELECT o.id, o.blob_id FROM objects o
       WHERE o.type = 'log'
       AND json_extract(o.metadata_json, '$.run_id') = ?
       AND json_extract(o.metadata_json, '$.stream') = ?
       ORDER BY o.id DESC LIMIT 1`,
      [log.run_id, log.stream]
    );

    let blobId: number;
    let objectId: number;
    if (existingLogObj) {
      blobId = existingLogObj.blob_id;
      objectId = existingLogObj.id;
    } else {
      // Create initial log object with manifest ownership wiring (S2.C3)
      const now = Date.now();
      const blob = createBlob({
        bucket: "logs",
        checksum: "",
        checksum_bytes: null,
        s3_key: null,
        s3_bucket: null,
        payload: null,
        size_bytes: 0,
        last_referenced_at: now,
      });

      // Resolve manifest_id from the run's current_manifest_id
      const manifestId = run.current_manifest_id ?? null;

      const obj = createObject({
        type: "log",
        blob_id: blob.id,
        provider: null,
        provider_object_id: null,
        metadata_json: JSON.stringify({
          run_id: log.run_id,
          stream: log.stream,
        }),
        expires_at: null,
        current_manifest_id: manifestId,
        accessed_at: null,
        updated_at: now,
      });
      blobId = blob.id;
      objectId = obj.id;

      // Wire into manifest ownership: object_tags + manifest_resources
      addObjectTag(obj.id, "run_id", String(log.run_id));
      addObjectTag(obj.id, "stream", log.stream);
      if (manifestId) {
        try {
          addResourceToManifest(manifestId, "object", String(obj.id));
        } catch {
          // Manifest may be sealed or missing — non-fatal for log ingestion
        }
      }
    }

    appendLogData(blobId, Buffer.from(log.data, "utf-8"), objectId);

    // Forward to CLI WebSocket subscribers
    sseManager.broadcastLog(String(log.run_id), {
      stream: log.stream === "sync_complete" ? "stdout" : log.stream,
      data: log.data,
      timestamp: log.timestamp,
      sequence: log.sequence,
    });

    // Handle sync_complete: triggers CLAIMED -> ACTIVE transition
    if (log.stream === "sync_complete") {
      await handleSyncComplete(log.run_id, log.sync_success ?? false);
    }

    return {
      ack: true,
      bytes_received: log.data.length,
    };
  });

  // ─── SSE Commands Stream ─────────────────────────────────────────────

  app.get("/v1/agent/commands", ({ query, request, set }) => {
    const instanceId = query.instance_id as string;
    const allocationId = query.allocation_id as string | undefined;

    // Auth check (SSE uses query param for token since EventSource can't set headers)
    const authError = requireAuth({ instance_id: instanceId }, request, query as Record<string, string>);
    if (authError) {
      set.status = 401;
      return authError;
    }

    // Returns a raw SSE Response, not JSON
    return sseManager.createCommandStream(instanceId, allocationId, request);
  });

  // ─── Status (deprecated but kept for compatibility) ──────────────────

  app.post("/v1/agent/status", async ({ body, request, set }) => {
    const status = body as AgentStatusRequest;

    // Auth check - need to get instance_id from run_id
    const allocation = queryOne<Allocation>(
      "SELECT * FROM allocations WHERE run_id = ?",
      [status.run_id]
    );
    // If allocation exists, verify auth. If no allocation yet, skip auth (backward compat).
    if (allocation) {
      const authError = requireAuth({ instance_id: allocation.instance_id }, request);
      if (authError) {
        set.status = 401;
        return authError;
      }
    }

    const workflowState = mapStatusToWorkflowState(status.status);

    const updates: Partial<Run> = {
      workflow_state: workflowState,
    };

    if (status.exit_code !== undefined) {
      updates.exit_code = status.exit_code;
    }

    if (["completed", "failed", "timeout"].includes(status.status)) {
      updates.finished_at = Date.now();
    }

    updateRun(status.run_id, updates);

    return { ack: true };
  });

  // ─── Spot Interrupt Start (stub for Slice 1) ─────────────────────────

  app.post("/v1/agent/spot-interrupt-start", async ({ body, request, set }) => {
    const req = body as SpotInterruptStartRequest;

    // Auth check - need to get instance_id from run_id
    const allocation = queryOne<Allocation>(
      "SELECT * FROM allocations WHERE run_id = ?",
      [req.run_id]
    );
    // If allocation exists, verify auth. If no allocation yet, skip auth (backward compat).
    if (allocation) {
      const authError = requireAuth({ instance_id: allocation.instance_id }, request);
      if (authError) {
        set.status = 401;
        return authError;
      }
    }

    return { ack: true };
  });

  // ─── Spot Interrupt Complete (stub for Slice 1) ───────────────────────

  app.post("/v1/agent/spot-interrupt-complete", async ({ body, request, set }) => {
    const req = body as SpotInterruptCompleteRequest;

    // Auth check - need to get instance_id from run_id
    const allocation = queryOne<Allocation>(
      "SELECT * FROM allocations WHERE run_id = ?",
      [req.run_id]
    );
    // If allocation exists, verify auth. If no allocation yet, skip auth (backward compat).
    if (allocation) {
      const authError = requireAuth({ instance_id: allocation.instance_id }, request);
      if (authError) {
        set.status = 401;
        return authError;
      }
    }

    return { ack: true };
  });

  // ─── Heartbeat Panic Start (stub for Slice 1) ────────────────────────

  app.post("/v1/agent/heartbeat-panic-start", async ({ body, request, set }) => {
    const req = body as HeartbeatPanicStartRequest;

    // Auth check - need to get instance_id from run_id
    const allocation = queryOne<Allocation>(
      "SELECT * FROM allocations WHERE run_id = ?",
      [req.run_id]
    );
    // If allocation exists, verify auth. If no allocation yet, skip auth (backward compat).
    if (allocation) {
      const authError = requireAuth({ instance_id: allocation.instance_id }, request);
      if (authError) {
        set.status = 401;
        return authError;
      }
    }

    return { ack: true };
  });

  // ─── Heartbeat Panic Complete (stub for Slice 1) ──────────────────────

  app.post("/v1/agent/heartbeat-panic-complete", async ({ body, request, set }) => {
    const req = body as HeartbeatPanicCompleteRequest;

    // Auth check - need to get instance_id from run_id
    const allocation = queryOne<Allocation>(
      "SELECT * FROM allocations WHERE run_id = ?",
      [req.run_id]
    );
    // If allocation exists, verify auth. If no allocation yet, skip auth (backward compat).
    if (allocation) {
      const authError = requireAuth({ instance_id: allocation.instance_id }, request);
      if (authError) {
        set.status = 401;
        return authError;
      }
    }

    return { ack: true };
  });

  // ─── Agent File Download ─────────────────────────────────────────────

  app.get("/v1/agent/download/:filename", async ({ params, set }) => {
    const allowedFiles = ["agent.py", "executor.py", "heartbeat.py", "logs.py", "sse.py"];
    const filename = params.filename;

    if (!allowedFiles.includes(filename)) {
      set.status = 404;
      return { error: { code: "NOT_FOUND", message: `Agent file not found: ${filename}` } };
    }

    // Resolve path to impl/agent/ directory (3 levels up from api/ to impl/)
    const agentDir = new URL("../../../agent", import.meta.url).pathname;
    const file = Bun.file(`${agentDir}/${filename}`);

    if (!await file.exists()) {
      set.status = 404;
      return { error: { code: "NOT_FOUND", message: `Agent file not found on disk: ${filename}` } };
    }

    return new Response(file, {
      headers: {
        "Content-Type": "text/x-python",
        "Content-Disposition": `attachment; filename=${filename}`,
      },
    });
  });

  // ─── Panic Diagnostics (stub for Slice 1) ────────────────────────────

  app.post("/v1/instances/:id/panic", async ({ params, body, request, set }) => {
    const panic = body as PanicDiagnosticsRequest;

    // Auth check
    const authError = requireAuth(panic, request);
    if (authError) {
      set.status = 401;
      return authError;
    }

    const now = Date.now();

    // Store panic record for post-mortem analysis
    execute(
      `INSERT INTO instance_panic_logs
       (instance_id, reason, last_state_json, diagnostics_json, error_logs_json, timestamp, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      [
        params.id,
        panic.reason,
        JSON.stringify(panic.last_state),
        JSON.stringify(panic.diagnostics),
        JSON.stringify(panic.error_logs),
        panic.timestamp,
        now,
      ]
    );

    return { ack: true, stored: true };
  });
}

// =============================================================================
// Handler Helpers
// =============================================================================

/**
 * Handle sync_complete log: trigger CLAIMED -> ACTIVE transition.
 * Called when the agent reports that file sync is done.
 */
export async function handleSyncComplete(
  runId: number,
  syncSuccess: boolean
): Promise<void> {
  if (!syncSuccess) {
    updateRun(runId, { workflow_state: "launch-run:sync-failed" });

    // Fail the CLAIMED allocation so waitForEvent detects the failure immediately
    const alloc = queryOne<Allocation>(
      "SELECT * FROM allocations WHERE run_id = ? AND status = 'CLAIMED'",
      [runId]
    );
    if (alloc) {
      failAllocation(alloc.id, "CLAIMED");
    }
    return;
  }

  // Find the CLAIMED allocation for this run and transition to ACTIVE
  const alloc = queryOne<Allocation>(
    "SELECT * FROM allocations WHERE run_id = ? AND status = 'CLAIMED'",
    [runId]
  );

  if (alloc) {
    activateAllocation(alloc.id);
  }
}

/**
 * Map agent status strings to run workflow states.
 */
export function mapStatusToWorkflowState(status: string): string {
  const mapping: Record<string, string> = {
    started: "launch-run:running",
    completed: "launch-run:complete",
    failed: "launch-run:failed",
    timeout: "launch-run:timeout",
  };
  return mapping[status] ?? `launch-run:${status}`;
}

/**
 * Get the unique identifier for this control plane instance.
 */
export function getControlPlaneId(): string {
  return process.env.CONTROL_PLANE_ID ?? "cp-default";
}

// =============================================================================
// Real Agent Bridge
// =============================================================================

/**
 * Creates a real AgentBridge implementation that communicates with agents
 * via SSE commands (replacing the mock bridge from Step 6).
 */
export function createRealAgentBridge(): AgentBridge {
  return {
    async sendStartRun(msg: Record<string, unknown>) {
      const instanceId = String(msg.instanceId);
      // Map FileManifestEntry[] (path, checksum, sizeBytes) to AgentFileEntry[] (path, checksum, url)
      const rawFiles = (msg.files as Array<{ path: string; checksum: string; sizeBytes?: number }>) || [];
      const agentFiles = rawFiles.map((f) => ({
        path: f.path,
        checksum: f.checksum,
        // For Slice 1: agent downloads from control plane. Real impl would use presigned S3 URLs.
        url: `/v1/blobs/by-checksum/${encodeURIComponent(f.checksum)}`,
      }));
      const command: StartRunMessage = {
        type: "start_run",
        command_id: sseManager.getNextCommandId(),
        run_id: msg.runId as number,
        allocation_id: msg.allocationId as number,
        command: msg.command as string,
        workdir: msg.workdir as string,
        files: agentFiles,
        artifacts: [],
      };
      await sseManager.sendCommand(instanceId, command);
    },

    async waitForEvent(
      instanceId: number,
      eventType: string,
      opts: { runId: number; timeout: number }
    ) {
      const deadline = Date.now() + opts.timeout;

      while (Date.now() < deadline) {
        if (eventType === "sync_complete") {
          // Check if allocation transitioned CLAIMED -> ACTIVE (means sync is done)
          const alloc = queryOne<Allocation>(
            "SELECT * FROM allocations WHERE run_id = ? AND (status = ? OR status = ?)",
            [opts.runId, "ACTIVE", "FAILED"]
          );
          if (alloc) {
            if (alloc.status === "FAILED") {
              throw Object.assign(
                new Error("File sync failed"),
                { code: "SYNC_FAILED", category: "internal" }
              );
            }
            return { success: true };
          }
        } else if (eventType === "run_complete") {
          // Check if run has finished
          const run = queryOne<Run>(
            "SELECT * FROM runs WHERE id = ? AND finished_at IS NOT NULL",
            [opts.runId]
          );
          if (run) {
            return {
              exitCode: run.exit_code,
              spotInterrupted: run.spot_interrupted === 1,
            };
          }
        }

        await new Promise((r) => setTimeout(r, 1000));
      }

      throw Object.assign(
        new Error(`Timeout waiting for ${eventType}`),
        { code: "OPERATION_TIMEOUT", category: "timeout" }
      );
    },
  };
}
