// api/agent.ts - Agent-Facing Endpoints
// Handles HTTP endpoints that Python agents on compute instances call.
// Agent→control: HTTP POST. Control→agent: SSE (managed by SSEManager).

import { Elysia } from "elysia";
import { updateRunRecord } from "../resource/run";
import { getInstanceRecord } from "../resource/instance";
import {
  queryOne,
  queryMany,
  execute,
  createBlob,
  createObject,
  addObjectTag,
  addResourceToManifest,
  type Allocation,
  type Run,
  type Manifest,
} from "../material/db";
import { createBlobWithDedup, appendLogData } from "../material/storage";
import { activateAllocation, failAllocation } from "../workflow/state-transitions";
import { stateEvents, STATE_EVENT } from "../workflow/events";
import { sseManager } from "./sse-protocol";
import { extendAllocationDebugHold } from "../resource/allocation";
import type { AgentBridge } from "../workflow/nodes/start-run";
import type { StartRunMessage } from "@skyrepl/contracts";
import { TIMING } from "@skyrepl/contracts";
import { extractToken, verifyInstanceToken } from "./middleware/auth";
import { getControlPlaneId } from "../config";

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
  pending_command_acks?: number[];
  dropped_logs_count?: number;
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
  instance_id: number;
  action: string;
  action_time?: string;
}

export interface SpotInterruptCompleteRequest {
  instance_id: number;
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
): { error: { code: string; message: string; category: string } } | null {
  const instanceId = body?.instance_id;
  if (!instanceId) {
    return { error: { code: "UNAUTHORIZED", message: "Missing instance_id", category: "auth" } };
  }

  const token = extractToken(request, query);
  if (!token) {
    // No token provided: check if this instance requires auth
    // Instances without a token hash allow unauthenticated access (backward compat)
    const instance = getInstanceRecord(Number(instanceId));
    if (instance?.registration_token_hash) {
      return { error: { code: "UNAUTHORIZED", message: "Missing authentication token", category: "auth" } };
    }
    return null; // no token required for this instance
  }

  if (!verifyInstanceToken(Number(instanceId), token)) {
    return { error: { code: "UNAUTHORIZED", message: "Invalid authentication token", category: "auth" } };
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

    // Reject heartbeats from terminated instances (#8.09: DMS bypass prevention)
    const instance = getInstanceRecord(Number(hb.instance_id));
    if (!instance || instance.workflow_state.startsWith("terminate:")) {
      set.status = 409;
      return {
        error: {
          code: "INSTANCE_TERMINATED",
          message: "Instance is terminated; heartbeat rejected",
          category: "conflict",
        },
      };
    }

    // Update instance heartbeat timestamp and Tailscale fields
    if (hb.tailscale_ip !== undefined || hb.tailscale_status !== undefined) {
      execute(
        "UPDATE instances SET last_heartbeat = ?, tailscale_ip = ?, tailscale_status = ? WHERE id = ?",
        [
          Date.now(),
          hb.tailscale_ip ?? null,
          hb.tailscale_status ?? null,
          hb.instance_id,
        ]
      );
    } else {
      execute("UPDATE instances SET last_heartbeat = ? WHERE id = ?", [
        Date.now(),
        hb.instance_id,
      ]);
    }

    // Process command acknowledgments from agent
    const pendingAcks = hb.pending_command_acks;
    if (pendingAcks && pendingAcks.length > 0) {
      sseManager.processCommandAcks(String(hb.instance_id), pendingAcks);
    }

    // Retry unacknowledged commands (30s timeout, max 3 retries)
    sseManager.retryUnackedCommands(String(hb.instance_id), TIMING.COMMAND_ACK_TIMEOUT_MS);

    // Process SSH activity: extend debug holds for allocations with active SSH sessions
    // Spec §5.4: extend only COMPLETE allocations that already have a debug_hold_until set.
    // ACTIVE allocations are still running and don't need a hold extension.
    if (hb.active_allocations && hb.active_allocations.length > 0) {
      for (const entry of hb.active_allocations) {
        if (!entry.has_ssh_sessions) continue;

        const alloc = queryOne<Allocation>(
          "SELECT * FROM allocations WHERE id = ?",
          [entry.allocation_id]
        );

        // Only extend existing holds on COMPLETE allocations (§5.4 line 390)
        if (!alloc || alloc.status !== "COMPLETE" || alloc.debug_hold_until === null) {
          continue;
        }

        const result = await extendAllocationDebugHold(entry.allocation_id, {
          extensionMs: TIMING.SSH_EXTENSION_MS,
          maxExtensionMs: TIMING.MAX_SSH_EXTENSION_MS,
        });

        if (!result.success) {
          console.log(
            `[heartbeat] SSH hold cap reached for allocation ${entry.allocation_id} — not extending`
          );
        }
      }
    }

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
      return { error: { code: "RUN_NOT_FOUND", message: "Run not found", category: "not_found" } };
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

  // ─── Artifact Upload (#AGENT-07) ──────────────────────────────────────

  app.post("/v1/agent/artifacts", async ({ body, request, set }) => {
    const req = body as {
      run_id: number;
      path: string;
      checksum: string;
      size_bytes: number;
      content_base64: string;
    };

    // Auth check via run → allocation → instance
    const run = queryOne<Run>("SELECT * FROM runs WHERE id = ?", [req.run_id]);
    if (!run) {
      set.status = 404;
      return { error: { code: "RUN_NOT_FOUND", message: "Run not found", category: "not_found" } };
    }
    const allocation = queryOne<Allocation>(
      "SELECT * FROM allocations WHERE run_id = ?",
      [req.run_id]
    );
    if (allocation) {
      const authError = requireAuth({ instance_id: allocation.instance_id }, request);
      if (authError) {
        set.status = 401;
        return authError;
      }
    }

    const manifestId = run.current_manifest_id ?? null;
    const now = Date.now();

    try {
      // Decode base64 content
      const data = Buffer.from(req.content_base64, "base64");

      // Create or deduplicate blob
      const blobId = createBlobWithDedup("artifacts", req.checksum, data);

      // Create artifact object
      const obj = createObject({
        type: "artifact",
        blob_id: blobId,
        provider: null,
        provider_object_id: null,
        metadata_json: JSON.stringify({
          run_id: req.run_id,
          path: req.path,
          checksum: req.checksum,
          size_bytes: req.size_bytes,
        }),
        expires_at: null,
        current_manifest_id: manifestId,
        accessed_at: null,
        updated_at: now,
      });

      // Tag for lookups
      addObjectTag(obj.id, "run_id", String(req.run_id));
      addObjectTag(obj.id, "artifact_path", req.path);

      // Wire into manifest
      if (manifestId) {
        try {
          addResourceToManifest(manifestId, "object", String(obj.id));
        } catch {
          console.warn("[agent] Manifest sealed during artifact upload", { objectId: obj.id, manifestId });
        }
      }

      return { ack: true, artifacts_stored: 1 };
    } catch (error) {
      console.warn("[agent] Failed to store artifact", {
        path: req.path,
        error,
      });
      set.status = 500;
      return { error: { code: "INTERNAL_ERROR", message: "Failed to store artifact", category: "internal" } };
    }
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

    // Auth check - require allocation to exist (#12.04: no auth bypass for unknown runs)
    const allocation = queryOne<Allocation>(
      "SELECT * FROM allocations WHERE run_id = ?",
      [status.run_id]
    );
    if (!allocation) {
      set.status = 404;
      return {
        error: {
          code: "ALLOCATION_NOT_FOUND",
          message: "No allocation found for this run",
          category: "not_found",
        },
      };
    }
    const authError = requireAuth({ instance_id: allocation.instance_id }, request);
    if (authError) {
      set.status = 401;
      return authError;
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

    updateRunRecord(status.run_id, updates);

    // Emit run:finished event for EventEmitter-based waiters
    if (["completed", "failed", "timeout"].includes(status.status)) {
      stateEvents.emit(STATE_EVENT.RUN_FINISHED, {
        runId: status.run_id,
        exitCode: status.exit_code ?? null,
        spotInterrupted: false,
      });
    }

    // Close CLI WebSocket log subscribers with terminal status
    if (["completed", "failed", "timeout"].includes(status.status)) {
      sseManager.closeRunSubscribers(String(status.run_id), {
        type: "status",
        status: status.status as "completed" | "failed" | "timeout",
        exit_code: status.exit_code,
      });
    }

    return { ack: true };
  });

  // ─── Spot Interrupt Start ─────────────────────────────────────────────
  //
  // POST /v1/agent/spot-interrupt-start
  //
  // Called by the agent's SpotMonitor when a spot interruption notice is
  // detected. Marks all CLAIMED/ACTIVE allocations on the instance as
  // spot_interrupted so the workflow engine can surface this to callers.

  app.post("/v1/agent/spot-interrupt-start", async ({ body, request, set }) => {
    const b = (body ?? {}) as Record<string, unknown>;
    const instanceId = Number(b.instance_id);
    const action = String(b.action ?? "unknown");
    const actionTime = b.action_time != null ? String(b.action_time) : null;

    if (!instanceId) {
      set.status = 400;
      return { error: { code: "INVALID_INPUT", message: "instance_id is required", category: "validation" } };
    }

    // Auth: verify the request comes from the named instance
    const authError = requireAuth({ instance_id: instanceId }, request);
    if (authError) {
      set.status = 401;
      return authError;
    }

    // Find all CLAIMED or ACTIVE allocations on this instance that carry a run
    const activeAllocs = queryMany<{ run_id: number }>(
      "SELECT run_id FROM allocations WHERE instance_id = ? AND status IN ('CLAIMED', 'ACTIVE') AND run_id IS NOT NULL",
      [instanceId]
    );

    for (const alloc of activeAllocs) {
      // Idempotent: only set flag once
      execute(
        "UPDATE runs SET spot_interrupted = 1, workflow_state = 'launch-run:spot-interrupted' WHERE id = ? AND spot_interrupted = 0",
        [alloc.run_id]
      );
    }

    console.log(`[agent] Spot interrupt start: instance=${instanceId} action=${action} action_time=${actionTime} affected_runs=${activeAllocs.length}`);

    return { ack: true };
  });

  // ─── Spot Interrupt Complete ──────────────────────────────────────────
  //
  // POST /v1/agent/spot-interrupt-complete
  //
  // Called by the agent's SpotMonitor after it has cancelled the active run
  // and is about to let the VM be reclaimed. This is a best-effort signal;
  // the instance will vanish shortly after so we just acknowledge it.

  app.post("/v1/agent/spot-interrupt-complete", async ({ body, request, set }) => {
    const b = (body ?? {}) as Record<string, unknown>;
    const instanceId = Number(b.instance_id);

    if (!instanceId) {
      set.status = 400;
      return { error: { code: "INVALID_INPUT", message: "instance_id is required", category: "validation" } };
    }

    // Auth: verify the request comes from the named instance
    const authError = requireAuth({ instance_id: instanceId }, request);
    if (authError) {
      set.status = 401;
      return authError;
    }

    console.log(`[agent] Spot interrupt complete: instance=${instanceId}`);

    return { ack: true };
  });

  // ─── Heartbeat Panic Start (stub for Slice 1) ────────────────────────

  app.post("/v1/agent/heartbeat-panic-start", async ({ body, request, set }) => {
    const req = body as HeartbeatPanicStartRequest;

    // NOT YET IMPLEMENTED — deferred to future slice
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

    // NOT YET IMPLEMENTED — deferred to future slice
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
    const allowedFiles = ["agent.py", "executor.py", "heartbeat.py", "logs.py", "sse.py", "http_client.py", "artifacts.py", "tailscale.py", "spot.py"];
    const filename = params.filename;

    if (!allowedFiles.includes(filename)) {
      set.status = 404;
      return { error: { code: "RESOURCE_NOT_FOUND", message: `Agent file not found: ${filename}`, category: "not_found" } };
    }

    // Resolve path to impl/agent/ directory (3 levels up from api/ to impl/)
    const agentDir = new URL("../../../agent", import.meta.url).pathname;
    const file = Bun.file(`${agentDir}/${filename}`);

    if (!await file.exists()) {
      set.status = 404;
      return { error: { code: "RESOURCE_NOT_FOUND", message: `Agent file not found on disk: ${filename}`, category: "not_found" } };
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

    // NOT YET IMPLEMENTED — deferred to future slice
    // Auth check
    const authError = requireAuth(panic, request);
    if (authError) {
      set.status = 401;
      return authError;
    }

    const now = Date.now();

    // Fetch instance to get tenant_id for the panic log record
    const instance = getInstanceRecord(Number(params.id));
    if (!instance) {
      set.status = 404;
      return { error: { code: "RESOURCE_NOT_FOUND", message: "Instance not found", category: "not_found" } };
    }

    // Store panic record for post-mortem analysis
    execute(
      `INSERT INTO instance_panic_logs
       (instance_id, tenant_id, reason, last_state_json, diagnostics_json, error_logs_json, timestamp, created_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        params.id,
        instance.tenant_id,
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
    updateRunRecord(runId, { workflow_state: "launch-run:sync-failed" });

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
      // First, check current state (event may have already fired before we started listening)
      if (eventType === "sync_complete") {
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

      // Wait for event with timeout
      return new Promise((resolve, reject) => {
        const timeoutId = setTimeout(() => {
          cleanup();
          reject(Object.assign(
            new Error(`Timeout waiting for ${eventType}`),
            { code: "OPERATION_TIMEOUT", category: "timeout" }
          ));
        }, Math.max(0, opts.timeout));

        const eventName = eventType === "sync_complete"
          ? STATE_EVENT.ALLOCATION_STATUS_CHANGED
          : STATE_EVENT.RUN_FINISHED;

        const handler = (data: any) => {
          if (eventType === "sync_complete") {
            if (data.runId !== opts.runId) return; // Not our run
            if (data.toStatus === "ACTIVE") {
              cleanup();
              resolve({ success: true });
            } else if (data.toStatus === "FAILED") {
              cleanup();
              reject(Object.assign(
                new Error("File sync failed"),
                { code: "SYNC_FAILED", category: "internal" }
              ));
            }
          } else if (eventType === "run_complete") {
            if (data.runId !== opts.runId) return; // Not our run
            cleanup();
            resolve({
              exitCode: data.exitCode,
              spotInterrupted: data.spotInterrupted,
            });
          }
        };

        function cleanup() {
          clearTimeout(timeoutId);
          stateEvents.off(eventName, handler);
        }

        stateEvents.on(eventName, handler);
      });
    },
  };
}
