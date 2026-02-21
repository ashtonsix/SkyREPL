// api/routes/operations.ts — registerOperationRoutes(): launch-run, terminate, cancel, workflow status, + delegated routes

import { Elysia, t } from "elysia";
import { Type } from "@sinclair/typebox";
import { getInstanceRecord } from "../../resource/instance";
import {
  getWorkflow,
  getWorkflowNodes,
  getAllocation,
  execute,
} from "../../material/db";
import { launchRun } from "../../intent/launch-run";
import type { LaunchRunInput } from "../../intent/launch-run";
import { terminateInstance } from "../../intent/terminate-instance";
import {
  SkyREPLError,
  httpStatusForError,
  errorToApiError,
  LaunchRunRequestSchema,
} from "@skyrepl/contracts";
import {
  checkIdempotencyKey,
  storeIdempotencyResponse,
} from "../middleware/idempotency";
import { getAuthContext, checkPermission } from "../middleware/auth";
import { PROVIDER_CAPABILITIES } from "../../provider/types";
import { isProviderRegistered } from "../../provider/registry";
import {
  getTailscaleClient,
  TailscaleConfigError,
} from "../../provider/feature/tailscale-api";
import { sseManager } from "../sse-protocol";
import { checkBudget, registerAdminRoutes } from "./admin";
import { registerBlobRoutes } from "./blobs";

// ─── Request Body Schemas ─────────────────────────────────────────────────────

const TerminateInstanceRequestSchema = Type.Object({
  instanceId: Type.Optional(Type.Number({ description: "Instance ID (numeric)" })),
  instance_id: Type.Optional(Type.Number({ description: "Instance ID (snake_case alias)" })),
});

const CancelWorkflowRequestSchema = Type.Object({
  reason: Type.Optional(Type.String({ default: "user_requested" })),
});

const ExtendAllocationRequestSchema = Type.Object({
  duration_ms: Type.Number({ exclusiveMinimum: 0, description: "Extension duration in milliseconds" }),
});

/** Elysia params schema: coerces URL :id param from string to number */
const IdParams = t.Object({ id: t.Numeric() });

export function registerOperationRoutes(app: Elysia<any>): void {
  // ─── Launch Run ──────────────────────────────────────────────────────

  app.post("/v1/workflows/launch-run", async ({ body, set, request }) => {
    const auth = getAuthContext(request);
    if (auth && !checkPermission(auth, "launch_run")) {
      set.status = 403;
      return { error: { code: "FORBIDDEN", message: "Insufficient permissions", category: "auth" } };
    }

    // Budget preflight (TENANT-03): reject if tenant or user budget exceeded
    if (auth) {
      const budgetCheck = checkBudget(auth.tenantId, auth.userId);
      if (budgetCheck) {
        set.status = 429;
        return { error: { code: "BUDGET_EXCEEDED", message: budgetCheck, category: "rate_limit" } };
      }
    }

    // Validate provider name (static capabilities OR runtime-registered, e.g. "mock" in tests)
    const providerName = body.provider ?? "orbstack";
    if (!(providerName in PROVIDER_CAPABILITIES) && !isProviderRegistered(providerName)) {
      set.status = 400;
      return {
        error: {
          code: "INVALID_INPUT",
          message: `Unknown provider "${providerName}". Valid providers: ${Object.keys(PROVIDER_CAPABILITIES).join(", ")}`,
          category: "validation",
        },
      };
    }

    // HTTP-level idempotency via Idempotency-Key header (§9.7).
    // Body-level idempotency_key is handled separately by the workflow engine's
    // deduplication logic (which is more lenient — returns existing workflow
    // regardless of param differences).
    const httpIdempotencyKey = request.headers.get("idempotency-key") ?? undefined;
    let paramsHash: string | undefined;

    if (httpIdempotencyKey) {
      const bodyText = JSON.stringify(body);
      const check = checkIdempotencyKey(httpIdempotencyKey, "launch-run", bodyText);
      if (check.hit) {
        set.status = check.status;
        return check.body;
      }
      paramsHash = check.paramsHash;
    }

    // Build LaunchRunInput (runId will be assigned by launchRun after creating the Run record)
    const input: Omit<LaunchRunInput, "runId"> = {
      command: body.command,
      spec: body.spec,
      workdir: body.workdir ?? "/workspace",
      provider: providerName,
      region: body.region || undefined,
      instanceType: (body as any).instance_type ?? undefined,
      env: body.env ?? undefined,
      maxDurationMs: body.max_duration_ms,
      holdDurationMs: body.hold_duration_ms ?? 300_000,
      createSnapshot: body.create_snapshot ?? false,
      initChecksum: body.init_checksum ?? undefined,
      files: (body.files ?? []).map((f: { path: string; checksum: string; size_bytes?: number }) => ({
        path: f.path,
        checksum: f.checksum,
        sizeBytes: f.size_bytes,
      })),
      artifactPatterns: body.artifact_patterns ?? [],
      idempotencyKey: body.idempotency_key,
      tenantId: auth?.tenantId,
    };

    try {
      // launchRun creates the Run record internally, so it doesn't need runId
      const workflow = await launchRun(input as LaunchRunInput);
      const workflowInput = workflow.input_json ? JSON.parse(workflow.input_json) : {};
      set.status = 202;
      const responseBody = {
        workflow_id: workflow.id,
        run_id: workflowInput.runId ?? null,
        status: workflow.status,
        status_url: `/v1/workflows/${workflow.id}/status`,
        stream_url: `/v1/workflows/${workflow.id}/stream`,
      };

      // Store HTTP-level idempotency response
      if (httpIdempotencyKey && paramsHash) {
        storeIdempotencyResponse(
          httpIdempotencyKey,
          "launch-run",
          paramsHash,
          202,
          JSON.stringify(responseBody),
          workflow.id,
        );
      }

      return responseBody;
    } catch (err) {
      if (err instanceof SkyREPLError) {
        const status = httpStatusForError(err);
        const errorBody = errorToApiError(err);

        // Store HTTP-level idempotency response for client errors
        if (httpIdempotencyKey && paramsHash && status < 500) {
          storeIdempotencyResponse(
            httpIdempotencyKey,
            "launch-run",
            paramsHash,
            status,
            JSON.stringify(errorBody),
          );
        }

        set.status = status;
        return errorBody;
      }
      console.error("[routes] launch-run error:", err);
      set.status = 500;
      return {
        error: {
          code: "INTERNAL_ERROR",
          message: err instanceof Error ? err.message : "Failed to launch run",
          category: "internal",
        },
      };
    }
  }, { body: LaunchRunRequestSchema });

  // ─── Workflow Cancel ──────────────────────────────────────────────────

  app.post("/v1/workflows/:id/cancel", async ({ params, body, set, request }) => {
    const auth = getAuthContext(request);
    if (auth && !checkPermission(auth, "cancel_workflow")) {
      set.status = 403;
      return { error: { code: "FORBIDDEN", message: "Insufficient permissions", category: "auth" } };
    }

    const workflowId = params.id;

    // Tenant isolation: verify workflow belongs to caller's tenant
    const workflow = getWorkflow(workflowId);
    if (!workflow || (auth && workflow.tenant_id !== auth.tenantId)) {
      set.status = 404;
      return { error: { code: "WORKFLOW_NOT_FOUND", message: `Workflow ${workflowId} not found`, category: "not_found" } };
    }

    const reason = body?.reason ?? "user_requested";

    // Import cancelWorkflow from engine
    const { cancelWorkflow } = await import("../../workflow/engine");
    const result = await cancelWorkflow(workflowId, reason);

    if (!result.success && result.status === "not_found") {
      set.status = 404;
      return { error: { code: "WORKFLOW_NOT_FOUND", message: `Workflow ${workflowId} not found`, category: "not_found" } };
    }

    // After successful cancel, send cancel_run SSE command to the agent
    if (result.success || result.status === "cancelled") {
      const workflow = getWorkflow(workflowId);
      if (workflow) {
        try {
          const nodes = getWorkflowNodes(workflowId);
          const allocNode = nodes.find(n => n.node_id === "create-allocation");
          const wfInput = workflow.input_json ? JSON.parse(workflow.input_json) : {};
          const runId = wfInput.runId;
          if (allocNode?.output_json) {
            const output = JSON.parse(allocNode.output_json);
            if (output.instanceId && runId) {
              const { sseManager } = await import("../sse-protocol");
              await sseManager.sendCommand(String(output.instanceId), {
                type: "cancel_run",
                command_id: Math.floor(Math.random() * 1000000),
                run_id: runId,
              });
            }
          }
        } catch (err) {
          console.warn("[routes] Failed to send cancel_run SSE command", { workflowId, error: err });
        }
      }
    }

    return {
      workflow_id: workflowId,
      status: result.status,
      cancelled: result.success,
    };
  }, { params: IdParams, body: CancelWorkflowRequestSchema });

  // ─── Workflow Status ──────────────────────────────────────────────────

  app.get("/v1/workflows/:id/status", ({ params, set, request }) => {
    const auth = getAuthContext(request);
    const id = params.id;

    const workflow = getWorkflow(id);
    if (!workflow || (auth && workflow.tenant_id !== auth.tenantId)) {
      return new Response(
        JSON.stringify({ error: { code: "WORKFLOW_NOT_FOUND", message: `Workflow ${id} not found`, category: "not_found" } }),
        { status: 404, headers: { "Content-Type": "application/json" } }
      );
    }

    const nodes = getWorkflowNodes(id);
    const completedNodes = nodes.filter((n) => n.status === "completed").length;
    const failedNodes = nodes.filter((n) => n.status === "failed").length;
    const totalNodes = nodes.length;

    const result: Record<string, unknown> = {
      workflow_id: workflow.id,
      type: workflow.type,
      status: workflow.status,
      current_node: workflow.current_node,
      nodes_total: totalNodes,
      nodes_completed: completedNodes,
      nodes_failed: failedNodes,
      started_at: workflow.started_at,
      finished_at: workflow.finished_at,
      progress: {
        completed_nodes: completedNodes,
        total_nodes: totalNodes,
        percentage: totalNodes > 0 ? Math.round((completedNodes / totalNodes) * 100) : 0,
      },
      output: workflow.output_json ? JSON.parse(workflow.output_json) : null,
      error: null as unknown,
    };

    if (workflow.status === "failed") {
      const failedNode = nodes.find((n) => n.status === "failed");
      if (failedNode?.error_json) {
        try {
          const errorData = JSON.parse(failedNode.error_json);
          result.error = {
            code: errorData.code ?? "INTERNAL_ERROR",
            message: errorData.message ?? "Workflow failed",
            category: errorData.category ?? "internal",
            node_id: failedNode.node_id,
            details: errorData.details,
          };
        } catch {
          result.error = {
            code: "INTERNAL_ERROR",
            message: "Workflow failed",
            category: "internal",
            node_id: failedNode.node_id,
          };
        }
      } else if (workflow.error_json) {
        try {
          result.error = JSON.parse(workflow.error_json);
        } catch {
          result.error = {
            code: "INTERNAL_ERROR",
            message: "Workflow failed",
            category: "internal",
          };
        }
      }
    }

    return result;
  }, { params: IdParams });

  // ─── Terminate Instance ─────────────────────────────────────────────

  app.post("/v1/workflows/terminate-instance", async ({ body, set, request }) => {
    const auth = getAuthContext(request);
    if (auth && !checkPermission(auth, "terminate_instance")) {
      set.status = 403;
      return { error: { code: "FORBIDDEN", message: "Insufficient permissions", category: "auth" } };
    }

    const instanceId = Number(body?.instanceId ?? body?.instance_id);
    if (!instanceId || !Number.isFinite(instanceId)) {
      set.status = 400;
      return {
        error: {
          code: "INVALID_INPUT",
          message: "instanceId is required and must be a number",
          category: "validation",
        },
      };
    }

    // Verify instance belongs to the caller's tenant
    if (auth) {
      const instance = getInstanceRecord(instanceId);
      if (!instance || instance.tenant_id !== auth.tenantId) {
        set.status = 404;
        return { error: { code: "INSTANCE_NOT_FOUND", message: `Instance ${instanceId} not found`, category: "not_found" } };
      }
    }

    try {
      const workflow = await terminateInstance({ instanceId, tenantId: auth?.tenantId });
      set.status = 202;
      return {
        workflow_id: workflow.id,
        instance_id: instanceId,
        status: workflow.status,
      };
    } catch (err) {
      if (err instanceof SkyREPLError) {
        const status = httpStatusForError(err);
        set.status = status;
        return errorToApiError(err);
      }
      console.error("[routes] terminate-instance error:", err);
      set.status = 500;
      return {
        error: {
          code: "INTERNAL_ERROR",
          message: err instanceof Error ? err.message : "Failed to terminate instance",
          category: "internal",
        },
      };
    }
  }, { body: TerminateInstanceRequestSchema });

  // ─── Allocation: Extend Debug Hold ──────────────────────────────────
  //
  // POST /v1/allocations/:id/extend
  //
  // Extends the debug_hold_until timestamp on a COMPLETE allocation.
  // Body: { duration_ms: number }

  app.post("/v1/allocations/:id/extend", async ({ params, body, set, request }) => {
    const auth = getAuthContext(request);
    const allocationId = Number(params.id);

    const { getAllocation, execute } = await import("../../material/db");
    const allocation = getAllocation(allocationId);
    if (!allocation || (auth && allocation.tenant_id !== auth.tenantId)) {
      set.status = 404;
      return { error: { code: "ALLOCATION_NOT_FOUND", message: `Allocation ${allocationId} not found`, category: "not_found" } };
    }

    const durationMs = body.duration_ms;

    const { extendAllocationDebugHold } = await import("../../resource/allocation");
    const result = await extendAllocationDebugHold(allocationId, { extensionMs: durationMs });

    if (!result.success) {
      set.status = 422;
      return { error: { code: "INVALID_STATE", message: `Cannot extend debug hold: allocation is in state ${allocation.status}`, category: "validation" } };
    }

    const updated = getAllocation(allocationId)!;
    return { data: updated };
  }, { params: IdParams, body: ExtendAllocationRequestSchema });

  // ─── Allocation: Release Debug Hold ─────────────────────────────────
  //
  // POST /v1/allocations/:id/release
  //
  // Clears the debug_hold_until on a COMPLETE allocation (set to null).

  app.post("/v1/allocations/:id/release", async ({ params, set, request }) => {
    const auth = getAuthContext(request);
    const allocationId = Number(params.id);

    const { getAllocation, execute } = await import("../../material/db");
    const allocation = getAllocation(allocationId);
    if (!allocation || (auth && allocation.tenant_id !== auth.tenantId)) {
      set.status = 404;
      return { error: { code: "ALLOCATION_NOT_FOUND", message: `Allocation ${allocationId} not found`, category: "not_found" } };
    }

    if (allocation.status !== "COMPLETE") {
      set.status = 422;
      return { error: { code: "INVALID_STATE", message: `Cannot release debug hold: allocation is in state ${allocation.status}`, category: "validation" } };
    }

    execute(
      "UPDATE allocations SET debug_hold_until = NULL, updated_at = ? WHERE id = ?",
      [Date.now(), allocationId]
    );

    const updated = getAllocation(allocationId)!;
    return { data: updated };
  }, { params: IdParams });

  // ─── Tailscale: JIT install ──────────────────────────────────────────
  //
  // POST /v1/instances/:id/tailscale-ensure
  //
  // Idempotent. Returns immediately with the current Tailscale status.
  // If not installed / failed, sends trigger_tailscale to the agent via SSE
  // and transitions the instance to 'installing'. Callers poll until 'ready'.

  app.post("/v1/instances/:id/tailscale-ensure", async ({ params, set, request }) => {
    const auth = getAuthContext(request);
    const instanceId = Number(params.id);

    const instance = getInstanceRecord(instanceId);
    if (!instance || (auth && instance.tenant_id !== auth.tenantId)) {
      set.status = 404;
      return {
        error: {
          code: "INSTANCE_NOT_FOUND",
          message: `Instance ${instanceId} not found`,
          category: "not_found",
        },
      };
    }

    const currentStatus = instance.tailscale_status ?? "not_installed";

    // Already ready — return immediately
    if (currentStatus === "ready") {
      return { status: "ready", ip: instance.tailscale_ip };
    }

    // Install in flight — let caller poll
    if (currentStatus === "installing") {
      return { status: "installing" };
    }

    // not_installed or failed — kick off install
    let authKey: string;
    try {
      const tsClient = getTailscaleClient();
      const keyResult = await tsClient.createAuthKey({
        ephemeral: true,
        preauthorized: true,
        description: `skyrepl-instance-${instanceId}`,
      });
      authKey = keyResult.key;
    } catch (err) {
      if (err instanceof TailscaleConfigError) {
        console.warn("[routes] Tailscale not configured:", err.message);
        set.status = 503;
        return {
          error: {
            code: "TAILSCALE_NOT_CONFIGURED",
            message: err.message,
            category: "configuration",
          },
        };
      }
      console.error("[routes] Failed to create Tailscale auth key:", err);
      set.status = 500;
      return {
        error: {
          code: "INTERNAL_ERROR",
          message: err instanceof Error ? err.message : "Failed to create Tailscale auth key",
          category: "internal",
        },
      };
    }

    // Send trigger_tailscale command to agent (queued if agent not connected)
    await sseManager.sendCommand(String(instanceId), {
      type: "trigger_tailscale",
      auth_key: authKey,
    } as any);

    return { status: "installing" };
  }, { params: IdParams });

  // ─── Tailscale: status query ─────────────────────────────────────────
  //
  // GET /v1/instances/:id/tailscale-status
  //
  // Returns current tailscale_status and tailscale_ip from last heartbeat.

  app.get("/v1/instances/:id/tailscale-status", ({ params, set, request }) => {
    const auth = getAuthContext(request);
    const instanceId = Number(params.id);

    const instance = getInstanceRecord(instanceId);
    if (!instance || (auth && instance.tenant_id !== auth.tenantId)) {
      set.status = 404;
      return {
        error: {
          code: "INSTANCE_NOT_FOUND",
          message: `Instance ${instanceId} not found`,
          category: "not_found",
        },
      };
    }

    return {
      instance_id: instanceId,
      tailscale_status: instance.tailscale_status ?? "not_installed",
      tailscale_ip: instance.tailscale_ip ?? null,
    };
  }, { params: IdParams });

  // Delegate blob and admin routes
  registerBlobRoutes(app);
  registerAdminRoutes(app);
}
