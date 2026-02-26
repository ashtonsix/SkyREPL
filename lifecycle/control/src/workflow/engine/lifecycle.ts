// workflow/engine/lifecycle.ts - submit, recover, cancel, pause, resume, retry,
// blueprint/executor registries, shutdown coordination, createWorkflowEngine

import type {
  NodeExecutor,
  WorkflowSubmission,
  WorkflowSubmissionResult,
  WorkflowBlueprint,
} from "../engine.types";
import {
  getWorkflow,
  getWorkflowNodes,
  createWorkflow,
  createWorkflowNode,
  updateWorkflow,
  createManifest,
  queryOne,
  queryMany,
  findActiveWorkflows,
  type Workflow,
} from "../../material/db";
import { TIMING } from "@skyrepl/contracts";
import { Value } from "@sinclair/typebox/value";
import {
  failWorkflow as failWorkflowTransition,
  cancelWorkflow as cancelWorkflowTransition,
  pauseWorkflow as pauseWorkflowTransition,
  resumeWorkflow as resumeWorkflowTransition,
  atomicTransition,
  failNode,
  resetNodeForRetry,
  skipNode,
} from "../state-transitions";
import { executeLoop } from "./executor";
import { activeLoops, isShutdownRequested as _isShutdownRequested, _setShutdownRequested, _clearActiveLoops } from "./executor";
import { handleWorkflowFailure, handleCancellation, handleCancellingPoll } from "./retry";
import { sleep, sealManifestSafe, POLL_INTERVAL_MS, MAX_SUBWORKFLOW_DEPTH } from "./helpers";
import { workflowEvents } from "../events";

// =============================================================================
// Engine Shutdown Coordination
// =============================================================================

export function requestEngineShutdown(): void {
  _setShutdownRequested(true);
}

export function isEngineShutdownRequested(): boolean {
  return _isShutdownRequested();
}

export async function awaitEngineQuiescence(timeoutMs = 30_000): Promise<void> {
  if (activeLoops.size === 0) return;
  const deadline = Date.now() + timeoutMs;
  while (activeLoops.size > 0 && Date.now() < deadline) {
    await Promise.race([
      Promise.allSettled([...activeLoops]),
      sleep(Math.min(1000, deadline - Date.now())),
    ]);
  }
  if (activeLoops.size > 0) {
    console.warn(`[workflow] Engine quiescence timeout: ${activeLoops.size} loop(s) still active`);
  }
}

export function resetEngineShutdown(): void {
  _setShutdownRequested(false);
  _clearActiveLoops();
}

// =============================================================================
// Node Executor Registry
// =============================================================================

const nodeExecutors = new Map<string, NodeExecutor>();

export function registerNodeExecutor(executor: NodeExecutor): void {
  nodeExecutors.set(executor.name, executor);
}

export function getNodeExecutor(type: string): NodeExecutor | undefined {
  return nodeExecutors.get(type);
}

/** Clear all registered node executors. Test-only. */
export function clearNodeExecutors(): void {
  nodeExecutors.clear();
}

// =============================================================================
// Blueprint Registry
// =============================================================================

const blueprints = new Map<string, WorkflowBlueprint>();

export function registerBlueprint(blueprint: WorkflowBlueprint): void {
  blueprints.set(blueprint.type, blueprint);
}

/** Clear all registered blueprints. Test-only. */
export function clearBlueprints(): void {
  blueprints.clear();
}

export function getBlueprint(type: string): WorkflowBlueprint {
  const bp = blueprints.get(type);
  if (!bp) {
    throw new Error(`Blueprint not found: ${type}`);
  }
  return bp;
}

/** Non-throwing blueprint lookup. Returns undefined if not found. */
export function tryGetBlueprint(type: string): WorkflowBlueprint | undefined {
  return blueprints.get(type);
}

// =============================================================================
// WorkflowEngine Interface
// =============================================================================

export interface WorkflowEngine {
  submit(request: WorkflowSubmission): Promise<WorkflowSubmissionResult>;
  get(workflowId: number): Promise<Workflow | null>;
  cancel(workflowId: number, reason: string): Promise<{ success: boolean; status: string }>;
  pause(workflowId: number): Promise<void>;
  resume(workflowId: number): Promise<void>;
  retry(workflowId: number): Promise<number>;
}

// =============================================================================
// Submit
// =============================================================================

export async function submit(
  request: WorkflowSubmission
): Promise<WorkflowSubmissionResult> {
  // Idempotency check
  if (request.idempotencyKey) {
    const existing = queryOne<Workflow>(
      "SELECT * FROM workflows WHERE idempotency_key = ?",
      [request.idempotencyKey]
    );
    if (existing) {
      return {
        workflowId: existing.id,
        status: "deduplicated",
        existingWorkflowId: existing.id,
      };
    }
  }

  // Get blueprint (validates the workflow type)
  const blueprint = getBlueprint(request.type);

  // Validate workflow input against schema (fail fast on bad input)
  if (blueprint.inputSchema) {
    Value.Parse(blueprint.inputSchema, request.input);
  }

  // Determine depth for subworkflow support
  let depth = 0;
  if (request.parentWorkflowId != null) {
    const parent = getWorkflow(request.parentWorkflowId);
    if (parent) {
      depth = parent.depth + 1;
      if (depth > MAX_SUBWORKFLOW_DEPTH) {
        throw new Error(
          `Max subworkflow depth (${MAX_SUBWORKFLOW_DEPTH}) exceeded`
        );
      }
    }
  }

  const now = Date.now();
  const timeoutMs = request.timeout ?? TIMING.DEFAULT_WORKFLOW_TIMEOUT_MS;
  const timeoutAt = now + timeoutMs;

  // Create workflow record first (need the ID for manifest)
  const workflow = createWorkflow({
    type: request.type,
    parent_workflow_id: request.parentWorkflowId ?? null,
    depth,
    status: "pending",
    current_node: null,
    input_json: JSON.stringify(request.input),
    output_json: null,
    error_json: null,
    manifest_id: null, // Will be updated after manifest creation
    trace_id: null,
    idempotency_key: request.idempotencyKey ?? null,
    timeout_ms: timeoutMs,
    timeout_at: timeoutAt,
    started_at: null,
    finished_at: null,
    updated_at: now,
  }, request.tenantId);

  // Create DRAFT manifest for resource tracking
  const manifest = createManifest(workflow.id, { tenant_id: request.tenantId });

  // Update workflow with manifest_id
  updateWorkflow(workflow.id, { manifest_id: manifest.id });

  // Create nodes from blueprint definition
  for (const [nodeId, nodeDef] of Object.entries(blueprint.nodes)) {
    const dependsOn =
      nodeDef.dependsOn && nodeDef.dependsOn.length > 0
        ? JSON.stringify(nodeDef.dependsOn)
        : null;

    createWorkflowNode({
      workflow_id: workflow.id,
      node_id: nodeId,
      node_type: nodeDef.type,
      status: "pending",
      input_json: JSON.stringify(nodeDef.inputSchema ?? {}),
      output_json: null,
      error_json: null,
      depends_on: dependsOn,
      attempt: 0,
      retry_reason: null,
      started_at: null,
      finished_at: null,
      updated_at: now,
    });
  }

  // Start execution loop (fire-and-forget)
  executeLoop(workflow.id).catch((err) => {
    console.error("[workflow] executeLoop unhandled error", {
      workflowId: workflow.id,
      error: err,
    });
    try {
      failWorkflowTransition(workflow.id, JSON.stringify({
        code: "INTERNAL_ERROR",
        message: `executeLoop crashed: ${err?.message ?? err}`,
      }));
      // Step 11: seal manifest on crash path so it doesn't stay DRAFT forever
      if (manifest.id) {
        sealManifestSafe(manifest.id);
      }
    } catch (_) { /* already terminal or DB unavailable */ }
  });

  return { workflowId: workflow.id, status: "created" };
}

// =============================================================================
// Cancellation
// =============================================================================

export async function cancelWorkflow(
  workflowId: number,
  reason: string
): Promise<{ success: boolean; status: string }> {
  const workflow = getWorkflow(workflowId);
  if (!workflow) {
    return { success: false, status: "not_found" };
  }

  // Already in terminal state -- idempotent success (EX7 fix)
  const terminalStates = ["completed", "failed", "cancelled"];
  if (terminalStates.includes(workflow.status)) {
    return { success: true, status: workflow.status };
  }

  // Already cancelling -- idempotent success
  if (workflow.status === "cancelling") {
    return { success: true, status: "cancelled" };
  }

  // For pending workflows: skip straight to cancelled (no running nodes to wait on)
  if (workflow.status === "pending") {
    // A19 fix: use atomicTransition for pending->cancelled to guard against a concurrent
    // executeLoop that may have already started the workflow (pending->running). If the
    // transition is lost, re-read the new status and fall through to the running cancel path.
    const pendingToCancelledResult = atomicTransition<Workflow>(
      "workflows",
      workflowId,
      "pending",
      "cancelled",
      { error_json: JSON.stringify({ code: "CANCELLED", reason }), finished_at: Date.now() }
    );

    if (!pendingToCancelledResult.success) {
      // Race lost: executeLoop already transitioned pending->running. Re-read and handle.
      const current = getWorkflow(workflowId);
      if (current && (current.status === "running" || current.status === "paused")) {
        const result = cancelWorkflowTransition(workflowId);
        if (result.success) {
          updateWorkflow(workflowId, {
            error_json: JSON.stringify({ code: "CANCELLED", reason }),
          });
          // Two-phase: let the execute loop drain running nodes before finalizing.
          return { success: true, status: "cancelled" };
        }
      }
      return { success: false, status: current?.status ?? workflow.status };
    }

    // Transition succeeded: skip all pending nodes now that workflow is cancelled.
    const allNodes = getWorkflowNodes(workflowId);
    for (const node of allNodes) {
      if (node.status === "pending") {
        skipNode(node.id);
      }
    }

    // A18 fix: seal the manifest so it doesn't stay DRAFT indefinitely.
    const wfAfterCancel = getWorkflow(workflowId);
    if (wfAfterCancel?.manifest_id) {
      try {
        sealManifestSafe(wfAfterCancel.manifest_id);
      } catch (err) {
        console.warn("[workflow] Failed to seal manifest on pending cancel", {
          workflowId,
          manifestId: wfAfterCancel.manifest_id,
          error: err,
        });
      }
    }

    // A18 fix: emit workflow_failed event so SSE clients learn about the cancellation.
    workflowEvents.emit("workflow_failed", {
      workflowId,
      error: "Workflow cancelled",
      timestamp: Date.now(),
    });

    return { success: true, status: "cancelled" };
  }

  // Only running or paused workflows can be cancelled (besides pending handled above)
  if (workflow.status !== "running" && workflow.status !== "paused") {
    return { success: false, status: workflow.status };
  }

  // Atomically guarded cancellation: running/paused -> cancelling (B5 fix)
  const result = cancelWorkflowTransition(workflowId);
  if (result.success) {
    // Update error_json separately after successful transition
    updateWorkflow(workflowId, {
      error_json: JSON.stringify({ code: "CANCELLED", reason }),
    });
    // Two-phase integrity-first cancellation (Track B, WL-060):
    // Check if there are running nodes that need draining. If not, finalize
    // immediately (handles manually-created workflows without an execute loop).
    // If there are running nodes, the execute loop's handleCancellingPoll will
    // perform Phase 1 (skip pending, send SSE, cancel children) and drain
    // running/compensating nodes before finalizing.
    const nodes = getWorkflowNodes(workflowId);
    const hasRunningNodes = nodes.some(n => n.status === "running");
    if (!hasRunningNodes) {
      const { handleCancellation } = await import("./retry");
      await handleCancellation(workflowId);
    }
    return { success: true, status: "cancelled" };
  }

  return { success: false, status: workflow.status };
}

// =============================================================================
// Crash Recovery
// =============================================================================

export async function recoverWorkflows(): Promise<void> {
  // Handle stale pending workflows
  const stalePending = queryMany<Workflow>(
    `SELECT * FROM workflows WHERE status = 'pending' AND updated_at < ?`,
    [Date.now() - TIMING.PENDING_WORKFLOW_TIMEOUT_MS]
  );

  for (const workflow of stalePending) {
    failWorkflowTransition(
      workflow.id,
      JSON.stringify({ code: "PENDING_TIMEOUT", message: "Workflow exceeded pending timeout" })
    );
    if (workflow.manifest_id) {
      try {
        sealManifestSafe(workflow.manifest_id);
      } catch (err) {
        console.warn("[workflow] Failed to seal manifest for stale pending workflow", {
          workflowId: workflow.id,
          error: err,
        });
      }
    }
    console.warn("[workflow] Timed out stale pending workflow", {
      workflowId: workflow.id,
    });
  }

  // Resume running workflows
  const staleWorkflows = findActiveWorkflows();
  if (staleWorkflows.length === 0) return;

  console.log(`[workflow] Recovering ${staleWorkflows.length} workflow(s) from crash...`);

  for (const workflow of staleWorkflows) {
    const nodes = getWorkflowNodes(workflow.id);

    for (const node of nodes) {
      if (node.status === "running") {
        const executor = nodeExecutors.get(node.node_type);
        if (executor?.idempotent) {
          // Idempotent node: safe to retry -- first fail, then reset to pending
          const failResult = failNode(node.id, JSON.stringify({
            code: "CRASH_RECOVERY",
            message: "Node was running during crash -- idempotent, resetting for retry",
            category: "crash",
          }));
          if (failResult.success) {
            resetNodeForRetry(node.id, "crash_recovery");
            console.log(`[workflow] Reset idempotent node ${node.node_id} to pending (crash recovery)`);
          }
        } else {
          // Non-idempotent node: unsafe to retry -- mark failed
          failNode(node.id, JSON.stringify({
            code: "CRASH_RECOVERY",
            message: "Node was running during crash -- non-idempotent, marked failed",
            category: "crash",
          }));
          console.log(`[workflow] Failed non-idempotent node ${node.node_id} (crash recovery)`);
        }
      }
    }

    // Re-enter the execute loop for workflows that had idempotent nodes reset
    const updatedNodes = getWorkflowNodes(workflow.id);
    const hasResetNodes = updatedNodes.some(n => n.status === "pending");
    const hasFailedNodes = updatedNodes.some(n => n.status === "failed");

    if (workflow.status === "cancelling") {
      // Cancelling workflow interrupted by crash: re-enter execute loop to complete cancellation
      executeLoop(workflow.id).catch((err) => {
        console.error("[workflow] executeLoop unhandled error (cancelling recovery)", {
          workflowId: workflow.id,
          error: err,
        });
      });
    } else if (hasResetNodes && !hasFailedNodes) {
      // Resume the workflow
      executeLoop(workflow.id).catch((err) => {
        console.error("[workflow] executeLoop unhandled error", {
          workflowId: workflow.id,
          error: err,
        });
        try {
          failWorkflowTransition(workflow.id, JSON.stringify({
            code: "INTERNAL_ERROR",
            message: `executeLoop crashed: ${err?.message ?? err}`,
          }));
        } catch (_) { /* already terminal or DB unavailable */ }
      });
    } else if (hasFailedNodes) {
      // Fail the workflow
      handleWorkflowFailure(workflow.id, updatedNodes);
    }
  }
}

export function isIdempotent(nodeType: string): boolean {
  const executor = nodeExecutors.get(nodeType);
  return executor?.idempotent ?? false;
}

// =============================================================================
// Create Engine
// =============================================================================

export function createWorkflowEngine(): WorkflowEngine {
  return {
    submit,

    async get(workflowId: number): Promise<Workflow | null> {
      return getWorkflow(workflowId);
    },

    cancel: cancelWorkflow,

    async pause(workflowId: number): Promise<void> {
      const result = pauseWorkflowTransition(workflowId);
      if (!result.success) {
        throw new Error(`Cannot pause workflow ${workflowId}: ${result.reason}`);
      }
    },

    async resume(workflowId: number): Promise<void> {
      const result = resumeWorkflowTransition(workflowId);
      if (!result.success) {
        throw new Error(`Cannot resume workflow ${workflowId}: ${result.reason}`);
      }
    },

    async retry(workflowId: number): Promise<number> {
      const workflow = getWorkflow(workflowId);
      if (!workflow) throw new Error(`Workflow ${workflowId} not found`);

      // SD-G1-07: Only failed workflows can be retried
      if (workflow.status !== "failed") {
        throw Object.assign(
          new Error(`Cannot retry workflow ${workflowId}: status is ${workflow.status}, expected 'failed'`),
          { code: "INVALID_STATE", category: "validation" }
        );
      }

      const originalInput = workflow.input_json ? JSON.parse(workflow.input_json) : {};
      const originalKey = workflow.idempotency_key;
      const newKey = originalKey ? `${originalKey}:retry:${Date.now()}` : undefined;

      // SD-G1-06: Preserve parent_workflow_id and timeout from the original workflow
      const result = await submit({
        type: workflow.type,
        input: originalInput,
        idempotencyKey: newKey,
        parentWorkflowId: workflow.parent_workflow_id ?? undefined,
        timeout: workflow.timeout_ms ?? undefined,
      });
      return result.workflowId;
    },
  };
}

// =============================================================================
// runSingleStep Convenience Wrapper (#WF-06)
// =============================================================================

/**
 * Convenience wrapper: create a 1-node workflow, execute it, and return the result.
 * Hides DAG ceremony for simple atomic operations.
 */
export async function runSingleStep(
  type: string,
  nodeType: string,
  input: Record<string, unknown>,
  options?: { idempotencyKey?: string; timeout?: number; parentWorkflowId?: number }
): Promise<{ workflowId: number; output: unknown; status: string }> {
  // Register a temporary blueprint for this single-step invocation
  const blueprintType = `_single-step:${nodeType}:${Date.now()}:${Math.random().toString(36).slice(2, 8)}`;

  registerBlueprint({
    type: blueprintType,
    entryNode: "step",
    nodes: {
      step: { type: nodeType, dependsOn: [] },
    },
  });

  try {
    const result = await submit({
      type: blueprintType,
      input,
      idempotencyKey: options?.idempotencyKey,
      timeout: options?.timeout,
      parentWorkflowId: options?.parentWorkflowId,
    });

    // Poll for completion
    const pollTimeout = options?.timeout ?? TIMING.DEFAULT_WORKFLOW_TIMEOUT_MS;
    const deadline = Date.now() + pollTimeout;

    while (Date.now() < deadline) {
      const workflow = getWorkflow(result.workflowId);
      if (!workflow) {
        throw new Error(`Workflow ${result.workflowId} not found during polling`);
      }

      if (workflow.status === "completed") {
        const output = workflow.output_json ? JSON.parse(workflow.output_json) : {};
        // Extract the single node's output from the output map
        const nodeOutput = (output as Record<string, unknown>).step ?? output;
        return { workflowId: result.workflowId, output: nodeOutput, status: "completed" };
      }

      if (workflow.status === "failed" || workflow.status === "cancelled") {
        const error = workflow.error_json ? JSON.parse(workflow.error_json) : { message: "Unknown error" };
        return { workflowId: result.workflowId, output: error, status: workflow.status };
      }

      await sleep(POLL_INTERVAL_MS);
    }

    // Timed out waiting for completion
    return { workflowId: result.workflowId, output: null, status: "timeout" };
  } finally {
    // Unregister the temporary blueprint to avoid registry bloat
    blueprints.delete(blueprintType);
  }
}
