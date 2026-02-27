// workflow/engine/retry.ts - Retry strategy, workflow completion/failure/timeout/cancellation

import type {
  RetryDecision,
  NodeError,
} from "../engine.types";
import {
  getWorkflow,
  getWorkflowNodes,
  queryMany,
  type Workflow,
  type WorkflowNode,
} from "../../material/db";
import { TIMING } from "@skyrepl/contracts";
import {
  completeWorkflow as completeWorkflowTransition,
  failWorkflow as failWorkflowTransition,
  cancelWorkflow as cancelWorkflowTransition,
  finalizeCancellation,
  failNode,
  resetNodeForRetry,
  skipNode,
} from "../state-transitions";
import {
  applyRetryWithAlternative,
} from "../patterns";
import { updateRunRecord } from "../../resource/run";
import { workflowEvents } from "../events";
import { stateEvents, STATE_EVENT } from "../events";
import { sleep, sealManifestSafe } from "./helpers";

// =============================================================================
// Retry Strategy
// =============================================================================

export const ERROR_RETRY_MAPPING: Record<
  string,
  {
    retryable: boolean;
    strategy: "exponential_backoff" | "alternative" | "fallback";
    maxRetries: number;
  }
> = {
  // Retryable -- retry with alternative provider/region (Ch06 $6.6.1)
  // Same request to same region would fail again; try a different region or provider.
  CAPACITY_ERROR: { retryable: true, strategy: "alternative", maxRetries: 3 },
  REGION_UNAVAILABLE: {
    retryable: true,
    strategy: "alternative",
    maxRetries: 3,
  },
  QUOTA_EXCEEDED: { retryable: true, strategy: "alternative", maxRetries: 3 },

  // Retryable -- exponential backoff (same provider, delay between attempts)
  // Transient failures where the same request may succeed after waiting.
  PROVIDER_INTERNAL: {
    retryable: true,
    strategy: "exponential_backoff",
    maxRetries: 3,
  },
  NETWORK_ERROR: {
    retryable: true,
    strategy: "exponential_backoff",
    maxRetries: 3,
  },
  RATE_LIMITED: {
    retryable: true,
    strategy: "exponential_backoff",
    maxRetries: 5,
  },
  RATE_LIMIT_ERROR: {
    retryable: true,
    strategy: "exponential_backoff",
    maxRetries: 5,
  },
  TIMEOUT_ERROR: {
    retryable: true,
    strategy: "exponential_backoff",
    maxRetries: 2,
  },
  OPERATION_TIMEOUT: {
    retryable: true,
    strategy: "exponential_backoff",
    maxRetries: 2,
  },

  // Retryable -- fallback path (fundamentally different approach needed)
  // Resource lost mid-operation; relaunch on on-demand instead of retrying spot.
  // maxRetries=3: attempt=1 retries spot, attempt=2 creates on-demand fallback via RWA.
  // The on-demand fallback is a new node, so the original node's attempt never reaches 3.
  SPOT_INTERRUPTED: { retryable: true, strategy: "fallback", maxRetries: 3 },

  // Retryable -- unexpected errors (plain Error with no code, §normalizeToNodeError)
  // Conservative: 2 retries with exponential backoff before permanent failure.
  INTERNAL_ERROR: {
    retryable: true,
    strategy: "exponential_backoff",
    maxRetries: 2,
  },

  // Non-retryable -- fail immediately (deterministic failures, retry won't help)
  VALIDATION_ERROR: {
    retryable: false,
    strategy: "exponential_backoff",
    maxRetries: 0,
  },
  INVALID_STATE_TRANSITION: {
    retryable: false,
    strategy: "exponential_backoff",
    maxRetries: 0,
  },
  DATABASE_ERROR: {
    retryable: false,
    strategy: "exponential_backoff",
    maxRetries: 0,
  },
  AUTH_ERROR: {
    retryable: false,
    strategy: "exponential_backoff",
    maxRetries: 0,
  },
  INVALID_SPEC: {
    retryable: false,
    strategy: "exponential_backoff",
    maxRetries: 0,
  },
  UNSUPPORTED_OPERATION: {
    retryable: false,
    strategy: "exponential_backoff",
    maxRetries: 0,
  },
};

export function determineRetryStrategy(
  error: NodeError,
  attempt: number
): RetryDecision {
  const mapping = ERROR_RETRY_MAPPING[error.code];

  if (!mapping || !mapping.retryable) {
    return { shouldRetry: false, strategy: null, delayMs: 0, maxAttempts: 0 };
  }

  if (attempt >= mapping.maxRetries) {
    return {
      shouldRetry: false,
      strategy: null,
      delayMs: 0,
      maxAttempts: mapping.maxRetries,
    };
  }

  // Exponential backoff: min(2^attempt * base, max)
  // Provider-supplied retry_after_ms takes precedence over computed backoff.
  const providerDelay = (error.details?.retry_after_ms as number) ?? 0;
  const computedBackoff = mapping.strategy === "exponential_backoff"
    ? Math.min(
        Math.pow(2, attempt) * TIMING.RETRY_BASE_DELAY_MS,
        TIMING.RETRY_MAX_DELAY_MS
      )
    : 0;
  const delayMs = providerDelay > 0 ? providerDelay : computedBackoff;

  return {
    shouldRetry: true,
    strategy:
      mapping.strategy === "exponential_backoff"
        ? "same_params"
        : mapping.strategy === "alternative"
          ? "alternative"
          : "fallback",
    delayMs,
    maxAttempts: mapping.maxRetries,
  };
}

export async function handleRetry(
  workflowId: number,
  node: WorkflowNode,
  error: NodeError,
  decision: RetryDecision
): Promise<void> {
  if (decision.delayMs > 0) {
    await sleep(decision.delayMs);
  }

  switch (decision.strategy) {
    case "same_params":
      // Simple retry: fail first (resetNodeForRetry requires failed status), then reset
      failNode(node.id, JSON.stringify({ code: error.code, message: error.message, retried: true }));
      resetNodeForRetry(node.id, `retry_${error.code}`);
      break;

    case "alternative":
      // Compensate failed node before trying alternative
      failNode(
        node.id,
        JSON.stringify({
          code: error.code,
          message: error.message,
          retried_with: "alternative",
        })
      );
      {
        const { compensateFailedNode } = await import("../compensation");
        await compensateFailedNode(workflowId, node.node_id);
      }
      {
        const currentInput: Record<string, unknown> = node.input_json
          ? JSON.parse(node.input_json)
          : {};

        // Use error.details?.alternative_regions if the spawner already populated them,
        // otherwise ask orbital to discover alternatives for us.
        let alternativeRegions = error.details?.alternative_regions as string[] | undefined;
        let alternativeSpec: string | undefined;
        let alternativeProvider: string | undefined;

        if (!alternativeRegions?.length) {
          try {
            const { resolveAlternativeViaOrbital } = await import("../../provider/orbital");
            const altResult = await resolveAlternativeViaOrbital(
              String(currentInput.spec ?? ""),
              String(currentInput.provider ?? ""),
              String(currentInput.spec ?? ""),
            );
            if (altResult && altResult.alternatives.length > 0) {
              const alt = altResult.alternatives[0]!;
              alternativeRegions = alt.instance.regions;
              alternativeSpec = alt.spec;
              alternativeProvider = alt.provider;
            }
          } catch {
            // Orbital unavailable — no alternative available, node stays failed
          }
        }

        const alternativeRegion = alternativeRegions?.[0];
        if (alternativeRegion) {
          applyRetryWithAlternative(workflowId, node.node_id, {
            id: `${node.node_id}-alt-${node.attempt + 1}`,
            type: node.node_type,
            input: {
              ...currentInput,
              region: alternativeRegion,
              ...(alternativeSpec !== undefined ? { spec: alternativeSpec } : {}),
              ...(alternativeProvider !== undefined ? { provider: alternativeProvider } : {}),
            },
          });
        }
      }
      break;

    case "fallback": {
      // Spot interrupted: retry spot once, then fallback to on-demand (§6.6.1)
      // Note: startNode increments attempt before execute, so first execution has attempt=1
      const currentInput = node.input_json ? JSON.parse(node.input_json) : {};
      const alreadyOnDemand = currentInput.is_spot === false;

      if (alreadyOnDemand) {
        // Already running as on-demand fallback -- no further fallback possible, just fail
        failNode(node.id, JSON.stringify({
          code: error.code,
          message: error.message,
          retried_with: "fallback_exhausted",
        }));
      } else if (node.attempt <= 1) {
        failNode(node.id, JSON.stringify({ code: error.code, message: error.message, retried_with: "fallback_spot_retry" }));
        resetNodeForRetry(node.id, "spot_retry");
      } else {
        // Fail the node, apply RWA to swap in an on-demand fallback node
        failNode(
          node.id,
          JSON.stringify({
            code: error.code,
            message: error.message,
            retried_with: "fallback_on_demand",
          })
        );
        {
          // Compensate the failed spot node before launching on-demand fallback.
          // Best-effort: if compensation throws, proceed with fallback anyway.
          // Compensation is cleanup; the on-demand fallback is the critical path.
          try {
            const { compensateFailedNode } = await import("../compensation");
            await compensateFailedNode(workflowId, node.node_id);
          } catch (compError) {
            console.error("[retry] Compensation failed during spot→on-demand fallback, proceeding with fallback anyway", {
              workflowId,
              nodeId: node.node_id,
              error: compError instanceof Error ? compError.message : String(compError),
            });
          }
        }
        {
          // Create on-demand fallback node via RWA pattern: same type, is_spot=false
          applyRetryWithAlternative(workflowId, node.node_id, {
            id: `${node.node_id}-ondemand`,
            type: node.node_type,
            input: { ...currentInput, is_spot: false },
          });
        }
      }
      break;
    }
  }
}

// =============================================================================
// PFO Branch Failure Handling
// =============================================================================

export function handlePfoBranchFailure(workflowId: number, failedBranchNodeId: string): void {
  const allNodes = getWorkflowNodes(workflowId);

  // Find the PFO join node: any node whose depends_on includes this branch
  // AND whose retry_reason starts with "pfo_join:"
  const joinNode = allNodes.find((n) => {
    if (!n.retry_reason?.startsWith("pfo_join:")) return false;
    const deps: string[] = n.depends_on ? JSON.parse(n.depends_on) : [];
    return deps.includes(failedBranchNodeId);
  });

  if (!joinNode) return;

  // Extract join mode from retry_reason
  const joinMode = joinNode.retry_reason!.replace("pfo_join:", "");

  if (joinMode !== "first-failure-cancels") return;

  // Cancel all other pending/running PFO branch siblings
  const joinDeps: string[] = joinNode.depends_on ? JSON.parse(joinNode.depends_on) : [];

  for (const siblingNodeId of joinDeps) {
    if (siblingNodeId === failedBranchNodeId) continue;

    const sibling = allNodes.find((n) => n.node_id === siblingNodeId);
    if (!sibling) continue;

    if (sibling.status === "pending") {
      skipNode(sibling.id);
    } else if (sibling.status === "running") {
      failNode(sibling.id, JSON.stringify({
        code: "PFO_CANCELLED",
        message: "PFO branch cancelled: sibling branch failed in first-failure-cancels mode",
      }));
    }
  }
}

// =============================================================================
// Workflow Completion Helpers
// =============================================================================

export async function handleWorkflowComplete(
  workflowId: number,
  nodes: WorkflowNode[]
): Promise<void> {
  // Gather outputs from all completed nodes
  const outputMap: Record<string, unknown> = {};
  for (const node of nodes) {
    if (node.status === "completed" && node.output_json) {
      outputMap[node.node_id] = JSON.parse(node.output_json);
    }
  }

  // Seal the manifest
  const workflow = getWorkflow(workflowId);
  if (workflow?.manifest_id) {
    try {
      sealManifestSafe(workflow.manifest_id);
    } catch (err) {
      console.warn("[workflow] Failed to seal manifest on completion", {
        workflowId,
        manifestId: workflow.manifest_id,
        error: err,
      });
    }
  }

  completeWorkflowTransition(workflowId, outputMap);

  // Hook 4: workflow_completed
  workflowEvents.emit("workflow_completed", {
    workflowId,
    output: outputMap,
    timestamp: Date.now(),
  });
}

export async function handleWorkflowFailure(
  workflowId: number,
  nodes: WorkflowNode[]
): Promise<void> {
  // Single-node compensation scope ($6.5, $7, $9):
  // Only the failed node gets compensated -- completed nodes are NOT rolled back.
  // Note: inline compensation already happens in executeNode when the node first
  // fails. This is a safety net for cases where the failed node wasn't compensated
  // inline (e.g., crash recovery).
  const failedNodes = nodes.filter((n) => n.status === "failed");
  for (const failedNode of failedNodes) {
    try {
      const { compensateFailedNode } = await import("../compensation");
      const result = await compensateFailedNode(workflowId, failedNode.node_id);
      if (!result.success) {
        console.warn("[workflow] Compensation failed for node", {
          workflowId,
          nodeId: failedNode.node_id,
          error: result.error?.message,
        });
      }
    } catch (err) {
      console.warn("[workflow] Compensation error for failed node", {
        workflowId,
        nodeId: failedNode.node_id,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  // Clean up any ACTIVE/CLAIMED allocations created by this workflow.
  // The finalize-run node (which normally completes allocations) didn't run,
  // so we must ensure no allocation is left in a non-terminal state.
  const allocNode = nodes.find(n => n.node_id === "create-allocation" && n.output_json);
  if (allocNode?.output_json) {
    try {
      const allocOutput = JSON.parse(allocNode.output_json);
      if (allocOutput.allocationId) {
        const { failAllocationAnyState } = await import("../state-transitions");
        failAllocationAnyState(allocOutput.allocationId);
      }
    } catch (err) {
      console.warn("[workflow] Failed to clean up allocation on workflow failure", {
        workflowId,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  // Find the first failed node for error context
  const firstFailedNode = failedNodes[0];
  const errorJson = firstFailedNode?.error_json ?? JSON.stringify({
    code: "WORKFLOW_FAILED",
    message: "One or more nodes failed",
  });

  // Seal the manifest
  const workflow = getWorkflow(workflowId);
  if (workflow?.manifest_id) {
    try {
      sealManifestSafe(workflow.manifest_id);
    } catch (err) {
      console.warn("[workflow] Failed to seal manifest on failure", {
        workflowId,
        manifestId: workflow.manifest_id,
        error: err,
      });
    }
  }

  failWorkflowTransition(workflowId, errorJson);

  // Sync run.workflow_state (#3.01/#3.04: keep run state consistent with workflow)
  const wfInput = workflow?.input_json ? JSON.parse(workflow.input_json) : {};
  if (wfInput.runId) {
    try {
      updateRunRecord(wfInput.runId, {
        workflow_state: "launch-run:failed",
        finished_at: Date.now(),
      });
    } catch (err) {
      console.warn("[workflow] Failed to sync run state on workflow failure", {
        workflowId,
        runId: wfInput.runId,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  // Hook 5: workflow_failed
  const errorObj = (() => { try { return JSON.parse(errorJson); } catch { return {}; } })();
  workflowEvents.emit("workflow_failed", {
    workflowId,
    error: errorObj.message ?? "Workflow failed",
    nodeId: firstFailedNode?.node_id,
    timestamp: Date.now(),
  });
}

// =============================================================================
// Workflow Timeout
// =============================================================================

export async function handleWorkflowTimeout(
  workflowId: number
): Promise<void> {
  const nodes = getWorkflowNodes(workflowId);
  const runningNodes = nodes.filter((n) => n.status === "running");

  for (const node of runningNodes) {
    failNode(
      node.id,
      JSON.stringify({ code: "WORKFLOW_TIMEOUT", message: "Workflow exceeded timeout" })
    );
  }

  // Skip pending nodes
  const pendingNodes = nodes.filter((n) => n.status === "pending");
  for (const node of pendingNodes) {
    skipNode(node.id);
  }

  // Cancel child workflows (#WF-03: timeout propagation)
  // NOTE: cancelWorkflow is imported from lifecycle.ts -- circular import resolved at call time
  const { cancelWorkflow } = await import("./lifecycle");
  const childWorkflows = queryMany<Workflow>(
    "SELECT * FROM workflows WHERE parent_workflow_id = ? AND status NOT IN ('completed', 'failed', 'cancelled')",
    [workflowId]
  );
  for (const child of childWorkflows) {
    await cancelWorkflow(child.id, "parent_timeout");
  }

  // Seal manifest
  const workflow = getWorkflow(workflowId);
  if (workflow?.manifest_id) {
    try {
      sealManifestSafe(workflow.manifest_id);
    } catch (err) {
      console.warn("[workflow] Failed to seal manifest on timeout", {
        workflowId,
        error: err,
      });
    }
  }

  failWorkflowTransition(
    workflowId,
    JSON.stringify({
      code: "WORKFLOW_TIMEOUT",
      message: "Workflow exceeded timeout",
    })
  );

  // Sync run.workflow_state (#3.01/#3.04)
  const wfInput = workflow?.input_json ? JSON.parse(workflow.input_json) : {};
  if (wfInput.runId) {
    try {
      updateRunRecord(wfInput.runId, {
        workflow_state: "launch-run:timeout",
        finished_at: Date.now(),
      });
    } catch (err) {
      console.warn("[workflow] Failed to sync run state on timeout", {
        workflowId,
        runId: wfInput.runId,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }
}

// =============================================================================
// Cancellation — Two-Phase Integrity-First Protocol (Track B, WL-060)
// =============================================================================

// Phase 1 init tracking: records which workflows have already had their
// one-time cancellation setup performed (skip pending, send SSE, cancel children).
const cancelInitDone = new Set<number>();

/** Reset cancel init tracking. Test-only. */
export function _clearCancelInitState(): void {
  cancelInitDone.clear();
}

/**
 * Phase 1 — Immediate cancellation setup (called once when cancelling detected).
 * - Skips all pending nodes (prevents new work from starting)
 * - Sends cancel_run SSE to agent (fire-and-forget)
 * - Cancels child workflows (propagate cancellation immediately so children
 *   can begin their own drain, avoiding deadlocks with parent nodes that
 *   are awaiting child completion)
 * - Does NOT finalize, seal manifest, or clean allocations.
 */
async function handleCancellingInit(workflowId: number): Promise<void> {
  if (cancelInitDone.has(workflowId)) return;
  cancelInitDone.add(workflowId);

  const nodes = getWorkflowNodes(workflowId);

  // Skip pending nodes
  for (const node of nodes) {
    if (node.status === "pending") {
      skipNode(node.id);
    }
  }

  // Send cancel_run to agent via SSE (fire-and-forget, §6.7)
  const wf = getWorkflow(workflowId);
  const wfInputForCancel = wf?.input_json ? JSON.parse(wf.input_json) : {};
  if (wfInputForCancel.runId) {
    const spawnNode = nodes.find(n => n.node_type === "spawn-instance" && n.output_json);
    const instanceId = spawnNode?.output_json
      ? JSON.parse(spawnNode.output_json).providerId
      : undefined;
    if (instanceId) {
      try {
        const { commandBus } = await import("../../events/command-bus");
        commandBus.sendCommand(instanceId, {
          type: "cancel_run" as const,
          command_id: Date.now(),
          run_id: wfInputForCancel.runId,
        });
      } catch {
        // Fire-and-forget: failure is logged by SSE layer
      }
    }
  }

  // Cancel child workflows immediately (#WF-03: cancel propagation).
  // Children must be cancelled in Phase 1 (not deferred to finalization)
  // to avoid deadlocks: parent nodes that spawn subworkflows and await
  // handle.wait() are "running" during the drain phase.
  const { cancelWorkflow } = await import("./lifecycle");
  const childWorkflows = queryMany<Workflow>(
    "SELECT * FROM workflows WHERE parent_workflow_id = ? AND status NOT IN ('completed', 'failed', 'cancelled')",
    [workflowId]
  );
  for (const child of childWorkflows) {
    await cancelWorkflow(child.id, "parent_cancelled");
  }
}

/**
 * Finalization — Cleanup after the drain phase completes.
 * Clean allocations, seal manifest, finalizeCancellation.
 * Note: child workflow cancellation is in Phase 1 (handleCancellingInit).
 */
async function handleCancellingFinalize(workflowId: number): Promise<void> {
  const nodes = getWorkflowNodes(workflowId);

  // Complete any ACTIVE/CLAIMED allocations for this workflow's run.
  const allocNode = nodes.find(n => n.node_id === "create-allocation" && n.output_json);
  if (allocNode?.output_json) {
    try {
      const allocOutput = JSON.parse(allocNode.output_json);
      if (allocOutput.allocationId) {
        const { completeAllocation } = await import("../state-transitions");
        const result = completeAllocation(allocOutput.allocationId);
        if (!result.success) {
          const { failAllocationAnyState } = await import("../state-transitions");
          failAllocationAnyState(allocOutput.allocationId);
        }
      }
    } catch (err) {
      console.warn("[workflow] Failed to complete allocation on cancellation", {
        workflowId,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  }

  // Seal manifest
  const workflow = getWorkflow(workflowId);
  if (workflow?.manifest_id) {
    try {
      sealManifestSafe(workflow.manifest_id);
    } catch (err) {
      console.warn("[workflow] Failed to seal manifest on cancellation", {
        workflowId,
        error: err,
      });
    }
  }

  // Finalize the cancellation transition (cancelling -> cancelled).
  const cancellationResult = finalizeCancellation(workflowId);
  cancelInitDone.delete(workflowId);
  if (!cancellationResult.success) {
    return;
  }

  // Sync run.workflow_state and emit run:finished (#3.01/#3.04)
  const wfInput = workflow?.input_json ? JSON.parse(workflow.input_json) : {};
  if (wfInput.runId) {
    try {
      updateRunRecord(wfInput.runId, {
        workflow_state: "launch-run:cancelled",
        finished_at: Date.now(),
      });
    } catch (err) {
      console.warn("[workflow] Failed to sync run state on cancellation", {
        workflowId,
        runId: wfInput.runId,
        error: err instanceof Error ? err.message : String(err),
      });
    }
    stateEvents.emit(STATE_EVENT.RUN_FINISHED, {
      runId: wfInput.runId,
      exitCode: null,
      spotInterrupted: false,
    });
  }

  workflowEvents.emit("workflow_failed", {
    workflowId,
    error: "Workflow cancelled",
    timestamp: Date.now(),
  });
}

/**
 * Phase 2 — Polling drain loop (called each execute loop iteration while status='cancelling').
 *
 * Checks three conditions in order:
 * a) Running nodes still active? Let them complete normally.
 * b) Compensation timeout exceeded? Force-fail remaining running nodes.
 * c) Agent ack or verification timeout? Check run_complete or CANCEL_VERIFICATION_MS elapsed.
 *
 * Returns true when finalization is complete (caller should break the loop).
 * Returns false when drain is still in progress (caller should continue polling).
 */
export async function handleCancellingPoll(
  workflowId: number,
  workflow: Workflow
): Promise<boolean> {
  // Phase 1: one-time init (skip pending, send SSE, cancel children)
  await handleCancellingInit(workflowId);

  const nodes = getWorkflowNodes(workflowId);
  const runningNodes = nodes.filter(n => n.status === "running");

  // Condition (a)+(b): Running nodes still active
  if (runningNodes.length > 0) {
    const cancelAnchorTime = workflow.updated_at;
    const elapsed = Date.now() - cancelAnchorTime;

    if (elapsed > TIMING.CANCEL_COMPENSATION_TIMEOUT_MS) {
      // Compensation timeout exceeded — force-fail remaining running nodes.
      console.warn("[workflow] Cancel compensation timeout exceeded, force-failing running nodes", {
        workflowId,
        runningNodeCount: runningNodes.length,
        elapsedMs: elapsed,
      });
      for (const node of runningNodes) {
        failNode(node.id, JSON.stringify({
          code: "CANCEL_COMPENSATION_TIMEOUT",
          message: "Node force-failed: cancellation compensation timeout exceeded",
        }));
      }
      // Fall through to finalization
    } else {
      // Still draining — continue polling
      return false;
    }
  }

  // All nodes are now terminal. Condition (c): agent ack or verification timeout.
  const wfInput = workflow.input_json ? JSON.parse(workflow.input_json) : {};
  if (wfInput.runId) {
    const cancelAnchorTime = workflow.updated_at;
    const elapsed = Date.now() - cancelAnchorTime;

    const { getRun } = await import("../../material/db");
    const run = getRun(wfInput.runId);
    const agentAcked = run?.finished_at != null;

    if (!agentAcked && elapsed < TIMING.CANCEL_VERIFICATION_MS) {
      return false;
    }
  }

  // All conditions satisfied — finalize
  await handleCancellingFinalize(workflowId);
  return true;
}

/**
 * Legacy handleCancellation — immediate (non-polling) cancellation handler.
 * Used by lifecycle.ts cancelWorkflow() for crash recovery and by the legacy
 * handleCancellation export. Performs Phase 1 + finalization in a single call.
 */
export async function handleCancellation(workflowId: number): Promise<void> {
  await handleCancellingInit(workflowId);
  await handleCancellingFinalize(workflowId);
}
