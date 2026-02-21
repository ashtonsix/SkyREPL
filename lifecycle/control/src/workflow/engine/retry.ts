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
  SPOT_INTERRUPTED: { retryable: true, strategy: "fallback", maxRetries: 1 },

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
  // Respects retry_after_ms from error details if present (RATE_LIMITED)
  const delayMs =
    mapping.strategy === "exponential_backoff"
      ? Math.min(
          Math.pow(2, attempt) * TIMING.RETRY_BASE_DELAY_MS,
          TIMING.RETRY_MAX_DELAY_MS
        )
      : (error.details?.retry_after_ms as number) ?? 0;

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
        const alternativeRegions = error.details?.alternative_regions as
          | string[]
          | undefined;
        const alternativeRegion = alternativeRegions?.[0];
        if (alternativeRegion) {
          const currentInput = node.input_json
            ? JSON.parse(node.input_json)
            : {};
          applyRetryWithAlternative(workflowId, node.node_id, {
            id: `${node.node_id}-alt-${node.attempt + 1}`,
            type: node.node_type,
            input: { ...currentInput, region: alternativeRegion },
          });
        }
      }
      break;

    case "fallback":
      // Spot interrupted: retry spot once, then fallback to on-demand
      if (node.attempt === 0) {
        failNode(node.id, JSON.stringify({ code: error.code, message: error.message, retried_with: "fallback_spot_retry" }));
        resetNodeForRetry(node.id, "spot_retry");
      } else {
        // Fail the node, apply conditional branch for on-demand fallback
        failNode(
          node.id,
          JSON.stringify({
            code: error.code,
            message: error.message,
            retried_with: "fallback",
          })
        );
        const currentInput = node.input_json
          ? JSON.parse(node.input_json)
          : {};
        // NOT YET IMPLEMENTED -- deferred to future slice
        // CB pattern stub - full implementation needs a joinNode
        // For Slice 1, just log the intent
        console.warn(
          "[workflow] Fallback pattern triggered but CB requires joinNode config",
          { workflowId, nodeId: node.node_id, input: currentInput }
        );
      }
      break;
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
    }
    // Note: running siblings will eventually complete or fail on their own;
    // the DAG engine handles the final failure via handleWorkflowFailure.
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
// Cancellation
// =============================================================================

export async function handleCancellation(workflowId: number): Promise<void> {
  const nodes = getWorkflowNodes(workflowId);

  // Skip pending nodes
  for (const node of nodes) {
    if (node.status === "pending") {
      skipNode(node.id);
    }
  }

  // Cancel child workflows (#WF-03: cancel propagation)
  // NOTE: cancelWorkflow is imported from lifecycle.ts -- circular import resolved at call time
  const { cancelWorkflow } = await import("./lifecycle");
  const childWorkflows = queryMany<Workflow>(
    "SELECT * FROM workflows WHERE parent_workflow_id = ? AND status NOT IN ('completed', 'failed', 'cancelled')",
    [workflowId]
  );
  for (const child of childWorkflows) {
    await cancelWorkflow(child.id, "parent_cancelled");
  }

  // Complete any ACTIVE/CLAIMED allocations for this workflow's run.
  // The finalize-run node (which normally calls completeAllocation) was
  // skipped, so we must clean up here to avoid stuck ACTIVE allocations.
  const allocNode = nodes.find(n => n.node_id === "create-allocation" && n.output_json);
  if (allocNode?.output_json) {
    try {
      const allocOutput = JSON.parse(allocNode.output_json);
      if (allocOutput.allocationId) {
        const { completeAllocation } = await import("../state-transitions");
        const result = completeAllocation(allocOutput.allocationId);
        if (!result.success) {
          // Try failing it instead (may be CLAIMED, not ACTIVE)
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

  // Finalize the cancellation transition
  cancelWorkflowTransition(workflowId);

  // Emit workflow_failed event so SSE clients learn about the cancellation
  workflowEvents.emit("workflow_failed", {
    workflowId,
    error: "Workflow cancelled",
    timestamp: Date.now(),
  });
}
