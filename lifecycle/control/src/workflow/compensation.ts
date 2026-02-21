// workflow/compensation.ts - Compensation Framework (#WF-01)
// Single-node compensation only: compensate the failed node inline during
// executeNode. No cascade through completed nodes (§6.5, §7, §9).

import type { NodeContext } from "./engine.types";
import {
  getWorkflowNodes,
  updateWorkflowNode,
  queryOne,
  type WorkflowNode,
} from "../material/db";
import { getNodeExecutor, buildNodeContext, engineSleep } from "./engine";
import { TIMING } from "@skyrepl/contracts";

// =============================================================================
// Types
// =============================================================================

export interface CompensationResult {
  success: boolean;
  error?: Error;
  skippedReason?: "no_handler" | "resource_transferred" | "already_compensated";
}

// =============================================================================
// Retryable Error Codes (for compensation retry)
// =============================================================================

const RETRYABLE_CODES = ["RATE_LIMITED", "RATE_LIMIT_ERROR", "NETWORK_ERROR", "PROVIDER_INTERNAL", "TIMEOUT_ERROR"];

function isRetryableError(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  const code = (error as Record<string, unknown>).code;
  if (typeof code !== "string") return false;
  return RETRYABLE_CODES.some((rc) => code.includes(rc));
}

// =============================================================================
// compensateWithRetry - Timeout + retry wrapper
// =============================================================================

export async function compensateWithRetry(
  fn: () => Promise<void>,
  maxRetries = 3
): Promise<void> {
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    let timerId: Timer | undefined;
    try {
      // Race against compensation timeout (with proper cleanup to avoid SM-24 timer leak)
      const timeoutPromise = new Promise<never>((_, reject) => {
        timerId = setTimeout(
          () =>
            reject(
              Object.assign(new Error("Compensation timed out"), {
                code: "COMPENSATION_TIMEOUT",
              })
            ),
          TIMING.COMPENSATION_TIMEOUT_MS
        );
      });

      await Promise.race([fn(), timeoutPromise]);
      clearTimeout(timerId);
      // Success — return immediately
      return;
    } catch (error) {
      clearTimeout(timerId);
      if (attempt < maxRetries && isRetryableError(error)) {
        // Exponential backoff: 1s, 2s, 4s (uses engine sleep for test shimming)
        const delay = Math.pow(2, attempt) * 1000;
        await engineSleep(delay);
        continue;
      }
      // Non-retryable or final attempt — propagate
      throw error;
    }
  }
}

// =============================================================================
// shouldCompensate - Resource transfer check
// =============================================================================

export function shouldCompensate(
  workflowId: number,
  node: WorkflowNode
): { compensate: boolean; reason?: "resource_transferred" } {
  // Parse output_json to find any resource IDs emitted by this node
  if (!node.output_json) return { compensate: true };

  let output: Record<string, unknown>;
  try {
    output = JSON.parse(node.output_json);
  } catch {
    return { compensate: true };
  }

  // Check for common resource ID patterns in output
  const resourceIds: string[] = [];
  for (const key of ["instanceId", "resourceId", "allocationId"]) {
    if (output[key] != null) {
      resourceIds.push(String(output[key]));
    }
  }

  if (resourceIds.length === 0) return { compensate: true };

  // Check if any emitted resource has been claimed by another workflow's manifest
  for (const resourceId of resourceIds) {
    const transferred = queryOne<{ id: number }>(
      `SELECT 1 as id FROM manifest_resources mr
       JOIN workflows w ON w.manifest_id = mr.manifest_id
       WHERE mr.resource_id = ? AND w.id != ?`,
      [resourceId, workflowId]
    );
    if (transferred) {
      return { compensate: false, reason: "resource_transferred" };
    }
  }

  return { compensate: true };
}

// =============================================================================
// compensateFailedNode - Single-node compensation
// =============================================================================

export async function compensateFailedNode(
  workflowId: number,
  failedNodeId: string
): Promise<CompensationResult> {
  // Get the workflow node from DB
  const nodes = getWorkflowNodes(workflowId);
  const node = nodes.find((n) => n.node_id === failedNodeId);

  if (!node) {
    return { success: true, skippedReason: "already_compensated" };
  }

  // If node is not in a terminal-failure state, skip
  if (node.status !== "failed" && node.status !== "completed") {
    return { success: true, skippedReason: "already_compensated" };
  }

  // Check if already compensated
  if (node.error_json) {
    try {
      const errorData = JSON.parse(node.error_json);
      if (errorData.compensated) {
        return { success: true, skippedReason: "already_compensated" };
      }
    } catch {
      // Not valid JSON, continue
    }
  }

  // Check if resources have been transferred
  const check = shouldCompensate(workflowId, node);
  if (!check.compensate) {
    return { success: true, skippedReason: "resource_transferred" };
  }

  // Look up the node executor
  const executor = getNodeExecutor(node.node_type);
  if (!executor?.compensate) {
    return { success: true, skippedReason: "no_handler" };
  }

  // Build a NodeContext with output from the node
  const ctx = buildNodeContext(workflowId, node);
  if (node.output_json) {
    try {
      (ctx as { output?: unknown }).output = JSON.parse(node.output_json);
    } catch {
      // Invalid output_json, leave output undefined
    }
  }

  try {
    await compensateWithRetry(() => executor.compensate!(ctx));

    // Mark node as compensated by updating error_json
    const existingError = node.error_json
      ? (() => {
          try {
            return JSON.parse(node.error_json);
          } catch {
            return { raw: node.error_json };
          }
        })()
      : {};

    updateWorkflowNode(node.id, {
      error_json: JSON.stringify({ ...existingError, compensated: true }),
    });

    return { success: true };
  } catch (error) {
    const err = error instanceof Error ? error : new Error(String(error));
    console.error("[workflow] Compensation failed for node", {
      workflowId,
      nodeId: failedNodeId,
      error: err.message,
    });
    return { success: false, error: err };
  }
}

