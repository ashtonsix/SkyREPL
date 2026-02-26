// workflow/engine/executor.ts - executeLoop, _executeLoopInner, executeNode

import type {
  WorkflowNode,
} from "../../material/db";
import {
  getWorkflow,
  getWorkflowNodes,
  findReadyNodes,
} from "../../material/db";
import { TIMING } from "@skyrepl/contracts";
import { Value } from "@sinclair/typebox/value";
import {
  startWorkflow,
  failWorkflow as failWorkflowTransition,
  startNode,
  completeNode,
  failNode,
  skipNode,
} from "../state-transitions";
import { workflowEvents } from "../events";
import { buildNodeContext } from "./context";
import { normalizeToNodeError, createTimeout, sleep, sealManifestSafe, POLL_INTERVAL_MS } from "./helpers";
import { determineRetryStrategy, handleRetry, handlePfoBranchFailure, handleWorkflowTimeout, handleWorkflowComplete, handleWorkflowFailure, handleCancellation } from "./retry";

// =============================================================================
// Module-level state (shared with lifecycle.ts)
// =============================================================================

// These are imported by lifecycle.ts for shutdown coordination.
// The activeLoops set tracks in-flight execute loops for graceful shutdown.
export const activeLoops = new Set<Promise<void>>();
// Tracks which workflowIds currently have an active executeLoop to prevent
// duplicate loops (e.g., when recoverWorkflows races with submit).
export const activeWorkflowIds = new Set<number>();
let shutdownRequested = false;

export function isShutdownRequested(): boolean {
  return shutdownRequested;
}

export function _setShutdownRequested(value: boolean): void {
  shutdownRequested = value;
}

export function _clearActiveLoops(): void {
  activeLoops.clear();
  activeWorkflowIds.clear();
}

// The blueprints and nodeExecutors maps live in lifecycle.ts (the registries).
// We import them at call time to avoid circular dependency issues at module load.

// =============================================================================
// Execute Loop
// =============================================================================

export async function executeLoop(workflowId: number): Promise<void> {
  // Track in-flight node execution promises (fire-and-forget with tracking)
  const inFlight = new Map<string, Promise<void>>();

  const loopPromise = _executeLoopInner(workflowId, inFlight);
  activeLoops.add(loopPromise);
  try {
    await loopPromise;
  } finally {
    activeLoops.delete(loopPromise);
  }
}

async function _executeLoopInner(
  workflowId: number,
  inFlight: Map<string, Promise<void>>,
): Promise<void> {
  try {
    // Transition workflow to 'running'
    const startResult = startWorkflow(workflowId);
    if (!startResult.success) {
      // Another loop is already executing this workflow (e.g., from recoverWorkflows).
      // Only continue if the workflow is in a 'cancelling' state that still needs handling.
      const wf = getWorkflow(workflowId);
      if (!wf || wf.status !== "cancelling") {
        return; // Another loop is handling this workflow
      }
      // Fall through: cancelling workflows need handleCancellation even without startWorkflow success
    }

    while (true) {
      // Check engine shutdown signal before any DB access
      if (shutdownRequested) {
        console.log(`[workflow] Engine shutdown requested, abandoning workflow ${workflowId}`);
        break;
      }

      // Reload workflow from DB
      const workflow = getWorkflow(workflowId);
      if (!workflow) {
        console.error("[workflow] Workflow not found in executeLoop", {
          workflowId,
        });
        break;
      }

      // Check cancellation (handle both 'cancelling' intermediate and 'cancelled' terminal)
      if (workflow.status === "cancelling" || workflow.status === "cancelled") {
        await handleCancellation(workflowId);
        break;
      }

      // Check for paused state -- spin-wait until resumed, cancelled, or shutdown
      if (workflow.status === "paused") {
        await sleep(POLL_INTERVAL_MS);
        continue;
      }

      // Check workflow-level timeout
      if (workflow.timeout_at && Date.now() > workflow.timeout_at) {
        await handleWorkflowTimeout(workflowId);
        break;
      }

      // Find ready nodes
      const readyNodes = findReadyNodes(workflowId);

      if (readyNodes.length === 0) {
        const allNodes = getWorkflowNodes(workflowId);
        const pending = allNodes.filter((n) => n.status === "pending").length;
        const running = allNodes.filter((n) => n.status === "running").length;

        // Count truly unhandled failures: exclude failed nodes whose CB fallback ran
        const failedNodes = allNodes.filter((n) => n.status === "failed");
        const handledByFallback = new Set<string>();
        for (const n of allNodes) {
          if (n.retry_reason?.startsWith("cb_fallback_for:") &&
              (n.status === "completed" || n.status === "skipped")) {
            handledByFallback.add(n.retry_reason.replace("cb_fallback_for:", ""));
          }
        }
        const unhandledFailed = failedNodes.filter(n => !handledByFallback.has(n.node_id)).length;

        if (running > 0) {
          if (inFlight.size > 0) {
            // Await any in-flight node completion, with periodic wake-up for timeout checks
            await Promise.race([...inFlight.values(), sleep(POLL_INTERVAL_MS)]);
          } else {
            // Crash recovery: nodes running in DB but not tracked locally
            await sleep(POLL_INTERVAL_MS);
          }
          continue;
        }
        if (pending === 0 && unhandledFailed === 0) {
          // All nodes completed (or failed with successful fallback)
          await handleWorkflowComplete(workflowId, allNodes);
          break;
        }
        if (unhandledFailed > 0) {
          // Unrecoverable failure
          await handleWorkflowFailure(workflowId, allNodes);
          break;
        }
        if (pending > 0 && unhandledFailed === 0) {
          // Stuck: pending nodes exist but none are ready and none are running.
          // Caused by an unresolvable dependency (e.g., depends on a non-existent node).
          // This state cannot self-resolve — fail immediately to avoid spinning CPU.
          const errorJson = JSON.stringify({
            code: "UNRESOLVABLE_DEPENDENCY",
            message: "Workflow stuck: pending nodes with no ready or running nodes and no failures — likely a dependency on a non-existent node",
          });
          failWorkflowTransition(workflowId, errorJson);
          const wf = getWorkflow(workflowId);
          if (wf?.manifest_id) {
            sealManifestSafe(wf.manifest_id);
          }
          break;
        }
      }

      // Fire-and-forget: start ready nodes, track promises
      for (const node of readyNodes) {
        const p = executeNode(workflowId, node).finally(() => {
          inFlight.delete(node.node_id);
        });
        inFlight.set(node.node_id, p);
      }

      // Wait for at least one node to complete before re-checking for newly-unblocked nodes
      // Include periodic wake-up so timeout/cancellation checks run even during long-running nodes
      if (inFlight.size > 0) {
        await Promise.race([...inFlight.values(), sleep(POLL_INTERVAL_MS)]);
      }
    }
  } catch (err) {
    // Catch any unhandled error: fail the workflow
    console.error("[workflow] executeLoop caught unhandled error", {
      workflowId,
      error: err,
    });
    try {
      const errorJson = JSON.stringify({
        code: "INTERNAL_ERROR",
        message: err instanceof Error ? err.message : String(err),
      });
      failWorkflowTransition(workflowId, errorJson);
      // Step 11: seal manifest on crash path so it doesn't stay DRAFT forever
      const wf = getWorkflow(workflowId);
      if (wf?.manifest_id) {
        sealManifestSafe(wf.manifest_id);
      }
    } catch (failErr) {
      console.error("[workflow] Failed to fail workflow after error", {
        workflowId,
        error: failErr,
      });
    }
  }
}

// =============================================================================
// Execute Node
// =============================================================================

export async function executeNode(
  workflowId: number,
  node: WorkflowNode
): Promise<void> {
  // Import registries from lifecycle.ts (resolved at call time, not module load)
  const { getNodeExecutor, tryGetBlueprint } = await import("./lifecycle");

  // Get executor from registry
  const executor = getNodeExecutor(node.node_type);
  if (!executor) {
    // Must transition to running first, then fail (failNode requires running status)
    startNode(node.id);
    failNode(node.id, JSON.stringify({ code: "UNKNOWN_NODE_TYPE", message: `No executor registered for node type: ${node.node_type}` }));
    return;
  }

  // Mark node running BEFORE execution (crash recovery semantics)
  const transition = startNode(node.id);
  if (!transition.success) {
    // Another runner already started this node (RACE_LOST or WRONG_STATE)
    return;
  }

  // Hook 1: node_started
  workflowEvents.emit("node_started", {
    workflowId,
    nodeId: node.node_id,
    nodeType: node.node_type,
    timestamp: Date.now(),
  });

  // Build NodeContext
  const ctx = buildNodeContext(workflowId, node);

  // Pre-execution validation (§6.2): if validate() rejects, fail immediately
  // with VALIDATION_ERROR. No retries, no compensation (node never executed).
  if (executor.validate) {
    try {
      await executor.validate(ctx);
    } catch (validationError) {
      const nodeError = normalizeToNodeError(validationError);
      // Force VALIDATION_ERROR code if not already set
      if (nodeError.code !== "VALIDATION_ERROR") {
        nodeError.code = "VALIDATION_ERROR";
        nodeError.category = "validation";
      }
      // Mark as compensated: true — node never executed, nothing to compensate.
      // This prevents handleWorkflowFailure safety net from calling compensate().
      failNode(node.id, JSON.stringify({ ...nodeError, compensated: true }));
      workflowEvents.emit("node_failed", {
        workflowId,
        nodeId: node.node_id,
        nodeType: node.node_type,
        error: nodeError.message,
        timestamp: Date.now(),
      });
      return;
    }
  }

  try {
    // Execute with node-level timeout
    let nodeTimeoutMs = TIMING.DEFAULT_NODE_TIMEOUT_MS;
    const workflow = getWorkflow(workflowId);
    if (workflow) {
      const bp = tryGetBlueprint(workflow.type);
      const nodeDef = bp?.nodes[node.node_id];
      if (nodeDef?.timeout) {
        nodeTimeoutMs = nodeDef.timeout;
      }
    }
    const timeout = createTimeout(nodeTimeoutMs);
    try {
      const output = await Promise.race([
        executor.execute(ctx),
        timeout.promise,
      ]);
      timeout.clear();

      // Validate node output against schema (from blueprint)
      const wfForSchema = getWorkflow(workflowId);
      const bpForSchema = wfForSchema ? tryGetBlueprint(wfForSchema.type) : undefined;
      const outputSchema = bpForSchema?.nodeOutputSchemas?.[node.node_type];
      if (outputSchema && output && typeof output === "object") {
        if (!Value.Check(outputSchema, output)) {
          const errors = [...Value.Errors(outputSchema, output)];
          console.warn(`[workflow] Node output validation failed for ${node.node_type}`, {
            errors: errors.map(e => ({ path: e.path, message: e.message })),
          });
        }
      }

      // Mark node completed AFTER success
      const outputRecord =
        output && typeof output === "object" ? (output as Record<string, unknown>) : {};
      completeNode(node.id, outputRecord);

      // CB try-fallback: if this node succeeded and a fallback node exists for it, skip it (§6.3.2)
      const allNodesForCb = getWorkflowNodes(workflowId);
      for (const candidate of allNodesForCb) {
        if (candidate.retry_reason === `cb_fallback_for:${node.node_id}` && candidate.status === "pending") {
          skipNode(candidate.id);
        }
      }

      // Hook 2: node_completed
      workflowEvents.emit("node_completed", {
        workflowId,
        nodeId: node.node_id,
        nodeType: node.node_type,
        output: outputRecord,
        timestamp: Date.now(),
      });
    } catch (error) {
      timeout.clear();
      throw error;
    }
  } catch (error) {
    // Normalize error to NodeError shape
    const nodeError = normalizeToNodeError(error);

    // Re-read node from DB to get post-startNode attempt value (SM-05 fix)
    const freshNodes = getWorkflowNodes(workflowId);
    const freshNode = freshNodes.find(n => n.node_id === node.node_id);
    const currentAttempt = freshNode?.attempt ?? node.attempt;

    // Determine retry strategy
    const decision = determineRetryStrategy(nodeError, currentAttempt);

    if (decision.shouldRetry) {
      await handleRetry(workflowId, freshNode ?? node, nodeError, decision);
    } else {
      // No retry - fail the node
      failNode(node.id, JSON.stringify(nodeError));

      // Hook 3: node_failed
      workflowEvents.emit("node_failed", {
        workflowId,
        nodeId: node.node_id,
        nodeType: node.node_type,
        error: nodeError.message,
        timestamp: Date.now(),
      });

      // PFO first-failure-cancels: if this is a PFO branch, check join mode
      if (node.retry_reason === "pfo_branch") {
        handlePfoBranchFailure(workflowId, node.node_id);
      }

      // Compensate the failed node if handler exists
      if (executor?.compensate) {
        const { compensateFailedNode } = await import("../compensation");
        const result = await compensateFailedNode(workflowId, node.node_id);
        if (!result.success) {
          console.warn("[workflow] Compensation failed for node", {
            workflowId,
            nodeId: node.node_id,
            error: result.error?.message,
          });
        }
      }
    }
  }
}
