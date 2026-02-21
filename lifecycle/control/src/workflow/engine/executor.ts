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
let shutdownRequested = false;

export function isShutdownRequested(): boolean {
  return shutdownRequested;
}

export function _setShutdownRequested(value: boolean): void {
  shutdownRequested = value;
}

export function _clearActiveLoops(): void {
  activeLoops.clear();
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
    startWorkflow(workflowId);

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

      // Check cancellation
      if (workflow.status === "cancelled") {
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
        const failed = allNodes.filter((n) => n.status === "failed").length;

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
        if (pending === 0 && failed === 0) {
          // All nodes completed successfully
          await handleWorkflowComplete(workflowId, allNodes);
          break;
        }
        if (failed > 0) {
          // Unrecoverable failure
          await handleWorkflowFailure(workflowId, allNodes);
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
