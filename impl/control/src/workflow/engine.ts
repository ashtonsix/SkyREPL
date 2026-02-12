// workflow/engine.ts - Workflow Execution Engine

import type {
  NodeExecutor,
  WorkflowSubmission,
  WorkflowSubmissionResult,
  WorkflowBlueprint,
  NodeContext,
  RetryDecision,
  NodeError,
  WorkflowError,
  SubworkflowHandle,
  SubworkflowResult,
} from "./engine.types";
import {
  getWorkflow,
  getWorkflowNodes,
  createWorkflow,
  createWorkflowNode,
  updateWorkflow,
  updateWorkflowNode,
  createManifest,
  getManifest,
  addResourceToManifest,
  queryOne,
  queryMany,
  transaction,
  findReadyNodes,
  findActiveWorkflows,
  type Workflow,
  type WorkflowNode,
} from "../material/db";
import { TIMING } from "@skyrepl/shared";
import { Value } from "@sinclair/typebox/value";
import { NODE_OUTPUT_SCHEMAS, WORKFLOW_INPUT_SCHEMAS } from "../intent/launch-run.schema";
import {
  startWorkflow,
  completeWorkflow as completeWorkflowTransition,
  failWorkflow as failWorkflowTransition,
  cancelWorkflow as cancelWorkflowTransition,
  pauseWorkflow as pauseWorkflowTransition,
  resumeWorkflow as resumeWorkflowTransition,
  startNode,
  completeNode,
  failNode,
  resetNodeForRetry,
  skipNode,
  sealManifest as sealManifestTransition,
} from "./state-transitions";
import {
  applyConditionalBranch,
  applyRetryWithAlternative,
  applyInsertAndReconverge,
  applyParallelFanOut,
  type InsertAndReconvergeConfig,
  type ParallelFanOutConfig,
} from "./patterns";

// =============================================================================
// Constants
// =============================================================================

export const MAX_SUBWORKFLOW_DEPTH = 3;
export const MAX_PARALLEL_BRANCHES = 16;

/** Polling interval for crash recovery: nodes running in DB but not tracked locally */
const POLL_INTERVAL_MS = 100;

// =============================================================================
// Engine Shutdown Coordination
// =============================================================================

let shutdownRequested = false;
const activeLoops = new Set<Promise<void>>();

export function requestEngineShutdown(): void {
  shutdownRequested = true;
}

export function isEngineShutdownRequested(): boolean {
  return shutdownRequested;
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
  shutdownRequested = false;
  activeLoops.clear();
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

// =============================================================================
// Blueprint Registry
// =============================================================================

const blueprints = new Map<string, WorkflowBlueprint>();

export function registerBlueprint(blueprint: WorkflowBlueprint): void {
  blueprints.set(blueprint.type, blueprint);
}

export function getBlueprint(type: string): WorkflowBlueprint {
  const bp = blueprints.get(type);
  if (!bp) {
    throw new Error(`Blueprint not found: ${type}`);
  }
  return bp;
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
  const inputSchema = WORKFLOW_INPUT_SCHEMAS[request.type];
  if (inputSchema) {
    Value.Parse(inputSchema, request.input);
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
  });

  // Create DRAFT manifest for resource tracking
  const manifest = createManifest(workflow.id);

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
      attempt: 1,
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
    } catch (_) { /* already terminal or DB unavailable */ }
  });

  return { workflowId: workflow.id, status: "created" };
}

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

      // Check for paused state — spin-wait until resumed, cancelled, or shutdown
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
  // Get executor from registry
  const executor = nodeExecutors.get(node.node_type);
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

  // Build NodeContext
  const ctx = buildNodeContext(workflowId, node);

  try {
    // Execute with node-level timeout
    let nodeTimeoutMs = TIMING.DEFAULT_NODE_TIMEOUT_MS;
    const workflow = getWorkflow(workflowId);
    if (workflow) {
      const bp = blueprints.get(workflow.type);
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

      // Validate node output against schema
      const outputSchema = NODE_OUTPUT_SCHEMAS[node.node_type];
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
    } catch (error) {
      timeout.clear();
      throw error;
    }
  } catch (error) {
    // Normalize error to NodeError shape
    const nodeError = normalizeToNodeError(error);

    // Determine retry strategy
    const decision = determineRetryStrategy(nodeError, node.attempt);

    if (decision.shouldRetry) {
      await handleRetry(workflowId, node, nodeError, decision);
    } else {
      // No retry - fail the node
      failNode(node.id, JSON.stringify(nodeError));

      // PFO first-failure-cancels: if this is a PFO branch, check join mode
      if (node.retry_reason === "pfo_branch") {
        handlePfoBranchFailure(workflowId, node.node_id);
      }

      // Compensate the failed node if handler exists
      if (executor?.compensate) {
        const { compensateFailedNode } = await import("./compensation");
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

// =============================================================================
// Build Node Context
// =============================================================================

export function buildNodeContext(
  workflowId: number,
  node: WorkflowNode
): NodeContext {
  const workflow = getWorkflow(workflowId);
  const manifestId = workflow?.manifest_id ?? 0;

  const ctx: NodeContext = {
    workflowId,
    nodeId: node.node_id,
    input: node.input_json ? JSON.parse(node.input_json) : {},
    workflowInput: workflow?.input_json ? JSON.parse(workflow.input_json) : {},
    manifestId,

    emitResource(type: string, id: number | string, cleanupPriority: number): void {
      try {
        addResourceToManifest(manifestId, type, String(id), {
          cleanupPriority,
        });
      } catch (err) {
        console.error("[workflow] emitResource failed", {
          workflowId,
          nodeId: node.node_id,
          type,
          id,
          error: err,
        });
      }
    },

    async claimResource(
      _targetManifestId: number,
      resourceType: string,
      resourceId: string
    ): Promise<boolean> {
      const { claimResourceAtomic } = await import("../resource/manifest");
      return claimResourceAtomic(manifestId, resourceType, resourceId);
    },

    applyPattern(pattern: string, config: unknown): void {
      // Delegate to pattern functions
      switch (pattern) {
        case "conditional-branch":
          applyConditionalBranch(
            workflowId,
            config as Parameters<typeof applyConditionalBranch>[1]
          );
          break;
        case "retry-with-alternative":
          {
            const rwaConfig = config as {
              failedNodeId: string;
              alternativeNode: Parameters<typeof applyRetryWithAlternative>[2];
            };
            applyRetryWithAlternative(
              workflowId,
              rwaConfig.failedNodeId,
              rwaConfig.alternativeNode
            );
          }
          break;
        case "insert-and-reconverge":
          applyInsertAndReconverge(
            workflowId,
            config as InsertAndReconvergeConfig
          );
          break;
        case "parallel-fan-out":
          applyParallelFanOut(
            workflowId,
            config as ParallelFanOutConfig
          );
          break;
        default:
          console.warn("[workflow] Unknown pattern", { pattern });
      }
    },

    getNodeOutput(nodeId: string): unknown {
      const targetNode = queryOne<WorkflowNode>(
        "SELECT * FROM workflow_nodes WHERE workflow_id = ? AND node_id = ?",
        [workflowId, nodeId]
      );
      if (!targetNode || !targetNode.output_json) return null;
      return JSON.parse(targetNode.output_json);
    },

    log(
      level: "info" | "warn" | "error" | "debug",
      message: string,
      data?: Record<string, unknown>
    ): void {
      // Console-based logging for now; OTel deferred
      const logFn =
        level === "error"
          ? console.error
          : level === "warn"
            ? console.warn
            : level === "debug"
              ? console.debug
              : console.log;
      logFn(`[workflow:${node.node_type}] ${message}`, {
        workflowId,
        nodeId: node.node_id,
        ...data,
      });
    },

    checkCancellation(): void {
      const current = getWorkflow(workflowId);
      if (current && current.status === "cancelled") {
        throw Object.assign(new Error("Workflow cancelled"), {
          code: "CANCELLED",
          category: "internal" as const,
        });
      }
    },

    async sleep(ms: number): Promise<void> {
      await sleep(ms);
    },

    async spawnSubworkflow(
      type: string,
      input: unknown
    ): Promise<SubworkflowHandle> {
      const wf = getWorkflow(workflowId);
      const depth = (wf?.depth ?? 0) + 1;
      if (depth > MAX_SUBWORKFLOW_DEPTH) {
        throw new Error(
          `Max subworkflow depth (${MAX_SUBWORKFLOW_DEPTH}) exceeded`
        );
      }

      const parentId = workflowId; // capture parent workflow ID to avoid shadowing

      const result = await submit({
        type,
        input: input as Record<string, unknown>,
        parentWorkflowId: parentId,
      });

      return {
        workflowId: result.workflowId,
        async wait(): Promise<SubworkflowResult> {
          // Poll until subworkflow completes, with parent cancellation awareness
          while (true) {
            // Check if parent was cancelled or failed
            const parent = getWorkflow(parentId);
            if (parent && (parent.status === "cancelled" || parent.status === "failed")) {
              await cancelWorkflow(result.workflowId, "parent_cancelled");
            }

            const sub = getWorkflow(result.workflowId);
            if (!sub) {
              return { status: "failed", error: "Subworkflow not found" };
            }
            if (
              sub.status === "completed" ||
              sub.status === "failed" ||
              sub.status === "cancelled"
            ) {
              return {
                status: sub.status as SubworkflowResult["status"],
                output: sub.output_json
                  ? JSON.parse(sub.output_json)
                  : undefined,
                error: sub.error_json ?? undefined,
                manifestId: sub.manifest_id ?? undefined,
              };
            }
            await sleep(POLL_INTERVAL_MS);
          }
        },
      };
    },
  };

  return ctx;
}

// =============================================================================
// Retry Strategy
// =============================================================================

const ERROR_RETRY_MAPPING: Record<
  string,
  {
    retryable: boolean;
    strategy: "exponential_backoff" | "alternative" | "fallback";
    maxRetries: number;
  }
> = {
  CAPACITY_UNAVAILABLE: {
    retryable: true,
    strategy: "alternative",
    maxRetries: 3,
  },
  PROVIDER_API_ERROR: {
    retryable: true,
    strategy: "exponential_backoff",
    maxRetries: 3,
  },
  PROVIDER_INTERNAL: {
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
  SPOT_INTERRUPTED: { retryable: true, strategy: "fallback", maxRetries: 1 },
  OPERATION_TIMEOUT: {
    retryable: true,
    strategy: "exponential_backoff",
    maxRetries: 2,
  },
  // Non-retryable errors
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
  INSUFFICIENT_PERMISSIONS: {
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
        const { compensateFailedNode } = await import("./compensation");
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
        // NOT YET IMPLEMENTED — deferred to future slice
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

function handlePfoBranchFailure(workflowId: number, failedBranchNodeId: string): void {
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

async function handleWorkflowComplete(
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
}

async function handleWorkflowFailure(
  workflowId: number,
  nodes: WorkflowNode[]
): Promise<void> {
  // Run reverse-order compensation on completed nodes
  try {
    const { compensateWorkflow } = await import("./compensation");
    await compensateWorkflow(workflowId);
  } catch (err) {
    console.warn("[workflow] Compensation rollback failed", {
      workflowId,
      error: err instanceof Error ? err.message : String(err),
    });
  }

  // Find the first failed node for error context
  const failedNode = nodes.find((n) => n.status === "failed");
  const errorJson = failedNode?.error_json ?? JSON.stringify({
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

  // Already in terminal state — idempotent success (EX7 fix)
  const terminalStates = ["completed", "failed", "cancelled"];
  if (terminalStates.includes(workflow.status)) {
    return { success: true, status: workflow.status };
  }

  // Only running or paused workflows can be cancelled
  if (workflow.status !== "running" && workflow.status !== "paused") {
    return { success: false, status: workflow.status };
  }

  // Atomically guarded cancellation (B5 fix)
  const result = cancelWorkflowTransition(workflowId);
  if (result.success) {
    // Update error_json separately after successful transition
    updateWorkflow(workflowId, {
      error_json: JSON.stringify({ code: "CANCELLED", reason }),
    });
    return { success: true, status: "cancelled" };
  }

  return { success: false, status: workflow.status };
}

export async function handleCancellation(workflowId: number): Promise<void> {
  const nodes = getWorkflowNodes(workflowId);

  // Skip pending nodes
  for (const node of nodes) {
    if (node.status === "pending") {
      skipNode(node.id);
    }
  }

  // Cancel child workflows (#WF-03: cancel propagation)
  const childWorkflows = queryMany<Workflow>(
    "SELECT * FROM workflows WHERE parent_workflow_id = ? AND status NOT IN ('completed', 'failed', 'cancelled')",
    [workflowId]
  );
  for (const child of childWorkflows) {
    await cancelWorkflow(child.id, "parent_cancelled");
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

  // Finalize the cancellation transition
  cancelWorkflowTransition(workflowId);
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
          // Idempotent node: safe to retry — first fail, then reset to pending
          const failResult = failNode(node.id, JSON.stringify({
            code: "CRASH_RECOVERY",
            message: "Node was running during crash — idempotent, resetting for retry",
            category: "crash",
          }));
          if (failResult.success) {
            resetNodeForRetry(node.id, "crash_recovery");
            console.log(`[workflow] Reset idempotent node ${node.node_id} to pending (crash recovery)`);
          }
        } else {
          // Non-idempotent node: unsafe to retry — mark failed
          failNode(node.id, JSON.stringify({
            code: "CRASH_RECOVERY",
            message: "Node was running during crash — non-idempotent, marked failed",
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

    if (hasResetNodes && !hasFailedNodes) {
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
// Error Mapping
// =============================================================================

export function mapErrorToHttpStatus(error: WorkflowError): number {
  switch (error.category) {
    case "validation":
      return 400;
    case "auth":
      return error.code === "UNAUTHORIZED" ? 401 : 403;
    case "not_found":
      return 404;
    case "conflict":
      return 409;
    case "rate_limit":
      return 429;
    case "timeout":
      return 504;
    case "provider":
      return 502;
    case "internal":
      return 500;
    default:
      return 500;
  }
}

// =============================================================================
// Observability
// =============================================================================

export function calculateTopologySignature(workflowId: number): string {
  const nodes = getWorkflowNodes(workflowId);

  const structure = {
    nodes: nodes
      .filter((n) => n.status !== "skipped")
      .map((n) => ({
        type: n.node_type,
        deps: (
          n.depends_on ? (JSON.parse(n.depends_on) as string[]) : []
        ).sort(),
      }))
      .sort((a, b) => a.type.localeCompare(b.type)),
  };

  // Use Bun's built-in hashing
  const hasher = new Bun.CryptoHasher("sha256");
  hasher.update(JSON.stringify(structure));
  return hasher.digest("hex").slice(0, 16);
}

// =============================================================================
// Find Ready Nodes (in-memory version for engine callers)
// =============================================================================

/**
 * In-memory version: find ready nodes from a given array.
 * Used when we already have nodes loaded.
 */
export function findReadyNodesFromArray(
  nodes: WorkflowNode[]
): WorkflowNode[] {
  const completedOrSkipped = new Set(
    nodes
      .filter((n) => n.status === "completed" || n.status === "skipped")
      .map((n) => n.node_id)
  );

  return nodes.filter((n) => {
    if (n.status !== "pending") return false;
    const deps: string[] = n.depends_on
      ? JSON.parse(n.depends_on)
      : [];
    return deps.every((depId) => completedOrSkipped.has(depId));
  });
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

      const originalInput = workflow.input_json ? JSON.parse(workflow.input_json) : {};
      const originalKey = workflow.idempotency_key;
      const newKey = originalKey ? `${originalKey}:retry:${Date.now()}` : undefined;

      const result = await submit({
        type: workflow.type,
        input: originalInput,
        idempotencyKey: newKey,
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

// =============================================================================
// Helpers
// =============================================================================

/**
 * Seal a manifest using atomic state transition.
 * Computes expiresAt from manifest retention_ms or default 30 days.
 * Idempotent: returns success if manifest is already sealed (EX2 fix).
 */
function sealManifestSafe(manifestId: number): void {
  const manifest = getManifest(manifestId);
  if (!manifest) return;

  // Already sealed — idempotent success
  if (manifest.status === "SEALED") return;

  const now = Date.now();
  const expiresAt = manifest.retention_ms != null
    ? now + manifest.retention_ms
    : now + TIMING.DEFAULT_MANIFEST_RETENTION_MS;

  const result = sealManifestTransition(manifestId, expiresAt);
  if (!result.success) {
    console.warn(`[workflow] Failed to seal manifest ${manifestId}: ${result.reason}`);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function createTimeout(ms: number): { promise: Promise<never>; clear: () => void } {
  let timerId: Timer;
  const promise = new Promise<never>((_, reject) => {
    timerId = setTimeout(
      () =>
        reject(
          Object.assign(new Error("Operation timed out"), {
            code: "OPERATION_TIMEOUT",
            category: "timeout" as const,
            message: `Operation timed out after ${ms}ms`,
          })
        ),
      ms
    );
  });
  return { promise, clear: () => clearTimeout(timerId!) };
}

function normalizeToNodeError(error: unknown): NodeError {
  if (
    error &&
    typeof error === "object" &&
    "code" in error &&
    typeof (error as Record<string, unknown>).code === "string"
  ) {
    const e = error as Record<string, unknown>;
    return {
      code: e.code as string,
      message:
        (e.message as string) ??
        (error instanceof Error ? error.message : String(error)),
      category: (e.category as string) ?? "internal",
      details: (e.details as Record<string, unknown>) ?? undefined,
    };
  }
  if (error instanceof Error) {
    return {
      code: "INTERNAL_ERROR",
      message: error.message,
      category: "internal",
    };
  }
  return {
    code: "INTERNAL_ERROR",
    message: String(error),
    category: "internal",
  };
}
