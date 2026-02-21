// workflow/patterns.ts - Workflow Pattern Implementations
// Stub: All function bodies throw "not implemented"

import {
  getWorkflowNodes,
  createWorkflowNode,
  updateWorkflowNode,
  queryOne,
  type WorkflowNode,
} from "../material/db";
import type { InlineNodeDef } from "./engine.types";

// =============================================================================
// Constants
// =============================================================================

export const MAX_PARALLEL_BRANCHES = 16;
export const MAX_RETRY_ATTEMPTS = 3;
export const MAX_IAR_DEPTH = 1;

// =============================================================================
// Pattern Types
// =============================================================================

export type PatternType =
  | "single-step"
  | "insert-and-reconverge"
  | "conditional-branch"
  | "parallel-fan-out"
  | "retry-with-alternative";

// =============================================================================
// Config Types
// =============================================================================

export interface InsertAndReconvergeConfig {
  insertedNode: InlineNodeDef;
  beforeNode: string;
}

export interface ConditionalBranchConfig {
  triggerOnError?: boolean;
  condition?: boolean;
  option1: InlineNodeDef;
  option2: InlineNodeDef;
  joinNode: string;
}

export interface ParallelFanOutConfig {
  branches: InlineNodeDef[];
  joinNode: string;
  joinMode?: "all-succeed" | "first-failure-cancels";
  sourceNodeId?: string;
}

// =============================================================================
// Insert-and-Reconverge (IAR)
// =============================================================================

export function applyInsertAndReconverge(
  workflowId: number,
  config: InsertAndReconvergeConfig
): void {
  const { insertedNode, beforeNode } = config;

  // 1. Find the target node
  const target = queryOne<WorkflowNode>(
    "SELECT * FROM workflow_nodes WHERE workflow_id = ? AND node_id = ?",
    [workflowId, beforeNode]
  );

  if (!target) {
    throw new Error(`Target node '${beforeNode}' not found`);
  }

  // 2. Validate target is pending
  if (target.status !== "pending") {
    throw new Error(
      `Target node '${beforeNode}' must be pending, got: ${target.status}`
    );
  }

  // 3. Check IAR depth — don't allow insertion before a node that was itself IAR-inserted
  if (target.retry_reason === "iar_inserted") {
    throw new Error(
      `Cannot apply IAR to node '${beforeNode}': already IAR-inserted (max depth ${MAX_IAR_DEPTH})`
    );
  }

  // 4. Get target's current dependencies
  const targetDeps: string[] = target.depends_on
    ? (JSON.parse(target.depends_on) as string[])
    : [];

  // 5. Create the inserted node with the SAME dependencies as the target
  //    (it runs after the same predecessors complete)
  createWorkflowNode({
    workflow_id: workflowId,
    node_id: insertedNode.id,
    node_type: insertedNode.type,
    status: "pending",
    input_json: JSON.stringify(insertedNode.input),
    output_json: null,
    error_json: null,
    depends_on: target.depends_on, // Same predecessors as target
    attempt: 0,
    retry_reason: "iar_inserted", // Mark as IAR-inserted for depth checking
    started_at: null,
    finished_at: null,
    updated_at: Date.now(),
  });

  // 6. Update the target node to ALSO depend on the inserted node
  //    Target now depends on: [...originalDeps, insertedNode.id]
  const newDeps = [...targetDeps, insertedNode.id];
  updateWorkflowNode(target.id, {
    depends_on: JSON.stringify(newDeps),
  });
}

// =============================================================================
// Conditional Branch (CB)
// =============================================================================

export function applyConditionalBranch(
  workflowId: number,
  config: ConditionalBranchConfig
): void {
  // Validate join node exists
  const joinNode = queryOne<WorkflowNode>(
    "SELECT * FROM workflow_nodes WHERE workflow_id = ? AND node_id = ?",
    [workflowId, config.joinNode]
  );

  if (!joinNode) {
    throw new Error(`Join node '${config.joinNode}' not found`);
  }

  if (config.triggerOnError) {
    // Mode 2: Try-fallback mode
    // Create option1 node as 'pending'
    createWorkflowNode({
      workflow_id: workflowId,
      node_id: config.option1.id,
      node_type: config.option1.type,
      status: "pending",
      input_json: JSON.stringify(config.option1.input),
      output_json: null,
      error_json: null,
      depends_on: JSON.stringify([]),
      attempt: 0,
      retry_reason: null,
      started_at: null,
      finished_at: null,
      updated_at: Date.now(),
    });

    // Create option2 node as 'pending' with trigger_on_failure_of
    // Note: trigger_on_failure_of is not in the WorkflowNode interface yet,
    // but the L2 pseudocode shows it should be there. For now, we'll create
    // the node as pending and rely on the engine to handle the failure trigger.
    createWorkflowNode({
      workflow_id: workflowId,
      node_id: config.option2.id,
      node_type: config.option2.type,
      status: "pending",
      input_json: JSON.stringify(config.option2.input),
      output_json: null,
      error_json: null,
      depends_on: JSON.stringify([]),
      attempt: 0,
      retry_reason: null,
      started_at: null,
      finished_at: null,
      updated_at: Date.now(),
    });

    // Update join node to depend on both branches
    updateJoinDependencies(workflowId, config.joinNode, [
      config.option1.id,
      config.option2.id,
    ]);
  } else {
    // Mode 1: Deterministic mode
    if (config.condition === undefined) {
      throw new Error("condition is required when triggerOnError is false");
    }

    const chosen = config.condition ? config.option1 : config.option2;
    const skipped = config.condition ? config.option2 : config.option1;

    // Create chosen branch node as 'pending'
    createWorkflowNode({
      workflow_id: workflowId,
      node_id: chosen.id,
      node_type: chosen.type,
      status: "pending",
      input_json: JSON.stringify(chosen.input),
      output_json: null,
      error_json: null,
      depends_on: JSON.stringify([]),
      attempt: 0,
      retry_reason: null,
      started_at: null,
      finished_at: null,
      updated_at: Date.now(),
    });

    // Create skipped branch node as 'skipped'
    createWorkflowNode({
      workflow_id: workflowId,
      node_id: skipped.id,
      node_type: skipped.type,
      status: "skipped",
      input_json: JSON.stringify(skipped.input),
      output_json: null,
      error_json: null,
      depends_on: JSON.stringify([]),
      attempt: 0,
      retry_reason: null,
      started_at: null,
      finished_at: null,
      updated_at: Date.now(),
    });

    // Update join node to depend on both branches (skipped counts as satisfied)
    updateJoinDependencies(workflowId, config.joinNode, [chosen.id, skipped.id]);
  }
}

// =============================================================================
// Parallel Fan-Out (PFO)
// =============================================================================

export function applyParallelFanOut(
  workflowId: number,
  config: ParallelFanOutConfig
): void {
  const { branches, joinNode: joinNodeId, joinMode = "all-succeed", sourceNodeId } = config;

  // 1. Validate branch count
  if (branches.length < 1) {
    throw new Error("PFO requires at least 1 branch");
  }
  if (branches.length > MAX_PARALLEL_BRANCHES) {
    throw new Error(
      `PFO branch count (${branches.length}) exceeds MAX_PARALLEL_BRANCHES (${MAX_PARALLEL_BRANCHES})`
    );
  }

  // 2. Validate join node exists and is pending
  const joinNode = queryOne<WorkflowNode>(
    "SELECT * FROM workflow_nodes WHERE workflow_id = ? AND node_id = ?",
    [workflowId, joinNodeId]
  );

  if (!joinNode) {
    throw new Error(`Join node '${joinNodeId}' not found`);
  }

  if (joinNode.status !== "pending") {
    throw new Error(
      `Join node '${joinNodeId}' must be pending, got: ${joinNode.status}`
    );
  }

  // 3. Determine source dependencies for branches
  const sourceDeps: string[] = sourceNodeId ? [sourceNodeId] : [];

  // 4. Create branch nodes
  const branchNodeIds: string[] = [];
  for (const branch of branches) {
    createWorkflowNode({
      workflow_id: workflowId,
      node_id: branch.id,
      node_type: branch.type,
      status: "pending",
      input_json: JSON.stringify(branch.input),
      output_json: null,
      error_json: null,
      depends_on: sourceDeps.length > 0 ? JSON.stringify(sourceDeps) : null,
      attempt: 0,
      retry_reason: "pfo_branch",
      started_at: null,
      finished_at: null,
      updated_at: Date.now(),
    });
    branchNodeIds.push(branch.id);
  }

  // 5. Update join node: depends on ALL branches, store join mode metadata
  updateJoinDependencies(workflowId, joinNodeId, branchNodeIds);

  // 6. Store join mode in retry_reason field for engine to inspect
  updateWorkflowNode(joinNode.id, {
    retry_reason: `pfo_join:${joinMode}`,
  });
}

// =============================================================================
// Retry with Alternative (RWA)
// =============================================================================

export function applyRetryWithAlternative(
  workflowId: number,
  failedNodeId: string,
  alternativeNode: InlineNodeDef
): void {
  // Create the alternative node in the DB as 'pending'
  createWorkflowNode({
    workflow_id: workflowId,
    node_id: alternativeNode.id,
    node_type: alternativeNode.type,
    status: "pending",
    input_json: JSON.stringify(alternativeNode.input),
    output_json: null,
    error_json: null,
    depends_on: JSON.stringify([]),
    attempt: 0,
    retry_reason: `alternative_for_${failedNodeId}`,
    started_at: null,
    finished_at: null,
    updated_at: Date.now(),
  });

  // Update downstream nodes to depend on the alternative node
  const downstreamNodes = getDownstreamNodes(workflowId, failedNodeId);
  for (const downstream of downstreamNodes) {
    // Parse current dependencies
    const currentDeps = downstream.depends_on
      ? (JSON.parse(downstream.depends_on) as string[])
      : [];

    // Replace failedNodeId with alternativeNode.id
    const updatedDeps = currentDeps.map((dep) =>
      dep === failedNodeId ? alternativeNode.id : dep
    );

    // Update the downstream node
    updateWorkflowNode(downstream.id, {
      depends_on: JSON.stringify(updatedDeps),
    });
  }
}

// =============================================================================
// Helpers
// =============================================================================

export function getPredecessors(
  workflowId: number,
  nodeId: string
): WorkflowNode[] {
  // Get the target node
  const node = queryOne<WorkflowNode>(
    "SELECT * FROM workflow_nodes WHERE workflow_id = ? AND node_id = ?",
    [workflowId, nodeId]
  );

  if (!node) return [];

  // Parse the depends_on JSON array
  const deps = node.depends_on ? JSON.parse(node.depends_on) as string[] : [];

  if (deps.length === 0) return [];

  // Query all workflow nodes
  const allNodes = getWorkflowNodes(workflowId);

  // Filter to nodes whose id appears in the depends_on array
  return allNodes.filter(n => deps.includes(n.node_id));
}

export function getDownstreamNodes(
  workflowId: number,
  nodeId: string
): WorkflowNode[] {
  // Query all workflow nodes for this workflow
  const allNodes = getWorkflowNodes(workflowId);

  // Filter to nodes whose depends_on JSON array contains the given nodeId
  return allNodes.filter(node => {
    if (!node.depends_on) return false;
    const deps = JSON.parse(node.depends_on) as string[];
    return deps.includes(nodeId);
  });
}

export function updateJoinDependencies(
  workflowId: number,
  joinNodeId: string,
  newDeps: string[]
): void {
  // Get the join node
  const joinNode = queryOne<WorkflowNode>(
    "SELECT * FROM workflow_nodes WHERE workflow_id = ? AND node_id = ?",
    [workflowId, joinNodeId]
  );

  if (!joinNode) return;

  // Parse current dependencies
  const currentDeps = joinNode.depends_on ? JSON.parse(joinNode.depends_on) as string[] : [];

  // Merge and deduplicate
  const mergedDeps = [...new Set([...currentDeps, ...newDeps])];

  // Update the join node
  updateWorkflowNode(joinNode.id, {
    depends_on: JSON.stringify(mergedDeps),
  });
}
