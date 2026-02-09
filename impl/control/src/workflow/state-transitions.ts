// workflow/state-transitions.ts - State Machine Ownership and Transition Rules
// Canonical source for ALL resource state transitions (BT6 consolidation).

import type { Allocation, Workflow, WorkflowNode, Manifest } from "../material/db";
import { getDatabase, queryOne, transaction } from "../material/db";
import type { SQLQueryBindings } from "bun:sqlite";

// =============================================================================
// Transition Result Type
// =============================================================================

export type TransitionResult<T> =
  | { success: true; data: T }
  | { success: false; reason: "NOT_FOUND" | "WRONG_STATE" | "RACE_LOST"; current?: T };

export function transitionSuccess<T>(data: T): TransitionResult<T> {
  return { success: true, data };
}

export function transitionFailure<T>(
  reason: "NOT_FOUND" | "WRONG_STATE" | "RACE_LOST",
  current?: T
): TransitionResult<T> {
  return { success: false, reason, current };
}

// =============================================================================
// Table Name Whitelist (SQL injection prevention)
// =============================================================================

const ALLOWED_TABLES = new Set([
  "allocations",
  "manifests",
  "workflows",
  "workflow_nodes",
]);

function assertValidTable(table: string): void {
  if (!ALLOWED_TABLES.has(table)) {
    throw new Error(`Invalid table name for atomic transition: ${table}`);
  }
}

// =============================================================================
// Generic Two-Phase CAS
// =============================================================================

export function atomicTransition<T extends { id: number; status: string; updated_at: number }>(
  table: string,
  id: number,
  fromStatus: string | string[],
  toStatus: string,
  additionalUpdates?: Record<string, unknown>
): TransitionResult<T> {
  assertValidTable(table);

  const db = getDatabase();

  return transaction(() => {
    // Phase 1: Read current state
    const current = queryOne<T>(`SELECT * FROM ${table} WHERE id = ?`, [id]);

    if (!current) {
      return transitionFailure<T>("NOT_FOUND");
    }

    // Check status (supports single status or array of valid source statuses)
    const validStatuses = Array.isArray(fromStatus) ? fromStatus : [fromStatus];
    if (!validStatuses.includes(current.status)) {
      return transitionFailure<T>("WRONG_STATE", current);
    }

    // Phase 2: CAS update with updated_at guard
    const now = Date.now();
    const updateFields: Record<string, unknown> = { status: toStatus, updated_at: now, ...additionalUpdates };
    const keys = Object.keys(updateFields);
    const setClause = keys.map(k => `${k} = ?`).join(", ");
    const values = keys.map(k => updateFields[k]);

    const result = db.prepare(
      `UPDATE ${table} SET ${setClause} WHERE id = ? AND updated_at = ?`
    ).run(...([...values, id, current.updated_at] as SQLQueryBindings[]));

    if (result.changes === 0) {
      // Race lost - row was modified between SELECT and UPDATE
      const latest = queryOne<T>(`SELECT * FROM ${table} WHERE id = ?`, [id]);
      return transitionFailure<T>("RACE_LOST", latest ?? undefined);
    }

    // Return updated record
    const updated = queryOne<T>(`SELECT * FROM ${table} WHERE id = ?`, [id]);
    return transitionSuccess<T>(updated!);
  });
}

// =============================================================================
// Allocation State Machine
// =============================================================================

export type AllocationStatus = "AVAILABLE" | "CLAIMED" | "ACTIVE" | "COMPLETE" | "FAILED";

export const ALLOCATION_TRANSITIONS: Record<AllocationStatus, AllocationStatus[]> = {
  AVAILABLE: ["CLAIMED", "FAILED"],
  CLAIMED: ["ACTIVE", "FAILED"],
  ACTIVE: ["COMPLETE", "FAILED"],
  COMPLETE: [],
  FAILED: [],
};

/**
 * Claim an available allocation for a run.
 * AVAILABLE -> CLAIMED. Sets run_id, claimed_at.
 *
 * Uses custom CAS (not atomicTransition) because it also validates run_id IS NULL.
 */
export function claimAllocation(
  allocationId: number,
  runId: number
): TransitionResult<Allocation> {
  const db = getDatabase();

  return transaction(() => {
    // Phase 1: Read and verify AVAILABLE + no run_id
    const allocation = queryOne<Allocation>(
      "SELECT * FROM allocations WHERE id = ? AND status = ? AND run_id IS NULL",
      [allocationId, "AVAILABLE"]
    );

    if (!allocation) {
      // Could be not found, wrong status, or already claimed
      const current = queryOne<Allocation>("SELECT * FROM allocations WHERE id = ?", [allocationId]);
      if (!current) {
        return transitionFailure<Allocation>("NOT_FOUND");
      }
      return transitionFailure<Allocation>("WRONG_STATE", current);
    }

    // Phase 2: CAS update
    const now = Date.now();
    const result = db.prepare(`
      UPDATE allocations
      SET run_id = ?, status = 'CLAIMED', updated_at = ?
      WHERE id = ? AND updated_at = ?
    `).run(
      ...[runId, now, allocationId, allocation.updated_at] as SQLQueryBindings[]
    );

    if (result.changes === 0) {
      const latest = queryOne<Allocation>("SELECT * FROM allocations WHERE id = ?", [allocationId]);
      return transitionFailure<Allocation>("RACE_LOST", latest ?? undefined);
    }

    const updated = queryOne<Allocation>("SELECT * FROM allocations WHERE id = ?", [allocationId]);
    return transitionSuccess<Allocation>(updated!);
  });
}

/**
 * Activate a claimed allocation after sync completes.
 * CLAIMED -> ACTIVE.
 */
export function activateAllocation(
  allocationId: number
): TransitionResult<Allocation> {
  return atomicTransition<Allocation>(
    "allocations",
    allocationId,
    "CLAIMED",
    "ACTIVE"
  );
}

/**
 * Complete an active allocation after run finishes.
 * ACTIVE -> COMPLETE. Sets completed_at (BT2 fix).
 *
 * Special: If allocation is already FAILED, treat as success (run completed
 * despite infrastructure failure).
 */
export function completeAllocation(
  allocationId: number,
  options?: { debugHoldUntil?: number }
): TransitionResult<Allocation> {
  const db = getDatabase();

  return transaction(() => {
    const allocation = queryOne<Allocation>(
      "SELECT * FROM allocations WHERE id = ?",
      [allocationId]
    );

    if (!allocation) {
      return transitionFailure<Allocation>("NOT_FOUND");
    }

    // Special case: If already FAILED, treat as success
    // (run completed despite infrastructure failure)
    if (allocation.status === "FAILED") {
      return transitionSuccess<Allocation>(allocation);
    }

    if (allocation.status !== "ACTIVE") {
      return transitionFailure<Allocation>("WRONG_STATE", allocation);
    }

    const now = Date.now();
    const result = db.prepare(`
      UPDATE allocations
      SET status = 'COMPLETE', completed_at = ?, debug_hold_until = ?, updated_at = ?
      WHERE id = ? AND updated_at = ?
    `).run(
      ...[now, options?.debugHoldUntil ?? null, now, allocationId, allocation.updated_at] as SQLQueryBindings[]
    );

    if (result.changes === 0) {
      const latest = queryOne<Allocation>("SELECT * FROM allocations WHERE id = ?", [allocationId]);
      // If race lost to FAILED transition, treat as success
      if (latest?.status === "FAILED") {
        return transitionSuccess<Allocation>(latest);
      }
      return transitionFailure<Allocation>("RACE_LOST", latest ?? undefined);
    }

    const updated = queryOne<Allocation>("SELECT * FROM allocations WHERE id = ?", [allocationId]);
    return transitionSuccess<Allocation>(updated!);
  });
}

/**
 * Fail an allocation due to infrastructure issues.
 * [AVAILABLE | CLAIMED | ACTIVE] -> FAILED.
 * Sets completed_at (BT2 fix: ALWAYS set completed_at on terminal states).
 *
 * @param expectedStatus - The status we expect the allocation to be in.
 *   This MUST be specified to prevent accidental state corruption.
 */
export function failAllocation(
  allocationId: number,
  expectedStatus: "AVAILABLE" | "CLAIMED" | "ACTIVE"
): TransitionResult<Allocation> {
  return atomicTransition<Allocation>(
    "allocations",
    allocationId,
    expectedStatus,
    "FAILED",
    { completed_at: Date.now() }
  );
}

/**
 * Fail allocation from any non-terminal state.
 * Use when you need to fail regardless of current state (e.g., instance terminated).
 * Sets completed_at (BT2 fix).
 *
 * WARNING: Use failAllocation() with explicit expectedStatus when possible.
 * This variant should only be used for cleanup when state is uncertain.
 */
export function failAllocationAnyState(
  allocationId: number
): TransitionResult<Allocation> {
  return atomicTransition<Allocation>(
    "allocations",
    allocationId,
    ["AVAILABLE", "CLAIMED", "ACTIVE"],
    "FAILED",
    { completed_at: Date.now() }
  );
}

// =============================================================================
// Manifest State Machine
// =============================================================================

export type ManifestStatus = "DRAFT" | "SEALED";

/**
 * Seal a draft manifest.
 * DRAFT -> SEALED. Sets sealed_at (via released_at column), expires_at.
 */
export function sealManifest(
  manifestId: number,
  expiresAt: number
): TransitionResult<Manifest> {
  return atomicTransition<Manifest>(
    "manifests",
    manifestId,
    "DRAFT",
    "SEALED",
    { released_at: Date.now(), expires_at: expiresAt }
  );
}

// =============================================================================
// Workflow State Machine
// =============================================================================

export type WorkflowStatus =
  | "pending"
  | "running"
  | "completed"
  | "failed"
  | "cancelled"
  | "rolling_back";

/**
 * Start a pending workflow.
 * pending -> running. Sets started_at.
 */
export function startWorkflow(
  workflowId: number
): TransitionResult<Workflow> {
  return atomicTransition<Workflow>(
    "workflows",
    workflowId,
    "pending",
    "running",
    { started_at: Date.now() }
  );
}

/**
 * Complete a running workflow.
 * running -> completed. Sets finished_at, output_json.
 */
export function completeWorkflow(
  workflowId: number,
  output: Record<string, unknown>
): TransitionResult<Workflow> {
  return atomicTransition<Workflow>(
    "workflows",
    workflowId,
    "running",
    "completed",
    { output_json: JSON.stringify(output), finished_at: Date.now() }
  );
}

/**
 * Fail a running or rolling-back workflow.
 * [pending, running, rolling_back] -> failed. Sets finished_at, error_json.
 */
export function failWorkflow(
  workflowId: number,
  error: string
): TransitionResult<Workflow> {
  return atomicTransition<Workflow>(
    "workflows",
    workflowId,
    ["pending", "running", "rolling_back"],
    "failed",
    { error_json: error, finished_at: Date.now() }
  );
}

/**
 * Cancel a running workflow.
 * running -> cancelled. Sets finished_at.
 */
export function cancelWorkflow(
  workflowId: number
): TransitionResult<Workflow> {
  return atomicTransition<Workflow>(
    "workflows",
    workflowId,
    "running",
    "cancelled",
    { finished_at: Date.now() }
  );
}

/**
 * Start rollback for a failed workflow.
 * running -> rolling_back.
 */
export function startRollback(
  workflowId: number
): TransitionResult<Workflow> {
  return atomicTransition<Workflow>(
    "workflows",
    workflowId,
    "running",
    "rolling_back"
  );
}

// =============================================================================
// Node State Machine
// =============================================================================

export type NodeStatus = "pending" | "running" | "completed" | "failed" | "skipped";

/**
 * Start a pending node.
 * pending -> running. Sets started_at, increments attempt.
 *
 * Uses custom transaction (not atomicTransition) because it needs
 * `attempt = attempt + 1` which is a SQL expression, not a static value.
 */
export function startNode(
  nodeId: number
): TransitionResult<WorkflowNode> {
  const db = getDatabase();

  return transaction(() => {
    const node = queryOne<WorkflowNode>("SELECT * FROM workflow_nodes WHERE id = ?", [nodeId]);

    if (!node) {
      return transitionFailure<WorkflowNode>("NOT_FOUND");
    }

    if (node.status !== "pending") {
      return transitionFailure<WorkflowNode>("WRONG_STATE", node);
    }

    const now = Date.now();
    const result = db.prepare(`
      UPDATE workflow_nodes
      SET status = 'running', started_at = ?, attempt = attempt + 1, updated_at = ?
      WHERE id = ? AND updated_at = ?
    `).run(
      ...[now, now, nodeId, node.updated_at] as SQLQueryBindings[]
    );

    if (result.changes === 0) {
      const latest = queryOne<WorkflowNode>("SELECT * FROM workflow_nodes WHERE id = ?", [nodeId]);
      return transitionFailure<WorkflowNode>("RACE_LOST", latest ?? undefined);
    }

    const updated = queryOne<WorkflowNode>("SELECT * FROM workflow_nodes WHERE id = ?", [nodeId]);
    return transitionSuccess<WorkflowNode>(updated!);
  });
}

/**
 * Complete a running node.
 * running -> completed. Sets finished_at, output_json.
 */
export function completeNode(
  nodeId: number,
  output: Record<string, unknown>
): TransitionResult<WorkflowNode> {
  const db = getDatabase();

  return transaction(() => {
    const node = queryOne<WorkflowNode>("SELECT * FROM workflow_nodes WHERE id = ?", [nodeId]);

    if (!node) {
      return transitionFailure<WorkflowNode>("NOT_FOUND");
    }

    if (node.status !== "running") {
      return transitionFailure<WorkflowNode>("WRONG_STATE", node);
    }

    const now = Date.now();
    const result = db.prepare(`
      UPDATE workflow_nodes
      SET status = 'completed', output_json = ?, finished_at = ?, updated_at = ?
      WHERE id = ? AND updated_at = ?
    `).run(
      ...[JSON.stringify(output), now, now, nodeId, node.updated_at] as SQLQueryBindings[]
    );

    if (result.changes === 0) {
      const latest = queryOne<WorkflowNode>("SELECT * FROM workflow_nodes WHERE id = ?", [nodeId]);
      return transitionFailure<WorkflowNode>("RACE_LOST", latest ?? undefined);
    }

    const updated = queryOne<WorkflowNode>("SELECT * FROM workflow_nodes WHERE id = ?", [nodeId]);
    return transitionSuccess<WorkflowNode>(updated!);
  });
}

/**
 * Fail a running node.
 * running -> failed. Sets finished_at, error_json.
 */
export function failNode(
  nodeId: number,
  error: string
): TransitionResult<WorkflowNode> {
  const db = getDatabase();

  return transaction(() => {
    const node = queryOne<WorkflowNode>("SELECT * FROM workflow_nodes WHERE id = ?", [nodeId]);

    if (!node) {
      return transitionFailure<WorkflowNode>("NOT_FOUND");
    }

    if (node.status !== "running") {
      return transitionFailure<WorkflowNode>("WRONG_STATE", node);
    }

    const now = Date.now();
    const result = db.prepare(`
      UPDATE workflow_nodes
      SET status = 'failed', error_json = ?, finished_at = ?, updated_at = ?
      WHERE id = ? AND updated_at = ?
    `).run(
      ...[error, now, now, nodeId, node.updated_at] as SQLQueryBindings[]
    );

    if (result.changes === 0) {
      const latest = queryOne<WorkflowNode>("SELECT * FROM workflow_nodes WHERE id = ?", [nodeId]);
      return transitionFailure<WorkflowNode>("RACE_LOST", latest ?? undefined);
    }

    const updated = queryOne<WorkflowNode>("SELECT * FROM workflow_nodes WHERE id = ?", [nodeId]);
    return transitionSuccess<WorkflowNode>(updated!);
  });
}

/**
 * Reset a failed node for retry.
 * failed -> pending. Sets retry_reason, clears error_json and finished_at.
 */
export function resetNodeForRetry(
  nodeId: number,
  retryReason: string
): TransitionResult<WorkflowNode> {
  const db = getDatabase();

  return transaction(() => {
    const node = queryOne<WorkflowNode>("SELECT * FROM workflow_nodes WHERE id = ?", [nodeId]);

    if (!node) {
      return transitionFailure<WorkflowNode>("NOT_FOUND");
    }

    if (node.status !== "failed") {
      return transitionFailure<WorkflowNode>("WRONG_STATE", node);
    }

    const now = Date.now();
    const result = db.prepare(`
      UPDATE workflow_nodes
      SET status = 'pending', error_json = NULL, finished_at = NULL, retry_reason = ?, updated_at = ?
      WHERE id = ? AND updated_at = ?
    `).run(
      ...[retryReason, now, nodeId, node.updated_at] as SQLQueryBindings[]
    );

    if (result.changes === 0) {
      const latest = queryOne<WorkflowNode>("SELECT * FROM workflow_nodes WHERE id = ?", [nodeId]);
      return transitionFailure<WorkflowNode>("RACE_LOST", latest ?? undefined);
    }

    const updated = queryOne<WorkflowNode>("SELECT * FROM workflow_nodes WHERE id = ?", [nodeId]);
    return transitionSuccess<WorkflowNode>(updated!);
  });
}

/**
 * Skip a pending node.
 * pending -> skipped. Sets finished_at.
 */
export function skipNode(
  nodeId: number
): TransitionResult<WorkflowNode> {
  return atomicTransition<WorkflowNode>(
    "workflow_nodes",
    nodeId,
    "pending",
    "skipped",
    { finished_at: Date.now() }
  );
}
