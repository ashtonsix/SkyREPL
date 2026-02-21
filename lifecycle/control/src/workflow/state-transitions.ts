// workflow/state-transitions.ts - State Machine Ownership and Transition Rules
// Canonical source for ALL resource state transitions (BT6 consolidation).

import type { Allocation, Workflow, WorkflowNode, Manifest } from "../material/db";
import { getDatabase, queryOne, transaction } from "../material/db";
import type { SQLQueryBindings } from "bun:sqlite";
import type { AllocationStatus } from "@skyrepl/contracts";
import { stateEvents, STATE_EVENT } from "./events";

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
// Generic Two-Phase Atomic Transition
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

    // Phase 2: Optimistic-lock update with updated_at guard
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

export const ALLOCATION_TRANSITIONS: Record<AllocationStatus, AllocationStatus[]> = {
  AVAILABLE: ["CLAIMED", "FAILED"],
  CLAIMED: ["AVAILABLE", "ACTIVE", "FAILED"],
  ACTIVE: ["COMPLETE", "FAILED"],
  COMPLETE: [],
  FAILED: [],
};

/**
 * Claim an available allocation for a run.
 * AVAILABLE -> CLAIMED. Sets run_id, claimed_at.
 *
 * Uses custom atomic transition (not atomicTransition) because it also validates run_id IS NULL.
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

    // Phase 2: Optimistic-lock update
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
 * Release a claimed allocation back to the warm pool.
 * CLAIMED -> AVAILABLE. Clears run_id.
 *
 * Used by compensation to undo a claim without failing the allocation.
 * Uses custom atomic transition (not atomicTransition) because it also clears run_id.
 */
export function releaseAllocation(
  allocationId: number
): TransitionResult<Allocation> {
  const db = getDatabase();

  return transaction(() => {
    // Phase 1: Read and verify CLAIMED
    const allocation = queryOne<Allocation>(
      "SELECT * FROM allocations WHERE id = ? AND status = ?",
      [allocationId, "CLAIMED"]
    );

    if (!allocation) {
      const current = queryOne<Allocation>("SELECT * FROM allocations WHERE id = ?", [allocationId]);
      if (!current) {
        return transitionFailure<Allocation>("NOT_FOUND");
      }
      return transitionFailure<Allocation>("WRONG_STATE", current);
    }

    // Phase 2: Optimistic-lock update — clear run_id, return to AVAILABLE
    const now = Date.now();
    const result = db.prepare(`
      UPDATE allocations
      SET run_id = NULL, status = 'AVAILABLE', updated_at = ?
      WHERE id = ? AND updated_at = ?
    `).run(
      ...[now, allocationId, allocation.updated_at] as SQLQueryBindings[]
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
  const result = atomicTransition<Allocation>(
    "allocations",
    allocationId,
    "CLAIMED",
    "ACTIVE"
  );
  if (result.success) {
    stateEvents.emit(STATE_EVENT.ALLOCATION_STATUS_CHANGED, {
      allocationId,
      runId: result.data.run_id,
      fromStatus: "CLAIMED",
      toStatus: "ACTIVE",
    });
  }
  return result;
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
  const result = atomicTransition<Allocation>(
    "allocations",
    allocationId,
    expectedStatus,
    "FAILED",
    { completed_at: Date.now() }
  );
  if (result.success) {
    stateEvents.emit(STATE_EVENT.ALLOCATION_STATUS_CHANGED, {
      allocationId,
      runId: result.data.run_id,
      fromStatus: expectedStatus,
      toStatus: "FAILED",
    });
  }
  return result;
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
  // Read current status before transition for event payload
  const before = queryOne<Allocation>("SELECT * FROM allocations WHERE id = ?", [allocationId]);
  const result = atomicTransition<Allocation>(
    "allocations",
    allocationId,
    ["AVAILABLE", "CLAIMED", "ACTIVE"],
    "FAILED",
    { completed_at: Date.now() }
  );
  if (result.success) {
    stateEvents.emit(STATE_EVENT.ALLOCATION_STATUS_CHANGED, {
      allocationId,
      runId: result.data.run_id,
      fromStatus: before?.status ?? "UNKNOWN",
      toStatus: "FAILED",
    });
  }
  return result;
}

// =============================================================================
// Manifest State Machine
// =============================================================================

export type ManifestStatus = "DRAFT" | "SEALED";

/**
 * Seal a draft manifest.
 * DRAFT -> SEALED. Sets sealed_at (via released_at column), expires_at.
 * Also detaches resources: clears current_manifest_id on all resources
 * linked via manifest_resources (SD-02).
 */
export function sealManifest(
  manifestId: number,
  expiresAt: number
): TransitionResult<Manifest> {
  const db = getDatabase();
  return transaction(() => {
    // Phase 1: Read and validate manifest
    const manifest = queryOne<Manifest>(
      "SELECT * FROM manifests WHERE id = ?",
      [manifestId]
    );
    if (!manifest) return transitionFailure<Manifest>("NOT_FOUND");
    if (manifest.status !== "DRAFT") return transitionFailure<Manifest>("WRONG_STATE", manifest);

    // Phase 2: Update manifest status with optimistic lock
    const now = Date.now();
    const result = db.prepare(
      `UPDATE manifests SET status = 'SEALED', released_at = ?, expires_at = ?, updated_at = ?
       WHERE id = ? AND status = 'DRAFT' AND updated_at = ?`
    ).run(now, expiresAt, now, manifestId, manifest.updated_at);

    if (result.changes === 0) return transitionFailure<Manifest>("RACE_LOST", manifest);

    // Phase 3: Detach resources — clear current_manifest_id on linked resources
    const RESOURCE_TABLES: Record<string, string> = {
      instance: "instances",
      run: "runs",
      allocation: "allocations",
      object: "objects",
    };
    for (const [resourceType, table] of Object.entries(RESOURCE_TABLES)) {
      db.prepare(
        `UPDATE ${table} SET current_manifest_id = NULL
         WHERE current_manifest_id = ?
           AND id IN (SELECT resource_id FROM manifest_resources WHERE manifest_id = ? AND resource_type = ?)`
      ).run(manifestId, manifestId, resourceType);
    }

    // Mark manifest-owned resources as released so isManifestExpired and
    // listExpiredManifests correctly identify this manifest as expired
    // (Step 9: manifest cleanup deadlock fix)
    db.prepare(
      `UPDATE manifest_resources SET owner_type = 'released'
       WHERE manifest_id = ? AND owner_type = 'manifest'`
    ).run(manifestId);

    const sealed = queryOne<Manifest>("SELECT * FROM manifests WHERE id = ?", [manifestId]);
    return transitionSuccess(sealed!);
  });
}

// =============================================================================
// Workflow State Machine
// =============================================================================

export type WorkflowStatus =
  | "pending"
  | "running"
  | "paused"
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
 * Fail a running, paused, or rolling-back workflow.
 * [pending, running, paused, rolling_back] -> failed. Sets finished_at, error_json.
 */
export function failWorkflow(
  workflowId: number,
  error: string
): TransitionResult<Workflow> {
  return atomicTransition<Workflow>(
    "workflows",
    workflowId,
    ["pending", "running", "paused", "rolling_back"],
    "failed",
    { error_json: error, finished_at: Date.now() }
  );
}

/**
 * Cancel a running or paused workflow.
 * [running, paused] -> cancelled. Sets finished_at.
 */
export function cancelWorkflow(
  workflowId: number
): TransitionResult<Workflow> {
  return atomicTransition<Workflow>(
    "workflows",
    workflowId,
    ["running", "paused"],
    "cancelled",
    { finished_at: Date.now() }
  );
}

/**
 * Pause a running workflow.
 * running -> paused.
 */
export function pauseWorkflow(
  workflowId: number
): TransitionResult<Workflow> {
  return atomicTransition<Workflow>(
    "workflows",
    workflowId,
    "running",
    "paused"
  );
}

/**
 * Resume a paused workflow.
 * paused -> running.
 */
export function resumeWorkflow(
  workflowId: number
): TransitionResult<Workflow> {
  return atomicTransition<Workflow>(
    "workflows",
    workflowId,
    "paused",
    "running"
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
