// background/reconciliation.ts - Allocation Reconciliation (#LIFE-11)
//
// Comprehensive reconciliation runner with 6 subtask types per spec section 5.5.
// Runs periodically as a safety net to catch allocations stuck in inconsistent states.
// The warm pool reconciliation in main.ts continues to run separately (it also
// handles instance termination). This reconciliation task is an additional safety net.

import { queryMany, type Allocation } from "../material/db";
import {
  failAllocation,
  failAllocationAnyState,
  completeAllocation,
} from "../workflow/state-transitions";
import { TIMING } from "@skyrepl/shared";

// =============================================================================
// Result Type
// =============================================================================

export interface ReconciliationResult {
  claimedTimeout: number;
  warmPoolHealth: number;
  orphanedClaims: number;
  stalledTransitions: number;
  providerStateSync: number;
  allocationAging: number;
}

// =============================================================================
// Main Entry Point
// =============================================================================

export async function runReconciliation(): Promise<ReconciliationResult> {
  const result: ReconciliationResult = {
    claimedTimeout: 0,
    warmPoolHealth: 0,
    orphanedClaims: 0,
    stalledTransitions: 0,
    providerStateSync: 0,
    allocationAging: 0,
  };

  result.claimedTimeout = reconcileClaimedTimeout();
  result.warmPoolHealth = reconcileWarmPoolHealth();
  result.orphanedClaims = reconcileOrphanedClaims();
  result.stalledTransitions = reconcileStalledTransitions();
  result.providerStateSync = reconcileProviderStateSync();
  result.allocationAging = reconcileAllocationAging();

  const total = Object.values(result).reduce((a, b) => a + b, 0);
  if (total > 0) {
    console.log(`[reconciliation] Fixed ${total} allocation(s):`, result);
  }

  return result;
}

// =============================================================================
// Subtask 1: CLAIMED Timeout
// =============================================================================

/**
 * Find allocations stuck in CLAIMED state longer than CLAIMED_TIMEOUT_MS.
 * These were claimed by a workflow but never activated or released.
 * Transition: CLAIMED -> FAILED.
 */
function reconcileClaimedTimeout(): number {
  const cutoff = Date.now() - TIMING.CLAIMED_TIMEOUT_MS;
  const stale = queryMany<Allocation>(
    "SELECT * FROM allocations WHERE status = 'CLAIMED' AND updated_at < ?",
    [cutoff]
  );
  let count = 0;
  for (const alloc of stale) {
    const r = failAllocation(alloc.id, "CLAIMED");
    if (r.success) count++;
  }
  return count;
}

// =============================================================================
// Subtask 2: Warm Pool Health
// =============================================================================

/**
 * Find AVAILABLE allocations where the instance's heartbeat is stale.
 * These are in the warm pool but the instance is dead.
 * Transition: AVAILABLE -> FAILED.
 */
function reconcileWarmPoolHealth(): number {
  const heartbeatCutoff = Date.now() - TIMING.STALE_DETECTION_MS;
  const stale = queryMany<Allocation>(
    `SELECT a.* FROM allocations a
     JOIN instances i ON a.instance_id = i.id
     WHERE a.status = 'AVAILABLE' AND i.last_heartbeat < ?`,
    [heartbeatCutoff]
  );
  let count = 0;
  for (const alloc of stale) {
    const r = failAllocation(alloc.id, "AVAILABLE");
    if (r.success) count++;
  }
  return count;
}

// =============================================================================
// Subtask 3: Orphaned Claims
// =============================================================================

/**
 * Find CLAIMED allocations where the associated run has already finished.
 * These were claimed but never activated, and the run completed or failed
 * through other means (e.g., timeout, cancellation).
 * Transition: CLAIMED -> FAILED.
 */
function reconcileOrphanedClaims(): number {
  const orphaned = queryMany<Allocation>(
    `SELECT a.* FROM allocations a
     JOIN runs r ON a.run_id = r.id
     WHERE a.status = 'CLAIMED' AND r.finished_at IS NOT NULL`,
    []
  );
  let count = 0;
  for (const alloc of orphaned) {
    const r = failAllocation(alloc.id, "CLAIMED");
    if (r.success) count++;
  }
  return count;
}

// =============================================================================
// Subtask 4: Stalled Transitions
// =============================================================================

/**
 * Find ACTIVE allocations where the run has already finished.
 * The run completed but the allocation was never transitioned.
 * Transition: ACTIVE -> COMPLETE.
 */
function reconcileStalledTransitions(): number {
  const stalled = queryMany<Allocation>(
    `SELECT a.* FROM allocations a
     JOIN runs r ON a.run_id = r.id
     WHERE a.status = 'ACTIVE' AND r.finished_at IS NOT NULL`,
    []
  );
  let count = 0;
  for (const alloc of stalled) {
    const r = completeAllocation(alloc.id);
    if (r.success) count++;
  }
  return count;
}

// =============================================================================
// Subtask 5: Provider State Sync
// =============================================================================

/**
 * Find non-terminal allocations on instances that have been terminated.
 * Checks the instance workflow_state in the DB (no provider API call needed).
 * Transition: any non-terminal -> FAILED.
 */
function reconcileProviderStateSync(): number {
  const terminated = queryMany<Allocation>(
    `SELECT a.* FROM allocations a
     JOIN instances i ON a.instance_id = i.id
     WHERE a.status NOT IN ('COMPLETE', 'FAILED')
       AND i.workflow_state LIKE 'terminate:%'`,
    []
  );
  let count = 0;
  for (const alloc of terminated) {
    const r = failAllocationAnyState(alloc.id);
    if (r.success) count++;
  }
  return count;
}

// =============================================================================
// Subtask 6: Allocation Aging
// =============================================================================

/**
 * Find AVAILABLE allocations that have been in the warm pool too long.
 * These exceed WARM_POOL_EXPIRY_MS and should be cleaned up.
 * Transition: AVAILABLE -> FAILED.
 */
function reconcileAllocationAging(): number {
  const cutoff = Date.now() - TIMING.WARM_POOL_EXPIRY_MS;
  const expired = queryMany<Allocation>(
    "SELECT * FROM allocations WHERE status = 'AVAILABLE' AND created_at < ?",
    [cutoff]
  );
  let count = 0;
  for (const alloc of expired) {
    const r = failAllocation(alloc.id, "AVAILABLE");
    if (r.success) count++;
  }
  return count;
}
