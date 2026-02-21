// resource/allocation.ts - Allocation State Machine, Warm Pool, Debug Holds

import type { Allocation } from "../material/db";
import type { AllocationStatus } from "@skyrepl/contracts";

import {
  getAllocation,
  createAllocation,
  findWarmAllocation,
  countInstanceAllocations,
  findStaleClaimed,
  findExpiredAvailable,
  getInstance,
  queryMany,
  queryOne,
  execute,
} from "../material/db";
import { getProvider } from "../provider/registry";
import {
  ALLOCATION_TRANSITIONS,
  claimAllocation,
  completeAllocation,
  failAllocation,
} from "../workflow/state-transitions";
import { TIMING } from "@skyrepl/contracts";

export { ALLOCATION_TRANSITIONS };

// =============================================================================
// Types
// =============================================================================

export interface WarmPoolQuery {
  provider?: string;
  spec?: string;
  initChecksum?: string;
  region?: string;
  tenantId?: number;
  excludeInstanceIds?: number[];
}

export interface WarmPoolMatch extends Allocation {
  spec: string;
  init_checksum: string | null;
  region: string;
  checksum_score: number;
}

export function validateAllocationTransition(
  currentStatus: AllocationStatus,
  newStatus: AllocationStatus
): { valid: boolean; error?: string } {
  const allowed = ALLOCATION_TRANSITIONS[currentStatus];
  if (allowed.includes(newStatus)) {
    return { valid: true };
  }
  return {
    valid: false,
    error: `Invalid transition: ${currentStatus} -> ${newStatus}. Allowed transitions from ${currentStatus}: ${allowed.length > 0 ? allowed.join(", ") : "none (terminal state)"}`,
  };
}

export function allowsSSHAccess(allocation: Allocation): boolean {
  if (allocation.status === "ACTIVE") {
    return true;
  }
  if (
    allocation.status === "COMPLETE" &&
    allocation.debug_hold_until !== null &&
    allocation.debug_hold_until > Date.now()
  ) {
    return true;
  }
  return false;
}

export function isInWarmPool(status: AllocationStatus): boolean {
  return status === "AVAILABLE";
}

// =============================================================================
// Warm Pool Operations
// =============================================================================

export async function createAvailableAllocation(
  instanceId: number,
  options?: { user?: string; workdir?: string; manifestId?: number }
): Promise<Allocation> {
  const count = countInstanceAllocations(instanceId);
  const workdir = options?.workdir ?? "/workspace";
  const user = options?.user ?? "default";

  return createAllocation({
    run_id: null,
    instance_id: instanceId,
    status: "AVAILABLE",
    current_manifest_id: options?.manifestId ?? null,
    user,
    workdir,
    debug_hold_until: null,
    completed_at: null,
  });
}

export async function queryWarmPool(
  query: WarmPoolQuery,
  limit?: number
): Promise<WarmPoolMatch[]> {
  const effectiveLimit = limit ?? 10;
  const heartbeatCutoff = Date.now() - TIMING.STALE_DETECTION_MS;
  const params: unknown[] = [];

  let sql = `
    SELECT a.*, i.spec, i.init_checksum, i.region,
      CASE
        WHEN ? IS NOT NULL AND i.init_checksum = ? THEN 100
        WHEN ? IS NOT NULL AND i.init_checksum IS NULL THEN 50
        WHEN ? IS NULL THEN 50
        ELSE 0
      END as checksum_score
    FROM allocations a
    JOIN instances i ON a.instance_id = i.id
    WHERE a.status = 'AVAILABLE' AND a.run_id IS NULL
      AND (i.workflow_state LIKE '%:complete' OR i.workflow_state = 'launch-run:provisioning')
      AND i.last_heartbeat > ?
  `;

  // Checksum scoring params (4 references to initChecksum)
  params.push(
    query.initChecksum ?? null,
    query.initChecksum ?? null,
    query.initChecksum ?? null,
    query.initChecksum ?? null
  );
  params.push(heartbeatCutoff);

  if (query.spec) {
    sql += " AND i.spec = ?";
    params.push(query.spec);
  }

  if (query.region) {
    sql += " AND i.region = ?";
    params.push(query.region);
  }

  if (query.excludeInstanceIds && query.excludeInstanceIds.length > 0) {
    const placeholders = query.excludeInstanceIds.map(() => "?").join(", ");
    sql += ` AND a.instance_id NOT IN (${placeholders})`;
    params.push(...query.excludeInstanceIds);
  }

  // Exclude mismatched checksums when initChecksum is specified
  if (query.initChecksum) {
    sql += " AND (i.init_checksum = ? OR i.init_checksum IS NULL)";
    params.push(query.initChecksum);
  }

  sql += `
    ORDER BY checksum_score DESC, a.created_at ASC
    LIMIT ?
  `;
  params.push(effectiveLimit);

  return queryMany<WarmPoolMatch>(sql, params);
}

export async function claimWarmPoolAllocation(
  query: WarmPoolQuery,
  runId: number,
  maxRetries?: number
): Promise<{ success: boolean; allocation?: Allocation; error?: string }> {
  const retries = maxRetries ?? 3;

  for (let attempt = 0; attempt <= retries; attempt++) {
    // Find a warm allocation matching the query
    const warm = findWarmAllocation(
      {
        spec: query.spec ?? "",
        region: query.region,
        tenantId: query.tenantId!,
      },
      query.initChecksum
    );

    if (!warm) {
      return { success: false, error: "No warm allocation available" };
    }

    // Attempt to claim it
    const result = claimAllocation(warm.id, runId);

    if (result.success) {
      return { success: true, allocation: result.data };
    }

    if (!result.success && result.reason === "RACE_LOST" && attempt < retries) {
      // Linear backoff: 100ms * attempt
      const delay = 100 * (attempt + 1);
      await new Promise((resolve) => setTimeout(resolve, delay));
      continue;
    }

    // Non-retryable failure
    return {
      success: false,
      error: `Claim failed: ${result.reason}`,
    };
  }

  return { success: false, error: "Max retries exceeded" };
}

// =============================================================================
// Active-Complete Transition
// =============================================================================

export async function transitionToComplete(
  allocationId: number,
  options: {
    exitCode: number;
    keepOnComplete?: boolean;
    debugHoldDurationMs?: number;
  }
): Promise<void> {
  const debugHoldUntil = options.debugHoldDurationMs
    ? Date.now() + options.debugHoldDurationMs
    : undefined;

  const result = completeAllocation(allocationId, {
    debugHoldUntil: debugHoldUntil,
  });

  if (!result.success) {
    return; // Already complete or failed — not an error
  }

  const allocation = result.data;

  // Check replenishment eligibility
  const eligible = await checkReplenishmentEligibility(
    allocation.instance_id,
    allocationId
  );

  if (eligible) {
    await createAvailableAllocation(allocation.instance_id);
  }
}

export async function checkReplenishmentEligibility(
  instanceId: number,
  completedAllocationId: number
): Promise<boolean> {
  // 1. Check exit code was 0 — get allocation, then get associated run
  const allocation = getAllocation(completedAllocationId);
  if (!allocation || !allocation.run_id) return false;

  const run = queryMany<{ exit_code: number | null }>(
    "SELECT exit_code FROM runs WHERE id = ?",
    [allocation.run_id]
  );
  if (run.length === 0) return false;

  // 2. Check instance heartbeat is fresh
  const instance = getInstance(instanceId);
  if (!instance) return false;

  if (Date.now() - instance.last_heartbeat > TIMING.STALE_DETECTION_MS) {
    return false;
  }

  // 3. Check instance has fewer than MAX_ALLOCATIONS_PER_INSTANCE non-terminal allocations
  const nonTerminalCount = countInstanceAllocations(instanceId);
  if (nonTerminalCount >= TIMING.MAX_ALLOCATIONS_PER_INSTANCE) {
    return false;
  }

  // 4. Check instance was not spot-interrupted
  const spotRun = queryMany<{ spot_interrupted: number }>(
    "SELECT spot_interrupted FROM runs WHERE id = ?",
    [allocation.run_id]
  );
  if (spotRun.length > 0 && spotRun[0].spot_interrupted !== 0) {
    return false;
  }

  return true;
}

// =============================================================================
// Debug Hold Extension
// =============================================================================

export async function extendAllocationDebugHold(
  allocationId: number,
  options: { extensionMs: number; maxExtensionMs?: number }
): Promise<{ success: boolean; newDebugHoldUntil?: number }> {
  const allocation = getAllocation(allocationId);
  if (!allocation) {
    return { success: false };
  }

  if (allocation.status !== "COMPLETE") {
    return { success: false };
  }

  const maxExtension =
    options.maxExtensionMs ?? TIMING.MAX_SSH_EXTENSION_MS;

  // Cap extension at maxExtensionMs
  const cappedExtension = Math.min(options.extensionMs, maxExtension);
  let newHoldUntil = Date.now() + cappedExtension;

  // Cap at absolute max: 24h from created_at
  const absoluteMax = allocation.created_at + TIMING.ABSOLUTE_MAX_HOLD_MS;
  newHoldUntil = Math.min(newHoldUntil, absoluteMax);

  execute(
    "UPDATE allocations SET debug_hold_until = ?, updated_at = ? WHERE id = ?",
    [newHoldUntil, Date.now(), allocationId]
  );

  return { success: true, newDebugHoldUntil: newHoldUntil };
}

// =============================================================================
// Termination Blocking
// =============================================================================

export async function canTerminateInstance(
  instanceId: number
): Promise<boolean> {
  // Check for COMPLETE allocations with active debug holds
  const holdingAllocations = queryMany<Allocation>(
    "SELECT * FROM allocations WHERE instance_id = ? AND status = 'COMPLETE' AND debug_hold_until IS NOT NULL AND debug_hold_until > ?",
    [instanceId, Date.now()]
  );
  if (holdingAllocations.length > 0) {
    return false;
  }

  // Check for any non-terminal allocations
  const nonTerminal = queryMany<Allocation>(
    "SELECT * FROM allocations WHERE instance_id = ? AND status NOT IN ('COMPLETE', 'FAILED')",
    [instanceId]
  );
  if (nonTerminal.length > 0) {
    return false;
  }

  return true;
}

// =============================================================================
// Background Reconciliation
// =============================================================================

export async function reconcileClaimedAllocations(): Promise<void> {
  const cutoff = Date.now() - TIMING.CLAIMED_TIMEOUT_MS;
  const stale = findStaleClaimed(cutoff);

  for (const allocation of stale) {
    failAllocation(allocation.id, "CLAIMED");
  }
}

export async function holdExpiryTask(): Promise<void> {
  const now = Date.now();
  const expired = queryMany<Allocation>(
    "SELECT * FROM allocations WHERE status = 'COMPLETE' AND debug_hold_until IS NOT NULL AND (debug_hold_until < ? OR (completed_at IS NOT NULL AND debug_hold_until > completed_at + ?))",
    [now, TIMING.ABSOLUTE_MAX_HOLD_MS]
  );

  for (const allocation of expired) {
    execute(
      "UPDATE allocations SET debug_hold_until = NULL, updated_at = ? WHERE id = ?",
      [now, allocation.id]
    );
  }

  if (expired.length > 0) {
    console.log(`[hold-expiry] Cleared ${expired.length} expired debug hold(s)`);
  }
}

// =============================================================================
// Background: Warm Pool Reconciliation
// =============================================================================

/**
 * Warm pool background reconciliation:
 * 1. Timeout stale CLAIMED allocations (> CLAIMED_TIMEOUT_MS)
 * 2. Expire old AVAILABLE allocations (> WARM_POOL_EXPIRY_MS)
 *
 * For stale CLAIMED: transition to FAILED (the run will fail and crash recovery handles the rest).
 * For expired AVAILABLE: transition to FAILED and terminate the instance if no other active allocations.
 */
export async function warmPoolReconciliation(): Promise<void> {
  const now = Date.now();

  // 1. Timeout stale CLAIMED allocations
  const claimedCutoff = now - TIMING.CLAIMED_TIMEOUT_MS;
  const staleClaimed = findStaleClaimed(claimedCutoff);
  for (const alloc of staleClaimed) {
    const result = failAllocation(alloc.id, "CLAIMED");
    if (result.success) {
      console.log(`[warm-pool] Timed out stale CLAIMED allocation ${alloc.id} (instance ${alloc.instance_id})`);
    }
  }

  // 2. Expire old AVAILABLE allocations
  const availableCutoff = now - TIMING.WARM_POOL_EXPIRY_MS;
  const expiredAvailable = findExpiredAvailable(availableCutoff);
  for (const alloc of expiredAvailable) {
    const result = failAllocation(alloc.id, "AVAILABLE");
    if (result.success) {
      console.log(`[warm-pool] Expired stale AVAILABLE allocation ${alloc.id} (instance ${alloc.instance_id})`);

      // Check if instance has any remaining non-terminal allocations
      const remaining = queryOne<{ count: number }>(
        "SELECT COUNT(*) as count FROM allocations WHERE instance_id = ? AND status NOT IN ('COMPLETE', 'FAILED')",
        [alloc.instance_id]
      );
      if (remaining && remaining.count === 0) {
        // No active allocations — terminate the instance
        try {
          const instance = getInstance(alloc.instance_id);
          if (instance?.provider_id) {
            const provider = await getProvider(instance.provider as any);
            await provider.terminate(instance.provider_id);
            console.log(`[warm-pool] Terminated instance ${alloc.instance_id} (no active allocations)`);
          }
        } catch (err) {
          console.warn(`[warm-pool] Failed to terminate instance ${alloc.instance_id}:`, err);
        }
      }
    }
  }
}
