// background/reconciliation.ts - Allocation Reconciliation (#LIFE-11)
//
// Comprehensive reconciliation runner with 6 subtask types per spec section 5.5.
// Runs periodically as a safety net to catch allocations stuck in inconsistent states.
// The warm pool reconciliation in main.ts continues to run separately (it also
// handles instance termination). This reconciliation task is an additional safety net.
//
// Also contains two-phase spawn reconciliation (#WF2-01):
//   - reconcilePendingSpawn: compares one spawn:pending DB record against provider API
//   - reconcileStalePendingSpawns: periodic sweep over all stale spawn:pending records

import { queryMany, queryOne, updateInstance, findStaleClaimed, findExpiredAvailable, emitAuditEvent, type Allocation, type Instance } from "../material/db"; // raw-db: boutique queries (spawn:pending sweep, JOINs for warm pool health/orphaned claims/stalled transitions/provider sync), see WL-057
import { computeMeteringStopData } from "../billing/metering";
import {
  failAllocation,
  failAllocationAnyState,
  completeAllocation,
} from "../workflow/state-transitions";
import { getProvider } from "../provider/registry";
import type { ProviderName } from "../provider/types";
import { TIMING } from "@skyrepl/contracts";
import { materializeInstance } from "../resource/instance";

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
  stalePendingSpawns: number;
}

// =============================================================================
// Two-Phase Spawn Reconciliation (#WF2-01)
// =============================================================================

/**
 * Result of reconciling a single spawn:pending instance record against the
 * provider API. See spec §8.1.1 (Orphan Reconciliation).
 */
export type PendingSpawnReconcileResult =
  | { outcome: "found"; providerId: string }
  | { outcome: "not_found" }
  | { outcome: "multiple_found"; providerId: string }
  | { outcome: "no_idempotent_spawn" }
  | { outcome: "skipped_non_pending" };

/**
 * Compare one spawn:pending DB record against the provider API.
 *
 * Uses the spawn idempotency key tag (`skyrepl:spawn_key`) to look up the
 * instance by the naming triple that was encoded when spawn was called. This
 * requires `provider.capabilities.idempotentSpawn = true`.
 *
 * Outcomes:
 *   - Provider has exactly one match → update DB with provider_id and state
 *   - Provider has no match          → mark instance as spawn:error
 *   - Provider has multiple matches  → log error, pick first, flag for review
 *   - Provider doesn't support idempotentSpawn → skip (no API to query by key)
 *   - Instance is not spawn:pending  → skip (already resolved)
 */
export async function reconcilePendingSpawn(
  instance: Instance
): Promise<PendingSpawnReconcileResult> {
  // Guard: only process spawn:pending instances
  if (instance.workflow_state !== "spawn:pending") {
    return { outcome: "skipped_non_pending" };
  }

  const provider = await getProvider(instance.provider as ProviderName);

  if (!provider.capabilities.idempotentSpawn) {
    // Provider has no tag-based lookup — cannot reconcile; caller handles fallback.
    return { outcome: "no_idempotent_spawn" };
  }

  // The spawn_idempotency_key stored in DB matches the clientToken used in spawn():
  // format "spawn:{controlId}-{manifestId}-{instanceId}" (see aws.ts clientToken).
  // We search by the skyrepl:spawn_key tag that aws.ts writes on every instance.
  const spawnKey = instance.spawn_idempotency_key;
  if (!spawnKey) {
    // No key recorded — cannot look up by tag; treat as not found.
    updateInstance(instance.id, {
      workflow_state: "spawn:error",
      workflow_error: "no spawn_idempotency_key recorded; cannot reconcile",
      last_heartbeat: Date.now(),
    });
    return { outcome: "not_found" };
  }

  let matches;
  try {
    matches = await provider.list({
      tags: { "skyrepl:spawn_key": spawnKey },
      includeTerminated: false,
    });
  } catch (err) {
    // Provider API error — skip for now; will retry on next sweep
    console.warn(
      `[reconcile-spawn] Provider list() failed for instance ${instance.id}:`,
      err instanceof Error ? err.message : String(err)
    );
    return { outcome: "not_found" };
  }

  if (matches.length === 0) {
    // Instance never made it to the provider — definitively failed
    updateInstance(instance.id, {
      workflow_state: "spawn:error",
      workflow_error: "provider has no matching instance; spawn never completed",
      last_heartbeat: Date.now(),
    });
    return { outcome: "not_found" };
  }

  if (matches.length > 1) {
    console.error(
      `[reconcile-spawn] Multiple provider instances matched spawn key '${spawnKey}' ` +
      `for DB instance ${instance.id} — picking first, flagging for review`
    );
  }

  const providerInstance = matches[0]!;
  updateInstance(instance.id, {
    provider_id: providerInstance.id,
    ip: providerInstance.ip ?? null,
    region: providerInstance.region ?? instance.region,
    workflow_state: "launch-run:provisioning",
    last_heartbeat: Date.now(),
  });

  if (matches.length > 1) {
    return { outcome: "multiple_found", providerId: providerInstance.id };
  }
  return { outcome: "found", providerId: providerInstance.id };
}

/**
 * Periodic sweep: reconcile all spawn:pending instance records older than the
 * stale threshold (PENDING_SPAWN_STALE_THRESHOLD_MS, default 10 minutes).
 *
 * For each stale record, calls reconcilePendingSpawn. If the provider has no
 * matching instance (or doesn't support idempotentSpawn), marks as spawn:error.
 *
 * Returns the number of records processed.
 */
export async function reconcileStalePendingSpawns(): Promise<number> {
  const cutoff = Date.now() - TIMING.PENDING_SPAWN_STALE_THRESHOLD_MS;

  const staleInstances = queryMany<Instance>(
    "SELECT * FROM instances WHERE workflow_state = 'spawn:pending' AND created_at < ?",
    [cutoff]
  );

  let processed = 0;

  for (const instance of staleInstances) {
    const result = await reconcilePendingSpawn(instance);

    switch (result.outcome) {
      case "found":
      case "multiple_found":
        console.log(
          `[reconcile-spawn] Recovered instance ${instance.id} → provider_id ${result.providerId}`
        );
        break;
      case "not_found":
        console.log(
          `[reconcile-spawn] Instance ${instance.id} not found in provider — marked spawn:error`
        );
        break;
      case "no_idempotent_spawn":
        // Provider can't be queried by key — mark as error to unblock the workflow
        updateInstance(instance.id, {
          workflow_state: "spawn:error",
          workflow_error: "provider does not support idempotentSpawn; cannot reconcile",
          last_heartbeat: Date.now(),
        });
        console.log(
          `[reconcile-spawn] Instance ${instance.id} provider lacks idempotentSpawn — marked spawn:error`
        );
        break;
      case "skipped_non_pending":
        // Race condition: already resolved between our query and the call — fine
        break;
    }

    processed++;
  }

  if (processed > 0) {
    console.log(`[reconcile-spawn] Processed ${processed} stale spawn:pending instance(s)`);
  }

  return processed;
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
    stalePendingSpawns: 0,
  };

  result.claimedTimeout = reconcileClaimedTimeout();
  result.warmPoolHealth = reconcileWarmPoolHealth();
  result.orphanedClaims = reconcileOrphanedClaims();
  result.stalledTransitions = reconcileStalledTransitions();
  result.providerStateSync = await reconcileProviderStateSync();
  result.allocationAging = reconcileAllocationAging();
  result.stalePendingSpawns = await reconcileStalePendingSpawns();

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
  const stale = findStaleClaimed(cutoff);
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
 * Discover externally-terminated instances via the materializer, then
 * clean up their allocations.
 *
 * Phase 1: Materialize all active (non-terminal) instances. For each,
 *   materializeInstance calls provider.get(). If the provider says the
 *   instance is gone, mark_terminated fires (§3.3 M28) — the instance's
 *   workflow_state is set to terminate:complete in the DB.
 *
 * Phase 2: Find non-terminal allocations on terminated instances and
 *   fail them (existing DB-side check, now catches both previously-known
 *   and freshly-discovered terminated instances).
 */
async function reconcileProviderStateSync(): Promise<number> {
  // Phase 1: Materialize active instances to discover drift.
  // Individual materializeInstance uses provider.get() (definitive signal),
  // not provider.list() (ambiguous absence). Errors are caught inside
  // materializeInstance — provider unreachable falls back to DB state.
  const active = queryMany<Instance>(
    `SELECT * FROM instances
     WHERE workflow_state NOT LIKE 'terminate:%'
       AND workflow_state NOT LIKE '%:error'
       AND workflow_state NOT LIKE '%:compensated'
       AND provider_id IS NOT NULL`,
    []
  );

  for (const inst of active) {
    await materializeInstance(inst.id, { tier: "batch" });
  }

  // Phase 2: Clean up allocations on terminated instances.
  // Also emit retroactive metering_stop for any terminated instance that doesn't have one yet.
  const terminated = queryMany<{ alloc_id: number; instance_id: number; tenant_id: number; provider: string | null; provider_id: string | null; spec: string | null; region: string | null }>(
    `SELECT a.id as alloc_id, i.id as instance_id, i.tenant_id, i.provider, i.provider_id, i.spec, i.region
     FROM allocations a
     JOIN instances i ON a.instance_id = i.id
     WHERE a.status NOT IN ('COMPLETE', 'FAILED')
       AND i.workflow_state LIKE 'terminate:%'`,
    []
  );
  let count = 0;
  const emittedMeteringStop = new Set<number>();
  for (const row of terminated) {
    const r = failAllocationAnyState(row.alloc_id);
    if (r.success) count++;

    // Emit retroactive metering_stop once per instance (dedupe_key prevents double-counting).
    // Uses same dedupe_key as the normal lifecycle stop — idempotent if stop already emitted.
    // NOTE: If NO prior stop exists, the retroactive stop lacks window_start_ms (unknown),
    // so v_cost_priced computes $0. C-1 pre-computed amount_cents is not available in the
    // retroactive path — the audit trail is complete but the cost amount requires manual recovery.
    //
    // NOTE: Spot interrupt complete (POST /v1/agent/spot-interrupt-complete) does not emit
    // metering_stop directly. Billing gap between spot signal and reconciler detection is
    // covered here. Deferred: add metering_stop to the spot-interrupt-complete handler.
    if (!emittedMeteringStop.has(row.instance_id) && row.provider_id) {
      emittedMeteringStop.add(row.instance_id);
      const reconcileNow = Date.now();
      // Get full instance for tenant_id
      const inst = queryOne<Instance>("SELECT * FROM instances WHERE id = ?", [row.instance_id]);
      if (inst) {
        try {
          // C-1: Compute amount_cents from paired metering_start and price observations
          const meterData = computeMeteringStopData(
            inst.id,
            inst.provider ?? "",
            inst.spec ?? "",
            inst.region ?? null,
            inst.is_spot === 1,
            reconcileNow
          );

          emitAuditEvent({
            event_type: "metering_stop",
            tenant_id: inst.tenant_id,
            instance_id: inst.id,
            provider: inst.provider ?? undefined,
            spec: inst.spec ?? undefined,
            region: inst.region ?? undefined,
            source: "reconciliation",
            is_cost: true,
            is_usage: true,
            is_reconciliation: true,
            data: {
              provider_resource_id: inst.provider_id,
              metering_window_end_ms: reconcileNow,
              retroactive: true,
              ...meterData,
            },
            dedupe_key: `${inst.provider}:${inst.provider_id}:metering_stop`,
            occurred_at: reconcileNow,
          });
        } catch {
          // Non-fatal: dedupe_key violation means metering_stop already emitted (expected)
        }
      }
    }
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
  const expired = findExpiredAvailable(cutoff);
  let count = 0;
  for (const alloc of expired) {
    const r = failAllocation(alloc.id, "AVAILABLE");
    if (r.success) count++;
  }
  return count;
}

// Metering stop enrichment: shared helper in billing/metering.ts
