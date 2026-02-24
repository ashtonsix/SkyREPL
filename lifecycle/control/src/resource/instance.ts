// resource/instance.ts - Instance Resource Operations
//
// MATERIALIZATION BOUNDARY — instance resources
// READS: use materializeInstance() / materializeInstanceBatch() (provider-enriched, cached, drift-detecting)
// WRITES: use createInstanceRecord(), updateInstanceRecord() (direct DB, no materialization needed)
// Raw DB reads (getInstanceRecordRaw) bypass materialization — use only when you need the DB record, not the resource.

import type { Instance } from "../material/db";
import type { Materialized, MaterializeOptions } from "@skyrepl/contracts";
import {
  getInstance,
  createInstance,
  updateInstance,
  listInstances,
  queryMany,
} from "../material/db";
import { TIMING } from "@skyrepl/contracts";
import { stampMaterialized, resolveOptions, cachedMaterialize } from "./materializer";
import { cacheInvalidate } from "./cache";
import { getProvider } from "../provider/registry";
import type { ProviderInstance } from "../provider/types";
import { failAllocation } from "../workflow/state-transitions";

// =============================================================================
// Materializer (observed resource — consults provider state within TTL)
// =============================================================================

/**
 * Materialize a single instance. For non-terminal instances with a provider_id,
 * fetches external state from the provider within TTL. Drift (IP change, status
 * change) is written back to the DB as a side-effect — every materialization of
 * an observed resource is an implicit reconciliation check.
 */
export async function materializeInstance(id: number, opts?: MaterializeOptions): Promise<Materialized<Instance> | null> {
  const record = getInstance(id);
  if (!record) return null;

  // Terminal instances are DB-authoritative — no provider call needed
  if (isTerminalState(record.workflow_state) || !record.provider_id) {
    return stampMaterialized(record);
  }

  const { ttlMs, forceRefresh } = resolveOptions(opts, record.provider);
  const cacheKey = `instance:${id}`;

  return cachedMaterialize<Materialized<Instance>>(cacheKey, ttlMs, async () => {
    const enriched = await enrichFromProvider(record);
    return stampMaterialized(enriched);
  }, forceRefresh);
}

/**
 * Batch materialize instances. Groups by provider and issues provider.list()
 * for efficiency. Terminal instances skip provider calls.
 */
export async function materializeInstanceBatch(ids: number[], opts?: MaterializeOptions): Promise<Materialized<Instance>[]> {
  if (ids.length === 0) return [];
  const placeholders = ids.map(() => "?").join(", ");
  const records = queryMany<Instance>(`SELECT * FROM instances WHERE id IN (${placeholders})`, ids);

  const results: Materialized<Instance>[] = [];
  const needsProvider: Instance[] = [];

  for (const record of records) {
    if (isTerminalState(record.workflow_state) || !record.provider_id) {
      results.push(stampMaterialized(record));
    } else {
      needsProvider.push(record);
    }
  }

  if (needsProvider.length > 0) {
    // Group by provider for batch fetch
    const byProvider = new Map<string, Instance[]>();
    for (const inst of needsProvider) {
      const group = byProvider.get(inst.provider) ?? [];
      group.push(inst);
      byProvider.set(inst.provider, group);
    }

    for (const [providerName, instances] of byProvider) {
      try {
        const provider = await getProvider(providerName as any);
        const providerInstances = await provider.list({
          tags: { skyrepl: "true" },
        });

        // Index by provider_id for O(1) lookup
        const providerMap = new Map<string, ProviderInstance>();
        for (const pi of providerInstances) {
          providerMap.set(pi.id, pi);
        }

        for (const inst of instances) {
          const providerState = providerMap.get(inst.provider_id!);
          const enriched = applyProviderState(inst, providerState ?? null);
          results.push(stampMaterialized(enriched));
        }
      } catch (err) {
        // Provider unreachable — fall back to DB state
        console.warn(`[materialize] provider ${providerName} unreachable for batch:`, err);
        for (const inst of instances) {
          results.push(stampMaterialized(inst));
        }
      }
    }
  }

  return results;
}

export function isTerminalState(state: string): boolean {
  return (
    state === "terminate:complete" ||
    state.endsWith(":error") ||
    state.endsWith(":compensated")
  );
}

/**
 * Fetch external state from the provider and reconcile drift.
 * Falls back to DB state if provider is unreachable.
 *
 * §3.3 mark_terminated: when provider.get() returns null (instance
 * definitively gone), the DB record is updated to terminate:complete.
 * This is the "hard barrier" — dead instances are caught here, not
 * deferred to the orphan scanner.
 */
async function enrichFromProvider(record: Instance): Promise<Instance> {
  try {
    const provider = await getProvider(record.provider as any);
    const providerState = await provider.get(record.provider_id!, record.region ?? undefined);

    if (!providerState) {
      // provider.get() returning null is a definitive signal: this specific
      // instance does not exist. Mark as terminated in the DB.
      console.warn(
        `[materialize] Instance ${record.id} (${record.provider_id}) not found by provider ${record.provider} — marking terminated`
      );
      updateInstance(record.id, { workflow_state: "terminate:complete" });
      cacheInvalidate(`instance:${record.id}`);
      return { ...record, workflow_state: "terminate:complete" };
    }

    return applyProviderState(record, providerState);
  } catch (err) {
    console.warn(`[materialize] provider ${record.provider} unreachable for instance ${record.id}:`, err);
    return record;
  }
}

/**
 * Apply provider state to a DB record. Detects drift and writes corrections
 * back to the DB. Returns the reconciled record.
 */
function applyProviderState(record: Instance, providerState: ProviderInstance | null): Instance {
  if (!providerState) {
    // Provider says instance doesn't exist — if we think it's running, that's drift.
    // Don't auto-terminate here; the orphan scanner handles that.
    return record;
  }

  // Provider reports instance as dead or dying. "terminated" covers DO "archive"
  // and EC2 "terminated"; "terminating" covers EC2 "shutting-down" (brief window
  // before full termination). Both are definitive signals — mark_terminated applies.
  if (providerState.status === "terminated" || providerState.status === "terminating") {
    console.warn(
      `[materialize] Instance ${record.id} (${record.provider_id}) reported terminated by provider ${record.provider} — marking terminated`
    );
    updateInstance(record.id, { workflow_state: "terminate:complete" });
    cacheInvalidate(`instance:${record.id}`);
    return { ...record, workflow_state: "terminate:complete" };
  }

  const updates: Partial<Instance> = {};

  // IP drift
  if (providerState.ip && providerState.ip !== record.ip) {
    updates.ip = providerState.ip;
  }

  // Write drift corrections back to DB
  if (Object.keys(updates).length > 0) {
    updateInstance(record.id, updates);
    cacheInvalidate(`instance:${record.id}`);
    return { ...record, ...updates };
  }

  return record;
}

// =============================================================================
// Instance Lifecycle
// =============================================================================

export function createInstanceRecord(
  data: Omit<Instance, "id" | "created_at" | "tenant_id">,
  tenantId: number = 1
): Instance {
  return createInstance(data, tenantId);
}

export function getInstanceRecordRaw(id: number): Instance | null {
  return getInstance(id);
}

export function updateInstanceRecord(
  id: number,
  updates: Partial<Instance>
): Instance {
  return updateInstance(id, updates);
}

export function listInstanceRecords(filter?: {
  provider?: string;
  workflow_state?: string;
  spec?: string;
}): Instance[] {
  // listInstances from DB only supports provider and workflow_state filters.
  // For spec filtering, we query directly with SQL.
  if (filter?.spec) {
    let sql = "SELECT * FROM instances WHERE 1=1";
    const params: unknown[] = [];

    if (filter.provider) {
      sql += " AND provider = ?";
      params.push(filter.provider);
    }
    if (filter.workflow_state) {
      sql += " AND workflow_state = ?";
      params.push(filter.workflow_state);
    }
    sql += " AND spec = ?";
    params.push(filter.spec);

    return queryMany<Instance>(sql, params);
  }

  return listInstances(filter);
}

// =============================================================================
// Instance State Queries
// =============================================================================

export function isInstanceHealthy(instance: Instance): boolean {
  // Heartbeat must be fresh
  if (Date.now() - instance.last_heartbeat >= TIMING.STALE_DETECTION_MS) {
    return false;
  }

  // Workflow state must not be an error state
  if (
    instance.workflow_state.endsWith(":error") ||
    instance.workflow_state.endsWith(":compensated")
  ) {
    return false;
  }

  // Workflow state must not be terminal
  if (instance.workflow_state === "terminate:complete") {
    return false;
  }

  return true;
}

export function getInstancesByProvider(provider: string): Instance[] {
  return listInstances({ provider });
}

const TERMINAL_STATES = [
  "terminate:complete",
  "spawn:error",
  "terminate:error",
  "terminate:compensated",
  "launch-run:error",
  "launch-run:compensated",
];

export function getActiveInstances(): Instance[] {
  const placeholders = TERMINAL_STATES.map(() => "?").join(", ");
  return queryMany<Instance>(
    `SELECT * FROM instances WHERE workflow_state NOT IN (${placeholders})`,
    TERMINAL_STATES
  );
}

export function getStaleInstances(cutoffMs: number): Instance[] {
  const cutoff = Date.now() - cutoffMs;
  const placeholders = TERMINAL_STATES.map(() => "?").join(", ");
  return queryMany<Instance>(
    `SELECT * FROM instances WHERE last_heartbeat < ? AND workflow_state NOT IN (${placeholders})`,
    [cutoff, ...TERMINAL_STATES]
  );
}

// =============================================================================
// Heartbeat
// =============================================================================

export function updateHeartbeat(instanceId: number, timestamp: number): void {
  updateInstance(instanceId, { last_heartbeat: timestamp });
}

export function detectStaleHeartbeats(thresholdMs: number): Instance[] {
  const cutoff = Date.now() - thresholdMs;
  const placeholders = TERMINAL_STATES.map(() => "?").join(", ");
  return queryMany<Instance>(
    `SELECT * FROM instances WHERE last_heartbeat < ? AND workflow_state NOT IN (${placeholders})`,
    [cutoff, ...TERMINAL_STATES]
  );
}

// =============================================================================
// Background: Heartbeat Timeout Check
// =============================================================================

export async function heartbeatTimeoutCheck(): Promise<void> {
  const now = Date.now();

  // 1. Detect all degraded instances (> 2 minutes) — single query, partition locally
  const degradedInstances = detectStaleHeartbeats(TIMING.HEARTBEAT_DEGRADED_MS);

  for (const instance of degradedInstances) {
    const elapsed = now - instance.last_heartbeat;
    const isStale = elapsed >= TIMING.STALE_DETECTION_MS;

    if (isStale) {
      // Find and fail all ACTIVE allocations for this instance
      const activeAllocations = queryMany<{ id: number }>(
        "SELECT id FROM allocations WHERE instance_id = ? AND status = 'ACTIVE'",
        [instance.id]
      );

      let failed = 0;
      for (const alloc of activeAllocations) {
        const result = failAllocation(alloc.id, "ACTIVE");
        if (result.success) failed++;
      }

      if (activeAllocations.length > 0) {
        console.log(
          `[heartbeat] Instance ${instance.id} stale (last heartbeat ${elapsed}ms ago), failed ${failed}/${activeAllocations.length} active allocation(s)`
        );
      }
    } else {
      console.debug(
        `[heartbeat] Instance ${instance.id} degraded (last heartbeat ${elapsed}ms ago)`
      );
    }
  }
}
