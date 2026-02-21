// db/allocations.ts - Allocation CRUD + warm pool operations

import {
  ConflictError,
  TIMING,
  type AllocationStatus,
} from "@skyrepl/contracts";
import { getDatabase, queryOne, queryMany, execute } from "./helpers";

export interface Allocation {
  id: number;
  tenant_id: number;
  run_id: number | null;
  instance_id: number;
  status: AllocationStatus;
  current_manifest_id: number | null;
  user: string;
  workdir: string;
  debug_hold_until: number | null;
  claimed_by: number | null;
  created_at: number;
  updated_at: number;
  completed_at: number | null;
}

export function getAllocation(id: number): Allocation | null {
  return queryOne<Allocation>("SELECT * FROM allocations WHERE id = ?", [id]);
}

export function createAllocation(
  data: Omit<Allocation, "id" | "created_at" | "updated_at" | "tenant_id" | "claimed_by">,
  tenantId: number = 1
): Allocation {
  const now = Date.now();
  const db = getDatabase();

  try {
    const stmt = db.prepare(`
      INSERT INTO allocations (tenant_id, run_id, instance_id, status, current_manifest_id, user, workdir, debug_hold_until, completed_at, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);

    const result = stmt.run(
      tenantId,
      data.run_id,
      data.instance_id,
      data.status,
      data.current_manifest_id,
      data.user,
      data.workdir,
      data.debug_hold_until,
      data.completed_at,
      now,
      now
    );

    return getAllocation(result.lastInsertRowid as number)!;
  } catch (e) {
    const error = e as { code?: string };
    if (error.code === "SQLITE_CONSTRAINT_FOREIGNKEY") {
      throw new ConflictError("Instance not found");
    }
    throw e;
  }
}


export function deleteAllocation(id: number): void {
  const allocation = getAllocation(id);
  if (allocation && !["COMPLETE", "FAILED"].includes(allocation.status)) {
    throw new ConflictError("Cannot delete active allocation");
  }

  execute("DELETE FROM allocations WHERE id = ?", [id]);
}

/**
 * Find a warm allocation with BT5 scoring:
 *   100 = init_checksum matches (exact snapshot match)
 *   50  = init_checksum IS NULL (vanilla instance)
 *   0   = mismatch (excluded from results)
 * Tiebreaker: created_at ASC (FIFO — oldest first).
 */
export function findWarmAllocation(
  spec: { spec: string; region?: string; tenantId: number },
  initChecksum?: string
): Allocation | null {
  let sql: string;
  const params: unknown[] = [spec.spec, spec.tenantId];

  const heartbeatCutoff = Date.now() - TIMING.STALE_DETECTION_MS;

  if (initChecksum) {
    // Scoring: 100 for exact match, 50 for NULL (vanilla), exclude mismatches
    sql = `
      SELECT a.* FROM allocations a
      JOIN instances i ON a.instance_id = i.id
      WHERE a.status = 'AVAILABLE' AND a.run_id IS NULL
        AND i.spec = ?
        AND i.tenant_id = ?
        AND (i.workflow_state LIKE '%:complete' OR i.workflow_state = 'launch-run:provisioning')
        AND i.last_heartbeat > ?
        AND (i.init_checksum = ? OR i.init_checksum IS NULL)
    `;
    params.push(heartbeatCutoff, initChecksum);

    if (spec.region) {
      sql += " AND i.region = ?";
      params.push(spec.region);
    }

    // Order: exact match first (CASE returns 0 for match = sorts first with ASC on negative),
    // then vanilla, then FIFO within each group
    sql += ` ORDER BY
      CASE
        WHEN i.init_checksum = ? THEN 0
        WHEN i.init_checksum IS NULL THEN 1
      END ASC,
      a.created_at ASC
      LIMIT 1`;
    params.push(initChecksum);
  } else {
    // No checksum: any AVAILABLE allocation works, FIFO order
    sql = `
      SELECT a.* FROM allocations a
      JOIN instances i ON a.instance_id = i.id
      WHERE a.status = 'AVAILABLE' AND a.run_id IS NULL
        AND i.spec = ?
        AND i.tenant_id = ?
        AND (i.workflow_state LIKE '%:complete' OR i.workflow_state = 'launch-run:provisioning')
        AND i.last_heartbeat > ?
    `;
    params.push(heartbeatCutoff);

    if (spec.region) {
      sql += " AND i.region = ?";
      params.push(spec.region);
    }

    sql += " ORDER BY a.created_at ASC LIMIT 1";
  }

  return queryOne<Allocation>(sql, params);
}

/**
 * Count non-terminal allocations for an instance.
 * Used by replenishment logic to enforce MAX_ALLOCATIONS_PER_INSTANCE.
 */
export function countInstanceAllocations(instanceId: number): number {
  const result = queryOne<{ count: number }>(
    "SELECT COUNT(*) as count FROM allocations WHERE instance_id = ? AND status NOT IN ('COMPLETE', 'FAILED')",
    [instanceId]
  );
  return result?.count ?? 0;
}

/**
 * Find stale CLAIMED allocations older than the given cutoff time.
 * Used by background reconciliation to timeout abandoned claims.
 */
export function findStaleClaimed(cutoffTime: number): Allocation[] {
  return queryMany<Allocation>(
    "SELECT * FROM allocations WHERE status = 'CLAIMED' AND updated_at < ?",
    [cutoffTime]
  );
}

/**
 * Find expired AVAILABLE allocations older than the given cutoff time.
 * Used by background reconciliation to expire stale warm pool entries.
 */
export function findExpiredAvailable(cutoffTime: number): Allocation[] {
  return queryMany<Allocation>(
    "SELECT * FROM allocations WHERE status = 'AVAILABLE' AND created_at < ?",
    [cutoffTime]
  );
}

export function getWarmPoolStats(): {
  available: number;
  bySpec: Record<string, number>;
} {
  const total = queryOne<{ count: number }>(
    "SELECT COUNT(*) as count FROM allocations WHERE status = 'AVAILABLE'"
  );

  const bySpec = queryMany<{ spec: string; count: number }>(
    `SELECT i.spec || ':' || i.region as spec, COUNT(*) as count
     FROM allocations a
     JOIN instances i ON a.instance_id = i.id
     WHERE a.status = 'AVAILABLE'
     GROUP BY spec`
  );

  return {
    available: total?.count ?? 0,
    bySpec: Object.fromEntries(bySpec.map(r => [r.spec, r.count])),
  };
}
