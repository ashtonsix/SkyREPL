// ─────────────────────────────────────────────────────────────────────────────
// RAW DB LAYER — usage_records table
// No materializer exists for usage records yet.
// DB operations below — add new queries here, not at call sites.
// ─────────────────────────────────────────────────────────────────────────────
// db/usage.ts - Usage record operations

import { NotFoundError } from "@skyrepl/contracts";
import { getDatabase, queryOne, queryMany, execute } from "./helpers";

export interface UsageRecord {
  id: number;
  tenant_id: number;
  instance_id: number;
  allocation_id: number | null;
  run_id: number | null;
  provider: string;
  spec: string;
  region: string | null;
  is_spot: number;
  started_at: number;
  finished_at: number | null;
  duration_ms: number | null;
  estimated_cost_usd: number | null;
}

export function createUsageRecord(
  data: Omit<UsageRecord, "id" | "tenant_id">,
  tenantId: number = 1
): UsageRecord {
  const db = getDatabase();

  const stmt = db.prepare(`
    INSERT INTO usage_records (tenant_id, instance_id, allocation_id, run_id, provider, spec, region, is_spot, started_at, finished_at, duration_ms, estimated_cost_usd)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  const result = stmt.run(
    tenantId,
    data.instance_id,
    data.allocation_id,
    data.run_id,
    data.provider,
    data.spec,
    data.region,
    data.is_spot,
    data.started_at,
    data.finished_at,
    data.duration_ms,
    data.estimated_cost_usd
  );

  return queryOne<UsageRecord>("SELECT * FROM usage_records WHERE id = ?", [result.lastInsertRowid as number])!;
}

export function finishUsageRecord(
  id: number,
  finishedAt: number
): UsageRecord {
  const record = queryOne<UsageRecord>("SELECT * FROM usage_records WHERE id = ?", [id]);
  if (!record) {
    throw new NotFoundError("UsageRecord", id);
  }

  const durationMs = finishedAt - record.started_at;

  execute(
    `UPDATE usage_records SET finished_at = ?, duration_ms = ? WHERE id = ?`,
    [finishedAt, durationMs, id]
  );

  return queryOne<UsageRecord>("SELECT * FROM usage_records WHERE id = ?", [id])!;
}

export function getMonthlyCostByProvider(
  monthStart: number,
  monthEnd: number
): { provider: string; total_cost: number }[] {
  return queryMany<{ provider: string; total_cost: number }>(
    `SELECT provider, SUM(estimated_cost_usd) as total_cost
     FROM usage_records
     WHERE started_at >= ? AND started_at < ?
     GROUP BY provider`,
    [monthStart, monthEnd]
  );
}

export function getActiveUsageRecords(): UsageRecord[] {
  return queryMany<UsageRecord>(
    "SELECT * FROM usage_records WHERE finished_at IS NULL"
  );
}

/** Get open (unfinished) usage records for an instance. */
export function getOpenUsageRecordsForInstance(instanceId: number): UsageRecord[] {
  return queryMany<UsageRecord>(
    "SELECT * FROM usage_records WHERE instance_id = ? AND finished_at IS NULL",
    [instanceId]
  );
}

/** Get total cost for a tenant (all-time sum of estimated_cost_usd). */
export function getTotalCostByTenant(tenantId: number): number {
  const result = queryOne<{ total: number | null }>(
    "SELECT SUM(estimated_cost_usd) as total FROM usage_records WHERE tenant_id = ?",
    [tenantId]
  );
  return result?.total ?? 0;
}

/** Get total cost for a specific user within a tenant (via allocations.claimed_by FK). */
export function getUserCostByTenant(tenantId: number, userId: number): number {
  const result = queryOne<{ total: number | null }>(
    "SELECT SUM(ur.estimated_cost_usd) as total FROM usage_records ur JOIN allocations a ON ur.allocation_id = a.id WHERE ur.tenant_id = ? AND a.claimed_by = ?",
    [tenantId, userId]
  );
  return result?.total ?? 0;
}
