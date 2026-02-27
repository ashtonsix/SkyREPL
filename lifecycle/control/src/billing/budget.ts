// billing/budget.ts — Budget projection engine (WL-061-3B §3)
//
// Projects current spend against budget for a tenant (and optionally a user).
// DuckDB is the sole query path for all cost/billing data.

import { getTenant, getUser } from "../material/db";
import { getDuckDB, duckQueryOne } from "../material/duckdb";

// =============================================================================
// Types
// =============================================================================

export interface BudgetProjection {
  /** Fully settled cost in integer cents */
  settled_cents: number;
  /** Estimated cost from open metering windows in integer cents */
  estimated_cents: number;
  /** Cost from pending reconciliation events in integer cents */
  pending_cents: number;
  /** Total projected spend: settled + estimated + pending */
  total_cents: number;
  /** Budget ceiling in integer cents (0 = unlimited) */
  budget_cents: number;
  /** Remaining budget: budget_cents - total_cents (negative = overage) */
  remaining_cents: number;
  /** Remaining as percentage of budget (100 if no budget set) */
  remaining_pct: number;
  /** Period start (ms since epoch) */
  period_start_ms: number;
  /** Period end (ms since epoch) */
  period_end_ms: number;
}

// =============================================================================
// Helpers
// =============================================================================

/**
 * Convert a USD float to integer cents safely.
 * Uses Math.round to avoid floating-point drift (e.g. 1.005 * 100 = 100.49999...).
 * Clamps negative values to 0 (budget_usd should never be negative).
 */
export function usdToCents(usd: number): number {
  return Math.max(0, Math.round(usd * 100));
}

// =============================================================================
// Budget Projection
// =============================================================================

/**
 * Project budget usage for a tenant (and optionally a specific user) over a period.
 * Uses DuckDB v_cost_fees view — throws if DuckDB is not initialized.
 */
export async function projectBudget(params: {
  tenant_id: number;
  user_id?: number;
  period_start_ms: number;
  period_end_ms: number;
}): Promise<BudgetProjection> {
  const { tenant_id, user_id, period_start_ms, period_end_ms } = params;

  // Resolve budget ceiling
  let budgetCents = 0;
  if (user_id !== undefined) {
    const user = getUser(user_id);
    if (user?.budget_usd != null) {
      budgetCents = usdToCents(user.budget_usd);
    }
  } else {
    const tenant = getTenant(tenant_id);
    if (tenant?.budget_usd != null) {
      budgetCents = usdToCents(tenant.budget_usd);
    }
  }

  if (!getDuckDB()) {
    throw new Error("[budget] DuckDB not initialized — cannot project budget");
  }

  const userFilter = user_id !== undefined ? "AND user_id = ?" : "";
  const queryParams: unknown[] = [tenant_id, period_start_ms, period_end_ms];
  if (user_id !== undefined) queryParams.push(user_id);

  // Settled costs: metering_stop events with pre-computed amount_cents (C-1).
  const settledRow = await duckQueryOne<{ settled_cents: number }>(
    `SELECT COALESCE(SUM(amount_cents), 0) AS settled_cents
     FROM v_cost_fees
     WHERE tenant_id = ?
       AND event_type = 'metering_stop'
       AND occurred_at >= ?
       AND occurred_at <= ?
       ${userFilter}`,
    queryParams
  );

  // Pending reconciliation events
  const reconQueryParams: unknown[] = [tenant_id, period_start_ms, period_end_ms];
  if (user_id !== undefined) reconQueryParams.push(user_id);

  const pendingRow = await duckQueryOne<{ pending_cents: number }>(
    `SELECT COALESCE(SUM(amount_cents), 0) AS pending_cents
     FROM v_cost_fees
     WHERE tenant_id = ?
       AND is_reconciliation = true
       AND occurred_at >= ?
       AND occurred_at <= ?
       ${userFilter}`,
    reconQueryParams
  );

  // Open metering windows (metering_start without matching stop).
  // DuckDB returns amount_cents=0 for open windows (no window_end_ms).
  // This is a known design tradeoff — open-window estimation requires
  // elapsed time × hourly_rate_cents, which DuckDB views don't compute.
  const openParams: unknown[] = [tenant_id, period_start_ms, period_end_ms];
  if (user_id !== undefined) openParams.push(user_id);

  const openRow = await duckQueryOne<{ estimated_cents: number }>(
    `SELECT COALESCE(SUM(amount_cents), 0) AS estimated_cents
     FROM v_cost_fees
     WHERE tenant_id = ?
       AND event_type = 'metering_start'
       AND occurred_at >= ?
       AND occurred_at <= ?
       ${userFilter}`,
    openParams
  );

  const settledCents = Number(settledRow?.settled_cents ?? 0);
  const estimatedCents = Number(openRow?.estimated_cents ?? 0);
  const pendingCents = Number(pendingRow?.pending_cents ?? 0);
  const totalCents = settledCents + estimatedCents + pendingCents;

  const remainingCents = budgetCents > 0 ? budgetCents - totalCents : 0;
  const remainingPct = budgetCents > 0
    ? Math.max(0, Math.round((remainingCents / budgetCents) * 100))
    : 100;

  return {
    settled_cents: settledCents,
    estimated_cents: estimatedCents,
    pending_cents: pendingCents,
    total_cents: totalCents,
    budget_cents: budgetCents,
    remaining_cents: remainingCents,
    remaining_pct: remainingPct,
    period_start_ms,
    period_end_ms,
  };
}
