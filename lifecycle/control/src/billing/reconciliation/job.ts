// billing/reconciliation/job.ts — Daily billing reconciliation job (WL-061-4A)
//
// Compares SkyREPL's metered costs against provider billing API data.
// For each provider past its settlement window, fetches provider costs and
// cross-references against our audit_log records. Emits reconciliation events
// for discrepancies and orphan recovery.
//
// Divergence categories:
//   "clock_skew"       — usage crossed a period boundary differently on each side
//   "hidden_charge"    — provider billed for something we don't have a record of
//   "price_change"     — rate changed mid-period
//   "rounding"         — sub-cent rounding difference
//   "lost_event"       — we lost a metering event (metering_stop missing)
//   "orphan_cost"      — provider billed for a resource we never tracked
//   "regional_outage"  — provider billed despite reporting no usage (outage billing)

import { TIMING } from "@skyrepl/contracts";
import { classifyDiscrepancy } from "./alerts";
import { emitAuditEvent } from "../../material/db/audit";
import { queryMany } from "../../material/db/helpers";
import { getDuckDB, duckQuery } from "../../material/duckdb";
import type { ProviderCostEntry, ReconciliationProvider } from "./types";

// =============================================================================
// Divergence category type
// =============================================================================

export type DivergenceCategory =
  | "clock_skew"
  | "hidden_charge"
  | "price_change"
  | "rounding"
  | "lost_event"
  | "orphan_cost"
  | "regional_outage";

// =============================================================================
// Result types
// =============================================================================

export interface ReconciliationResult {
  /** Provider name */
  provider: string;
  /** Period that was reconciled */
  period: { start_ms: number; end_ms: number };
  /** Total provider cost entries checked */
  resources_checked: number;
  /** Number of entries with discrepancies above threshold */
  discrepancies: number;
  /** Number of reconciliation adjustment events emitted */
  adjustments_emitted: number;
  /** Number of orphan cost recovery events emitted */
  orphans_recovered: number;
}

// =============================================================================
// Our metered cost lookup (DuckDB or SQLite fallback)
// =============================================================================

interface OurCostRow {
  provider_resource_id: string;
  total_cents: number;
  tenant_id: number;
}

async function getOurMeteredCosts(
  provider: string,
  periodStartMs: number,
  periodEndMs: number
): Promise<OurCostRow[]> {
  // Try DuckDB v_cost_priced first
  if (getDuckDB()) {
    try {
      const rows = await duckQuery<{
        provider_resource_id: string;
        amount_cents: bigint | number;
        tenant_id: number;
      }>(
        `SELECT
          provider_resource_id,
          SUM(amount_cents) AS amount_cents,
          tenant_id
         FROM v_cost_priced
         WHERE provider = ?
           AND occurred_at >= ?
           AND occurred_at <= ?
           AND provider_resource_id IS NOT NULL
         GROUP BY provider_resource_id, tenant_id`,
        [provider, periodStartMs, periodEndMs]
      );

      return rows.map(r => ({
        provider_resource_id: r.provider_resource_id,
        total_cents: Number(r.amount_cents),
        tenant_id: Number(r.tenant_id),
      }));
    } catch (err) {
      console.warn(
        `[billing-recon] DuckDB query failed for ${provider}, falling back to SQLite:`,
        err instanceof Error ? err.message : String(err)
      );
    }
  }

  // SQLite fallback: sum amount_cents from audit_log data JSON
  const rows = queryMany<{ data: string; tenant_id: number }>(
    `SELECT data, tenant_id FROM audit_log
     WHERE is_cost = 1
       AND provider = ?
       AND occurred_at >= ?
       AND occurred_at <= ?
       AND json_extract(data, '$.provider_resource_id') IS NOT NULL`,
    [provider, periodStartMs, periodEndMs]
  );

  // Aggregate by resource ID
  const byResource = new Map<string, { cents: number; tenant_id: number }>();
  for (const row of rows) {
    try {
      const d = JSON.parse(row.data);
      const resourceId = d.provider_resource_id as string | undefined;
      if (!resourceId) continue;
      const cents = typeof d.amount_cents === "number" ? d.amount_cents : 0;
      const existing = byResource.get(resourceId);
      if (existing) {
        existing.cents += cents;
      } else {
        byResource.set(resourceId, { cents, tenant_id: row.tenant_id });
      }
    } catch { /* malformed data — skip */ }
  }

  return Array.from(byResource.entries()).map(([id, val]) => ({
    provider_resource_id: id,
    total_cents: val.cents,
    tenant_id: val.tenant_id,
  }));
}

// =============================================================================
// Default tenant ID (used for reconciliation events without a specific tenant)
// =============================================================================

function getDefaultTenantId(): number {
  // Reconciliation events are emitted at tenant_id=1 (platform level) unless
  // we can identify the specific tenant from our metered cost records.
  return 1;
}

// =============================================================================
// Infer divergence category from discrepancy shape
// =============================================================================

function inferDivergenceCategory(
  ourCents: number,
  theirCents: number,
  absoluteDiff: number
): DivergenceCategory {
  // Sub-cent difference → rounding
  if (absoluteDiff <= 1) return "rounding";

  // We have no record at all → lost event (we metered 0, they billed something)
  if (ourCents === 0 && theirCents > 0) return "lost_event";

  // Very small percentage difference → clock skew (boundary effects)
  const pct = theirCents > 0 ? (absoluteDiff / theirCents) * 100 : 0;
  if (pct < 2) return "clock_skew";

  // Default to hidden charge (provider billed more than we expected)
  return "hidden_charge";
}

// =============================================================================
// Main reconciliation job
// =============================================================================

/**
 * Run daily billing reconciliation for all providers past their settlement window.
 *
 * For each provider:
 * 1. Check if the reconciliation period has passed the settlement window.
 * 2. Fetch provider's billed costs via the provider's billing API adapter.
 * 3. Query our metered costs from audit_log / v_cost_priced.
 * 4. For each provider cost entry, compare against our records.
 * 5. Emit reconciliation audit events for discrepancies above threshold.
 * 6. Emit orphan recovery events for resources the provider billed but we never tracked.
 *
 * @param options.providerNames     Optional list of provider names to limit reconciliation.
 * @param options.providerOverrides Optional array of provider adapters to use directly (bypasses dynamic imports).
 * @param options.period            Optional period to reconcile. Defaults to yesterday.
 * @returns Array of ReconciliationResult, one per provider.
 */
export async function runBillingReconciliation(options?: {
  providerNames?: string[];
  providerOverrides?: ReconciliationProvider[];
  period?: { start_ms: number; end_ms: number };
}): Promise<ReconciliationResult[]> {
  // Resolve default reconciliation period: yesterday (settled)
  const now = Date.now();
  const defaultPeriodEnd = new Date(now);
  defaultPeriodEnd.setUTCHours(0, 0, 0, 0); // midnight UTC today
  const defaultPeriodStart = new Date(defaultPeriodEnd.getTime() - 86_400_000); // yesterday

  const periodStartMs = options?.period?.start_ms ?? defaultPeriodStart.getTime();
  const periodEndMs = options?.period?.end_ms ?? defaultPeriodEnd.getTime();

  // Period as ISO date strings (YYYY-MM-DD) for provider APIs
  const periodStart = new Date(periodStartMs).toISOString().slice(0, 10);
  const periodEnd = new Date(periodEndMs).toISOString().slice(0, 10);

  // Resolve provider list: use overrides directly, or load via dynamic imports
  let allProviders: ReconciliationProvider[];
  if (options?.providerOverrides) {
    allProviders = options.providerOverrides;
  } else {
    const { awsReconciliationProvider } = await import("./aws");
    const { lambdaReconciliationProvider } = await import("./lambda");
    const { digitaloceanReconciliationProvider } = await import("./digitalocean");
    const { runpodReconciliationProvider } = await import("./runpod");

    allProviders = [
      awsReconciliationProvider,
      lambdaReconciliationProvider,
      digitaloceanReconciliationProvider,
      runpodReconciliationProvider,
    ];
  }

  // Filter to requested providers if specified
  const providers = options?.providerNames
    ? allProviders.filter(p => options.providerNames!.includes(p.name))
    : allProviders;

  const results: ReconciliationResult[] = [];

  for (const provider of providers) {
    const result: ReconciliationResult = {
      provider: provider.name,
      period: { start_ms: periodStartMs, end_ms: periodEndMs },
      resources_checked: 0,
      discrepancies: 0,
      adjustments_emitted: 0,
      orphans_recovered: 0,
    };

    // Check settlement window: skip if the period has not yet settled
    const periodAge = now - periodEndMs;
    if (periodAge < provider.settlementWindowMs) {
      console.log(
        `[billing-recon] ${provider.name}: period ends ${periodEnd}, ` +
        `settlement window ${provider.settlementWindowMs / 3600000}h not yet passed — skipping (estimated)`
      );
      results.push(result);
      continue;
    }

    // Fetch provider billed costs
    let providerCosts: ProviderCostEntry[];
    try {
      providerCosts = await provider.fetchBilledCosts({ start: periodStart, end: periodEnd });
    } catch (err: any) {
      if (err?.code === "MISSING_CREDENTIALS") {
        console.log(
          `[billing-recon] ${provider.name}: no credentials configured, skipping`
        );
      } else {
        console.error(
          `[billing-recon] ${provider.name}: fetchBilledCosts failed:`,
          err instanceof Error ? err.message : String(err)
        );
      }
      results.push(result);
      continue;
    }

    if (providerCosts.length === 0) {
      console.log(`[billing-recon] ${provider.name}: no billed costs for ${periodStart}–${periodEnd}`);
      results.push(result);
      continue;
    }

    // Fetch our metered costs for this provider and period
    const ourCosts = await getOurMeteredCosts(provider.name, periodStartMs, periodEndMs);
    const ourCostByResource = new Map<string, OurCostRow>(
      ourCosts.map(r => [r.provider_resource_id, r])
    );

    result.resources_checked = providerCosts.length;

    // Compare each provider cost entry against our records
    for (const entry of providerCosts) {
      const ours = ourCostByResource.get(entry.provider_resource_id);
      const ourCents = ours?.total_cents ?? 0;
      const theirCents = entry.billed_amount_cents;

      const classification = classifyDiscrepancy(ourCents, theirCents);

      if (classification.level === "none") {
        // Costs match within acceptable tolerance — no action needed
        continue;
      }

      result.discrepancies++;

      const isOrphan = ours === undefined && theirCents > 0;
      const adjustmentCents = theirCents - ourCents;
      const tenantId = ours?.tenant_id ?? getDefaultTenantId();

      // Choose divergence category
      const divergenceCategory: DivergenceCategory = isOrphan
        ? "orphan_cost"
        : inferDivergenceCategory(ourCents, theirCents, classification.absoluteDiff);

      if (isOrphan) {
        // Provider billed for a resource we never tracked — orphan cost recovery
        try {
          emitAuditEvent({
            event_type: "reconciliation",
            tenant_id: tenantId,
            provider: entry.provider,
            source: "billing_reconciliation",
            is_cost: true,
            is_reconciliation: true,
            data: {
              reconciliation_scope: "orphan_cost_recovery",
              provider_resource_id: entry.provider_resource_id,
              our_amount_cents: 0,
              provider_amount_cents: theirCents,
              adjustment_cents: theirCents,
              divergence_category: "orphan_cost" as DivergenceCategory,
              period_start: periodStart,
              period_end: periodEnd,
              alert_level: classification.level,
              absolute_diff_cents: classification.absoluteDiff,
              percentage_diff: classification.percentageDiff,
            },
            dedupe_key: `billing-recon:${entry.provider}:${entry.provider_resource_id}:${periodStart}:orphan`,
            occurred_at: now,
          });
          result.orphans_recovered++;
          result.adjustments_emitted++;
        } catch {
          // dedupe_key violation = already emitted; non-fatal
        }
      } else {
        // Known resource with a discrepancy — emit reconciliation adjustment
        try {
          emitAuditEvent({
            event_type: "reconciliation",
            tenant_id: tenantId,
            provider: entry.provider,
            source: "billing_reconciliation",
            is_cost: true,
            is_reconciliation: true,
            data: {
              reconciliation_scope: "cost_adjustment",
              provider_resource_id: entry.provider_resource_id,
              our_amount_cents: ourCents,
              provider_amount_cents: theirCents,
              adjustment_cents: adjustmentCents,
              divergence_category: divergenceCategory,
              period_start: periodStart,
              period_end: periodEnd,
              alert_level: classification.level,
              absolute_diff_cents: classification.absoluteDiff,
              percentage_diff: classification.percentageDiff,
            },
            dedupe_key: `billing-recon:${entry.provider}:${entry.provider_resource_id}:${periodStart}:adj`,
            occurred_at: now,
          });
          result.adjustments_emitted++;
        } catch {
          // dedupe_key violation = already emitted; non-fatal
        }
      }

      // Emit billing_incident for ALERT-level discrepancies
      if (classification.level === "alert") {
        try {
          emitAuditEvent({
            event_type: "billing_incident",
            tenant_id: tenantId,
            provider: entry.provider,
            source: "billing_reconciliation",
            is_cost: false,
            is_reconciliation: true,
            data: {
              incident_type: "billing_discrepancy_alert",
              provider_resource_id: entry.provider_resource_id,
              our_amount_cents: ourCents,
              provider_amount_cents: theirCents,
              adjustment_cents: adjustmentCents,
              divergence_category: divergenceCategory,
              period_start: periodStart,
              period_end: periodEnd,
              absolute_diff_cents: classification.absoluteDiff,
              percentage_diff: classification.percentageDiff,
            },
            dedupe_key: `billing-recon:${entry.provider}:${entry.provider_resource_id}:${periodStart}:incident`,
            occurred_at: now,
          });
        } catch {
          // dedupe_key violation = already emitted; non-fatal
        }
      }

      // Emit billing_incident for WARN-level discrepancies
      if (classification.level === "warn") {
        try {
          emitAuditEvent({
            event_type: "billing_incident",
            tenant_id: tenantId,
            provider: entry.provider,
            source: "billing_reconciliation",
            is_cost: false,
            is_reconciliation: true,
            data: {
              incident_type: "billing_discrepancy_warn",
              provider_resource_id: entry.provider_resource_id,
              our_amount_cents: ourCents,
              provider_amount_cents: theirCents,
              adjustment_cents: adjustmentCents,
              divergence_category: divergenceCategory,
              period_start: periodStart,
              period_end: periodEnd,
              absolute_diff_cents: classification.absoluteDiff,
              percentage_diff: classification.percentageDiff,
            },
            dedupe_key: `billing-recon:${entry.provider}:${entry.provider_resource_id}:${periodStart}:warn-incident`,
            occurred_at: now,
          });
        } catch {
          // dedupe_key violation = already emitted; non-fatal
        }
      }

      // Log warnings/alerts
      if (classification.level === "alert") {
        console.error(
          `[billing-recon] ALERT: ${entry.provider} resource ${entry.provider_resource_id}: ` +
          `our=${ourCents}¢ provider=${theirCents}¢ ` +
          `diff=${classification.absoluteDiff}¢ (${classification.percentageDiff.toFixed(1)}%) ` +
          `category=${divergenceCategory}`
        );
      } else {
        console.warn(
          `[billing-recon] WARN: ${entry.provider} resource ${entry.provider_resource_id}: ` +
          `our=${ourCents}¢ provider=${theirCents}¢ ` +
          `diff=${classification.absoluteDiff}¢ (${classification.percentageDiff.toFixed(1)}%) ` +
          `category=${divergenceCategory}`
        );
      }
    }

    if (result.discrepancies > 0) {
      console.warn(
        `[billing-recon] ${provider.name}: ${result.resources_checked} checked, ` +
        `${result.discrepancies} discrepancies, ` +
        `${result.adjustments_emitted} adjustments, ` +
        `${result.orphans_recovered} orphans`
      );
    } else {
      console.log(
        `[billing-recon] ${provider.name}: ${result.resources_checked} resources checked, all within thresholds`
      );
    }

    results.push(result);
  }

  return results;
}
