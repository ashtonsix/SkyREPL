// billing/reconciliation/lambda.ts — Lambda Labs billing API adapter (WL-061-4A)
//
// Lambda Labs has no programmatic billing API — costs must be uploaded manually
// (e.g. CSV export from the dashboard). Settlement window: 7 days.
//
// This module provides the interface for manual upload reconciliation.
// Real reconciliation requires the operator to call fetchLambdaBilledCost()
// with a pre-parsed cost array from a manual export.
//
// Manual export workflow:
//   1. Download invoice CSV from https://cloud.lambdalabs.com/billing
//   2. Parse into LambdaInvoiceRow[] (see shape below)
//   3. Call fetchLambdaBilledCost() or inject via the reconciliation job

import type { ProviderCostEntry, ReconciliationProvider } from "./types";
import { TIMING } from "@skyrepl/contracts";

// =============================================================================
// Lambda Labs invoice row shape (from manual CSV export)
// =============================================================================

export interface LambdaInvoiceRow {
  /** Instance ID from Lambda Labs dashboard */
  instance_id: string;
  /** ISO 8601 date string: when the instance started */
  start_time: string;
  /** ISO 8601 date string: when the instance terminated */
  end_time: string;
  /** Total cost in USD (float) */
  cost_usd: number;
  /** GPU type/spec string (e.g. "1x A100 SXM4 80 GB") */
  gpu_type?: string;
  /** Region (e.g. "us-east-1") */
  region?: string;
}

// =============================================================================
// Parse Lambda invoice rows into ProviderCostEntry[]
// =============================================================================

function parseLambdaInvoiceRows(rows: LambdaInvoiceRow[]): ProviderCostEntry[] {
  const entries: ProviderCostEntry[] = [];

  for (const row of rows) {
    const periodStartMs = new Date(row.start_time).getTime();
    const periodEndMs = new Date(row.end_time).getTime();

    if (isNaN(periodStartMs) || isNaN(periodEndMs)) {
      console.warn(`[billing-recon:lambda] Skipping row with invalid timestamps: instance_id=${row.instance_id}`);
      continue;
    }

    const billedCents = Math.round(row.cost_usd * 100);
    if (billedCents < 0) {
      console.warn(`[billing-recon:lambda] Skipping row with negative cost: instance_id=${row.instance_id}`);
      continue;
    }

    entries.push({
      provider: "lambda",
      provider_resource_id: row.instance_id,
      period_start_ms: periodStartMs,
      period_end_ms: periodEndMs,
      billed_amount_cents: billedCents,
      currency: "USD",
      raw_data: { row },
    });
  }

  return entries;
}

// =============================================================================
// fetchLambdaBilledCost
// =============================================================================

// Module-level store for manually uploaded Lambda costs (set via setLambdaCosts)
let _uploadedLambdaCosts: LambdaInvoiceRow[] | null = null;

/**
 * Upload Lambda Labs costs manually (no programmatic API available).
 * Call this before running reconciliation to inject costs from a CSV export.
 *
 * @param rows Parsed invoice rows from Lambda Labs dashboard CSV export.
 */
export function setLambdaCosts(rows: LambdaInvoiceRow[]): void {
  _uploadedLambdaCosts = rows;
  console.log(`[billing-recon:lambda] Uploaded ${rows.length} Lambda invoice row(s) for reconciliation`);
}

/**
 * Fetch Lambda Labs billed costs for the given period.
 *
 * Lambda Labs has no programmatic billing API. This function returns costs
 * that were previously uploaded via setLambdaCosts().
 *
 * If no costs have been uploaded, logs a warning and returns an empty array.
 *
 * @throws {Error} with code "MISSING_CREDENTIALS" if manual upload is explicitly
 *   required but nothing has been uploaded. In normal operation, returns [] with a warning.
 */
export async function fetchLambdaBilledCost(
  period: { start: string; end: string }
): Promise<ProviderCostEntry[]> {
  if (_uploadedLambdaCosts === null) {
    console.warn(
      `[billing-recon:lambda] No Lambda Labs invoice data uploaded. ` +
      `Lambda has no programmatic billing API — download a CSV from ` +
      `https://cloud.lambdalabs.com/billing and call setLambdaCosts() to enable reconciliation.`
    );
    return [];
  }

  // Filter to entries within the requested period
  const periodStartMs = new Date(period.start).getTime();
  const periodEndMs = new Date(period.end).getTime();

  const periodRows = _uploadedLambdaCosts.filter(row => {
    const rowStart = new Date(row.start_time).getTime();
    const rowEnd = new Date(row.end_time).getTime();
    // Include rows that overlap with the requested period
    return rowStart < periodEndMs && rowEnd > periodStartMs;
  });

  return parseLambdaInvoiceRows(periodRows);
}

// =============================================================================
// ReconciliationProvider implementation
// =============================================================================

export const lambdaReconciliationProvider: ReconciliationProvider = {
  name: "lambda",
  settlementWindowMs: TIMING.SETTLEMENT_WINDOW_LAMBDA_MS,
  fetchBilledCosts: fetchLambdaBilledCost,
};
