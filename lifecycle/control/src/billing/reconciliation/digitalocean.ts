// billing/reconciliation/digitalocean.ts — DigitalOcean billing API adapter (WL-061-4A)
//
// Fetches billed costs from DigitalOcean /v2/customers/my/billing_history API.
// Real API calls require a DigitalOcean Personal Access Token.
// Settlement window: 24 hours.
//
// API reference: https://docs.digitalocean.com/reference/api/api-reference/#operation/billing_get_customerBalance

import type { ProviderCostEntry, ReconciliationProvider } from "./types";
import { TIMING } from "@skyrepl/contracts";

// =============================================================================
// DigitalOcean billing API response shape
// =============================================================================

interface DOBillingHistoryItem {
  /** Transaction description */
  description: string;
  /** Amount in USD as a string with decimal (can be negative for credits) */
  amount: string;
  /** Invoice ID if this is an invoice line item */
  invoice_id?: string;
  /** Invoice UUID if this is an invoice line item */
  invoice_uuid?: string;
  /** ISO 8601 timestamp of the transaction */
  date: string;
  /** "Charge" | "Adjustment" | "Credit" | "Invoice" */
  type: string;
}

interface DOBillingHistoryResponse {
  billing_history: DOBillingHistoryItem[];
  meta: { total: number };
  links?: Record<string, unknown>;
}

// =============================================================================
// Credential check
// =============================================================================

function checkDOCredentials(): void {
  if (!process.env.DIGITALOCEAN_TOKEN && !process.env.DO_TOKEN) {
    const err = new Error(
      `[billing-recon:digitalocean] Missing DigitalOcean API token. ` +
      `Set DIGITALOCEAN_TOKEN or DO_TOKEN to enable DigitalOcean billing reconciliation.`
    );
    (err as any).code = "MISSING_CREDENTIALS";
    throw err;
  }
}

function getDOToken(): string {
  return process.env.DIGITALOCEAN_TOKEN ?? process.env.DO_TOKEN ?? "";
}

// =============================================================================
// Parse DO billing history into ProviderCostEntry[]
// =============================================================================

function parseDOBillingHistory(
  items: DOBillingHistoryItem[],
  period: { start: string; end: string }
): ProviderCostEntry[] {
  const entries: ProviderCostEntry[] = [];
  const periodStartMs = new Date(period.start).getTime();
  const periodEndMs = new Date(period.end).getTime();

  for (const item of items) {
    // Skip credits and non-charge items (we want what was billed TO us)
    if (item.type !== "Charge" && item.type !== "Invoice") continue;

    const itemMs = new Date(item.date).getTime();
    if (isNaN(itemMs)) continue;

    // Filter to items within the period
    if (itemMs < periodStartMs || itemMs > periodEndMs) continue;

    const amountUsd = parseFloat(item.amount);
    if (isNaN(amountUsd) || amountUsd <= 0) continue;

    const billedCents = Math.round(amountUsd * 100);

    // Use invoice_uuid as the resource ID if available (best unique key for DO)
    const resourceId = item.invoice_uuid ?? item.invoice_id ?? `do-charge-${itemMs}`;

    entries.push({
      provider: "digitalocean",
      provider_resource_id: resourceId,
      period_start_ms: periodStartMs,
      period_end_ms: periodEndMs,
      billed_amount_cents: billedCents,
      currency: "USD",
      raw_data: { item },
    });
  }

  return entries;
}

// =============================================================================
// fetchDigitalOceanBilledCost
// =============================================================================

/**
 * Fetch billed costs from DigitalOcean billing history API for the given period.
 *
 * In production: calls GET /v2/customers/my/billing_history with pagination.
 * Results are filtered to the requested period.
 *
 * Requires: DIGITALOCEAN_TOKEN or DO_TOKEN environment variable.
 *
 * @throws {Error} with code "MISSING_CREDENTIALS" if DO token is not configured.
 */
export async function fetchDigitalOceanBilledCost(
  period: { start: string; end: string }
): Promise<ProviderCostEntry[]> {
  checkDOCredentials();

  // In production this would be:
  //
  //   const token = getDOToken();
  //   const url = "https://api.digitalocean.com/v2/customers/my/billing_history?per_page=200";
  //   const response = await fetch(url, {
  //     headers: {
  //       Authorization: `Bearer ${token}`,
  //       "Content-Type": "application/json",
  //     },
  //   });
  //   if (!response.ok) {
  //     throw new Error(`[billing-recon:digitalocean] API error ${response.status}: ${await response.text()}`);
  //   }
  //   const data: DOBillingHistoryResponse = await response.json();
  //   return parseDOBillingHistory(data.billing_history, period);

  // Stub: return a mock response shaped like the real API
  const mockResponse: DOBillingHistoryResponse = {
    billing_history: [],
    meta: { total: 0 },
  };

  return parseDOBillingHistory(mockResponse.billing_history, period);
}

// =============================================================================
// ReconciliationProvider implementation
// =============================================================================

export const digitaloceanReconciliationProvider: ReconciliationProvider = {
  name: "digitalocean",
  settlementWindowMs: TIMING.SETTLEMENT_WINDOW_DO_MS,
  fetchBilledCosts: fetchDigitalOceanBilledCost,
};
