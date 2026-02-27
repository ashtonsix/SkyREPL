// billing/reconciliation/types.ts — Shared types for provider billing reconciliation (WL-061-4A)
//
// ProviderCostEntry: normalized cost record fetched from a provider's billing API.
// ReconciliationProvider: interface for provider billing API adapters.

// =============================================================================
// Provider Cost Entry
// =============================================================================

/**
 * Normalized cost record from a provider's billing API.
 * All monetary amounts are in INTEGER CENTS.
 */
export interface ProviderCostEntry {
  /** Provider name (e.g. "aws", "digitalocean", "runpod", "lambda") */
  provider: string;
  /** Provider-native resource identifier (e.g. AWS instance ID, DO droplet ID) */
  provider_resource_id: string;
  /** Billing period start — milliseconds since epoch */
  period_start_ms: number;
  /** Billing period end — milliseconds since epoch */
  period_end_ms: number;
  /** Amount billed by the provider for this resource in this period — INTEGER CENTS */
  billed_amount_cents: number;
  /** Currency code (e.g. "USD") */
  currency: string;
  /** Raw provider API response data for debugging and audit trail */
  raw_data: Record<string, unknown>;
}

// =============================================================================
// Reconciliation Provider Interface
// =============================================================================

/**
 * Adapter interface for a provider's billing API.
 * Each provider module implements this to expose its billed costs.
 */
export interface ReconciliationProvider {
  /** Provider name matching the provider registry key */
  name: string;
  /**
   * How long after usage ends before this provider's billing API reflects the charge.
   * Reconciliation should only run for periods older than this window.
   * Milliseconds.
   */
  settlementWindowMs: number;
  /**
   * Fetch billed costs from the provider's billing API for the given period.
   * @param period ISO 8601 date strings: { start: "YYYY-MM-DD", end: "YYYY-MM-DD" }
   * @returns Array of ProviderCostEntry, one per billed resource in the period.
   */
  fetchBilledCosts(period: { start: string; end: string }): Promise<ProviderCostEntry[]>;
}
