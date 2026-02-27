// billing/fees.ts — Orchestration and seat fee calculations
//
// All amounts in INTEGER CENTS.

// =============================================================================
// Orchestration Fee
// =============================================================================

/**
 * Compute the SkyREPL orchestration fee on top of provider cost.
 * BYO (bring-your-own credentials): 1%
 * Managed (SkyREPL-owned cloud account): 7%
 */
export function computeOrchestrationFee(params: {
  provider_cost_cents: number;
  account_type: "byo" | "managed";
}): number {
  const rate = params.account_type === "managed" ? 0.07 : 0.01;
  return Math.round(params.provider_cost_cents * rate);
}

// =============================================================================
// Seat Fee
// =============================================================================

// NOTE: enterprise seat fee is 0 — custom pricing negotiated externally, not hardcoded.
// Deferred: no mechanism to set per-tenant rate yet; enterprise stays 0 until per-tenant pricing is implemented.
const SEAT_FEE_MONTHLY_CENTS: Record<string, number> = {
  starter: 0,         // free
  team: 1_000,        // $10/mo per seat in cents
  enterprise: 0,      // custom pricing — placeholder
};

/**
 * Compute monthly seat fee total.
 * Starter: $0, Team: $10/mo per seat ($1000 cents), Enterprise: 0 (custom).
 */
export function computeSeatFee(params: {
  tier: "starter" | "team" | "enterprise";
  seat_count: number;
}): number {
  const perSeat = SEAT_FEE_MONTHLY_CENTS[params.tier] ?? 0;
  return perSeat * params.seat_count;
}
