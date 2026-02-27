// billing/providers/orbstack.ts — OrbStack pricing
//
// OrbStack is always free — amount_cents is always 0.
// Also used as the fallback for skyrepl_infra provider.

import type { PricingParams, PricingResult } from "../pricing";

export function computeOrbStackCost(params: PricingParams): PricingResult {
  return {
    amount_cents: 0,
    currency: params.currency,
    breakdown: params.metering_pairs.map(pair => ({
      start_ms: pair.start_ms,
      end_ms: pair.stop_ms,
      duration_ms: Math.max(0, pair.stop_ms - pair.start_ms),
      rate_per_hour: 0,
      amount_cents: 0,
      description: "OrbStack usage (free)",
    })),
  };
}
