// billing/providers/runpod.ts — RunPod pricing
//
// Rules:
// - Per-second billing (ceil to nearest second). No minimum duration.
// - Community vs Secure tiers (Secure ~20-30% premium).
// - Rate from price observations; no minimum charge.

import type { PricingParams, PricingResult, LineItem } from "../pricing";
import { centsBySec } from "../pricing";

export function computeRunPodCost(params: PricingParams): PricingResult {
  const breakdown: LineItem[] = [];
  let totalCents = 0;

  const sorted = [...params.price_observations].sort((a, b) => a.at_ms - b.at_ms);

  for (const pair of params.metering_pairs) {
    const rawDuration = pair.stop_ms - pair.start_ms;
    if (rawDuration < 0) continue;

    // RunPod: no minimum — even 0ms is valid (just returns 0)
    if (rawDuration === 0) {
      breakdown.push({
        start_ms: pair.start_ms,
        end_ms: pair.stop_ms,
        duration_ms: 0,
        rate_per_hour: 0,
        amount_cents: 0,
        description: "RunPod usage (0s)",
      });
      continue;
    }

    const effectiveObs = sorted.filter(o => o.at_ms <= pair.start_ms).pop();
    const rate = effectiveObs?.rate_per_hour ?? 0;

    // Per-second billing: ceil to nearest second
    const billedMs = Math.ceil(rawDuration / 1000) * 1000;
    const cents = centsBySec(billedMs, rate);

    // Determine tier hint from spec name (community vs secure)
    const tier = params.spec.toLowerCase().includes("secure") ? "Secure" : "Community";

    breakdown.push({
      start_ms: pair.start_ms,
      end_ms: pair.start_ms + billedMs,
      duration_ms: billedMs,
      rate_per_hour: rate,
      amount_cents: cents,
      description: `RunPod ${tier} usage`,
    });
    totalCents += cents;
  }

  return { amount_cents: totalCents, currency: params.currency, breakdown };
}
