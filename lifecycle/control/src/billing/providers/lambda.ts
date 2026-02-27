// billing/providers/lambda.ts — Lambda Labs pricing
//
// Rules:
// - Per-minute billing (1-minute increments, no hour-level rounding).
// - Effective duration: ceil(raw_ms / 60_000) * 60_000  (round up to minute)
// - Total: (effective_minutes / 60) * rate_per_hour  (in cents)
// - No minimum duration beyond the 1-minute ceil.
// - Fixed pricing (no spot market, no mid-window price changes).

import type { PricingParams, PricingResult, LineItem } from "../pricing";

const LAMBDA_MIN_INCREMENT_MS = 60_000; // 1-minute granularity

export function computeLambdaCost(params: PricingParams): PricingResult {
  const breakdown: LineItem[] = [];
  let totalCents = 0;

  for (const pair of params.metering_pairs) {
    const rawDuration = pair.stop_ms - pair.start_ms;
    if (rawDuration < 0) continue;

    // Find effective rate at start of window (Lambda has fixed rates, no mid-window changes)
    const sorted = [...params.price_observations].sort((a, b) => a.at_ms - b.at_ms);
    const effectiveObs = sorted.filter(o => o.at_ms <= pair.start_ms).pop();
    const rate = effectiveObs?.rate_per_hour ?? 0;

    // Round up to 1-minute increments (no hour-level rounding)
    const effectiveDurationMs = rawDuration === 0
      ? LAMBDA_MIN_INCREMENT_MS
      : Math.ceil(rawDuration / LAMBDA_MIN_INCREMENT_MS) * LAMBDA_MIN_INCREMENT_MS;
    const effectiveMinutes = effectiveDurationMs / LAMBDA_MIN_INCREMENT_MS;

    // Per-minute: (minutes / 60) * rate_per_hour * 100 (to cents)
    const cents = Math.round((effectiveMinutes / 60) * rate * 100);

    if (cents > 0 || rawDuration > 0) {
      breakdown.push({
        start_ms: pair.start_ms,
        end_ms: pair.start_ms + effectiveDurationMs,
        duration_ms: effectiveDurationMs,
        rate_per_hour: rate,
        amount_cents: cents,
        description: `Lambda Labs usage (${effectiveMinutes}min billed)`,
      });
      totalCents += cents;
    }
  }

  return { amount_cents: totalCents, currency: params.currency, breakdown };
}
