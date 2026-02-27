// billing/providers/aws.ts — AWS EC2 pricing
//
// Rules:
// - Per-second billing, 60-second minimum per metering window.
// - Spot: price varies between observations; split at price change boundaries.
// - All arithmetic in integer cents.

import type { PricingParams, PricingResult, LineItem } from "../pricing";
import { splitAtPriceObservations, centsBySec } from "../pricing";

const AWS_MIN_DURATION_MS = 60_000; // 60 seconds

export function computeAWSCost(params: PricingParams): PricingResult {
  const breakdown: LineItem[] = [];
  let totalCents = 0;

  for (const pair of params.metering_pairs) {
    const rawDuration = pair.stop_ms - pair.start_ms;
    if (rawDuration < 0) continue;

    // Apply 60-second minimum: if raw duration < 60s, bill for 60s at the
    // effective rate for the FIRST segment (the opening rate).
    if (rawDuration < AWS_MIN_DURATION_MS) {
      const effectiveObs = [...params.price_observations]
        .sort((a, b) => a.at_ms - b.at_ms)
        .filter(o => o.at_ms <= pair.start_ms)
        .pop();
      const rate = effectiveObs?.rate_per_hour ?? 0;
      const billedDuration = AWS_MIN_DURATION_MS;
      const cents = centsBySec(billedDuration, rate);
      breakdown.push({
        start_ms: pair.start_ms,
        end_ms: pair.start_ms + billedDuration,
        duration_ms: billedDuration,
        rate_per_hour: rate,
        amount_cents: cents,
        description: "AWS EC2 usage (60s minimum applied)",
      });
      totalCents += cents;
      continue;
    }

    // For windows >= 60s, split at price change boundaries.
    const segments = splitAtPriceObservations(pair.start_ms, pair.stop_ms, params.price_observations);

    for (const seg of segments) {
      const cents = centsBySec(seg.duration_ms, seg.rate_per_hour);
      breakdown.push({
        start_ms: seg.start_ms,
        end_ms: seg.end_ms,
        duration_ms: seg.duration_ms,
        rate_per_hour: seg.rate_per_hour,
        amount_cents: cents,
        description: params.is_spot ? "AWS EC2 Spot usage" : "AWS EC2 On-Demand usage",
      });
      totalCents += cents;
    }
  }

  return { amount_cents: totalCents, currency: params.currency, breakdown };
}
