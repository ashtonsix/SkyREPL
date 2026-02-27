// billing/providers/digitalocean.ts — DigitalOcean Droplet pricing
//
// Rules:
// - Per-second billing, minimum floor OR $0.01 (1 cent), whichever is greater.
// - Regular droplets: 60-second minimum.
// - GPU droplets: 5-minute (300s) minimum.
// - Monthly cap: 672 hours (DO caps at 672h/month for all droplets).
// - Stopped droplets still billed (must destroy to stop billing).

import type { PricingParams, PricingResult, LineItem } from "../pricing";
import { centsBySec } from "../pricing";

const DO_MIN_DURATION_MS = 60_000;      // 60 seconds (regular droplets)
const DO_GPU_MIN_DURATION_MS = 300_000;  // 5 minutes (GPU droplets)
const DO_MIN_CHARGE_CENTS = 1;           // $0.01 floor
const DO_MONTHLY_CAP_HOURS = 672;        // 672 hours

function isGpuDroplet(spec: string): boolean {
  return spec.toLowerCase().includes("gpu");
}

export function computeDigitalOceanCost(params: PricingParams): PricingResult {
  const breakdown: LineItem[] = [];
  let totalCents = 0;

  // Find effective rate (DO uses a single hourly rate, not spot)
  const sorted = [...params.price_observations].sort((a, b) => a.at_ms - b.at_ms);
  const minDuration = isGpuDroplet(params.spec) ? DO_GPU_MIN_DURATION_MS : DO_MIN_DURATION_MS;

  // Track cumulative billed hours across all windows — 672h/month cap is cumulative.
  const DO_MONTHLY_CAP_MS = DO_MONTHLY_CAP_HOURS * 3_600_000;
  let cumulativeBilledMs = 0;

  for (const pair of params.metering_pairs) {
    const rawDuration = pair.stop_ms - pair.start_ms;
    if (rawDuration < 0) continue;

    const effectiveObs = sorted.filter(o => o.at_ms <= pair.start_ms).pop();
    const rate = effectiveObs?.rate_per_hour ?? 0;

    // Apply minimum duration (60s regular, 300s GPU)
    const billedDuration = Math.max(rawDuration, minDuration);

    // Apply cumulative monthly cap: only bill up to the remaining hours within the cap.
    const remainingCapMs = Math.max(0, DO_MONTHLY_CAP_MS - cumulativeBilledMs);
    const cappedDuration = Math.min(billedDuration, remainingCapMs);
    cumulativeBilledMs += cappedDuration;

    // Compute raw cents from capped duration, then apply $0.01 floor.
    // If the window is entirely beyond the cap (cappedDuration === 0), cost is $0.
    let cents: number;
    if (cappedDuration === 0) {
      cents = 0;
    } else {
      cents = centsBySec(cappedDuration, rate);
      cents = Math.max(cents, DO_MIN_CHARGE_CENTS);
    }

    breakdown.push({
      start_ms: pair.start_ms,
      end_ms: pair.start_ms + billedDuration,
      duration_ms: billedDuration,
      rate_per_hour: rate,
      amount_cents: cents,
      description: isGpuDroplet(params.spec) ? "DigitalOcean GPU Droplet usage" : "DigitalOcean Droplet usage",
    });
    totalCents += cents;
  }

  return { amount_cents: totalCents, currency: params.currency, breakdown };
}
