// billing/pricing.ts — Per-provider cost computation + dispatch
//
// All amounts in INTEGER CENTS — never floating point dollars.
// Dispatch: switch on params.provider → per-provider implementation.

import { computeAWSCost } from "./providers/aws";
import { computeLambdaCost } from "./providers/lambda";
import { computeDigitalOceanCost } from "./providers/digitalocean";
import { computeRunPodCost } from "./providers/runpod";
import { computeOrbStackCost } from "./providers/orbstack";

// =============================================================================
// Public Interfaces
// =============================================================================

export interface PricingParams {
  provider: string;
  spec: string;
  region: string;
  is_spot: boolean;
  min_duration_ms: number;       // provider minimum (AWS: 60000, DO GPU: 300000, RunPod: 0)
  metering_pairs: Array<{ start_ms: number; stop_ms: number }>;
  price_observations: Array<{ at_ms: number; rate_per_hour: number }>;
  currency: string;
}

export interface PricingResult {
  amount_cents: number;          // INTEGER — never float
  currency: string;
  breakdown: LineItem[];
}

export interface LineItem {
  start_ms: number;
  end_ms: number;
  duration_ms: number;
  rate_per_hour: number;
  amount_cents: number;
  description: string;
}

// =============================================================================
// Dispatch
// =============================================================================

/**
 * Compute cost for a given set of metering windows and price observations.
 * Dispatches to the per-provider implementation.
 */
export function computeCost(params: PricingParams): PricingResult {
  switch (params.provider) {
    case "aws":
      return computeAWSCost(params);
    case "lambda":
    case "lambdalabs":
      return computeLambdaCost(params);
    case "digitalocean":
      return computeDigitalOceanCost(params);
    case "runpod":
      return computeRunPodCost(params);
    case "orbstack":
    case "skyrepl_infra":
      return computeOrbStackCost(params);
    default:
      // Unknown provider: fall back to per-second billing with no minimum
      return computeGenericCost(params);
  }
}

// =============================================================================
// Generic Fallback (per-second, no minimum)
// =============================================================================

function computeGenericCost(params: PricingParams): PricingResult {
  const breakdown: LineItem[] = [];
  let totalCents = 0;

  for (const pair of params.metering_pairs) {
    const segments = splitAtPriceObservations(pair.start_ms, pair.stop_ms, params.price_observations);
    for (const seg of segments) {
      const cents = centsBySec(seg.duration_ms, seg.rate_per_hour);
      breakdown.push({
        start_ms: seg.start_ms,
        end_ms: seg.end_ms,
        duration_ms: seg.duration_ms,
        rate_per_hour: seg.rate_per_hour,
        amount_cents: cents,
        description: `${params.provider} usage`,
      });
      totalCents += cents;
    }
  }

  return { amount_cents: totalCents, currency: params.currency, breakdown };
}

// =============================================================================
// Shared Helpers (exported for use in per-provider modules)
// =============================================================================

/**
 * Split a [start_ms, stop_ms) window at price observation boundaries.
 * Returns segments with the effective rate_per_hour for each segment.
 * Observations are sorted ascending by at_ms.
 */
export function splitAtPriceObservations(
  start_ms: number,
  stop_ms: number,
  observations: Array<{ at_ms: number; rate_per_hour: number }>
): Array<{ start_ms: number; end_ms: number; duration_ms: number; rate_per_hour: number }> {
  if (start_ms >= stop_ms) return [];

  // Find effective rate at start (latest observation at or before start)
  const sorted = [...observations].sort((a, b) => a.at_ms - b.at_ms);

  // Find observations that fall INSIDE the window (exclusive of start)
  const boundaries = sorted
    .filter(o => o.at_ms > start_ms && o.at_ms < stop_ms)
    .map(o => o.at_ms);

  // Build segment boundaries: [start, ...internal boundaries, stop]
  const breakpoints = [start_ms, ...boundaries, stop_ms];

  const segments: Array<{ start_ms: number; end_ms: number; duration_ms: number; rate_per_hour: number }> = [];

  for (let i = 0; i < breakpoints.length - 1; i++) {
    const segStart = breakpoints[i];
    const segEnd = breakpoints[i + 1];
    if (segStart >= segEnd) continue;

    // Find effective rate: latest observation at or before segStart
    const effectiveObs = sorted.filter(o => o.at_ms <= segStart).pop();
    const rate = effectiveObs?.rate_per_hour ?? 0;

    segments.push({
      start_ms: segStart,
      end_ms: segEnd,
      duration_ms: segEnd - segStart,
      rate_per_hour: rate,
    });
  }

  return segments;
}

/**
 * Convert duration_ms + rate_per_hour to INTEGER cents (per-second billing).
 * Uses integer arithmetic: avoids floating-point accumulation.
 */
export function centsBySec(duration_ms: number, rate_per_hour: number): number {
  // rate_per_hour → cents per ms: (rate * 100) / 3_600_000
  // Multiply first, divide last to preserve precision.
  return Math.round((duration_ms * rate_per_hour * 100) / 3_600_000);
}
