// billing/metering.ts — Shared metering stop enrichment helper
//
// Computes the enriched data fields for a metering_stop event:
// looks up the paired metering_start, retrieves price observations,
// calls computeCost() to produce amount_cents.
//
// Previously duplicated in finalize.ts, wait-for-boot.ts,
// cleanup-records.ts, and reconciliation.ts. Extracted here to
// avoid 6-way duplication after M-9/M-10 enrichment (WL-061-6-2).

import { queryOne } from "../material/db";
import { computeCost } from "./pricing";
import { computeOrchestrationFee } from "./fees";

export interface MeteringStopEnrichment {
  metering_window_start_ms?: number;
  amount_cents?: number;
  hourly_rate_cents?: number;
  fee_cents?: number;
  currency?: string;
}

/**
 * Compute the enriched data fields for a metering_stop event.
 * Looks up the paired metering_start event from the audit_log for the instance,
 * retrieves price observations, and calls computeCost() to produce amount_cents.
 *
 * Returns partial data to spread into the metering_stop event's data{} field.
 * Falls back gracefully if the start event or price observations are not found.
 */
export function computeMeteringStopData(
  instanceId: number,
  provider: string,
  spec: string,
  region: string | null,
  isSpot: boolean,
  stopMs: number
): MeteringStopEnrichment {
  try {
    // Look up the most recent metering boundary for this instance.
    // For warm pool reuse, prior metering_stop events (replenish) close
    // earlier windows. The new window starts where the last one ended,
    // not from the original metering_start. This prevents overlapping
    // windows that would double-count billing on paid providers.
    const prevStop = queryOne<{ data: string; occurred_at: number }>(
      `SELECT data, occurred_at FROM audit_log
       WHERE event_type = 'metering_stop'
         AND instance_id = ?
       ORDER BY occurred_at DESC LIMIT 1`,
      [instanceId]
    );

    const startEvent = queryOne<{ data: string; occurred_at: number }>(
      `SELECT data, occurred_at FROM audit_log
       WHERE event_type = 'metering_start'
         AND instance_id = ?
       ORDER BY occurred_at DESC LIMIT 1`,
      [instanceId]
    );

    if (!startEvent) {
      // No metering_start found — cannot compute amount
      return {};
    }

    let startData: Record<string, unknown> = {};
    try {
      startData = JSON.parse(startEvent.data);
    } catch { /* malformed */ }

    // Window starts from the most recent boundary: either the last
    // metering_stop's end_ms (warm pool reuse) or the metering_start.
    let startMs: number;
    if (prevStop && prevStop.occurred_at > startEvent.occurred_at) {
      // Previous stop exists and is more recent than the start event —
      // use its end_ms as the new window start (incremental billing).
      let prevStopData: Record<string, unknown> = {};
      try { prevStopData = JSON.parse(prevStop.data); } catch { /* malformed */ }
      startMs = typeof prevStopData.metering_window_end_ms === "number"
        ? prevStopData.metering_window_end_ms
        : prevStop.occurred_at;
    } else {
      startMs =
        typeof startData.metering_window_start_ms === "number"
          ? startData.metering_window_start_ms
          : startEvent.occurred_at;
    }

    if (startMs >= stopMs) {
      // Degenerate window — return start timestamp with zero cost
      return { metering_window_start_ms: startMs, amount_cents: 0, hourly_rate_cents: 0, fee_cents: 0, currency: "USD" };
    }

    // Retrieve price observations for this provider/spec/region
    const priceRow = queryOne<{ data: string; occurred_at: number }>(
      `SELECT data, occurred_at FROM audit_log
       WHERE event_type = 'price_observation'
         AND provider = ?
         AND spec = ?
         AND (region = ? OR region IS NULL)
       ORDER BY occurred_at DESC LIMIT 1`,
      [provider, spec, region]
    );

    const priceObservations: Array<{ at_ms: number; rate_per_hour: number }> = [];
    let effectiveRatePerHour = 0;
    if (priceRow) {
      try {
        const pd = JSON.parse(priceRow.data);
        if (typeof pd.rate_per_hour === "number") {
          priceObservations.push({ at_ms: priceRow.occurred_at, rate_per_hour: pd.rate_per_hour });
          effectiveRatePerHour = pd.rate_per_hour;
        }
      } catch { /* malformed */ }
    }

    // Also check if metering_start carried hourly_rate_cents (H-8 enrichment)
    if (effectiveRatePerHour === 0 && typeof startData.hourly_rate_cents === "number") {
      effectiveRatePerHour = startData.hourly_rate_cents / 100;
    }

    const result = computeCost({
      provider,
      spec,
      region: region ?? "",
      is_spot: isSpot,
      min_duration_ms: 0, // providers handle their own minimums internally
      metering_pairs: [{ start_ms: startMs, stop_ms: stopMs }],
      price_observations: priceObservations,
      currency: "USD",
    });

    const hourlyRateCents = Math.round(effectiveRatePerHour * 100);
    const feeCents = computeOrchestrationFee({
      provider_cost_cents: result.amount_cents,
      account_type: "byo", // default; tenant-aware fee applied in DuckDB view
    });

    return {
      metering_window_start_ms: startMs,
      amount_cents: result.amount_cents,
      hourly_rate_cents: hourlyRateCents,
      fee_cents: feeCents,
      currency: result.currency,
    };
  } catch {
    // Non-fatal: enrichment failure should not block metering_stop emission
    return {};
  }
}
