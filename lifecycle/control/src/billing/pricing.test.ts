// billing/pricing.test.ts — Pricing engine tests
//
// Property-based tests (80%) + per-provider edge cases (20%).
// All amounts in INTEGER CENTS.

import { describe, test, expect } from "bun:test";
import { computeCost, type PricingParams } from "./pricing";

// =============================================================================
// Helpers
// =============================================================================

function params(overrides: Partial<PricingParams> & { metering_pairs: Array<{ start_ms: number; stop_ms: number }> }): PricingParams {
  return {
    provider: "aws",
    spec: "t3.micro",
    region: "us-east-1",
    is_spot: false,
    min_duration_ms: 60_000,
    price_observations: [{ at_ms: 0, rate_per_hour: 1.00 }],
    currency: "USD",
    ...overrides,
  };
}

const BASE_MS = 1_700_000_000_000; // Fixed reference time

// =============================================================================
// §1: Property-Based Tests (80%)
// =============================================================================

describe("property: amount_cents >= 0", () => {
  test("AWS: amount_cents >= 0 for any valid window", () => {
    for (const dur of [0, 1_000, 60_000, 3_600_000, 7_200_000]) {
      const result = computeCost(params({
        provider: "aws",
        metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + dur }],
        price_observations: [{ at_ms: BASE_MS - 1000, rate_per_hour: 0.50 }],
      }));
      expect(result.amount_cents).toBeGreaterThanOrEqual(0);
    }
  });

  test("RunPod: amount_cents >= 0 for any valid window", () => {
    for (const dur of [0, 1, 100, 60_000, 3_600_000]) {
      const result = computeCost(params({
        provider: "runpod",
        min_duration_ms: 0,
        metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + dur }],
        price_observations: [{ at_ms: BASE_MS - 1000, rate_per_hour: 2.00 }],
      }));
      expect(result.amount_cents).toBeGreaterThanOrEqual(0);
    }
  });
});

describe("property: amount_cents is INTEGER (no fractional cents)", () => {
  test("AWS: no fractional cents", () => {
    const result = computeCost(params({
      provider: "aws",
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 7_777_777 }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 0.37 }],
    }));
    expect(Number.isInteger(result.amount_cents)).toBe(true);
    for (const item of result.breakdown) {
      expect(Number.isInteger(item.amount_cents)).toBe(true);
    }
  });

  test("DigitalOcean: no fractional cents", () => {
    const result = computeCost(params({
      provider: "digitalocean",
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 123_456 }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 0.015 }],
    }));
    expect(Number.isInteger(result.amount_cents)).toBe(true);
  });

  test("Lambda: no fractional cents", () => {
    const result = computeCost(params({
      provider: "lambda",
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 1_234_567 }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 0.80 }],
    }));
    expect(Number.isInteger(result.amount_cents)).toBe(true);
  });
});

describe("property: longer duration => equal or higher cost (monotonicity)", () => {
  test("AWS: 2h costs >= 1h", () => {
    const r1 = computeCost(params({
      provider: "aws",
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 3_600_000 }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 1.00 }],
    }));
    const r2 = computeCost(params({
      provider: "aws",
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 7_200_000 }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 1.00 }],
    }));
    expect(r2.amount_cents).toBeGreaterThanOrEqual(r1.amount_cents);
  });

  test("RunPod: 1 hour costs >= 1 minute", () => {
    const r1 = computeCost(params({
      provider: "runpod",
      min_duration_ms: 0,
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 60_000 }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 1.00 }],
    }));
    const r2 = computeCost(params({
      provider: "runpod",
      min_duration_ms: 0,
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 3_600_000 }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 1.00 }],
    }));
    expect(r2.amount_cents).toBeGreaterThanOrEqual(r1.amount_cents);
  });

  test("DigitalOcean: 5 minutes costs >= 30 seconds (both hit 60s min)", () => {
    const r1 = computeCost(params({
      provider: "digitalocean",
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 30_000 }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 1.00 }],
    }));
    const r2 = computeCost(params({
      provider: "digitalocean",
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 300_000 }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 1.00 }],
    }));
    expect(r2.amount_cents).toBeGreaterThanOrEqual(r1.amount_cents);
  });
});

describe("property: zero duration => minimum charge or zero", () => {
  test("AWS: 0ms duration => 60s minimum applies", () => {
    const result = computeCost(params({
      provider: "aws",
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 1.00 }],
    }));
    // AWS minimum is 60s at $1/hr = 1/60 * 100 cents = ~1.67 => rounds to 2 cents
    expect(result.amount_cents).toBeGreaterThan(0);
  });

  test("RunPod: 0ms duration => 0 cents (no minimum)", () => {
    const result = computeCost(params({
      provider: "runpod",
      min_duration_ms: 0,
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 1.00 }],
    }));
    expect(result.amount_cents).toBe(0);
  });

  test("OrbStack: always 0", () => {
    const result = computeCost(params({
      provider: "orbstack",
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS }],
      price_observations: [],
    }));
    expect(result.amount_cents).toBe(0);
  });
});

describe("property: price observation at midpoint => two sub-intervals", () => {
  test("AWS: price change at midpoint creates 2 breakdown items", () => {
    const mid = BASE_MS + 1_800_000; // 30 min into a 1h window
    const result = computeCost(params({
      provider: "aws",
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 3_600_000 }],
      price_observations: [
        { at_ms: BASE_MS - 1, rate_per_hour: 1.00 },   // before start
        { at_ms: mid, rate_per_hour: 2.00 },           // at midpoint
      ],
    }));
    expect(result.breakdown.length).toBe(2);
    // First half at $1/hr
    expect(result.breakdown[0].rate_per_hour).toBe(1.00);
    // Second half at $2/hr
    expect(result.breakdown[1].rate_per_hour).toBe(2.00);
  });
});

describe("property: SUM(breakdown.amount_cents) === total amount_cents", () => {
  test("AWS multi-segment breakdown sums to total", () => {
    const mid = BASE_MS + 1_800_000;
    const result = computeCost(params({
      provider: "aws",
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 3_600_000 }],
      price_observations: [
        { at_ms: BASE_MS - 1, rate_per_hour: 1.00 },
        { at_ms: mid, rate_per_hour: 2.00 },
      ],
    }));
    const sum = result.breakdown.reduce((acc, item) => acc + item.amount_cents, 0);
    expect(sum).toBe(result.amount_cents);
  });

  test("AWS multi-pair breakdown sums to total", () => {
    const result = computeCost(params({
      provider: "aws",
      metering_pairs: [
        { start_ms: BASE_MS, stop_ms: BASE_MS + 3_600_000 },
        { start_ms: BASE_MS + 7_200_000, stop_ms: BASE_MS + 10_800_000 },
      ],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 1.00 }],
    }));
    const sum = result.breakdown.reduce((acc, item) => acc + item.amount_cents, 0);
    expect(sum).toBe(result.amount_cents);
  });
});

// =============================================================================
// §2: Per-Provider Edge Cases (20%)
// =============================================================================

describe("AWS edge cases", () => {
  test("sub-minute (30s) => 60s minimum", () => {
    const result = computeCost(params({
      provider: "aws",
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 30_000 }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 1.00 }],
    }));
    // 60s at $1/hr = 100/60 cents = 1.666... => rounds to 2
    expect(result.amount_cents).toBe(2);
    expect(result.breakdown[0].duration_ms).toBe(60_000);
  });

  test("crossing price change => correct split", () => {
    // 1 hour window, price changes at 30min: $1/hr for first half, $3/hr for second
    const mid = BASE_MS + 1_800_000;
    const result = computeCost(params({
      provider: "aws",
      is_spot: true,
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 3_600_000 }],
      price_observations: [
        { at_ms: BASE_MS - 1, rate_per_hour: 1.00 },
        { at_ms: mid, rate_per_hour: 3.00 },
      ],
    }));
    // First half: 30min at $1/hr = 50 cents
    // Second half: 30min at $3/hr = 150 cents
    expect(result.breakdown.length).toBe(2);
    expect(result.breakdown[0].amount_cents).toBe(50);
    expect(result.breakdown[1].amount_cents).toBe(150);
    expect(result.amount_cents).toBe(200);
  });

  test("exactly 60s => billed for 60s (no minimum inflation)", () => {
    const result = computeCost(params({
      provider: "aws",
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 60_000 }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 1.00 }],
    }));
    // 60s at $1/hr = 100/60 = 1.666... => 2 cents
    expect(result.amount_cents).toBe(2);
  });

  test("no price observations => 0 cost", () => {
    const result = computeCost(params({
      provider: "aws",
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 3_600_000 }],
      price_observations: [],
    }));
    expect(result.amount_cents).toBe(0);
  });
});

describe("Lambda Labs edge cases (per-minute billing, NO hour rounding)", () => {
  test("30s => 1 minute billed", () => {
    const result = computeCost(params({
      provider: "lambda",
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 30_000 }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 1.00 }],
    }));
    // ceil(30s) = 1 min at $1/hr = (1/60) * 100 = 1.667 → 2 cents
    expect(result.amount_cents).toBe(2);
  });

  test("59 minutes => 59 minutes billed", () => {
    const result = computeCost(params({
      provider: "lambda",
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 59 * 60_000 }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 1.00 }],
    }));
    // 59 min at $1/hr = (59/60) * 100 = 98.33 → 98 cents
    expect(result.amount_cents).toBe(98);
  });

  test("61 minutes => 61 minutes billed (no hour rounding)", () => {
    const result = computeCost(params({
      provider: "lambda",
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 61 * 60_000 }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 1.00 }],
    }));
    // 61 min at $1/hr = (61/60) * 100 = 101.67 → 102 cents
    expect(result.amount_cents).toBe(102);
  });

  test("exactly 1 hour => 60 minutes billed", () => {
    const result = computeCost(params({
      provider: "lambda",
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 3_600_000 }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 1.00 }],
    }));
    expect(result.amount_cents).toBe(100);
  });

  test("0ms => 1 minute minimum billed", () => {
    const result = computeCost(params({
      provider: "lambda",
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 1.00 }],
    }));
    // 0ms → ceil to 1 min → (1/60) * 100 = 1.667 → 2 cents
    expect(result.amount_cents).toBe(2);
  });

  test("90s => 2 minutes billed", () => {
    const result = computeCost(params({
      provider: "lambda",
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 90_000 }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 1.00 }],
    }));
    // ceil(90s/60s) = 2 min at $1/hr = (2/60) * 100 = 3.33 → 3 cents
    expect(result.amount_cents).toBe(3);
  });
});

describe("DigitalOcean edge cases", () => {
  test("10s => max(60s, $0.01) minimum charge", () => {
    const result = computeCost(params({
      provider: "digitalocean",
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 10_000 }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 0.006 }], // very low rate
    }));
    // 60s at $0.006/hr = 0.006 * 100 / 60 = 0.01 cents → rounds to 0, then floor to 1
    expect(result.amount_cents).toBeGreaterThanOrEqual(1);
  });

  test("10s at $1/hr => 60s minimum = 2 cents", () => {
    const result = computeCost(params({
      provider: "digitalocean",
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 10_000 }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 1.00 }],
    }));
    // 60s at $1/hr = 100/60 = 1.666 → rounds to 2 (> $0.01 floor)
    expect(result.amount_cents).toBe(2);
  });

  test("672h cap is applied (single large window)", () => {
    // 1000h at $1/hr = $1000 = 100000 cents, but capped at 672h = 67200 cents
    const result = computeCost(params({
      provider: "digitalocean",
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 1_000 * 3_600_000 }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 1.00 }],
    }));
    const capCents = Math.round(672 * 1.00 * 100); // 67200
    expect(result.amount_cents).toBe(capCents);
  });

  test("672h cap is cumulative across multiple short windows", () => {
    // 10 windows of 100h each = 1000h total — cap should cut off after 672h
    // At $1/hr: windows 1-6 fully billed (600h), window 7 partially billed (72h), windows 8-10 free
    const windowDurationMs = 100 * 3_600_000;
    const metering_pairs = Array.from({ length: 10 }, (_, i) => ({
      start_ms: BASE_MS + i * windowDurationMs,
      stop_ms: BASE_MS + (i + 1) * windowDurationMs,
    }));
    const result = computeCost(params({
      provider: "digitalocean",
      metering_pairs,
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 1.00 }],
    }));
    const capCents = Math.round(672 * 1.00 * 100); // 67200
    expect(result.amount_cents).toBe(capCents);
    // Windows 8-10 (indices 7-9) must be $0 (fully beyond the cap)
    expect(result.breakdown[7]!.amount_cents).toBe(0);
    expect(result.breakdown[8]!.amount_cents).toBe(0);
    expect(result.breakdown[9]!.amount_cents).toBe(0);
    // Window 7 (index 6) should be partial: 72h out of 100h
    expect(result.breakdown[6]!.amount_cents).toBe(Math.round(72 * 1.00 * 100));
  });

  test("many small windows below individual cap but cumulative exceeds 672h", () => {
    // 1000 windows of 1h each at $1/hr — individually well under 672h cap, but cumulative = 1000h
    // Only the first 672 windows should be billed, rest $0
    const windowDurationMs = 3_600_000; // 1 hour
    const metering_pairs = Array.from({ length: 1000 }, (_, i) => ({
      start_ms: BASE_MS + i * windowDurationMs,
      stop_ms: BASE_MS + (i + 1) * windowDurationMs,
    }));
    const result = computeCost(params({
      provider: "digitalocean",
      metering_pairs,
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 1.00 }],
    }));
    const capCents = Math.round(672 * 1.00 * 100); // 67200
    expect(result.amount_cents).toBe(capCents);
    // Window at index 672 (673rd window) must be $0
    expect(result.breakdown[672]!.amount_cents).toBe(0);
    expect(result.breakdown[999]!.amount_cents).toBe(0);
  });

  test("windows within 672h total → no capping applied", () => {
    // 3 windows of 100h each = 300h total — well under 672h, no capping
    const windowDurationMs = 100 * 3_600_000;
    const metering_pairs = Array.from({ length: 3 }, (_, i) => ({
      start_ms: BASE_MS + i * windowDurationMs,
      stop_ms: BASE_MS + (i + 1) * windowDurationMs,
    }));
    const result = computeCost(params({
      provider: "digitalocean",
      metering_pairs,
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 1.00 }],
    }));
    // 300h at $1/hr = 30000 cents
    expect(result.amount_cents).toBe(Math.round(300 * 1.00 * 100));
  });
});

describe("RunPod edge cases (per-second billing, no minimum)", () => {
  test("500ms => 1 second billed (ceil to nearest second)", () => {
    const result = computeCost(params({
      provider: "runpod",
      min_duration_ms: 0,
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 500 }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 3600.00 }], // $1/sec
    }));
    // 500ms → ceil to 1000ms (1s) at $3600/hr = 100 cents
    expect(result.amount_cents).toBe(100);
    expect(result.breakdown[0].duration_ms).toBe(1000);
  });

  test("1001ms => 2 seconds billed", () => {
    const result = computeCost(params({
      provider: "runpod",
      min_duration_ms: 0,
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 1001 }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 3600.00 }],
    }));
    // 1001ms → ceil to 2000ms (2s) at $3600/hr = 200 cents
    expect(result.amount_cents).toBe(200);
    expect(result.breakdown[0].duration_ms).toBe(2000);
  });

  test("0ms => 0 cents (no minimum)", () => {
    const result = computeCost(params({
      provider: "runpod",
      min_duration_ms: 0,
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 1.00 }],
    }));
    expect(result.amount_cents).toBe(0);
  });

  test("community tier identified in description", () => {
    const result = computeCost(params({
      provider: "runpod",
      spec: "rtx4090-community",
      min_duration_ms: 0,
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 60_000 }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 1.00 }],
    }));
    expect(result.breakdown[0].description).toContain("Community");
  });

  test("secure tier identified in description", () => {
    const result = computeCost(params({
      provider: "runpod",
      spec: "rtx4090-secure",
      min_duration_ms: 0,
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 60_000 }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 1.00 }],
    }));
    expect(result.breakdown[0].description).toContain("Secure");
  });
});

describe("OrbStack edge cases", () => {
  test("always 0 regardless of duration and rate", () => {
    const result = computeCost(params({
      provider: "orbstack",
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 1_000 * 3_600_000 }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 9999.00 }],
    }));
    expect(result.amount_cents).toBe(0);
    for (const item of result.breakdown) {
      expect(item.amount_cents).toBe(0);
    }
  });

  test("orbstack: 0 for empty metering_pairs", () => {
    const result = computeCost(params({
      provider: "orbstack",
      metering_pairs: [],
      price_observations: [],
    }));
    expect(result.amount_cents).toBe(0);
  });
});

// =============================================================================
// §3: computeCost dispatch
// =============================================================================

describe("computeCost dispatch", () => {
  test("unknown provider falls back to generic (per-second, no minimum)", () => {
    const result = computeCost(params({
      provider: "unknown_provider",
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 3_600_000 }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 1.00 }],
    }));
    // 1 hour at $1/hr = 100 cents
    expect(result.amount_cents).toBe(100);
  });

  test("lambdalabs alias dispatches to Lambda provider", () => {
    const r1 = computeCost(params({
      provider: "lambda",
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 3_600_000 }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 1.00 }],
    }));
    const r2 = computeCost(params({
      provider: "lambdalabs",
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 3_600_000 }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 1.00 }],
    }));
    expect(r1.amount_cents).toBe(r2.amount_cents);
  });

  test("skyrepl_infra dispatches to OrbStack (free)", () => {
    const result = computeCost(params({
      provider: "skyrepl_infra",
      metering_pairs: [{ start_ms: BASE_MS, stop_ms: BASE_MS + 3_600_000 }],
      price_observations: [{ at_ms: BASE_MS - 1, rate_per_hour: 1.00 }],
    }));
    expect(result.amount_cents).toBe(0);
  });
});
