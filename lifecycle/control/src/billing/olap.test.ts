// billing/olap.test.ts — OLAP view chain integration tests
//
// Tests the full pipeline: audit events → DuckDB flush → v_cost_priced/v_cost_fees.
// Uses the test harness pattern from duckdb.test.ts.

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest } from "../../../tests/harness";
import {
  initDuckDB,
  closeDuckDB,
  duckQuery,
  duckQueryOne,
  duckExecute,
} from "../material/duckdb";

// =============================================================================
// Helpers
// =============================================================================

const BASE_MS = 1_700_000_000_000; // Fixed reference time

/**
 * Insert an audit_log row into DuckDB directly (bypassing the inbox flush).
 * Timestamps must be BigInt for correct BIGINT binding in DuckDB.
 */
async function insertAuditRow(params: {
  event_uuid: string;
  event_type: string;
  is_cost?: boolean;
  provider?: string;
  spec?: string;
  region?: string;
  data?: Record<string, unknown>;
  occurred_at?: number;
}): Promise<void> {
  const occurred_at = params.occurred_at ?? BASE_MS;
  await duckExecute(`
    INSERT INTO audit_log (
      id, event_uuid, event_type, tenant_id,
      provider, spec, region, source,
      is_cost, is_usage, is_attribution, is_reconciliation,
      data, occurred_at, created_at, event_version
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `, [
    Math.floor(Math.random() * 1_000_000),
    params.event_uuid,
    params.event_type,
    1,
    params.provider ?? "aws",
    params.spec ?? "t3.micro",
    params.region ?? "us-east-1",
    "test",
    params.is_cost ?? true,
    false,
    false,
    false,
    JSON.stringify(params.data ?? {}),
    BigInt(occurred_at),
    BigInt(occurred_at),
    1,
  ]);
}

/**
 * Insert a price_observation row.
 */
async function insertPriceObservation(params: {
  event_uuid: string;
  provider: string;
  spec: string;
  region: string;
  rate_per_hour: number;
  at_ms?: number;
}): Promise<void> {
  const at_ms = params.at_ms ?? BASE_MS - 1_000;
  await duckExecute(`
    INSERT INTO audit_log (
      id, event_uuid, event_type, tenant_id,
      provider, spec, region, source,
      is_cost, is_usage, is_attribution, is_reconciliation,
      data, occurred_at, created_at, event_version
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `, [
    Math.floor(Math.random() * 1_000_000),
    params.event_uuid,
    "price_observation",
    1,
    params.provider,
    params.spec,
    params.region,
    "test",
    false,
    false,
    false,
    false,
    JSON.stringify({ rate_per_hour: params.rate_per_hour }),
    BigInt(at_ms),
    BigInt(at_ms),
    1,
  ]);
}

// =============================================================================
// Test setup
// =============================================================================

describe("OLAP view chain integration", () => {
  let cleanup: () => Promise<void>;

  beforeEach(async () => {
    cleanup = setupTest();
    await initDuckDB(":memory:", ":memory:");
  });

  afterEach(async () => {
    await closeDuckDB();
    await cleanup();
  });

  // ---------------------------------------------------------------------------
  // v_cost_raw
  // ---------------------------------------------------------------------------

  test("v_cost_raw: cost event with metering window extracts JSON fields", async () => {
    await insertAuditRow({
      event_uuid: "raw-001",
      event_type: "metering_stop",
      is_cost: true,
      provider: "aws",
      spec: "t3.micro",
      region: "us-east-1",
      data: {
        rate_per_hour: 0.50,
        currency: "USD",
        metering_window_start_ms: BASE_MS,
        metering_window_end_ms: BASE_MS + 3_600_000,
        provider_resource_id: "i-abc123",
      },
    });

    const row = await duckQueryOne<{
      event_uuid: string;
      rate_per_hour: number;
      currency: string;
      window_start_ms: bigint;
      window_end_ms: bigint;
      provider_resource_id: string;
    }>("SELECT * FROM v_cost_raw WHERE event_uuid = 'raw-001'");

    expect(row).not.toBeNull();
    expect(Number(row!.rate_per_hour)).toBeCloseTo(0.50);
    expect(row!.currency).toBe("USD");
    expect(Number(row!.window_start_ms)).toBe(BASE_MS);
    expect(Number(row!.window_end_ms)).toBe(BASE_MS + 3_600_000);
    expect(row!.provider_resource_id).toBe("i-abc123");
  });

  test("v_cost_raw: enriched metering_stop extracts fee_cents_raw and hourly_rate_cents_raw", async () => {
    // Production format after C-1: metering_stop carries all enriched fields
    await insertAuditRow({
      event_uuid: "raw-enriched-001",
      event_type: "metering_stop",
      is_cost: true,
      provider: "aws",
      spec: "t3.micro",
      region: "us-east-1",
      data: {
        provider_resource_id: "i-enriched001",
        metering_window_start_ms: BASE_MS,
        metering_window_end_ms: BASE_MS + 3_600_000,
        amount_cents: 50,
        fee_cents: 1,
        hourly_rate_cents: 50,
        currency: "USD",
      },
      occurred_at: BASE_MS + 3_600_000,
    });

    const row = await duckQueryOne<{
      amount_cents_raw: number;
      fee_cents_raw: number;
      hourly_rate_cents_raw: number;
    }>("SELECT amount_cents_raw, fee_cents_raw, hourly_rate_cents_raw FROM v_cost_raw WHERE event_uuid = 'raw-enriched-001'");

    expect(row).not.toBeNull();
    expect(Number(row!.amount_cents_raw)).toBe(50);
    expect(Number(row!.fee_cents_raw)).toBe(1);
    expect(Number(row!.hourly_rate_cents_raw)).toBe(50);
  });

  test("v_cost_raw: non-cost event does not appear", async () => {
    await insertAuditRow({
      event_uuid: "noncost-001",
      event_type: "workflow.created",
      is_cost: false,
    });

    const rows = await duckQuery("SELECT event_uuid FROM v_cost_raw WHERE event_uuid = 'noncost-001'");
    expect(rows.length).toBe(0);
  });

  // ---------------------------------------------------------------------------
  // v_cost_attributed (placeholder)
  // ---------------------------------------------------------------------------

  test("v_cost_attributed: has attribution_weight=1.0 (placeholder)", async () => {
    await insertAuditRow({
      event_uuid: "attr-001",
      event_type: "metering_stop",
      is_cost: true,
    });

    const row = await duckQueryOne<{ attribution_weight: number; attributed_user_id: number | null }>(
      "SELECT attribution_weight, attributed_user_id FROM v_cost_attributed WHERE event_uuid = 'attr-001'"
    );

    expect(row).not.toBeNull();
    expect(Number(row!.attribution_weight)).toBeCloseTo(1.0);
    // attributed_user_id can be NULL (placeholder) or a user_id from attribution_start events
  });

  // ---------------------------------------------------------------------------
  // v_cost_priced: primary path — pre-computed amount_cents in stop event (C-1)
  // ---------------------------------------------------------------------------

  test("v_cost_priced: reads amount_cents directly from enriched metering_stop event", async () => {
    // Production format after C-1: metering_stop carries pre-computed amount_cents,
    // hourly_rate_cents, and fee_cents.
    // v_cost_priced should prefer amount_cents_raw over the ASOF JOIN computed value.
    await insertAuditRow({
      event_uuid: "enriched-001",
      event_type: "metering_stop",
      is_cost: true,
      provider: "aws",
      spec: "t3.micro",
      region: "us-east-1",
      data: {
        provider_resource_id: "i-enriched001",
        metering_window_start_ms: BASE_MS,
        metering_window_end_ms: BASE_MS + 3_600_000,
        amount_cents: 75,
        hourly_rate_cents: 75,
        fee_cents: 1,
        currency: "USD",
      },
      occurred_at: BASE_MS + 3_600_000,
    });

    const row = await duckQueryOne<{
      event_uuid: string;
      amount_cents: number;
      amount_cents_raw: number;
      hourly_rate_cents_raw: number;
    }>(
      "SELECT event_uuid, amount_cents, amount_cents_raw, hourly_rate_cents_raw FROM v_cost_priced WHERE event_uuid = 'enriched-001'"
    );

    expect(row).not.toBeNull();
    // Should read amount_cents=75 directly from the event, not compute from rate
    expect(Number(row!.amount_cents)).toBe(75);
    expect(Number(row!.amount_cents_raw)).toBe(75);
    expect(Number(row!.hourly_rate_cents_raw)).toBe(75);
  });

  // ---------------------------------------------------------------------------
  // v_cost_priced with ASOF JOIN (fallback path)
  // ---------------------------------------------------------------------------

  test("v_cost_priced: ASOF JOIN picks up price observation", async () => {
    // Insert price observation before the metering event
    await insertPriceObservation({
      event_uuid: "price-001",
      provider: "aws",
      spec: "t3.micro",
      region: "us-east-1",
      rate_per_hour: 0.50,
      at_ms: BASE_MS - 5_000,
    });

    // Insert metering event with 1h window
    await insertAuditRow({
      event_uuid: "meter-001",
      event_type: "metering_stop",
      is_cost: true,
      provider: "aws",
      spec: "t3.micro",
      region: "us-east-1",
      data: {
        metering_window_start_ms: BASE_MS,
        metering_window_end_ms: BASE_MS + 3_600_000, // 1 hour
      },
      occurred_at: BASE_MS + 3_600_000,
    });

    const row = await duckQueryOne<{ event_uuid: string; effective_rate: number; amount_cents: number }>(
      "SELECT event_uuid, effective_rate, amount_cents FROM v_cost_priced WHERE event_uuid = 'meter-001'"
    );

    expect(row).not.toBeNull();
    expect(Number(row!.effective_rate)).toBeCloseTo(0.50);
    // 1 hour at $0.50/hr = 50 cents
    expect(Number(row!.amount_cents)).toBe(50);
  });

  test("v_cost_priced: falls back to inline rate_per_hour when no price observation", async () => {
    await insertAuditRow({
      event_uuid: "meter-002",
      event_type: "metering_stop",
      is_cost: true,
      provider: "aws",
      spec: "t3.large",
      region: "us-west-2",
      data: {
        rate_per_hour: 0.80,
        metering_window_start_ms: BASE_MS,
        metering_window_end_ms: BASE_MS + 3_600_000,
      },
      occurred_at: BASE_MS + 3_600_000,
    });

    const row = await duckQueryOne<{ event_uuid: string; effective_rate: number; amount_cents: number }>(
      "SELECT event_uuid, effective_rate, amount_cents FROM v_cost_priced WHERE event_uuid = 'meter-002'"
    );

    expect(row).not.toBeNull();
    // Inline rate_per_hour from data JSON
    expect(Number(row!.effective_rate)).toBeCloseTo(0.80);
    expect(Number(row!.amount_cents)).toBe(80); // 1 hour at $0.80/hr = 80 cents
  });

  test("v_cost_priced: no price observation and no inline rate => amount_cents=0", async () => {
    await insertAuditRow({
      event_uuid: "meter-003",
      event_type: "metering_stop",
      is_cost: true,
      data: {
        metering_window_start_ms: BASE_MS,
        metering_window_end_ms: BASE_MS + 3_600_000,
        // No rate_per_hour in data
      },
      occurred_at: BASE_MS + 3_600_000,
    });

    const row = await duckQueryOne<{ amount_cents: number }>(
      "SELECT amount_cents FROM v_cost_priced WHERE event_uuid = 'meter-003'"
    );

    expect(row).not.toBeNull();
    expect(Number(row!.amount_cents)).toBe(0);
  });

  test("v_cost_priced: ASOF JOIN uses latest price before event", async () => {
    // Two price observations: $0.50 early, then $1.00 more recent
    await insertPriceObservation({
      event_uuid: "price-old",
      provider: "aws",
      spec: "t3.micro",
      region: "us-east-1",
      rate_per_hour: 0.50,
      at_ms: BASE_MS - 10_000,
    });
    await insertPriceObservation({
      event_uuid: "price-new",
      provider: "aws",
      spec: "t3.micro",
      region: "us-east-1",
      rate_per_hour: 1.00,
      at_ms: BASE_MS - 2_000,
    });

    await insertAuditRow({
      event_uuid: "meter-004",
      event_type: "metering_stop",
      is_cost: true,
      provider: "aws",
      spec: "t3.micro",
      region: "us-east-1",
      data: {
        metering_window_start_ms: BASE_MS,
        metering_window_end_ms: BASE_MS + 3_600_000,
      },
      occurred_at: BASE_MS + 3_600_000,
    });

    const row = await duckQueryOne<{ effective_rate: number; amount_cents: number }>(
      "SELECT effective_rate, amount_cents FROM v_cost_priced WHERE event_uuid = 'meter-004'"
    );

    expect(row).not.toBeNull();
    // Should use the most recent price (1.00) that was valid at occurred_at
    expect(Number(row!.effective_rate)).toBeCloseTo(1.00);
    expect(Number(row!.amount_cents)).toBe(100); // 1h at $1.00/hr = 100 cents
  });

  // ---------------------------------------------------------------------------
  // v_cost_fees
  // ---------------------------------------------------------------------------

  test("v_cost_fees: fee_cents = round(amount_cents * 0.01)", async () => {
    await insertPriceObservation({
      event_uuid: "price-fees",
      provider: "aws",
      spec: "t3.micro",
      region: "us-east-1",
      rate_per_hour: 1.00,
      at_ms: BASE_MS - 1_000,
    });

    await insertAuditRow({
      event_uuid: "meter-fees",
      event_type: "metering_stop",
      is_cost: true,
      provider: "aws",
      spec: "t3.micro",
      region: "us-east-1",
      data: {
        metering_window_start_ms: BASE_MS,
        metering_window_end_ms: BASE_MS + 3_600_000,
      },
      occurred_at: BASE_MS + 3_600_000,
    });

    const row = await duckQueryOne<{ amount_cents: number; fee_cents: number }>(
      "SELECT amount_cents, fee_cents FROM v_cost_fees WHERE event_uuid = 'meter-fees'"
    );

    expect(row).not.toBeNull();
    const amount = Number(row!.amount_cents);
    const fee = Number(row!.fee_cents);
    // 1h at $1/hr = 100 cents, fee = round(100 * 0.01) = 1 cent
    expect(amount).toBe(100);
    expect(fee).toBe(1);
  });

  test("v_cost_fees: prefers pre-computed fee_cents from enriched metering_stop", async () => {
    // C-1: enriched metering_stop carries fee_cents in data{}.
    // v_cost_fees should COALESCE and prefer this over the computed percentage.
    await insertAuditRow({
      event_uuid: "fee-enriched-001",
      event_type: "metering_stop",
      is_cost: true,
      provider: "aws",
      spec: "t3.micro",
      region: "us-east-1",
      data: {
        provider_resource_id: "i-fee001",
        metering_window_start_ms: BASE_MS,
        metering_window_end_ms: BASE_MS + 3_600_000,
        amount_cents: 100,
        fee_cents: 7, // pre-computed (e.g., 7% managed rate)
        currency: "USD",
      },
      occurred_at: BASE_MS + 3_600_000,
    });

    const row = await duckQueryOne<{ amount_cents: number; fee_cents: number }>(
      "SELECT amount_cents, fee_cents FROM v_cost_fees WHERE event_uuid = 'fee-enriched-001'"
    );

    expect(row).not.toBeNull();
    expect(Number(row!.amount_cents)).toBe(100);
    // Should use pre-computed fee_cents=7, NOT computed round(100 * 0.01) = 1
    expect(Number(row!.fee_cents)).toBe(7);
  });

  test("v_cost_fees: falls back to computed fee when fee_cents_raw absent", async () => {
    // Legacy event without fee_cents in data — should compute from percentage
    await insertPriceObservation({
      event_uuid: "price-fee-fallback",
      provider: "aws",
      spec: "t3.micro",
      region: "us-east-1",
      rate_per_hour: 1.00,
      at_ms: BASE_MS - 1_000,
    });

    await insertAuditRow({
      event_uuid: "fee-legacy-001",
      event_type: "metering_stop",
      is_cost: true,
      provider: "aws",
      spec: "t3.micro",
      region: "us-east-1",
      data: {
        metering_window_start_ms: BASE_MS,
        metering_window_end_ms: BASE_MS + 3_600_000,
        // No amount_cents, no fee_cents — legacy format
      },
      occurred_at: BASE_MS + 3_600_000,
    });

    const row = await duckQueryOne<{ amount_cents: number; fee_cents: number }>(
      "SELECT amount_cents, fee_cents FROM v_cost_fees WHERE event_uuid = 'fee-legacy-001'"
    );

    expect(row).not.toBeNull();
    // ASOF JOIN computes 1h at $1/hr = 100 cents
    expect(Number(row!.amount_cents)).toBe(100);
    // Computed: round(100 * 0.01) = 1
    expect(Number(row!.fee_cents)).toBe(1);
  });

  test("v_cost_fees: fee_cents is INTEGER", async () => {
    await insertPriceObservation({
      event_uuid: "price-feeint",
      provider: "aws",
      spec: "t3.micro",
      region: "us-east-1",
      rate_per_hour: 0.50,
      at_ms: BASE_MS - 1_000,
    });

    await insertAuditRow({
      event_uuid: "meter-feeint",
      event_type: "metering_stop",
      is_cost: true,
      provider: "aws",
      spec: "t3.micro",
      region: "us-east-1",
      data: {
        metering_window_start_ms: BASE_MS,
        metering_window_end_ms: BASE_MS + 3_600_000,
      },
      occurred_at: BASE_MS + 3_600_000,
    });

    const row = await duckQueryOne<{ fee_cents: number }>(
      "SELECT fee_cents FROM v_cost_fees WHERE event_uuid = 'meter-feeint'"
    );

    expect(row).not.toBeNull();
    expect(Number.isInteger(Number(row!.fee_cents))).toBe(true);
  });

  // ---------------------------------------------------------------------------
  // Full chain propagation
  // ---------------------------------------------------------------------------

  test("v_invoice_lines: reads from settlement_line_items (empty by default)", async () => {
    // v_invoice_lines now reads from settlement_line_items (WL-061-4B).
    // Cost events in audit_log do NOT appear directly in v_invoice_lines —
    // they are materialized into settlement_line_items by settlePeriod().
    // An empty settlement_line_items table should yield 0 rows.
    const rows = await duckQuery<{ line_uuid: string }>("SELECT line_uuid FROM v_invoice_lines");
    expect(rows.length).toBe(0);
  });

  test("v_invoice_lines: shows correct columns (line_uuid, batch_uuid, total_cents)", async () => {
    // Insert a synthetic settlement_line_items row to verify view shape
    await duckExecute(`
      INSERT INTO settlement_line_items (
        line_uuid, batch_uuid, tenant_id, description,
        amount_cents, fee_cents, period_start_ms, period_end_ms, currency
      ) VALUES ('LINE-001', 'BATCH-001', 1, 'Test line', 100, 1, ?, ?, 'USD')
    `, [BigInt(BASE_MS), BigInt(BASE_MS + 3_600_000)]);

    const row = await duckQueryOne<{
      line_uuid: string;
      batch_uuid: string;
      total_cents: number;
    }>("SELECT line_uuid, batch_uuid, total_cents FROM v_invoice_lines WHERE line_uuid = 'LINE-001'");

    expect(row).not.toBeNull();
    expect(row!.line_uuid).toBe("LINE-001");
    expect(row!.batch_uuid).toBe("BATCH-001");
    expect(Number(row!.total_cents)).toBe(101); // 100 + 1 fee
  });

  test("multiple cost events all appear in v_cost_fees", async () => {
    for (let i = 1; i <= 5; i++) {
      await insertAuditRow({
        event_uuid: `multi-${i}`,
        event_type: "metering_stop",
        is_cost: true,
      });
    }

    const rows = await duckQuery<{ event_uuid: string }>(
      "SELECT event_uuid FROM v_cost_fees WHERE event_uuid LIKE 'multi-%' ORDER BY event_uuid"
    );

    expect(rows.length).toBe(5);
  });

  // ---------------------------------------------------------------------------
  // Production format integration: full chain from production-shaped events
  // ---------------------------------------------------------------------------

  test("production format: metering_start + enriched metering_stop => non-zero cost through full chain", async () => {
    // Simulate the production event pair:
    // 1. metering_start (from wait-for-boot.ts): has metering_window_start_ms, hourly_rate_cents
    // 2. metering_stop (from finalize.ts): has both timestamps + amount_cents + hourly_rate_cents + fee_cents
    const startMs = BASE_MS;
    const stopMs = BASE_MS + 7_200_000; // 2 hours

    await insertAuditRow({
      event_uuid: "prod-start-001",
      event_type: "metering_start",
      is_cost: true,
      provider: "aws",
      spec: "t3.micro",
      region: "us-east-1",
      data: {
        provider_resource_id: "i-prod001",
        metering_window_start_ms: startMs,
        hourly_rate_cents: 50,
      },
      occurred_at: startMs,
    });

    await insertAuditRow({
      event_uuid: "prod-stop-001",
      event_type: "metering_stop",
      is_cost: true,
      provider: "aws",
      spec: "t3.micro",
      region: "us-east-1",
      data: {
        provider_resource_id: "i-prod001",
        metering_window_start_ms: startMs,
        metering_window_end_ms: stopMs,
        amount_cents: 100,  // 2h at $0.50/hr = 100 cents
        hourly_rate_cents: 50,
        fee_cents: 1,       // 1% BYO
        currency: "USD",
      },
      occurred_at: stopMs,
    });

    // Verify metering_stop produces non-zero cost through v_cost_fees
    const stopRow = await duckQueryOne<{
      amount_cents: number;
      fee_cents: number;
    }>("SELECT amount_cents, fee_cents FROM v_cost_fees WHERE event_uuid = 'prod-stop-001'");

    expect(stopRow).not.toBeNull();
    expect(Number(stopRow!.amount_cents)).toBe(100);
    expect(Number(stopRow!.fee_cents)).toBe(1);

    // Verify metering_start also appears in v_cost_fees (as an open window marker)
    const startRow = await duckQueryOne<{
      amount_cents: number;
      hourly_rate_cents_raw: number;
    }>("SELECT amount_cents, hourly_rate_cents_raw FROM v_cost_fees WHERE event_uuid = 'prod-start-001'");

    expect(startRow).not.toBeNull();
    // metering_start has no window_end_ms, so ASOF JOIN formula produces 0
    // (expected: estimated band is dead in DuckDB path, see SF-3 / TODO_AUDIT in budget.ts)
    expect(Number(startRow!.amount_cents)).toBe(0);
    // But hourly_rate_cents_raw is still extractable for SQLite fallback
    expect(Number(startRow!.hourly_rate_cents_raw)).toBe(50);
  });

  test("production format: metering_start open window has amount_cents=0 in DuckDB (SF-3)", async () => {
    // SF-3: metering_start events carry hourly_rate_cents but no window_end_ms.
    // In v_cost_priced, the COALESCE(c.window_end_ms - c.window_start_ms, 0) formula
    // produces 0 because window_end_ms is NULL for start events. The "estimated" band
    // is effectively dead in the DuckDB path — budget.ts SQLite fallback handles it.
    await insertAuditRow({
      event_uuid: "open-window-001",
      event_type: "metering_start",
      is_cost: true,
      provider: "aws",
      spec: "t3.micro",
      region: "us-east-1",
      data: {
        provider_resource_id: "i-open001",
        metering_window_start_ms: BASE_MS,
        hourly_rate_cents: 150,  // $1.50/hr
      },
      occurred_at: BASE_MS,
    });

    const row = await duckQueryOne<{
      amount_cents: number;
      window_start_ms: bigint | null;
      window_end_ms: bigint | null;
      hourly_rate_cents_raw: number;
    }>("SELECT amount_cents, window_start_ms, window_end_ms, hourly_rate_cents_raw FROM v_cost_fees WHERE event_uuid = 'open-window-001'");

    expect(row).not.toBeNull();
    // window_start_ms is set, window_end_ms is NULL
    expect(Number(row!.window_start_ms)).toBe(BASE_MS);
    expect(row!.window_end_ms).toBeNull();
    // amount_cents is 0 due to NULL window_end_ms (SF-3 known limitation)
    expect(Number(row!.amount_cents)).toBe(0);
    // hourly_rate_cents_raw is preserved for SQLite fallback estimation
    expect(Number(row!.hourly_rate_cents_raw)).toBe(150);
  });
});
