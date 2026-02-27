// billing/budget.test.ts — Tests for projectBudget (WL-061-3B §8)

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest } from "../../../tests/harness";
import { projectBudget } from "./budget";
import { emitAuditEvent } from "../material/db/audit";
import { updateTenant } from "../material/db/tenants";
import { initDuckDB, closeDuckDB, flushInboxToDuckDB } from "../material/duckdb";

let cleanup: () => Promise<void>;
beforeEach(async () => {
  cleanup = setupTest({});
  await initDuckDB(":memory:", ":memory:");
});
afterEach(async () => {
  await closeDuckDB();
  await cleanup();
});

const PERIOD_START = Date.now() - 7 * 24 * 60 * 60 * 1000; // 7 days ago
const PERIOD_END = Date.now() + 1000;

/** Emit event + flush to DuckDB so it's queryable via v_cost_fees */
async function emitAndFlush(event: Parameters<typeof emitAuditEvent>[0]) {
  emitAuditEvent(event);
  await flushInboxToDuckDB();
}

describe("projectBudget", () => {
  test("empty audit_log returns all-zero costs", async () => {
    const proj = await projectBudget({
      tenant_id: 1,
      period_start_ms: PERIOD_START,
      period_end_ms: PERIOD_END,
    });

    expect(proj.settled_cents).toBe(0);
    expect(proj.estimated_cents).toBe(0);
    expect(proj.pending_cents).toBe(0);
    expect(proj.total_cents).toBe(0);
  });

  test("empty audit_log with budget: remaining equals budget", async () => {
    updateTenant(1, { budgetUsd: 100 });

    const proj = await projectBudget({
      tenant_id: 1,
      period_start_ms: PERIOD_START,
      period_end_ms: PERIOD_END,
    });

    expect(proj.budget_cents).toBe(10000);
    expect(proj.remaining_cents).toBe(10000);
    expect(proj.remaining_pct).toBe(100);
  });

  test("one settled metering_stop event with enriched production shape", async () => {
    await emitAndFlush({
      event_type: "metering_stop",
      tenant_id: 1,
      instance_id: 1,
      source: "lifecycle",
      is_cost: true,
      data: {
        provider_resource_id: "i-test001",
        metering_window_start_ms: PERIOD_START + 500,
        metering_window_end_ms: PERIOD_START + 1000,
        amount_cents: 350,
        hourly_rate_cents: 100,
        fee_cents: 4,
        currency: "USD",
      },
      occurred_at: PERIOD_START + 1000,
    });

    const proj = await projectBudget({
      tenant_id: 1,
      period_start_ms: PERIOD_START,
      period_end_ms: PERIOD_END,
    });

    expect(proj.settled_cents).toBe(350);
    expect(proj.total_cents).toBe(350);
  });

  test("period_start_ms and period_end_ms filter events correctly", async () => {
    const insidePeriod = PERIOD_START + 1000;
    const outsidePeriod = PERIOD_START - 10000;

    await emitAndFlush({
      event_type: "metering_stop",
      tenant_id: 1,
      source: "test",
      is_cost: true,
      data: { amount_cents: 500 },
      occurred_at: insidePeriod,
    });

    await emitAndFlush({
      event_type: "metering_stop",
      tenant_id: 1,
      source: "test",
      is_cost: true,
      data: { amount_cents: 999 },
      occurred_at: outsidePeriod,
    });

    const proj = await projectBudget({
      tenant_id: 1,
      period_start_ms: PERIOD_START,
      period_end_ms: PERIOD_END,
    });

    expect(proj.settled_cents).toBe(500);
  });

  test("user-scoped projection only counts events for that user", async () => {
    await emitAndFlush({
      event_type: "metering_stop",
      tenant_id: 1,
      user_id: 42,
      source: "test",
      is_cost: true,
      data: { amount_cents: 200 },
      occurred_at: PERIOD_START + 1000,
    });

    await emitAndFlush({
      event_type: "metering_stop",
      tenant_id: 1,
      user_id: 99,
      source: "test",
      is_cost: true,
      data: { amount_cents: 800 },
      occurred_at: PERIOD_START + 2000,
    });

    const projUser42 = await projectBudget({
      tenant_id: 1,
      user_id: 42,
      period_start_ms: PERIOD_START,
      period_end_ms: PERIOD_END,
    });

    expect(projUser42.settled_cents).toBe(200);
  });

  test("tenant-scoped projection includes all users", async () => {
    await emitAndFlush({
      event_type: "metering_stop",
      tenant_id: 1,
      user_id: 42,
      source: "test",
      is_cost: true,
      data: { amount_cents: 200 },
      occurred_at: PERIOD_START + 1000,
    });

    await emitAndFlush({
      event_type: "metering_stop",
      tenant_id: 1,
      user_id: 99,
      source: "test",
      is_cost: true,
      data: { amount_cents: 800 },
      occurred_at: PERIOD_START + 2000,
    });

    const projTenant = await projectBudget({
      tenant_id: 1,
      period_start_ms: PERIOD_START,
      period_end_ms: PERIOD_END,
    });

    expect(projTenant.settled_cents).toBe(1000);
  });

  test("DuckDB unavailable: throws instead of silent fallback", async () => {
    await closeDuckDB();

    await expect(projectBudget({
      tenant_id: 1,
      period_start_ms: PERIOD_START,
      period_end_ms: PERIOD_END,
    })).rejects.toThrow("DuckDB not initialized");
  });

  test("returns correct period bounds", async () => {
    const proj = await projectBudget({
      tenant_id: 1,
      period_start_ms: PERIOD_START,
      period_end_ms: PERIOD_END,
    });

    expect(proj.period_start_ms).toBe(PERIOD_START);
    expect(proj.period_end_ms).toBe(PERIOD_END);
  });

  test("no budget set: budget_cents is 0, remaining_pct is 100", async () => {
    const proj = await projectBudget({
      tenant_id: 1,
      period_start_ms: PERIOD_START,
      period_end_ms: PERIOD_END,
    });

    expect(proj.budget_cents).toBe(0);
    expect(proj.remaining_pct).toBe(100);
  });

  test("metering_stop with enriched data: settled_cents reads amount_cents", async () => {
    await emitAndFlush({
      event_type: "metering_stop",
      tenant_id: 1,
      instance_id: 10,
      source: "test",
      is_cost: true,
      data: {
        provider_resource_id: "i-prod001",
        metering_window_start_ms: PERIOD_START + 1000,
        metering_window_end_ms: PERIOD_START + 3_601_000,
        amount_cents: 250,
        hourly_rate_cents: 250,
        fee_cents: 3,
        currency: "USD",
      },
      occurred_at: PERIOD_START + 3_601_000,
    });

    const proj = await projectBudget({
      tenant_id: 1,
      period_start_ms: PERIOD_START,
      period_end_ms: PERIOD_END,
    });

    expect(proj.settled_cents).toBe(250);
  });

  test("open metering_start: DuckDB returns 0 for open windows", async () => {
    // DuckDB open-window estimation returns 0 (no window_end_ms).
    // This is by design — open-window cost estimation would require
    // elapsed time × rate, which is not computable in the view chain.
    await emitAndFlush({
      event_type: "metering_start",
      tenant_id: 1,
      instance_id: 20,
      source: "test",
      is_cost: true,
      data: {
        provider_resource_id: "i-open001",
        metering_window_start_ms: PERIOD_START + 500,
        hourly_rate_cents: 500,
      },
      occurred_at: PERIOD_START + 500,
    });

    const proj = await projectBudget({
      tenant_id: 1,
      period_start_ms: PERIOD_START,
      period_end_ms: PERIOD_END,
    });

    // DuckDB returns 0 for open windows (documented design tradeoff)
    expect(proj.estimated_cents).toBe(0);
  });
});
