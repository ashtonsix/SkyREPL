// billing/settlement.test.ts — Settlement worker + Stripe integration tests (WL-061-4B)

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest } from "../../../tests/harness";
import {
  initDuckDB,
  closeDuckDB,
  duckExecute,
} from "../material/duckdb";
import {
  settlePeriod,
  getSettlementBatch,
  listSettlementBatches,
  getSettlementLineItems,
} from "./settlement";
import {
  createStripeInvoice,
  setStripeClientFactory,
  resetStripeClientFactory,
} from "./stripe";
import { creditDeposit, getCreditBalance } from "../material/db/credits";

// =============================================================================
// Helpers
// =============================================================================

const BASE_MS = 1_748_000_000_000; // Fixed reference epoch (~May 2025)
const PERIOD_START = BASE_MS;
const PERIOD_END   = BASE_MS + 30 * 24 * 60 * 60 * 1000; // +30 days

let uuidCounter = 0;
function uid(): string {
  return `test-${++uuidCounter}-${Date.now()}`;
}

/**
 * Insert a cost event directly into DuckDB audit_log.
 * Bypasses SQLite inbox for isolation.
 *
 * Uses the enriched production format (C-1): metering_stop events carry
 * pre-computed amount_cents so v_cost_priced reads it directly without
 * requiring an ASOF JOIN against price_observation events.
 *
 * Shape matches what production emitters emit after C-1:
 *   metering_stop: { provider_resource_id, metering_window_start_ms,
 *                    metering_window_end_ms, amount_cents, currency }
 */
async function insertCostEvent(params: {
  provider?: string;
  spec?: string;
  region?: string;
  run_id?: number;
  rate_per_hour?: number;
  window_ms?: number;        // duration in ms, default 3_600_000 (1h)
  occurred_at?: number;
  tenant_id?: number;
}): Promise<void> {
  const tenantId = params.tenant_id ?? 1;
  const occurredAt = params.occurred_at ?? (PERIOD_START + 1000);
  const windowMs = params.window_ms ?? 3_600_000;
  const ratePerHour = params.rate_per_hour ?? 1.00;
  // Pre-compute amount_cents: (window_ms / 3_600_000) * ratePerHour * 100
  const amountCents = Math.round((windowMs / 3_600_000) * ratePerHour * 100);

  await duckExecute(`
    INSERT INTO audit_log (
      id, event_uuid, event_type, tenant_id,
      provider, spec, region, source,
      is_cost, is_usage, is_attribution, is_reconciliation,
      run_id,
      data, occurred_at, created_at, event_version
    ) VALUES (?, ?, 'metering_stop', ?, ?, ?, ?, 'test', true, false, false, false, ?, ?, ?, ?, 1)
  `, [
    Math.floor(Math.random() * 100_000_000),
    uid(),
    tenantId,
    params.provider ?? "aws",
    params.spec ?? "t3.micro",
    params.region ?? "us-east-1",
    params.run_id ?? null,
    JSON.stringify({
      provider_resource_id: `test-resource-${uid()}`,
      metering_window_start_ms: occurredAt,
      metering_window_end_ms: occurredAt + windowMs,
      amount_cents: amountCents,
      currency: "USD",
    }),
    BigInt(occurredAt + windowMs),
    BigInt(occurredAt + windowMs),
  ]);
}

// =============================================================================
// Test setup
// =============================================================================

let cleanup: () => Promise<void>;
beforeEach(async () => {
  uuidCounter = 0;
  cleanup = setupTest({});
  await initDuckDB(":memory:", ":memory:");
});

afterEach(async () => {
  resetStripeClientFactory();
  await closeDuckDB();
  await cleanup();
});

// =============================================================================
// Settlement Worker Tests
// =============================================================================

describe("settlePeriod", () => {
  test("settle period with cost events produces correct line items and total", async () => {
    // 1 hour at $1/hr = 100 cents; fee = 1 cent; total = 101 cents
    await insertCostEvent({ rate_per_hour: 1.00, window_ms: 3_600_000 });

    const batch = await settlePeriod(1, { start_ms: PERIOD_START, end_ms: PERIOD_END });

    expect(batch.status).toBe("settled");
    expect(batch.tenant_id).toBe(1);
    expect(batch.period_start_ms).toBe(PERIOD_START);
    expect(batch.period_end_ms).toBe(PERIOD_END);
    expect(batch.settled_at).not.toBeNull();
    // amount_cents = 100, fee_cents = 1 (1% of 100)
    expect(batch.total_cents).toBe(100);
    expect(batch.fee_cents).toBe(1);

    const lines = await getSettlementLineItems(batch.batch_uuid);
    expect(lines.length).toBe(1);
    expect(lines[0].amount_cents).toBe(100);
    expect(lines[0].fee_cents).toBe(1);
    expect(lines[0].description).toContain("aws");
  });

  test("credit wallet is debited before invoicing", async () => {
    // Deposit 50 cents credit
    creditDeposit(1, 50);
    expect(getCreditBalance(1)).toBe(50);

    // 1h at $1/hr = 100 cents compute + 1 cent fee = 101 cents due
    await insertCostEvent({ rate_per_hour: 1.00, window_ms: 3_600_000 });

    const batch = await settlePeriod(1, { start_ms: PERIOD_START, end_ms: PERIOD_END });

    expect(batch.status).toBe("settled");
    // Credit applied should be min(balance=50, total_due=101) = 50
    expect(batch.credit_applied_cents).toBe(50);
    // Balance should be 0 now
    expect(getCreditBalance(1)).toBe(0);
  });

  test("batch status transitions: open then settled", async () => {
    // We can inspect SQLite directly via getSettlementBatch
    // After settlePeriod completes it should be 'settled', never 'open'
    const batch = await settlePeriod(1, { start_ms: PERIOD_START, end_ms: PERIOD_END });

    expect(batch.status).toBe("settled");
    // Verify batch is retrievable from SQLite by UUID
    const fetched = getSettlementBatch(batch.batch_uuid);
    expect(fetched).not.toBeNull();
    expect(fetched!.status).toBe("settled");
  });

  test("empty period with no cost events produces 0 total and 0 line items", async () => {
    const batch = await settlePeriod(1, { start_ms: PERIOD_START, end_ms: PERIOD_END });

    expect(batch.status).toBe("settled");
    expect(batch.total_cents).toBe(0);
    expect(batch.fee_cents).toBe(0);
    expect(batch.credit_applied_cents).toBe(0);

    const lines = await getSettlementLineItems(batch.batch_uuid);
    expect(lines.length).toBe(0);
  });

  test("settlement with multiple providers produces grouped line items", async () => {
    // AWS t3.micro: 1h at $1/hr = 100 cents
    await insertCostEvent({
      provider: "aws",
      spec: "t3.micro",
      region: "us-east-1",
      rate_per_hour: 1.00,
      window_ms: 3_600_000,
    });
    // DO: g-2vcpu-8gb: 2h at $0.50/hr = 100 cents
    await insertCostEvent({
      provider: "digitalocean",
      spec: "g-2vcpu-8gb",
      region: "nyc3",
      rate_per_hour: 0.50,
      window_ms: 7_200_000,
      occurred_at: PERIOD_START + 2000,
    });

    const batch = await settlePeriod(1, { start_ms: PERIOD_START, end_ms: PERIOD_END });

    const lines = await getSettlementLineItems(batch.batch_uuid);
    expect(lines.length).toBe(2);

    const providers = lines.map(l => l.provider).sort();
    expect(providers).toEqual(["aws", "digitalocean"]);

    // Both should have amount_cents = 100
    for (const line of lines) {
      expect(line.amount_cents).toBe(100);
    }

    // Total: 200 amount + 2 fee cents
    expect(batch.total_cents).toBe(200);
    expect(batch.fee_cents).toBe(2);
  });

  test("credit wallet not debited when balance is zero", async () => {
    await insertCostEvent({ rate_per_hour: 1.00, window_ms: 3_600_000 });

    const batch = await settlePeriod(1, { start_ms: PERIOD_START, end_ms: PERIOD_END });

    expect(batch.credit_applied_cents).toBe(0);
    expect(getCreditBalance(1)).toBe(0);
  });

  test("listSettlementBatches returns batches for tenant in descending order", async () => {
    await settlePeriod(1, { start_ms: PERIOD_START, end_ms: PERIOD_START + 1000 });
    await settlePeriod(1, { start_ms: PERIOD_START + 2000, end_ms: PERIOD_START + 3000 });

    const batches = listSettlementBatches(1);
    expect(batches.length).toBe(2);
    // Descending order by created_at
    expect(batches[0].created_at).toBeGreaterThanOrEqual(batches[1].created_at);
  });

  test("line items carry correct batch_uuid and tenant_id", async () => {
    await insertCostEvent({ rate_per_hour: 1.00, window_ms: 3_600_000 });

    const batch = await settlePeriod(1, { start_ms: PERIOD_START, end_ms: PERIOD_END });
    const lines = await getSettlementLineItems(batch.batch_uuid);

    expect(lines.length).toBeGreaterThan(0);
    for (const line of lines) {
      expect(line.batch_uuid).toBe(batch.batch_uuid);
      expect(line.tenant_id).toBe(1);
      expect(line.currency).toBe("USD");
    }
  });
});

// =============================================================================
// Stripe Invoice Tests (mocked)
// =============================================================================

describe("createStripeInvoice (mocked)", () => {
  test("createStripeInvoice produces correct API calls for each line item", async () => {
    const invoiceItemsCreated: any[] = [];
    let invoiceCreated: any = null;
    let invoiceFinalized: string | null = null;
    const callOrder: string[] = [];

    // Mock Stripe client
    const mockStripe = {
      invoiceItems: {
        create: async (params: any) => {
          callOrder.push("invoiceItems.create");
          invoiceItemsCreated.push(params);
          return { id: `ii_${Date.now()}` };
        },
      },
      invoices: {
        create: async (params: any) => {
          callOrder.push("invoices.create");
          invoiceCreated = params;
          return { id: "in_mock_001", status: "draft", hosted_invoice_url: null };
        },
        finalizeInvoice: async (id: string) => {
          callOrder.push("finalizeInvoice");
          invoiceFinalized = id;
          return { id, status: "open", hosted_invoice_url: "https://invoice.stripe.com/mock" };
        },
      },
    };

    setStripeClientFactory(() => mockStripe);
    process.env.STRIPE_SECRET_KEY = "sk_test_mock";
    process.env.STRIPE_CUSTOMER_ID = "cus_mock_tenant1";

    const batch = await settlePeriod(1, { start_ms: PERIOD_START, end_ms: PERIOD_END });

    await insertCostEvent({ rate_per_hour: 1.00, window_ms: 3_600_000 });
    // Re-settle so we have line items
    const batch2 = await settlePeriod(1, { start_ms: PERIOD_START + 100, end_ms: PERIOD_END });
    const lines = await getSettlementLineItems(batch2.batch_uuid);

    const result = await createStripeInvoice(batch2, lines);

    expect(result.invoice_id).toBe("in_mock_001");
    expect(result.status).toBe("open");
    expect(result.hosted_invoice_url).toBe("https://invoice.stripe.com/mock");
    expect(invoiceFinalized as unknown as string).toBe("in_mock_001");
    expect(invoiceCreated).not.toBeNull();

    // Verify call order: invoice created first, then items, then finalize
    expect(callOrder[0]).toBe("invoices.create");
    expect(callOrder[callOrder.length - 1]).toBe("finalizeInvoice");

    // Verify each invoice item has invoice: invoice.id
    for (const item of invoiceItemsCreated) {
      expect(item.invoice).toBe("in_mock_001");
    }

    // Verify collection_method is charge_automatically
    expect(invoiceCreated.collection_method).toBe("charge_automatically");

    // Cleanup
    delete process.env.STRIPE_SECRET_KEY;
    delete process.env.STRIPE_CUSTOMER_ID;
  });

  test("each line item carries skyrepl_settlement_batch_id in metadata", async () => {
    const invoiceItemsCreated: any[] = [];

    const mockStripe = {
      invoiceItems: {
        create: async (params: any) => {
          invoiceItemsCreated.push(params);
          return { id: `ii_${Date.now()}` };
        },
      },
      invoices: {
        create: async () => ({ id: "in_mock_002", status: "draft", hosted_invoice_url: null }),
        finalizeInvoice: async (id: string) => ({ id, status: "open", hosted_invoice_url: null }),
      },
    };

    setStripeClientFactory(() => mockStripe);
    process.env.STRIPE_SECRET_KEY = "sk_test_mock";
    process.env.STRIPE_CUSTOMER_ID = "cus_mock_tenant1";

    // Insert two cost events to get two line items
    await insertCostEvent({ provider: "aws", spec: "t3.micro", rate_per_hour: 1.00, window_ms: 3_600_000 });
    await insertCostEvent({ provider: "digitalocean", spec: "g-2vcpu-8gb", rate_per_hour: 0.50, window_ms: 7_200_000, occurred_at: PERIOD_START + 2000 });

    const batch = await settlePeriod(1, { start_ms: PERIOD_START, end_ms: PERIOD_END });
    const lines = await getSettlementLineItems(batch.batch_uuid);

    await createStripeInvoice(batch, lines);

    // All created invoice items should carry ONLY the batch ID in metadata
    for (const item of invoiceItemsCreated) {
      expect(item.metadata.skyrepl_settlement_batch_id).toBe(batch.batch_uuid);
      // Should NOT have extra metadata
      expect(Object.keys(item.metadata)).toEqual(["skyrepl_settlement_batch_id"]);
    }
    // Should have created an item per non-zero line
    expect(invoiceItemsCreated.length).toBe(lines.filter(l => l.amount_cents + l.fee_cents > 0).length);

    delete process.env.STRIPE_SECRET_KEY;
    delete process.env.STRIPE_CUSTOMER_ID;
  });

  test("missing STRIPE_SECRET_KEY throws clear error", async () => {
    delete process.env.STRIPE_SECRET_KEY;
    delete process.env.STRIPE_CUSTOMER_ID;

    const batch = await settlePeriod(1, { start_ms: PERIOD_START, end_ms: PERIOD_END });
    const lines = await getSettlementLineItems(batch.batch_uuid);

    let errorMessage = "";
    try {
      await createStripeInvoice(batch, lines);
    } catch (err: any) {
      errorMessage = err.message;
    }

    expect(errorMessage).toContain("Stripe not configured");
    expect(errorMessage).toContain("STRIPE_SECRET_KEY");
  });

  test("batch status updated to invoiced in SQLite after createStripeInvoice", async () => {
    const mockStripe = {
      invoiceItems: { create: async () => ({ id: "ii_x" }) },
      invoices: {
        create: async () => ({ id: "in_mock_003", status: "draft", hosted_invoice_url: null }),
        finalizeInvoice: async (id: string) => ({ id, status: "open", hosted_invoice_url: null }),
      },
    };

    setStripeClientFactory(() => mockStripe);
    process.env.STRIPE_SECRET_KEY = "sk_test_mock";
    process.env.STRIPE_CUSTOMER_ID = "cus_mock";

    await insertCostEvent({ rate_per_hour: 1.00, window_ms: 3_600_000 });
    const batch = await settlePeriod(1, { start_ms: PERIOD_START, end_ms: PERIOD_END });
    const lines = await getSettlementLineItems(batch.batch_uuid);

    await createStripeInvoice(batch, lines);

    const updated = getSettlementBatch(batch.batch_uuid);
    expect(updated!.status).toBe("invoiced");
    expect(updated!.stripe_invoice_id).toBe("in_mock_003");

    delete process.env.STRIPE_SECRET_KEY;
    delete process.env.STRIPE_CUSTOMER_ID;
  });
});
