// api/routes/billing.test.ts — Console drilldown billing API tests (WL-061-4B §5)
//
// Tests the billing HTTP API using in-process Elysia (app.handle()).
// Verifies tenant isolation, batch listing, line item retrieval, and cost queries.

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest } from "../../../../tests/harness";
import {
  initDuckDB,
  closeDuckDB,
  duckExecute,
} from "../../material/duckdb";
import { createServer } from "./server";
import { registerOperationRoutes } from "./operations";
import { registerResourceRoutes } from "./resources";
import { settlePeriod } from "../../billing/settlement";
import { createHash } from "node:crypto";
import { getDatabase } from "../../material/db/init";

// =============================================================================
// Helpers
// =============================================================================

const BASE_MS = 1_748_000_000_000;
const PERIOD_START = BASE_MS;
const PERIOD_END   = BASE_MS + 30 * 24 * 60 * 60 * 1000;

let uuidCounter = 0;
function uid(): string {
  return `api-test-${++uuidCounter}-${Date.now()}`;
}

function makeAdminKey(tenantId: number): string {
  const rawKey = "srk-billingtestad000000000000000";
  const hash = createHash("sha256").update(rawKey).digest("hex");
  const db = getDatabase();
  db.prepare(
    `INSERT OR IGNORE INTO api_keys (key_hash, name, tenant_id, role, permissions, expires_at, created_at)
     VALUES (?, 'billing-admin', ?, 'admin', 'all', ?, ?)`
  ).run(hash, tenantId, Date.now() + 86_400_000, Date.now());
  return rawKey;
}

function makeAdminKeyForTenant(tenantId: number, suffix: string): string {
  const rawKey = `srk-billingtest${suffix}000000000000`;
  const hash = createHash("sha256").update(rawKey).digest("hex");
  const db = getDatabase();
  db.prepare(
    `INSERT OR IGNORE INTO api_keys (key_hash, name, tenant_id, role, permissions, expires_at, created_at)
     VALUES (?, 'billing-admin-${suffix}', ?, 'admin', 'all', ?, ?)`
  ).run(hash, tenantId, Date.now() + 86_400_000, Date.now());
  return rawKey;
}

function makeTenant(name: string): number {
  const db = getDatabase();
  const result = db.prepare(
    `INSERT INTO tenants (name, seat_cap, budget_usd, created_at, updated_at)
     VALUES (?, 5, NULL, ?, ?)`
  ).run(name, Date.now(), Date.now());
  return result.lastInsertRowid as number;
}

/**
 * Insert a cost event directly into DuckDB audit_log.
 * Uses the enriched production format (C-1): metering_stop events carry
 * pre-computed amount_cents so v_cost_priced reads it directly without
 * requiring an ASOF JOIN against price_observation events.
 *
 * Shape matches production emitters after C-1:
 *   metering_stop: { provider_resource_id, metering_window_start_ms,
 *                    metering_window_end_ms, amount_cents, currency }
 */
async function insertCostEvent(params: {
  provider?: string;
  spec?: string;
  region?: string;
  run_id?: number;
  rate_per_hour?: number;
  window_ms?: number;
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

type ElysiaApp = ReturnType<typeof createServer>;

let cleanup: () => Promise<void>;
let app: ElysiaApp;
let adminKey: string;

beforeEach(async () => {
  uuidCounter = 0;
  cleanup = setupTest({});
  await initDuckDB(":memory:", ":memory:");

  app = createServer({ port: 0, corsOrigins: [], maxBodySize: 1_048_576 });
  registerResourceRoutes(app);
  registerOperationRoutes(app); // registers billing routes inside

  adminKey = makeAdminKey(1);
});

afterEach(async () => {
  await closeDuckDB();
  await cleanup();
});

function req(
  method: string,
  path: string,
  opts: { key?: string } = {}
): Promise<Response> {
  const headers: Record<string, string> = { "Content-Type": "application/json" };
  if (opts.key) headers["Authorization"] = `Bearer ${opts.key}`;
  return app.handle(
    new Request(`http://localhost${path}`, { method, headers })
  );
}

// =============================================================================
// GET /v1/billing/batches
// =============================================================================

describe("GET /v1/billing/batches", () => {
  test("returns empty list when no batches for tenant", async () => {
    const res = await req("GET", "/v1/billing/batches", { key: adminKey });
    expect(res.status).toBe(200);
    const body = await res.json() as any;
    expect(body.data).toEqual([]);
  });

  test("returns tenant's batches after settlement", async () => {
    await settlePeriod(1, { start_ms: PERIOD_START, end_ms: PERIOD_END });
    await settlePeriod(1, { start_ms: PERIOD_END, end_ms: PERIOD_END + 1000 });

    const res = await req("GET", "/v1/billing/batches", { key: adminKey });
    expect(res.status).toBe(200);
    const body = await res.json() as any;
    expect(body.data.length).toBe(2);
    expect(body.data.every((b: any) => b.tenant_id === 1)).toBe(true);
    expect(body.data.every((b: any) => b.status === "settled")).toBe(true);
  });

  test("does not return batches for other tenants", async () => {
    const otherTenantId = makeTenant("other-team");
    await settlePeriod(otherTenantId, { start_ms: PERIOD_START, end_ms: PERIOD_END });

    const res = await req("GET", "/v1/billing/batches", { key: adminKey });
    expect(res.status).toBe(200);
    const body = await res.json() as any;
    // Tenant 1 should not see tenant N's batches
    expect(body.data.length).toBe(0);
  });

  test("returns 401 without auth", async () => {
    const res = await req("GET", "/v1/billing/batches");
    expect(res.status).toBe(401);
  });
});

// =============================================================================
// GET /v1/billing/batches/:batch_uuid
// =============================================================================

describe("GET /v1/billing/batches/:batch_uuid", () => {
  test("returns batch summary for valid UUID", async () => {
    await insertCostEvent({ rate_per_hour: 1.00, window_ms: 3_600_000 });
    const batch = await settlePeriod(1, { start_ms: PERIOD_START, end_ms: PERIOD_END });

    const res = await req("GET", `/v1/billing/batches/${batch.batch_uuid}`, { key: adminKey });
    expect(res.status).toBe(200);
    const body = await res.json() as any;
    expect(body.data.batch_uuid).toBe(batch.batch_uuid);
    expect(body.data.status).toBe("settled");
    expect(body.data.total_cents).toBe(100);
  });

  test("returns 404 for unknown UUID", async () => {
    const res = await req("GET", "/v1/billing/batches/01NONEXISTENT00000000000000", { key: adminKey });
    expect(res.status).toBe(404);
  });

  test("returns 404 when batch belongs to another tenant", async () => {
    const otherTenantId = makeTenant("other-team");
    const batch = await settlePeriod(otherTenantId, { start_ms: PERIOD_START, end_ms: PERIOD_END });

    const res = await req("GET", `/v1/billing/batches/${batch.batch_uuid}`, { key: adminKey });
    expect(res.status).toBe(404);
  });
});

// =============================================================================
// GET /v1/billing/batches/:batch_uuid/lines
// =============================================================================

describe("GET /v1/billing/batches/:batch_uuid/lines", () => {
  test("returns correct line items for batch with cost events", async () => {
    await insertCostEvent({
      provider: "aws",
      spec: "t3.micro",
      region: "us-east-1",
      rate_per_hour: 1.00,
      window_ms: 3_600_000,
    });

    const batch = await settlePeriod(1, { start_ms: PERIOD_START, end_ms: PERIOD_END });

    const res = await req("GET", `/v1/billing/batches/${batch.batch_uuid}/lines`, { key: adminKey });
    expect(res.status).toBe(200);
    const body = await res.json() as any;
    expect(body.data.length).toBe(1);
    expect(body.data[0].amount_cents).toBe(100);
    expect(body.data[0].provider).toBe("aws");
    expect(body.data[0].batch_uuid).toBe(batch.batch_uuid);
  });

  test("returns empty lines for period with no cost events", async () => {
    const batch = await settlePeriod(1, { start_ms: PERIOD_START, end_ms: PERIOD_END });

    const res = await req("GET", `/v1/billing/batches/${batch.batch_uuid}/lines`, { key: adminKey });
    expect(res.status).toBe(200);
    const body = await res.json() as any;
    expect(body.data).toEqual([]);
  });

  test("tenant isolation: cannot access another tenant's line items", async () => {
    const otherTenantId = makeTenant("other-team");
    const batch = await settlePeriod(otherTenantId, { start_ms: PERIOD_START, end_ms: PERIOD_END });

    const res = await req("GET", `/v1/billing/batches/${batch.batch_uuid}/lines`, { key: adminKey });
    expect(res.status).toBe(404);
  });
});

// =============================================================================
// GET /v1/billing/cost
// =============================================================================

describe("GET /v1/billing/cost", () => {
  test("returns cost breakdown grouped by provider", async () => {
    await insertCostEvent({
      provider: "aws",
      spec: "t3.micro",
      region: "us-east-1",
      rate_per_hour: 1.00,
      window_ms: 3_600_000,
    });
    await insertCostEvent({
      provider: "digitalocean",
      spec: "g-2vcpu-8gb",
      region: "nyc3",
      rate_per_hour: 0.50,
      window_ms: 7_200_000,
      occurred_at: PERIOD_START + 2000,
    });

    const url = `/v1/billing/cost?period_start_ms=${PERIOD_START}&period_end_ms=${PERIOD_END}&group_by=provider`;
    const res = await req("GET", url, { key: adminKey });
    expect(res.status).toBe(200);
    const body = await res.json() as any;
    expect(body.data.length).toBe(2);

    const providers = body.data.map((r: any) => r.provider).sort();
    expect(providers).toEqual(["aws", "digitalocean"]);

    // Both should have total_amount_cents = 100
    for (const row of body.data) {
      expect(row.total_amount_cents).toBe(100);
    }
  });

  test("filter by provider returns only matching rows", async () => {
    await insertCostEvent({ provider: "aws", rate_per_hour: 1.00, window_ms: 3_600_000 });
    await insertCostEvent({
      provider: "digitalocean",
      rate_per_hour: 0.50,
      window_ms: 7_200_000,
      occurred_at: PERIOD_START + 2000,
    });

    const url = `/v1/billing/cost?period_start_ms=${PERIOD_START}&period_end_ms=${PERIOD_END}&provider=aws`;
    const res = await req("GET", url, { key: adminKey });
    expect(res.status).toBe(200);
    const body = await res.json() as any;
    expect(body.data.length).toBe(1);
    expect(body.data[0].provider).toBe("aws");
  });

  test("returns 400 when period params are missing", async () => {
    const res = await req("GET", "/v1/billing/cost", { key: adminKey });
    expect(res.status).toBe(400);
  });

  test("returns 401 without auth", async () => {
    const url = `/v1/billing/cost?period_start_ms=${PERIOD_START}&period_end_ms=${PERIOD_END}`;
    const res = await req("GET", url);
    expect(res.status).toBe(401);
  });

  test("response includes meta with group_by and period bounds", async () => {
    const url = `/v1/billing/cost?period_start_ms=${PERIOD_START}&period_end_ms=${PERIOD_END}&group_by=provider,spec`;
    const res = await req("GET", url, { key: adminKey });
    expect(res.status).toBe(200);
    const body = await res.json() as any;
    expect(body.meta).toBeDefined();
    expect(body.meta.period_start_ms).toBe(PERIOD_START);
    expect(body.meta.period_end_ms).toBe(PERIOD_END);
    expect(body.meta.group_by).toContain("provider");
    expect(body.meta.group_by).toContain("spec");
  });
});
