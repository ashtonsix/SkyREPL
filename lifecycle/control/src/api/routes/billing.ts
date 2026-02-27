// api/routes/billing.ts — Console drilldown billing API (WL-061-4B §5)
//
// Routes:
//   GET  /v1/billing/batches                  — list settlement batches for tenant
//   GET  /v1/billing/batches/:batch_uuid      — batch summary
//   GET  /v1/billing/batches/:batch_uuid/lines — line items (from DuckDB)
//   GET  /v1/billing/cost                     — cost query with grouping/filter

import { Elysia } from "elysia";
import { getAuthContext } from "../middleware/auth";
import {
  listSettlementBatches,
  getSettlementBatch,
  getSettlementLineItems,
} from "../../billing/settlement";
import { duckQuery } from "../../material/duckdb";

export function registerBillingRoutes(app: Elysia<any>): void {
  // ─── List settlement batches for tenant ──────────────────────────────
  //
  // GET /v1/billing/batches
  //
  // Returns all settlement batches for the authenticated tenant,
  // ordered by creation time descending.

  app.get("/v1/billing/batches", ({ request, set }) => {
    const auth = getAuthContext(request);
    if (!auth) {
      set.status = 401;
      return { error: { code: "UNAUTHORIZED", message: "Missing API key", category: "auth" } };
    }

    const batches = listSettlementBatches(auth.tenantId);
    return { data: batches };
  });

  // ─── Get single settlement batch ─────────────────────────────────────
  //
  // GET /v1/billing/batches/:batch_uuid

  app.get("/v1/billing/batches/:batch_uuid", ({ params, request, set }) => {
    const auth = getAuthContext(request);
    if (!auth) {
      set.status = 401;
      return { error: { code: "UNAUTHORIZED", message: "Missing API key", category: "auth" } };
    }

    const batch = getSettlementBatch(params.batch_uuid);
    if (!batch) {
      set.status = 404;
      return { error: { code: "NOT_FOUND", message: `Batch ${params.batch_uuid} not found`, category: "not_found" } };
    }

    // Tenant isolation
    if (batch.tenant_id !== auth.tenantId) {
      set.status = 404;
      return { error: { code: "NOT_FOUND", message: `Batch ${params.batch_uuid} not found`, category: "not_found" } };
    }

    return { data: batch };
  });

  // ─── Get settlement line items for a batch ───────────────────────────
  //
  // GET /v1/billing/batches/:batch_uuid/lines
  //
  // Queries DuckDB settlement_line_items for the given batch.

  app.get("/v1/billing/batches/:batch_uuid/lines", async ({ params, request, set }) => {
    const auth = getAuthContext(request);
    if (!auth) {
      set.status = 401;
      return { error: { code: "UNAUTHORIZED", message: "Missing API key", category: "auth" } };
    }

    // Verify batch exists and belongs to tenant
    const batch = getSettlementBatch(params.batch_uuid);
    if (!batch || batch.tenant_id !== auth.tenantId) {
      set.status = 404;
      return { error: { code: "NOT_FOUND", message: `Batch ${params.batch_uuid} not found`, category: "not_found" } };
    }

    try {
      const lines = await getSettlementLineItems(params.batch_uuid);
      return { data: lines };
    } catch (err) {
      console.error("[billing] getSettlementLineItems error:", err);
      set.status = 503;
      return { error: { code: "SERVICE_UNAVAILABLE", message: "Analytics unavailable", category: "internal" } };
    }
  });

  // ─── Cost query with grouping and filtering ──────────────────────────
  //
  // GET /v1/billing/cost
  //
  // Query params:
  //   period_start_ms  — required, epoch ms
  //   period_end_ms    — required, epoch ms
  //   user_id          — optional, filter by user
  //   group_by         — optional CSV: provider,spec,region,user_id,run_id,manifest_id
  //   provider         — optional, filter by provider
  //
  //
  // NOTE: No pagination — unbounded response for tenants with many cost events.
  // Deferred: add LIMIT/OFFSET or cursor pagination (see pagination.ts helpers).
  //
  // NOTE: v_cost_fees returns real amount_cents when metering_stop events carry
  // pre-computed amounts (C-1 fix). Legacy events without amount_cents produce $0.

  app.get("/v1/billing/cost", async ({ query, request, set }) => {
    const auth = getAuthContext(request);
    if (!auth) {
      set.status = 401;
      return { error: { code: "UNAUTHORIZED", message: "Missing API key", category: "auth" } };
    }

    const periodStartMs = Number(query.period_start_ms);
    const periodEndMs = Number(query.period_end_ms);

    if (!periodStartMs || !periodEndMs || isNaN(periodStartMs) || isNaN(periodEndMs)) {
      set.status = 400;
      return { error: { code: "INVALID_INPUT", message: "period_start_ms and period_end_ms are required", category: "validation" } };
    }

    if (periodEndMs <= periodStartMs) {
      set.status = 400;
      return { error: { code: "INVALID_INPUT", message: "period_end_ms must be greater than period_start_ms", category: "validation" } };
    }

    // Parse group_by dimensions
    const VALID_DIMENSIONS = new Set(["provider", "spec", "region", "user_id", "run_id", "manifest_id"]);
    const rawGroupBy = String(query.group_by ?? "provider");
    const groupDimensions = rawGroupBy
      .split(",")
      .map(d => d.trim())
      .filter(d => VALID_DIMENSIONS.has(d));

    if (groupDimensions.length === 0) {
      groupDimensions.push("provider");
    }

    // Build the SELECT and GROUP BY clauses
    const selectCols = groupDimensions.join(", ");
    const groupByCols = groupDimensions.join(", ");

    // Build WHERE conditions
    const conditions: string[] = [
      "tenant_id = ?",
      "occurred_at >= ?",
      "occurred_at < ?",
    ];
    const params: unknown[] = [
      auth.tenantId,
      BigInt(periodStartMs),
      BigInt(periodEndMs),
    ];

    if (query.user_id) {
      conditions.push("user_id = ?");
      params.push(Number(query.user_id));
    }
    if (query.provider) {
      conditions.push("provider = ?");
      params.push(String(query.provider));
    }

    const where = conditions.join(" AND ");

    try {
      const rows = await duckQuery<Record<string, unknown>>(
        `SELECT ${selectCols},
           SUM(amount_cents)::INTEGER   AS total_amount_cents,
           SUM(fee_cents)::INTEGER      AS total_fee_cents,
           SUM(amount_cents + fee_cents)::INTEGER AS total_cents,
           COUNT(*) AS event_count
         FROM v_cost_fees
         WHERE ${where}
         GROUP BY ${groupByCols}
         ORDER BY total_cents DESC`,
        params
      );

      // Normalize BigInt values to numbers for JSON serialization
      const normalized = rows.map(row => {
        const out: Record<string, unknown> = {};
        for (const [k, v] of Object.entries(row)) {
          out[k] = typeof v === "bigint" ? Number(v) : v;
        }
        return out;
      });

      return {
        data: normalized,
        meta: {
          tenant_id: auth.tenantId,
          period_start_ms: periodStartMs,
          period_end_ms: periodEndMs,
          group_by: groupDimensions,
        },
      };
    } catch (err) {
      console.error("[billing] cost query error:", err);
      set.status = 503;
      return { error: { code: "SERVICE_UNAVAILABLE", message: "Analytics unavailable", category: "internal" } };
    }
  });
}
