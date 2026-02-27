// billing/settlement.ts — Settlement worker (WL-061-4B §3)
//
// Settles a billing period for a tenant:
//   1. Creates a settlement_batches row (SQLite, status='open')
//   2. Queries v_cost_fees in DuckDB for the period → line items
//   3. Inserts settlement_line_items into DuckDB
//   4. Debits credit wallet if balance > 0
//   5. Updates batch to status='settled' with totals
//
// All amounts are INTEGER CENTS — no floating-point dollars.

import { queryOne, queryMany, execute } from "../material/db";
import { duckQuery, duckExecute } from "../material/duckdb";
import { getCreditBalance, creditDebit } from "../material/db/credits";
import { generateULID } from "../material/ulid";

// =============================================================================
// Types
// =============================================================================

export interface SettlementBatch {
  id: number;
  batch_uuid: string;
  tenant_id: number;
  period_start_ms: number;
  period_end_ms: number;
  status: string;
  total_cents: number | null;
  fee_cents: number | null;
  credit_applied_cents: number;
  stripe_invoice_id: string | null;
  created_at: number;
  settled_at: number | null;
}

export interface SettlementLineItem {
  line_uuid: string;
  batch_uuid: string;
  tenant_id: number;
  description: string;
  amount_cents: number;
  fee_cents: number;
  provider: string | null;
  spec: string | null;
  region: string | null;
  run_id: number | null;
  manifest_id: number | null;
  user_id: number | null;
  period_start_ms: number;
  period_end_ms: number;
  currency: string;
}

// Row shape returned by DuckDB v_cost_fees GROUP BY query
interface CostFeesGroupedRow {
  provider: string | null;
  spec: string | null;
  region: string | null;
  run_id: bigint | number | null;
  manifest_id: bigint | number | null;
  user_id: bigint | number | null;
  amount_cents: bigint | number;
  fee_cents: bigint | number;
  currency: string | null;
}

// =============================================================================
// settlePeriod
// =============================================================================

/**
 * Settle a billing period for a tenant.
 * Idempotency: UNIQUE(tenant_id, period_start_ms, period_end_ms) constraint
 * on settlement_batches prevents double-settlement at the DB level.
 */
export async function settlePeriod(tenantId: number, period: {
  start_ms: number;
  end_ms: number;
}): Promise<SettlementBatch> {
  const batchUuid = generateULID();
  const now = Date.now();

  // Check for orphaned open batches from previous crashes (zero line items, never settled)
  const orphanBatch = queryOne<SettlementBatch>(
    `SELECT * FROM settlement_batches
     WHERE tenant_id = ? AND period_start_ms = ? AND period_end_ms = ? AND status = 'open'`,
    [tenantId, period.start_ms, period.end_ms]
  );
  if (orphanBatch) {
    console.warn(
      `[settlement] Found orphaned open batch ${orphanBatch.batch_uuid} for tenant ${tenantId} ` +
      `period ${period.start_ms}-${period.end_ms} — removing before retry`
    );
    execute("DELETE FROM settlement_batches WHERE id = ?", [orphanBatch.id]);
  }

  // 1. INSERT settlement_batches row (SQLite), status='open'
  execute(
    `INSERT INTO settlement_batches
       (batch_uuid, tenant_id, period_start_ms, period_end_ms, status, credit_applied_cents, created_at)
     VALUES (?, ?, ?, ?, 'open', 0, ?)`,
    [batchUuid, tenantId, period.start_ms, period.end_ms, now]
  );

  const batchRow = queryOne<SettlementBatch>(
    "SELECT * FROM settlement_batches WHERE batch_uuid = ?",
    [batchUuid]
  );
  if (!batchRow) {
    throw new Error(`[settlement] Failed to create batch ${batchUuid}`);
  }

  // 2. Query v_cost_fees in DuckDB for the period, grouped by provider/spec/region/run_id
  // If DuckDB is unavailable or the query fails, propagate the error so the batch
  // stays 'open' and billingCycleTask() can retry. Settling with 0 line items when
  // data should exist is silent data loss — a failed batch that can be retried is
  // strictly better.
  let costRows: CostFeesGroupedRow[];
  try {
    costRows = await duckQuery<CostFeesGroupedRow>(`
    SELECT
      provider,
      spec,
      region,
      run_id,
      manifest_id,
      user_id,
      SUM(amount_cents)::INTEGER AS amount_cents,
      SUM(fee_cents)::INTEGER    AS fee_cents,
      MAX(currency)              AS currency
    FROM v_cost_fees
    WHERE tenant_id = ?
      AND occurred_at >= ?
      AND occurred_at <  ?
    GROUP BY provider, spec, region, run_id, manifest_id, user_id
    `, [tenantId, BigInt(period.start_ms), BigInt(period.end_ms)]);
  } catch (duckErr) {
    // DuckDB query failed — do NOT create a $0 settled batch.
    // Delete the open batch so the unique constraint doesn't block retries.
    execute("DELETE FROM settlement_batches WHERE batch_uuid = ?", [batchUuid]);
    throw new Error(
      `[settlement] DuckDB query failed for batch ${batchUuid}, batch removed for retry: ` +
      (duckErr instanceof Error ? duckErr.message : String(duckErr))
    );
  }

  // 3. Insert settlement_line_items into DuckDB and compute totals
  let totalAmountCents = 0;
  let totalFeeCents = 0;
  const lineItems: SettlementLineItem[] = [];

  for (const row of costRows) {
    const lineUuid = generateULID();
    const amountCents = Number(row.amount_cents);
    const feeCents = Number(row.fee_cents);
    const provider = row.provider ?? null;
    const spec = row.spec ?? null;
    const region = row.region ?? null;
    const runId = row.run_id != null ? Number(row.run_id) : null;
    const manifestId = row.manifest_id != null ? Number(row.manifest_id) : null;
    const userId = row.user_id != null ? Number(row.user_id) : null;
    const currency = row.currency ?? "USD";

    // Build human-readable description
    const parts: string[] = [];
    if (provider) parts.push(provider);
    if (spec) parts.push(spec);
    if (region) parts.push(region);
    if (runId != null) parts.push(`run:${runId}`);
    const description = parts.length > 0
      ? parts.join(" / ")
      : "Compute usage";

    try {
      await duckExecute(`
        INSERT INTO settlement_line_items (
          line_uuid, batch_uuid, tenant_id, description,
          amount_cents, fee_cents,
          provider, spec, region,
          run_id, manifest_id, user_id,
          period_start_ms, period_end_ms, currency
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      `, [
        lineUuid,
        batchUuid,
        tenantId,
        description,
        amountCents,
        feeCents,
        provider,
        spec,
        region,
        runId,
        manifestId,
        userId,
        BigInt(period.start_ms),
        BigInt(period.end_ms),
        currency,
      ]);
    } catch (err) {
      console.warn(`[settlement] Failed to insert line item ${lineUuid}:`, err);
      // Continue — partial settlement is better than full failure
    }

    totalAmountCents += amountCents;
    totalFeeCents += feeCents;

    lineItems.push({
      line_uuid: lineUuid,
      batch_uuid: batchUuid,
      tenant_id: tenantId,
      description,
      amount_cents: amountCents,
      fee_cents: feeCents,
      provider,
      spec,
      region,
      run_id: runId,
      manifest_id: manifestId,
      user_id: userId,
      period_start_ms: period.start_ms,
      period_end_ms: period.end_ms,
      currency,
    });
  }

  // 4. Debit credit wallet if balance > 0
  let creditAppliedCents = 0;
  try {
    const balance = getCreditBalance(tenantId);
    if (balance > 0) {
      const totalDue = totalAmountCents + totalFeeCents;
      const debitAmount = Math.min(balance, totalDue);
      if (debitAmount > 0) {
        creditDebit(tenantId, debitAmount);
        creditAppliedCents = debitAmount;
      }
    }
  } catch (err) {
    console.warn("[settlement] Credit debit failed, continuing without credit application:", err);
  }

  // 5. UPDATE settlement_batches: status='settled', totals, timestamps
  const settledAt = Date.now();
  execute(
    `UPDATE settlement_batches
     SET status = 'settled',
         total_cents = ?,
         fee_cents = ?,
         credit_applied_cents = ?,
         settled_at = ?
     WHERE batch_uuid = ?`,
    [totalAmountCents, totalFeeCents, creditAppliedCents, settledAt, batchUuid]
  );

  // 6. Return the updated batch
  const settled = queryOne<SettlementBatch>(
    "SELECT * FROM settlement_batches WHERE batch_uuid = ?",
    [batchUuid]
  );
  return settled!;
}

// =============================================================================
// Query Helpers
// =============================================================================

/**
 * Get a settlement batch by UUID from SQLite.
 * Returns null if not found.
 */
export function getSettlementBatch(batchUuid: string): SettlementBatch | null {
  return queryOne<SettlementBatch>(
    "SELECT * FROM settlement_batches WHERE batch_uuid = ?",
    [batchUuid]
  );
}

/**
 * List all settlement batches for a tenant, ordered by creation time descending.
 */
export function listSettlementBatches(tenantId: number): SettlementBatch[] {
  return queryMany<SettlementBatch>(
    "SELECT * FROM settlement_batches WHERE tenant_id = ? ORDER BY created_at DESC",
    [tenantId]
  );
}

/**
 * Get settlement line items for a batch from DuckDB.
 */
export async function getSettlementLineItems(batchUuid: string): Promise<SettlementLineItem[]> {
  const rows = await duckQuery<{
    line_uuid: string;
    batch_uuid: string;
    tenant_id: number;
    description: string;
    amount_cents: number | bigint;
    fee_cents: number | bigint;
    provider: string | null;
    spec: string | null;
    region: string | null;
    run_id: number | bigint | null;
    manifest_id: number | bigint | null;
    user_id: number | bigint | null;
    period_start_ms: bigint | number;
    period_end_ms: bigint | number;
    currency: string;
  }>(
    "SELECT * FROM settlement_line_items WHERE batch_uuid = ? ORDER BY line_uuid",
    [batchUuid]
  );

  return rows.map(r => ({
    line_uuid: r.line_uuid,
    batch_uuid: r.batch_uuid,
    tenant_id: Number(r.tenant_id),
    description: r.description,
    amount_cents: Number(r.amount_cents),
    fee_cents: Number(r.fee_cents),
    provider: r.provider,
    spec: r.spec,
    region: r.region,
    run_id: r.run_id != null ? Number(r.run_id) : null,
    manifest_id: r.manifest_id != null ? Number(r.manifest_id) : null,
    user_id: r.user_id != null ? Number(r.user_id) : null,
    period_start_ms: Number(r.period_start_ms),
    period_end_ms: Number(r.period_end_ms),
    currency: r.currency,
  }));
}
