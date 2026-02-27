// db/audit.ts - Audit log operations (event-sourced ledger, append-only)
// All billing-relevant events flow through emitAuditEvent().
// This replaces cost_events and usage_records (see WL-061-2B).

import { getDatabase, queryOne, queryMany, execute } from "./helpers";
import { generateULID } from "../ulid";

// =============================================================================
// Types
// =============================================================================

export interface AuditEventInput {
  event_type: string;
  tenant_id: number;
  instance_id?: number;
  allocation_id?: number;
  run_id?: number;
  manifest_id?: number;
  user_id?: number;
  parent_event_id?: number;
  provider?: string;
  spec?: string;
  region?: string;
  source: string; // REQUIRED: emitter identity
  is_cost?: boolean;
  is_usage?: boolean;
  is_attribution?: boolean;
  is_reconciliation?: boolean;
  data: Record<string, unknown>;
  dedupe_key?: string;
  occurred_at: number;
}

export interface AuditEvent {
  id: number;
  event_uuid: string;
  event_type: string;
  tenant_id: number;
  instance_id: number | null;
  allocation_id: number | null;
  run_id: number | null;
  manifest_id: number | null;
  user_id: number | null;
  parent_event_id: number | null;
  provider: string | null;
  spec: string | null;
  region: string | null;
  source: string | null;
  is_cost: number;
  is_usage: number;
  is_attribution: number;
  is_reconciliation: number;
  data: string;
  dedupe_key: string | null;
  occurred_at: number;
  created_at: number;
  event_version: number;
}

// =============================================================================
// emitAuditEvent
// =============================================================================

/**
 * Emit an audit event into the append-only audit_log table.
 * Synchronous (bun:sqlite is sync).
 *
 * Idempotent: if dedupe_key is set and already exists, returns the existing row.
 * Throws if source is missing or no classification boolean is set
 * (unless event_type === "price_observation").
 */
export function emitAuditEvent(input: AuditEventInput): AuditEvent {
  // Validate source
  if (!input.source) {
    throw new Error("emitAuditEvent: source is required");
  }

  // Validate at least one classification flag (unless informational price_observation)
  const isCost = input.is_cost ?? false;
  const isUsage = input.is_usage ?? false;
  const isAttribution = input.is_attribution ?? false;
  const isReconciliation = input.is_reconciliation ?? false;
  const hasClassification = isCost || isUsage || isAttribution || isReconciliation;

  if (!hasClassification && input.event_type !== "price_observation") {
    throw new Error(
      `emitAuditEvent: at least one of is_cost/is_usage/is_attribution/is_reconciliation must be true for event_type '${input.event_type}'`
    );
  }

  const db = getDatabase();
  const eventUuid = generateULID();
  const now = Date.now();
  const dataJson = JSON.stringify(input.data);

  try {
    const stmt = db.prepare(`
      INSERT INTO audit_log (
        event_uuid, event_type, tenant_id,
        instance_id, allocation_id, run_id, manifest_id, user_id, parent_event_id,
        provider, spec, region, source,
        is_cost, is_usage, is_attribution, is_reconciliation,
        data, dedupe_key, occurred_at, created_at, event_version
      ) VALUES (
        ?, ?, ?,
        ?, ?, ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?,
        ?, ?, ?, ?, 1
      )
    `);

    const result = stmt.run(
      eventUuid, input.event_type, input.tenant_id,
      input.instance_id ?? null, input.allocation_id ?? null,
      input.run_id ?? null, input.manifest_id ?? null,
      input.user_id ?? null, input.parent_event_id ?? null,
      input.provider ?? null, input.spec ?? null,
      input.region ?? null, input.source,
      isCost ? 1 : 0, isUsage ? 1 : 0,
      isAttribution ? 1 : 0, isReconciliation ? 1 : 0,
      dataJson, input.dedupe_key ?? null, input.occurred_at, now
    );

    const row = queryOne<AuditEvent>(
      "SELECT * FROM audit_log WHERE id = ?",
      [result.lastInsertRowid as number]
    );

    // Soft dependency: write to audit_inbox if it exists (WL-061-2A)
    _maybeWriteAuditInbox(row!);

    return row!;
  } catch (err: any) {
    // UNIQUE constraint on dedupe_key — idempotent: return existing row
    if (
      input.dedupe_key &&
      (err?.message?.includes("UNIQUE") || err?.message?.includes("unique"))
    ) {
      const existing = queryOne<AuditEvent>(
        "SELECT * FROM audit_log WHERE dedupe_key = ?",
        [input.dedupe_key]
      );
      if (existing) return existing;
    }
    throw err;
  }
}

// =============================================================================
// Inbox Feed (soft dependency on WL-061-2A)
// =============================================================================

let _inboxChecked = false;
let _inboxExists = false;

function _maybeWriteAuditInbox(row: AuditEvent): void {
  // Cache the table-existence check — avoid repeated pragma calls
  if (!_inboxChecked) {
    const result = queryOne<{ name: string }>(
      "SELECT name FROM sqlite_master WHERE type='table' AND name='audit_inbox'"
    );
    _inboxExists = result !== null;
    _inboxChecked = true;
  }

  if (!_inboxExists) return;

  try {
    const payload = JSON.stringify(row);
    execute(
      "INSERT OR IGNORE INTO audit_inbox (event_uuid, dedupe_key, payload) VALUES (?, ?, ?)",
      [row.event_uuid, row.dedupe_key, payload]
    );
  } catch {
    // Non-fatal: inbox write failure does not block event emission
  }
}

// =============================================================================
// Query Helpers
// =============================================================================

export function getAuditEvents(filters: {
  tenant_id?: number;
  event_type?: string;
  instance_id?: number;
  run_id?: number;
  since_ms?: number;
  limit?: number;
}): AuditEvent[] {
  const conditions: string[] = [];
  const params: unknown[] = [];

  if (filters.tenant_id !== undefined) {
    conditions.push("tenant_id = ?");
    params.push(filters.tenant_id);
  }
  if (filters.event_type !== undefined) {
    conditions.push("event_type = ?");
    params.push(filters.event_type);
  }
  if (filters.instance_id !== undefined) {
    conditions.push("instance_id = ?");
    params.push(filters.instance_id);
  }
  if (filters.run_id !== undefined) {
    conditions.push("run_id = ?");
    params.push(filters.run_id);
  }
  if (filters.since_ms !== undefined) {
    conditions.push("occurred_at >= ?");
    params.push(filters.since_ms);
  }

  const where = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";
  const limit = filters.limit !== undefined ? `LIMIT ${filters.limit}` : "";

  return queryMany<AuditEvent>(
    `SELECT * FROM audit_log ${where} ORDER BY occurred_at ASC ${limit}`,
    params
  );
}
