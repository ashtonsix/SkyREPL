// material/duckdb.ts — DuckDB OLAP engine module
//
// Mirrors the pattern of db/init.ts (SQLite singleton) for DuckDB.
// Provides: init, close, query helpers, audit inbox flush, OLAP schema DDL.
//
// All DuckDB operations are async (unlike bun:sqlite which is synchronous).
// The SQLite helpers (queryOne, queryMany, execute) remain sync.

import { queryMany, execute } from "./db/helpers";

// =============================================================================
// Module-level Singleton
// =============================================================================

let instance: any = null;
let defaultConn: any = null;

// =============================================================================
// OLAP Schema DDL
// =============================================================================

const AUDIT_LOG_DDL = `
  CREATE TABLE IF NOT EXISTS audit_log (
    id              BIGINT,
    event_uuid      VARCHAR NOT NULL,
    event_type      VARCHAR NOT NULL,
    tenant_id       INTEGER NOT NULL,
    instance_id     INTEGER,
    allocation_id   INTEGER,
    run_id          INTEGER,
    manifest_id     INTEGER,
    user_id         INTEGER,
    parent_event_id INTEGER,
    provider        VARCHAR,
    spec            VARCHAR,
    region          VARCHAR,
    source          VARCHAR,
    is_cost         BOOLEAN NOT NULL DEFAULT false,
    is_usage        BOOLEAN NOT NULL DEFAULT false,
    is_attribution  BOOLEAN NOT NULL DEFAULT false,
    is_reconciliation BOOLEAN NOT NULL DEFAULT false,
    data            JSON NOT NULL,
    dedupe_key      VARCHAR,
    occurred_at     BIGINT NOT NULL,
    created_at      BIGINT NOT NULL,
    event_version   INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (event_uuid)
  )
`;

const SETTLEMENT_LINE_ITEMS_DDL = `
  CREATE TABLE IF NOT EXISTS settlement_line_items (
    line_uuid       VARCHAR NOT NULL PRIMARY KEY,
    batch_uuid      VARCHAR NOT NULL,
    tenant_id       INTEGER NOT NULL,
    description     VARCHAR NOT NULL,
    amount_cents    INTEGER NOT NULL,
    fee_cents       INTEGER NOT NULL DEFAULT 0,
    provider        VARCHAR,
    spec            VARCHAR,
    region          VARCHAR,
    run_id          INTEGER,
    manifest_id     INTEGER,
    user_id         INTEGER,
    period_start_ms BIGINT NOT NULL,
    period_end_ms   BIGINT NOT NULL,
    currency        VARCHAR NOT NULL DEFAULT 'USD'
  )
`;

const VIEW_CHAIN_DDL = `
  -- v_cost_raw: cost events with standard data shape extracted from JSON.
  -- Uses json_extract_string() for VARCHAR fields (returns unquoted value),
  -- and json_extract()::TYPE for numeric/bigint fields.
  -- amount_cents_raw: pre-computed amount from enriched metering_stop events (C-1).
  --   When present, v_cost_priced prefers it over the computed window×rate formula.
  -- fee_cents_raw: pre-computed orchestration fee from enriched metering_stop events (C-1).
  --   When present, v_cost_fees prefers it over the computed percentage formula.
  -- hourly_rate_cents_raw: hourly rate in cents from enriched events.
  CREATE OR REPLACE VIEW v_cost_raw AS
  SELECT
    event_uuid, event_type, tenant_id,
    instance_id, allocation_id, run_id, manifest_id, user_id,
    provider, spec, region, source,
    is_reconciliation,
    occurred_at, created_at, event_version,
    json_extract(data, '$.quantity')::DOUBLE AS quantity,
    json_extract_string(data, '$.unit') AS unit,
    json_extract(data, '$.rate_per_hour')::DOUBLE AS rate_per_hour,
    json_extract_string(data, '$.currency') AS currency,
    json_extract(data, '$.metering_window_start_ms')::BIGINT AS window_start_ms,
    json_extract(data, '$.metering_window_end_ms')::BIGINT AS window_end_ms,
    json_extract_string(data, '$.provider_resource_id') AS provider_resource_id,
    json_extract(data, '$.amount_cents')::INTEGER AS amount_cents_raw,
    json_extract(data, '$.fee_cents')::INTEGER AS fee_cents_raw,
    json_extract(data, '$.hourly_rate_cents')::INTEGER AS hourly_rate_cents_raw,
    data
  FROM audit_log
  WHERE is_cost = true;

  -- v_cost_attributed: apply attribution weights (WL-061-3B §1)
  -- Time-bounded attribution: each cost event is attributed to the user whose
  -- attribution window [attribution_start, attribution_end) contains the cost
  -- event's occurred_at. Uses a lateral subquery to find the most recent
  -- attribution_start at or before the cost event on the same instance, then
  -- verifies the window is still open (no attribution_end before the cost event).
  -- If the window is closed or no attribution exists, defaults to weight 1.0
  -- with NULL user. JOIN key is instance_id (not allocation_id) because
  -- metering_start events may be emitted before allocation creation (M-3).
  CREATE OR REPLACE VIEW v_cost_attributed AS
  SELECT
    c.*,
    COALESCE(
      CASE WHEN a.attr_allocation_id IS NOT NULL
           AND NOT EXISTS (
             SELECT 1 FROM audit_log e
             WHERE e.event_type = 'attribution_end'
               AND e.allocation_id = a.attr_allocation_id
               AND e.occurred_at <= c.occurred_at
           )
      THEN a.weight END,
      1.0
    ) AS attribution_weight,
    CASE WHEN a.attr_allocation_id IS NOT NULL
         AND NOT EXISTS (
           SELECT 1 FROM audit_log e
           WHERE e.event_type = 'attribution_end'
             AND e.allocation_id = a.attr_allocation_id
             AND e.occurred_at <= c.occurred_at
         )
    THEN a.attributed_user_id END AS attributed_user_id
  FROM v_cost_raw c
  LEFT JOIN LATERAL (
    SELECT
      s.user_id AS attributed_user_id,
      json_extract(s.data, '$.attribution_weight')::DOUBLE AS weight,
      s.allocation_id AS attr_allocation_id
    FROM audit_log s
    WHERE s.event_type = 'attribution_start'
      AND s.instance_id = c.instance_id
      AND s.occurred_at <= c.occurred_at
    ORDER BY s.occurred_at DESC
    LIMIT 1
  ) a ON true;

  -- v_cost_priced: apply provider pricing.
  --
  -- C-1 fix: When a metering_stop event carries pre-computed amount_cents in data{}
  -- (enriched by the TS pricing engine at emit time), COALESCE uses it directly.
  -- This bypasses the ASOF JOIN zero-cost cascade and correctly applies per-provider
  -- billing minimums (AWS 60s, DO $0.01 floor, etc.) that the SQL formula cannot model.
  --
  -- The ASOF JOIN fallback path remains for metering_start events (open windows) and
  -- legacy stop events that lack pre-computed amount_cents. It produces 0 when no
  -- price_observation events exist — acceptable for the open-window estimate use case.
  --
  -- NOTE: The ASOF JOIN sentinel UNION ALL re-scans v_cost_attributed per query.
  -- For large tables this is O(n²). A static sentinel row would be cheaper — deferred.
  CREATE OR REPLACE VIEW v_cost_priced AS
  SELECT
    c.*,
    COALESCE(p.rate_per_hour, c.rate_per_hour) AS effective_rate,
    COALESCE(
      -- C-1: prefer pre-computed amount_cents from enriched metering_stop events
      -- (amount_cents_raw is extracted from data.amount_cents in v_cost_raw)
      c.amount_cents_raw,
      -- Fallback: ASOF JOIN formula (produces 0 when price_observations absent)
      CAST(ROUND(
        COALESCE(c.window_end_ms - c.window_start_ms, 0) / 3600000.0
        * COALESCE(p.rate_per_hour, c.rate_per_hour, 0)
        * 100
        * c.attribution_weight
      ) AS INTEGER)
    ) AS amount_cents
  FROM v_cost_attributed c
  ASOF JOIN (
    SELECT COALESCE(provider, '') AS provider, COALESCE(spec, '') AS spec,
           COALESCE(region, '') AS region, occurred_at AS at_ms,
           json_extract(data, '$.rate_per_hour')::DOUBLE AS rate_per_hour
    FROM audit_log
    WHERE event_type = 'price_observation'
    UNION ALL
    SELECT COALESCE(c2.provider, ''), COALESCE(c2.spec, ''), COALESCE(c2.region, ''),
           0 AS at_ms, NULL::DOUBLE AS rate_per_hour
    FROM v_cost_attributed c2
  ) p ON COALESCE(c.provider, '') = p.provider
      AND COALESCE(c.spec, '') = p.spec
      AND COALESCE(c.region, '') = p.region
      AND c.occurred_at >= p.at_ms;

  -- v_cost_fees: apply orchestration fee (H-1).
  -- C-1 fix: prefer pre-computed fee_cents from enriched metering_stop events.
  -- Default fallback: 1% BYO rate. Replaced with tenant-aware version after SQLite ATTACH.
  -- See V_COST_FEES_WITH_TENANTS_DDL below — applied when sqlitePath is available.
  CREATE OR REPLACE VIEW v_cost_fees AS
  SELECT *,
    COALESCE(
      fee_cents_raw,
      CAST(ROUND(amount_cents * 0.01) AS INTEGER)
    ) AS fee_cents
  FROM v_cost_priced;

  -- v_invoice_lines: settlement line items with total (WL-061-4B)
  -- NOTE(WL-061-5): v_invoice_lines reads from settlement_line_items (DuckDB-native),
  -- NOT directly from v_cost_fees. This means invoice lines are only available AFTER
  -- settlePeriod() has been called. The view chain v_cost_raw → ... → v_cost_fees
  -- is the analysis path; v_invoice_lines is the settlement materialization path.
  -- These are two separate query paths, not a single chain. The spec (WL-061-1 §4D)
  -- implies a single chain but the implementation splits at settlement materialization.
  -- This is the correct design — just not what the spec diagram suggests.
  CREATE OR REPLACE VIEW v_invoice_lines AS
  SELECT
    line_uuid,
    batch_uuid,
    tenant_id,
    description,
    amount_cents,
    fee_cents,
    amount_cents + fee_cents AS total_cents,
    provider,
    spec,
    region,
    run_id,
    manifest_id,
    user_id,
    period_start_ms,
    period_end_ms,
    currency
  FROM settlement_line_items;

  -- Persona views (WL-061-3B §7)
  CREATE OR REPLACE VIEW v_persona_researcher AS
  SELECT run_id, provider, spec, region,
    SUM(amount_cents) AS total_cents,
    MIN(occurred_at) AS first_event,
    MAX(occurred_at) AS last_event,
    COUNT(*) AS event_count
  FROM v_cost_fees
  WHERE run_id IS NOT NULL
  GROUP BY run_id, provider, spec, region
  ORDER BY last_event DESC;

  CREATE OR REPLACE VIEW v_persona_lab_pi AS
  SELECT attributed_user_id,
    (occurred_at / 604800000) AS week_bucket,
    SUM(amount_cents) AS total_cents,
    COUNT(DISTINCT run_id) AS run_count
  FROM v_cost_fees
  GROUP BY attributed_user_id, week_bucket
  ORDER BY week_bucket DESC;

  CREATE OR REPLACE VIEW v_persona_team_lead AS
  SELECT tenant_id,
    SUM(amount_cents) AS total_cents,
    SUM(fee_cents) AS total_fee_cents,
    COUNT(DISTINCT run_id) AS run_count,
    COUNT(DISTINCT attributed_user_id) AS user_count
  FROM v_cost_fees
  GROUP BY tenant_id;

  CREATE OR REPLACE VIEW v_persona_cto AS
  SELECT provider, spec,
    SUM(amount_cents) AS total_cents,
    COUNT(*) AS event_count,
    AVG(amount_cents) AS avg_cents
  FROM v_cost_fees
  GROUP BY provider, spec
  ORDER BY total_cents DESC;

  CREATE OR REPLACE VIEW v_persona_cfo AS
  SELECT provider,
    SUM(amount_cents) AS provider_cost_cents,
    SUM(fee_cents) AS orchestration_fee_cents,
    SUM(amount_cents + fee_cents) AS total_cents,
    COUNT(DISTINCT tenant_id) AS tenant_count
  FROM v_cost_fees
  GROUP BY provider
  ORDER BY total_cents DESC;

  CREATE OR REPLACE VIEW v_persona_accountant AS
  SELECT event_uuid, event_type, tenant_id, provider, spec, region,
    amount_cents, fee_cents,
    occurred_at, created_at
  FROM v_cost_fees
  ORDER BY occurred_at DESC;
`;

// =============================================================================
// Tenant-Aware Fee View DDL (H-1)
// =============================================================================

// Applied AFTER SQLite ATTACH succeeds. Recreates v_cost_fees with a LEFT JOIN
// to sqlite.tenants so managed tenants (7%) are distinguished from BYO (1%).
// The persona views that reference v_cost_fees are also recreated to pick up the
// corrected fee_cents values.
const V_COST_FEES_WITH_TENANTS_DDL = `
  -- H-1 fix: tenant-aware orchestration fee rate.
  -- C-1 fix: prefer pre-computed fee_cents from enriched metering_stop events.
  -- BYO (account_type = 'byo' or absent): 1%. Managed: 7%.
  CREATE OR REPLACE VIEW v_cost_fees AS
  SELECT
    p.*,
    COALESCE(
      p.fee_cents_raw,
      CAST(ROUND(
        p.amount_cents * CASE
          WHEN t.account_type = 'managed' THEN 0.07
          ELSE 0.01
        END
      ) AS INTEGER)
    ) AS fee_cents
  FROM v_cost_priced p
  LEFT JOIN sqlite.tenants t ON p.tenant_id = t.id;
`;

// =============================================================================
// Init / Close
// =============================================================================

/**
 * Initialize DuckDB engine at the given path, attaching SQLite for cross queries.
 * Runs OLAP schema DDL (audit_log table, view chain).
 * Stores result in module-level singleton.
 * Returns the DuckDB instance.
 */
export async function initDuckDB(path: string, sqlitePath: string): Promise<any> {
  const { DuckDBInstance } = await import("@duckdb/node-api");

  // Use local variables during init so the module singleton is only set
  // after full initialization (DDL complete). This prevents tests from
  // using getDuckDB() before the schema is ready.
  const localInstance = await DuckDBInstance.create(path);
  const localConn = await localInstance.connect();

  // Run OLAP schema DDL
  await localConn.run(AUDIT_LOG_DDL);
  // UNIQUE index on dedupe_key: prevents phantom duplicate cost rows when a bug produces
  // the same dedupe_key with a different event_uuid (which PRIMARY KEY alone cannot block).
  // DuckDB UNIQUE indexes follow SQL standard: multiple NULL values are permitted (NULLs
  // are considered distinct), so rows without a dedupe_key are unaffected.
  await localConn.run(
    `CREATE UNIQUE INDEX IF NOT EXISTS audit_log_dedupe_key_uidx ON audit_log (dedupe_key)`
  );
  await localConn.run(SETTLEMENT_LINE_ITEMS_DDL);
  await localConn.run(VIEW_CHAIN_DDL);

  // ATTACH SQLite for cross-database queries (skip for in-memory mode).
  // After a successful ATTACH, recreate v_cost_fees with the tenant-aware fee rate
  // (H-1): managed tenants pay 7%, BYO tenants pay 1%.
  if (sqlitePath && sqlitePath !== ":memory:") {
    try {
      await localConn.run(
        `ATTACH '${sqlitePath}' AS sqlite (TYPE sqlite, READ_ONLY)`
      );
      // ATTACH succeeded — upgrade v_cost_fees to use tenant-specific fee rates
      try {
        await localConn.run(V_COST_FEES_WITH_TENANTS_DDL);
      } catch (feeErr) {
        // Tenant-aware view failed (e.g., tenants table missing in attached DB) — keep 1% default
        console.warn("[duckdb] Failed to create tenant-aware v_cost_fees (using 1% default):", feeErr);
      }
    } catch (err) {
      // SQLite might be locked or unavailable — log and continue with 1% default
      console.warn("[duckdb] Failed to ATTACH SQLite (cross-db queries unavailable):", err);
    }
  }

  // Only assign to module singletons after full init
  instance = localInstance;
  defaultConn = localConn;

  return instance;
}

/**
 * Returns the DuckDB instance singleton, or null if not initialized.
 */
export function getDuckDB(): any | null {
  return instance;
}

/**
 * Returns the default DuckDB connection (lazy-creates if not yet open).
 * Main query entry point.
 */
export async function getDuckDBConnection(): Promise<any> {
  if (!instance) {
    throw new Error("[duckdb] DuckDB not initialized — call initDuckDB() first");
  }
  if (!defaultConn) {
    defaultConn = await instance.connect();
  }
  return defaultConn;
}

/**
 * Drain remaining inbox rows, close connection and instance.
 * Nulls the singleton.
 */
export async function closeDuckDB(): Promise<void> {
  if (!instance) return;

  // Drain remaining inbox rows before close.
  // flushInboxToDuckDB() calls getDuckDBConnection() which uses defaultConn (already set
  // after initDuckDB). The lazy-create branch in getDuckDBConnection() won't fire here.
  try {
    await flushInboxToDuckDB();
  } catch (err) {
    console.warn("[duckdb] flushInboxToDuckDB failed during shutdown:", err);
    // Continue — inbox rows are still in SQLite and will be retried on next startup
  }

  if (defaultConn) {
    try { defaultConn.closeSync(); } catch { /* ignore */ }
    defaultConn = null;
  }

  try { instance.closeSync(); } catch { /* ignore */ }
  instance = null;
}

// =============================================================================
// DuckDB Query Helpers (parallel to db/helpers.ts, but async)
// =============================================================================

/**
 * Bind parameters to a prepared statement using bindValue().
 * DuckDB's @duckdb/node-api uses explicit per-index binding (not variadic spread).
 *
 * NOTE: DuckDB's node-api treats JavaScript `number` as INT32. Large integer values
 * (e.g. Unix timestamps in milliseconds) MUST be passed as BigInt to avoid truncation.
 * This function automatically promotes safe integers > INT32_MAX to BigInt.
 */
function bindParams(stmt: any, params: unknown[]): void {
  const INT32_MAX = 2_147_483_647;
  for (let i = 0; i < params.length; i++) {
    let val = params[i];
    // Null values must use bindNull() — bindValue(null) throws in DuckDB node-api
    if (val === null || val === undefined) {
      stmt.bindNull(i + 1);
      continue;
    }
    // Promote large safe integers to BigInt to avoid INT32 truncation
    if (typeof val === "number" && Number.isInteger(val) && (val > INT32_MAX || val < -INT32_MAX)) {
      val = BigInt(val);
    }
    stmt.bindValue(i + 1, val);
  }
}

/**
 * Run a SQL query and return all rows as typed objects.
 */
export async function duckQuery<T>(sql: string, params?: unknown[]): Promise<T[]> {
  const conn = await getDuckDBConnection();
  let result: any;
  if (params && params.length > 0) {
    const prepared = await conn.prepare(sql);
    bindParams(prepared, params);
    result = await prepared.runAndReadAll();
  } else {
    result = await conn.runAndReadAll(sql);
  }
  return result.getRowObjects() as T[];
}

/**
 * Run a SQL query and return the first row, or null if no rows.
 */
export async function duckQueryOne<T>(sql: string, params?: unknown[]): Promise<T | null> {
  const rows = await duckQuery<T>(sql, params);
  return rows.length > 0 ? rows[0] : null;
}

/**
 * Execute a SQL statement (no return value).
 */
export async function duckExecute(sql: string, params?: unknown[]): Promise<void> {
  const conn = await getDuckDBConnection();
  if (params && params.length > 0) {
    const prepared = await conn.prepare(sql);
    bindParams(prepared, params);
    await prepared.run();
  } else {
    await conn.run(sql);
  }
}

// =============================================================================
// Audit Inbox → DuckDB Flush
// =============================================================================

interface InboxRow {
  id: number;
  payload: string;
}

/**
 * Flush unflushed audit_inbox rows from SQLite into DuckDB audit_log.
 * Returns the number of rows flushed.
 *
 * Error handling: if DuckDB INSERT fails, inbox rows remain unflushed
 * and will be retried on next tick. SQLite audit_log is the durable source.
 */
export async function flushInboxToDuckDB(limit?: number): Promise<{ flushed: number }> {
  if (!instance) {
    return { flushed: 0 };
  }

  const batchLimit = limit ?? 1000;

  // 1. Read unflushed rows from SQLite (synchronous)
  const rows = queryMany<InboxRow>(
    `SELECT id, payload FROM audit_inbox WHERE flushed = 0
     ORDER BY created_at LIMIT ?`,
    [batchLimit]
  );

  if (rows.length === 0) {
    // Run cleanup even if nothing to flush
    cleanupFlushedRows();
    return { flushed: 0 };
  }

  // 2. Parse payloads and batch INSERT into DuckDB audit_log
  const conn = await getDuckDBConnection();
  const ids: number[] = [];

  for (const row of rows) {
    let payload: Record<string, any>;
    try {
      payload = JSON.parse(row.payload);
    } catch (err) {
      console.warn(`[duckdb] Failed to parse audit_inbox payload id=${row.id}:`, err);
      continue;
    }

    try {
      // ON CONFLICT DO NOTHING handles duplicate event_uuid gracefully.
      // DuckDB does not support SQLite's "INSERT OR IGNORE" syntax.
      const stmt = await conn.prepare(`
        INSERT INTO audit_log (
          id, event_uuid, event_type, tenant_id,
          instance_id, allocation_id, run_id, manifest_id, user_id, parent_event_id,
          provider, spec, region, source,
          is_cost, is_usage, is_attribution, is_reconciliation,
          data, dedupe_key, occurred_at, created_at, event_version
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (event_uuid) DO NOTHING
      `);
      const insertParams = [
        payload.id ?? null,
        payload.event_uuid ?? "",
        payload.event_type ?? "",
        payload.tenant_id ?? 1,
        payload.instance_id ?? null,
        payload.allocation_id ?? null,
        payload.run_id ?? null,
        payload.manifest_id ?? null,
        payload.user_id ?? null,
        payload.parent_event_id ?? null,
        payload.provider ?? null,
        payload.spec ?? null,
        payload.region ?? null,
        payload.source ?? null,
        payload.is_cost ?? false,
        payload.is_usage ?? false,
        payload.is_attribution ?? false,
        payload.is_reconciliation ?? false,
        typeof payload.data === "string" ? payload.data : JSON.stringify(payload.data ?? {}),
        payload.dedupe_key ?? null,
        // Timestamps must be BigInt — JS `number` is bound as INT32 by DuckDB node-api
        BigInt(payload.occurred_at ?? Date.now()),
        BigInt(payload.created_at ?? Date.now()),
        payload.event_version ?? 1,
      ];
      bindParams(stmt, insertParams);
      await stmt.run();
      ids.push(row.id);
    } catch (err) {
      console.error(`[duckdb] Failed to INSERT audit_inbox row id=${row.id} into DuckDB:`, err);
      // Don't add to ids — row stays unflushed for retry
    }
  }

  if (ids.length > 0) {
    // 3. Mark rows as flushed in SQLite (synchronous)
    const placeholders = ids.map(() => "?").join(", ");
    execute(
      `UPDATE audit_inbox SET flushed = 1 WHERE id IN (${placeholders})`,
      ids
    );
  }

  // 4. Cleanup old flushed rows
  cleanupFlushedRows();

  return { flushed: ids.length };
}

/**
 * Delete flushed audit_inbox rows older than 24 hours.
 */
function cleanupFlushedRows(): void {
  try {
    execute(
      `DELETE FROM audit_inbox WHERE flushed = 1
       AND created_at < (unixepoch() * 1000 - 86400000)`,
      []
    );
  } catch {
    // Best-effort cleanup
  }
}
