// duckdb.test.ts — DuckDB module tests
//
// Tests: catalog integration, inbox flush, shutdown flush, view chain,
// background timer creation.
//
// Pattern: all describe blocks that need DuckDB use setupTest() (no duckdb opt-in)
// and explicitly await initDuckDB(":memory:", ":memory:") in beforeEach.
// This ensures the OLAP schema is fully initialized before any test body runs.

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest } from "../../../tests/harness";
import { createCatalog, createKVCache } from "@skyrepl/scaffold/src/catalog";
import { Database } from "bun:sqlite";
import {
  initDuckDB,
  closeDuckDB,
  getDuckDB,
  duckQuery,
  duckQueryOne,
  duckExecute,
  flushInboxToDuckDB,
} from "./duckdb";
import { execute, queryMany, queryOne } from "./db/helpers";

// =============================================================================
// Catalog Integration Tests
// =============================================================================

describe("catalog integration", () => {
  let cleanup: () => Promise<void>;

  beforeEach(async () => {
    cleanup = setupTest();
    await initDuckDB(":memory:", ":memory:");
  });

  afterEach(async () => {
    await closeDuckDB();
    await cleanup();
  });

  test("getDuckDB() returns instance after init", () => {
    const duck = getDuckDB();
    expect(duck).not.toBeNull();
  });

  test("getDuckDB() via catalog", () => {
    const sqlite = new Database(":memory:");
    sqlite.exec("PRAGMA journal_mode = WAL");
    const kv = createKVCache();
    const duck = getDuckDB();

    const catalog = createCatalog(
      { services: ["sqlite", "duckdb", "kv"], sqlitePath: ":memory:", ports: { control: 3000, orbital: 3002, proxy: 3001 }, version: "test" },
      sqlite,
      kv,
      duck
    );

    expect(catalog.getDuckDB()).not.toBeNull();
    expect(catalog.getDuckDB()).toBe(duck);

    sqlite.close();
  });

  test("getDuckDB() returns null when not configured", () => {
    const sqlite = new Database(":memory:");
    const kv = createKVCache();

    // Create catalog WITHOUT duckdb param
    const catalog = createCatalog(
      { services: ["sqlite", "kv"], sqlitePath: ":memory:", ports: { control: 3000, orbital: 3002, proxy: 3001 }, version: "test" },
      sqlite,
      kv
      // no duckdb param → null
    );

    expect(catalog.getDuckDB()).toBeNull();
    sqlite.close();
  });

  test("getSQLite() still works when DuckDB is configured", () => {
    const sqlite = new Database(":memory:");
    const kv = createKVCache();
    const duck = getDuckDB();

    const catalog = createCatalog(
      { services: ["sqlite", "duckdb", "kv"], sqlitePath: ":memory:", ports: { control: 3000, orbital: 3002, proxy: 3001 }, version: "test" },
      sqlite,
      kv,
      duck
    );

    const db = catalog.getSQLite();
    expect(db).toBe(sqlite);

    // Verify SQLite still works
    const result = db.prepare("SELECT 1 AS val").get() as { val: number };
    expect(result.val).toBe(1);

    sqlite.close();
  });
});

// =============================================================================
// Inbox Flush Tests
// =============================================================================

describe("inbox flush", () => {
  let cleanup: () => Promise<void>;

  beforeEach(async () => {
    cleanup = setupTest();
    await initDuckDB(":memory:", ":memory:");
  });

  afterEach(async () => {
    await closeDuckDB();
    await cleanup();
  });

  function insertInboxRow(uuid: string, isCost = false, createdAtMs?: number): void {
    const now = Date.now();
    const payload = {
      id: Math.floor(Math.random() * 1_000_000),
      event_uuid: uuid,
      event_type: "test.event",
      tenant_id: 1,
      instance_id: null,
      allocation_id: null,
      run_id: null,
      manifest_id: null,
      user_id: null,
      parent_event_id: null,
      provider: "orbstack",
      spec: "4vcpu-8gb",
      region: "local",
      source: "test",
      is_cost: isCost,
      is_usage: false,
      is_attribution: false,
      is_reconciliation: false,
      data: { test: true },
      dedupe_key: null,
      occurred_at: now,
      created_at: createdAtMs ?? now,
      event_version: 1,
    };

    if (createdAtMs !== undefined) {
      execute(
        `INSERT INTO audit_inbox (event_uuid, payload, flushed, created_at) VALUES (?, ?, 0, ?)`,
        [uuid, JSON.stringify(payload), createdAtMs]
      );
    } else {
      execute(
        `INSERT INTO audit_inbox (event_uuid, payload, flushed) VALUES (?, ?, 0)`,
        [uuid, JSON.stringify(payload)]
      );
    }
  }

  test("flushes 3 inbox rows to DuckDB audit_log", async () => {
    insertInboxRow("evt-001");
    insertInboxRow("evt-002");
    insertInboxRow("evt-003");

    const result = await flushInboxToDuckDB();
    expect(result.flushed).toBe(3);

    const rows = await duckQuery("SELECT event_uuid FROM audit_log ORDER BY event_uuid");
    expect(rows.length).toBe(3);
  });

  test("inbox rows marked flushed=1 after flush", async () => {
    insertInboxRow("evt-flush-1");
    insertInboxRow("evt-flush-2");

    await flushInboxToDuckDB();

    const unflushed = queryMany<{ id: number }>(
      "SELECT id FROM audit_inbox WHERE flushed = 0",
      []
    );
    expect(unflushed.length).toBe(0);

    const flushed = queryMany<{ id: number }>(
      "SELECT id FROM audit_inbox WHERE flushed = 1",
      []
    );
    expect(flushed.length).toBe(2);
  });

  test("second flush returns { flushed: 0 } when nothing unflushed", async () => {
    insertInboxRow("evt-second-1");

    const first = await flushInboxToDuckDB();
    expect(first.flushed).toBe(1);

    const second = await flushInboxToDuckDB();
    expect(second.flushed).toBe(0);
  });

  test("duplicate event_uuid: first flush succeeds, second flush finds nothing unflushed", async () => {
    insertInboxRow("evt-dup");

    const first = await flushInboxToDuckDB();
    expect(first.flushed).toBe(1);

    // DuckDB has exactly 1 row
    const rows = await duckQuery("SELECT event_uuid FROM audit_log WHERE event_uuid = 'evt-dup'");
    expect(rows.length).toBe(1);

    // Second flush: nothing unflushed
    const second = await flushInboxToDuckDB();
    expect(second.flushed).toBe(0);
  });

  test("returns { flushed: 0 } when DuckDB not initialized (singleton null)", async () => {
    // Flush any existing rows while DuckDB is up
    await flushInboxToDuckDB();

    // Close DuckDB — singleton becomes null
    await closeDuckDB();
    expect(getDuckDB()).toBeNull();

    // Insert rows while DuckDB is down
    insertInboxRow("evt-unavailable-1");
    insertInboxRow("evt-unavailable-2");

    // flushInboxToDuckDB guards on null singleton
    const result = await flushInboxToDuckDB();
    expect(result.flushed).toBe(0);

    // Inbox rows remain unflushed
    const unflushed = queryMany<{ id: number }>(
      "SELECT id FROM audit_inbox WHERE flushed = 0",
      []
    );
    expect(unflushed.length).toBe(2);

    // Re-init for afterEach cleanup
    await initDuckDB(":memory:", ":memory:");
  });

  test("cleanup: old flushed rows (>24h) are deleted during flush", async () => {
    const oldMs = Date.now() - 25 * 60 * 60 * 1000; // 25h ago

    // Insert an old already-flushed row directly with old timestamp
    execute(
      `INSERT INTO audit_inbox (event_uuid, payload, flushed, created_at) VALUES (?, ?, 1, ?)`,
      [
        "evt-old-flushed",
        JSON.stringify({
          event_uuid: "evt-old-flushed", event_type: "test", tenant_id: 1,
          data: {}, occurred_at: oldMs, created_at: oldMs, event_version: 1,
          is_cost: false, is_usage: false, is_attribution: false, is_reconciliation: false,
        }),
        oldMs,
      ]
    );

    // Also add a recent unflushed row to trigger the cleanup code path
    insertInboxRow("evt-recent-cleanup");
    await flushInboxToDuckDB(); // marks evt-recent-cleanup flushed + runs cleanup

    // Old flushed row should be gone
    const old = queryOne<{ id: number }>(
      "SELECT id FROM audit_inbox WHERE event_uuid = 'evt-old-flushed'",
      []
    );
    expect(old).toBeNull();

    // Recent flushed row should still exist
    const recent = queryOne<{ id: number }>(
      "SELECT id FROM audit_inbox WHERE event_uuid = 'evt-recent-cleanup'",
      []
    );
    expect(recent).not.toBeNull();
  });
});

// =============================================================================
// Shutdown Flush Test
// =============================================================================

describe("shutdown flush", () => {
  let cleanup: () => Promise<void>;

  beforeEach(async () => {
    cleanup = setupTest();
    await initDuckDB(":memory:", ":memory:");
  });

  afterEach(async () => {
    // DuckDB closed by test; re-init a stub so closeDuckDB in afterEach is a no-op
    if (!getDuckDB()) {
      await initDuckDB(":memory:", ":memory:");
    }
    await closeDuckDB();
    await cleanup();
  });

  test("closeDuckDB() flushes remaining inbox rows before close", async () => {
    // Insert 5 unflushed rows
    for (let i = 1; i <= 5; i++) {
      const uuid = `evt-shutdown-${i}`;
      const payload = {
        event_uuid: uuid, event_type: "test.shutdown", tenant_id: 1,
        data: {}, occurred_at: Date.now(), created_at: Date.now(),
        event_version: 1, is_cost: false, is_usage: false,
        is_attribution: false, is_reconciliation: false,
      };
      execute(
        `INSERT INTO audit_inbox (event_uuid, payload, flushed) VALUES (?, ?, 0)`,
        [uuid, JSON.stringify(payload)]
      );
    }

    // Close DuckDB — should flush all remaining rows first
    await closeDuckDB();

    // All 5 rows should be marked flushed=1
    const unflushed = queryMany<{ id: number }>(
      "SELECT id FROM audit_inbox WHERE flushed = 0",
      []
    );
    expect(unflushed.length).toBe(0);

    const flushed = queryMany<{ id: number }>(
      "SELECT id FROM audit_inbox WHERE flushed = 1",
      []
    );
    expect(flushed.length).toBe(5);
  });
});

// =============================================================================
// View Chain Smoke Tests
// =============================================================================

describe("view chain smoke tests", () => {
  let cleanup: () => Promise<void>;

  beforeEach(async () => {
    cleanup = setupTest();
    await initDuckDB(":memory:", ":memory:");
  });

  afterEach(async () => {
    await closeDuckDB();
    await cleanup();
  });

  async function insertAuditLogRow(uuid: string, isCost: boolean): Promise<void> {
    await duckExecute(`
      INSERT INTO audit_log (
        id, event_uuid, event_type, tenant_id,
        is_cost, is_usage, is_attribution, is_reconciliation,
        data, occurred_at, created_at, event_version
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `,
      [
        Math.floor(Math.random() * 1_000_000),
        uuid,
        isCost ? "instance.start" : "workflow.created",
        1,
        isCost,
        false,
        false,
        false,
        '{"key":"val"}',
        Date.now(),
        Date.now(),
        1,
      ]
    );
  }

  test("cost event appears in v_cost_raw", async () => {
    await insertAuditLogRow("cost-evt-001", true);
    const rows = await duckQuery("SELECT event_uuid FROM v_cost_raw WHERE event_uuid = 'cost-evt-001'");
    expect(rows.length).toBe(1);
  });

  test("non-cost event absent from v_cost_raw", async () => {
    await insertAuditLogRow("noncost-evt-001", false);
    const rows = await duckQuery("SELECT event_uuid FROM v_cost_raw WHERE event_uuid = 'noncost-evt-001'");
    expect(rows.length).toBe(0);
  });

  test("v_cost_priced has amount_cents=0 (placeholder)", async () => {
    await insertAuditLogRow("cost-evt-002", true);
    const row = await duckQueryOne<{ event_uuid: string; amount_cents: number }>(
      "SELECT event_uuid, amount_cents FROM v_cost_priced WHERE event_uuid = 'cost-evt-002'"
    );
    expect(row).not.toBeNull();
    expect(row!.amount_cents).toBe(0);
  });

  test("v_cost_attributed has attribution_weight=1.0 (placeholder)", async () => {
    await insertAuditLogRow("cost-evt-003", true);
    const row = await duckQueryOne<{ attribution_weight: number }>(
      "SELECT attribution_weight FROM v_cost_attributed WHERE event_uuid = 'cost-evt-003'"
    );
    expect(row).not.toBeNull();
    expect(Number(row!.attribution_weight)).toBeCloseTo(1.0);
  });

  test("v_invoice_lines reads from settlement_line_items (WL-061-4B)", async () => {
    // v_invoice_lines now reads from settlement_line_items, not audit_log.
    // A cost event in audit_log does NOT appear directly in v_invoice_lines —
    // it must be settled first via settlePeriod(). An empty table yields 0 rows.
    await insertAuditLogRow("cost-evt-004", true);
    const rows = await duckQuery<{ line_uuid: string }>("SELECT line_uuid FROM v_invoice_lines");
    expect(rows.length).toBe(0);
  });
});

// =============================================================================
// Background Timer Tests
// =============================================================================

describe("background timer", () => {
  let cleanup: () => Promise<void>;

  beforeEach(async () => {
    cleanup = setupTest();
    await initDuckDB(":memory:", ":memory:");
  });

  afterEach(async () => {
    if (!getDuckDB()) {
      // Re-init if test closed DuckDB, so afterEach closeDuckDB doesn't crash
      await initDuckDB(":memory:", ":memory:");
    }
    await closeDuckDB();
    await cleanup();
  });

  test("getDuckDB() returns non-null when DuckDB initialized", () => {
    // startBackgroundTasks() guards on getDuckDB() to create the flush timer
    const duck = getDuckDB();
    expect(duck).not.toBeNull();
  });

  test("getDuckDB() returns null when DuckDB not initialized", async () => {
    await closeDuckDB();
    expect(getDuckDB()).toBeNull();
    // Re-init so afterEach can close cleanly
    await initDuckDB(":memory:", ":memory:");
    expect(getDuckDB()).not.toBeNull();
  });
});

// =============================================================================
// Persona View Shape Tests (WL-061-3B §7)
// =============================================================================

describe("persona view shapes", () => {
  let cleanup: () => Promise<void>;

  beforeEach(async () => {
    cleanup = setupTest();
    await initDuckDB(":memory:", ":memory:");
  });

  afterEach(async () => {
    await closeDuckDB();
    await cleanup();
  });

  test("v_persona_researcher is queryable and returns expected columns", async () => {
    // Should not throw on empty data
    const rows = await duckQuery<{ run_id: number | null; total_cents: number }>(
      "SELECT run_id, total_cents FROM v_persona_researcher"
    );
    expect(Array.isArray(rows)).toBe(true);
  });

  test("v_persona_lab_pi is queryable and returns expected columns", async () => {
    const rows = await duckQuery<{ attributed_user_id: number | null; week_bucket: number; total_cents: number }>(
      "SELECT attributed_user_id, week_bucket, total_cents FROM v_persona_lab_pi"
    );
    expect(Array.isArray(rows)).toBe(true);
  });

  test("v_persona_team_lead is queryable and returns expected columns", async () => {
    const rows = await duckQuery<{ tenant_id: number; total_cents: number; run_count: number }>(
      "SELECT tenant_id, total_cents, run_count FROM v_persona_team_lead"
    );
    expect(Array.isArray(rows)).toBe(true);
  });

  test("v_persona_cto is queryable and returns expected columns", async () => {
    const rows = await duckQuery<{ provider: string; spec: string; total_cents: number }>(
      "SELECT provider, spec, total_cents FROM v_persona_cto"
    );
    expect(Array.isArray(rows)).toBe(true);
  });

  test("v_persona_cfo is queryable and returns expected columns", async () => {
    const rows = await duckQuery<{ provider: string; total_cents: number; tenant_count: number }>(
      "SELECT provider, total_cents, tenant_count FROM v_persona_cfo"
    );
    expect(Array.isArray(rows)).toBe(true);
  });

  test("v_persona_accountant is queryable and returns expected columns", async () => {
    const rows = await duckQuery<{ event_uuid: string; amount_cents: number }>(
      "SELECT event_uuid, amount_cents FROM v_persona_accountant"
    );
    expect(Array.isArray(rows)).toBe(true);
  });
});

// =============================================================================
// DuckDB Query Helper Tests
// =============================================================================

describe("duckdb query helpers", () => {
  let cleanup: () => Promise<void>;

  beforeEach(async () => {
    cleanup = setupTest();
    await initDuckDB(":memory:", ":memory:");
  });

  afterEach(async () => {
    await closeDuckDB();
    await cleanup();
  });

  test("duckQuery returns typed rows", async () => {
    await duckExecute("CREATE TABLE test_helpers (id INTEGER, name VARCHAR)");
    await duckExecute("INSERT INTO test_helpers VALUES (1, 'alice'), (2, 'bob')");

    const rows = await duckQuery<{ id: number; name: string }>("SELECT * FROM test_helpers ORDER BY id");
    expect(rows.length).toBe(2);
    expect(rows[0].name).toBe("alice");
    expect(rows[1].name).toBe("bob");
  });

  test("duckQueryOne returns first row or null when no rows", async () => {
    await duckExecute("CREATE TABLE test_qone (id INTEGER, val VARCHAR)");

    const empty = await duckQueryOne("SELECT * FROM test_qone");
    expect(empty).toBeNull();

    await duckExecute("INSERT INTO test_qone VALUES (1, 'x')");
    const row = await duckQueryOne<{ id: number; val: string }>("SELECT * FROM test_qone");
    expect(row).not.toBeNull();
    expect(row!.val).toBe("x");
  });

  test("duckExecute runs DDL and DML without return value", async () => {
    await duckExecute("CREATE TABLE test_exec (x INTEGER)");
    await duckExecute("INSERT INTO test_exec VALUES (42)");

    const rows = await duckQuery<{ x: number }>("SELECT * FROM test_exec");
    expect(rows.length).toBe(1);
    expect(Number(rows[0].x)).toBe(42);
  });
});
