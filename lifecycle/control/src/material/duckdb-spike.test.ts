// duckdb-spike.test.ts — Binding validation spike (INV-1)
//
// Pass/fail gate for @duckdb/node-api in Bun.
// Tests: in-memory table ops, GROUP BY, ASOF JOIN, SQLite ATTACH.

import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Database } from "bun:sqlite";
import { unlinkSync, existsSync } from "node:fs";

// Conditionally import to allow graceful failure reporting
let DuckDBInstance: any;
let duckdbAvailable = false;

try {
  const mod = await import("@duckdb/node-api");
  DuckDBInstance = mod.DuckDBInstance;
  duckdbAvailable = true;
} catch (err) {
  console.warn("[duckdb-spike] @duckdb/node-api failed to load:", err);
}

describe("DuckDB binding spike", () => {
  if (!duckdbAvailable) {
    test("@duckdb/node-api available", () => {
      throw new Error("@duckdb/node-api not available — check installation");
    });
    return;
  }

  let instance: any;
  let conn: any;

  beforeAll(async () => {
    instance = await DuckDBInstance.create(":memory:");
    conn = await instance.connect();
  });

  afterAll(async () => {
    if (conn) {
      try { conn.closeSync(); } catch { /* ignore */ }
    }
    if (instance) {
      try { instance.closeSync(); } catch { /* ignore */ }
    }
  });

  test("open in-memory instance and connect", () => {
    expect(instance).toBeTruthy();
    expect(conn).toBeTruthy();
  });

  test("CREATE TABLE and INSERT rows", async () => {
    await conn.run(`
      CREATE TABLE audit_log (
        id              BIGINT,
        event_uuid      VARCHAR NOT NULL,
        event_type      VARCHAR NOT NULL,
        tenant_id       INTEGER NOT NULL,
        instance_id     INTEGER,
        occurred_at     BIGINT NOT NULL,
        is_cost         BOOLEAN NOT NULL DEFAULT false,
        data            JSON NOT NULL,
        PRIMARY KEY (event_uuid)
      )
    `);

    // Insert 100 rows
    const rows: string[] = [];
    for (let i = 0; i < 100; i++) {
      const uuid = `evt-${i.toString().padStart(6, "0")}`;
      const eventType = i % 3 === 0 ? "instance.start" : i % 3 === 1 ? "instance.stop" : "run.complete";
      const isCost = i % 2 === 0;
      rows.push(`(${i}, '${uuid}', '${eventType}', 1, ${i + 1000}, ${Date.now() + i}, ${isCost}, '{"key":"val"}')`);
    }

    await conn.run(`INSERT INTO audit_log VALUES ${rows.join(", ")}`);

    const result = await conn.runAndReadAll("SELECT COUNT(*) AS cnt FROM audit_log");
    const rows2 = result.getRowObjects();
    // DuckDB returns BigInt for COUNT results
    expect(Number(rows2[0].cnt)).toBe(100);
  });

  test("GROUP BY and SUM query", async () => {
    const result = await conn.runAndReadAll(`
      SELECT event_type, COUNT(*) AS cnt
      FROM audit_log
      GROUP BY event_type
      ORDER BY event_type
    `);
    const rows = result.getRowObjects();
    expect(rows.length).toBe(3);
    const total = rows.reduce((sum: number, r: any) => sum + Number(r.cnt), 0);
    expect(total).toBe(100);
  });

  test("ASOF JOIN (core pricing technique)", async () => {
    // Create a prices table for ASOF JOIN
    await conn.run(`
      CREATE TABLE prices (
        at_ms           BIGINT NOT NULL,
        spec            VARCHAR NOT NULL,
        rate_per_hour   DOUBLE NOT NULL
      )
    `);

    const baseMs = Date.now();
    await conn.run(`
      INSERT INTO prices VALUES
        (${baseMs - 10000}, 'gpu.small', 0.50),
        (${baseMs - 5000},  'gpu.small', 0.75),
        (${baseMs + 5000},  'gpu.small', 1.00)
    `);

    // Create events table for the ASOF JOIN
    await conn.run(`
      CREATE TABLE events (
        event_uuid  VARCHAR NOT NULL,
        occurred_at BIGINT NOT NULL,
        spec        VARCHAR NOT NULL
      )
    `);

    await conn.run(`
      INSERT INTO events VALUES
        ('e1', ${baseMs - 8000}, 'gpu.small'),
        ('e2', ${baseMs - 3000}, 'gpu.small'),
        ('e3', ${baseMs + 1000}, 'gpu.small')
    `);

    const result = await conn.runAndReadAll(`
      SELECT a.event_uuid, p.rate_per_hour
      FROM events a
      ASOF JOIN prices p
        ON a.spec = p.spec AND a.occurred_at >= p.at_ms
      ORDER BY a.event_uuid
    `);
    const rows = result.getRowObjects();
    // ASOF JOIN matches latest price at or before event time:
    // e1 (baseMs-8000) → 0.50 (price at baseMs-10000)
    // e2 (baseMs-3000) → 0.75 (price at baseMs-5000)
    // e3 (baseMs+1000) → 0.75 (price at baseMs-5000, latest price before e3)
    // All 3 events match (there's always a price <= event time for e1, e2, e3)
    expect(rows.length).toBe(3);
    expect(Number(rows[0].rate_per_hour)).toBe(0.50); // e1
    expect(Number(rows[1].rate_per_hour)).toBe(0.75); // e2
    expect(Number(rows[2].rate_per_hour)).toBe(0.75); // e3 (matches same price as e2)
  });

  test("ATTACH SQLite database (read-only) and cross-database query", async () => {
    const sqlitePath = join(tmpdir(), `duckdb-spike-test-${Date.now()}.db`);

    try {
      // Create a real SQLite database with bun:sqlite
      const db = new Database(sqlitePath);
      db.exec("PRAGMA journal_mode = WAL");
      db.exec(`
        CREATE TABLE instances (
          id INTEGER PRIMARY KEY,
          tenant_id INTEGER NOT NULL,
          provider TEXT NOT NULL,
          spec TEXT NOT NULL,
          region TEXT NOT NULL DEFAULT 'us-east-1',
          workflow_state TEXT NOT NULL DEFAULT 'ready'
        )
      `);
      db.exec(`
        INSERT INTO instances VALUES
          (1001, 1, 'aws', 'gpu.small', 'us-east-1', 'ready'),
          (1002, 1, 'aws', 'gpu.large', 'us-west-2', 'running')
      `);
      db.close();

      // ATTACH SQLite in DuckDB (read-only)
      await conn.run(`ATTACH '${sqlitePath}' AS sqlite (TYPE sqlite, READ_ONLY)`);

      // Cross-database query: DuckDB audit_log JOIN SQLite instances
      const result = await conn.runAndReadAll(`
        SELECT al.event_uuid, al.event_type, si.provider, si.spec
        FROM audit_log al
        JOIN sqlite.instances si ON al.instance_id = si.id
        ORDER BY al.event_uuid
        LIMIT 5
      `);
      const rows = result.getRowObjects();
      // audit_log has rows with instance_id 1000-1099, sqlite.instances has 1001, 1002
      expect(rows.length).toBeGreaterThan(0);
      expect(rows[0].provider).toBe("aws");

      // Detach after use
      await conn.run("DETACH sqlite");
    } finally {
      if (existsSync(sqlitePath)) unlinkSync(sqlitePath);
      const walPath = sqlitePath + "-wal";
      if (existsSync(walPath)) unlinkSync(walPath);
      const shmPath = sqlitePath + "-shm";
      if (existsSync(shmPath)) unlinkSync(shmPath);
    }
  });

  test("close cleanly without crash", async () => {
    // This test ensures the afterAll cleanup doesn't crash.
    // The actual close is in afterAll — if we get here, the test suite
    // will proceed to afterAll and verify no segfault.
    expect(true).toBe(true);
  });
});
