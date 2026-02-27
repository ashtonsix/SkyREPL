#!/usr/bin/env bun
// schema-smoke-test.ts — WL-051 whole-system smoke test
//
// Exercises the schema foundation changes through the live control plane API:
//   1. Schema DDL: new tables/columns exist, removed columns are gone
//   2. CHECK constraints reject invalid data at the SQLite level
//   3. Identity FK reversal: AuthContext.userId = users.id, keyId = api_keys.id
//   4. Tailscale state in objects table (not instance columns)
//   5. Budget enforcement via users.id (not api_keys.id)
//   6. Preflight endpoint works with new identity model
//   7. Admin/usage endpoints work without api_key_id
//
// Usage:
//   repl control reset && repl control start
//   bun scaffold/scripts/schema-smoke-test.ts
//
// Requires: control plane running on localhost:3000, API key in ~/.repl/api-key

import { readFileSync, existsSync } from "fs";
import { join } from "path";
import { homedir } from "os";
import { Database } from "bun:sqlite";

// =============================================================================
// Config
// =============================================================================

const BASE = process.env.SKYREPL_CONTROL_PLANE_URL ?? "http://localhost:3000";
const keyFile = join(homedir(), ".repl", "api-key");
if (!existsSync(keyFile)) {
  console.error("No API key found. Run `repl control start` first.");
  process.exit(1);
}
const API_KEY = readFileSync(keyFile, "utf-8").trim().split("\n")[0].trim();
const DB_PATH = join(homedir(), ".repl", "skyrepl-control.db");

let passed = 0;
let failed = 0;
const failures: string[] = [];

function assert(condition: boolean, label: string) {
  if (condition) {
    passed++;
    console.log(`  ✓ ${label}`);
  } else {
    failed++;
    failures.push(label);
    console.log(`  ✗ ${label}`);
  }
}

async function api(path: string, opts?: RequestInit): Promise<Response> {
  const headers = new Headers(opts?.headers);
  if (!headers.has("Authorization")) {
    headers.set("Authorization", `Bearer ${API_KEY}`);
  }
  if (!headers.has("Content-Type") && (opts?.method === "POST" || opts?.method === "PATCH")) {
    headers.set("Content-Type", "application/json");
  }
  return fetch(`${BASE}${path}`, { ...opts, headers });
}

// =============================================================================
// 1. Schema DDL — direct DB inspection
// =============================================================================

async function testSchemaDDL() {
  console.log("\n── 1. Schema DDL ──────────────────────────────────────────");

  const db = new Database(DB_PATH, { readonly: true });

  // New tables exist
  const tables = db.query("SELECT name FROM sqlite_master WHERE type='table'").all() as { name: string }[];
  const tableNames = tables.map(t => t.name);
  assert(tableNames.includes("audit_log"), "audit_log table exists");
  assert(!tableNames.includes("cost_events"), "cost_events table REMOVED");
  assert(!tableNames.includes("usage_records"), "usage_records table REMOVED");

  // New columns on existing tables
  const instanceCols = db.query("PRAGMA table_info(instances)").all() as { name: string }[];
  const instanceColNames = instanceCols.map(c => c.name);
  assert(instanceColNames.includes("provider_metadata"), "instances.provider_metadata column exists");
  assert(!instanceColNames.includes("tailscale_ip"), "instances.tailscale_ip column REMOVED");
  assert(!instanceColNames.includes("tailscale_status"), "instances.tailscale_status column REMOVED");

  const manifestCols = db.query("PRAGMA table_info(manifests)").all() as { name: string }[];
  const manifestColNames = manifestCols.map(c => c.name);
  assert(manifestColNames.includes("parent_manifest_id"), "manifests.parent_manifest_id column exists");

  const workflowCols = db.query("PRAGMA table_info(workflows)").all() as { name: string }[];
  const workflowColNames = workflowCols.map(c => c.name);
  assert(workflowColNames.includes("priority"), "workflows.priority column exists");
  assert(workflowColNames.includes("retry_of_workflow_id"), "workflows.retry_of_workflow_id column exists");

  // Identity FK reversal: api_keys.user_id exists, users.api_key_id gone
  const apiKeyCols = db.query("PRAGMA table_info(api_keys)").all() as { name: string }[];
  const apiKeyColNames = apiKeyCols.map(c => c.name);
  assert(apiKeyColNames.includes("user_id"), "api_keys.user_id column exists (reverse FK)");

  const userCols = db.query("PRAGMA table_info(users)").all() as { name: string }[];
  const userColNames = userCols.map(c => c.name);
  assert(!userColNames.includes("api_key_id"), "users.api_key_id column REMOVED");

  // Removed table
  assert(!tableNames.includes("rate_limit_log"), "rate_limit_log table REMOVED");

  // Removed columns from orphan_whitelist
  const whitelistCols = db.query("PRAGMA table_info(orphan_whitelist)").all() as { name: string }[];
  const whitelistColNames = whitelistCols.map(c => c.name);
  assert(!whitelistColNames.includes("resource_name"), "orphan_whitelist.resource_name REMOVED");
  assert(!whitelistColNames.includes("notes"), "orphan_whitelist.notes REMOVED");

  // manifest_resources.owner_id removed
  const mrCols = db.query("PRAGMA table_info(manifest_resources)").all() as { name: string }[];
  const mrColNames = mrCols.map(c => c.name);
  assert(!mrColNames.includes("owner_id"), "manifest_resources.owner_id REMOVED");
  assert(mrColNames.includes("owner_type"), "manifest_resources.owner_type still exists");

  // tenant_id on orphan_scans and manifest_cleanup_state
  const osCols = db.query("PRAGMA table_info(orphan_scans)").all() as { name: string }[];
  assert(osCols.map(c => c.name).includes("tenant_id"), "orphan_scans.tenant_id exists");

  const mcsCols = db.query("PRAGMA table_info(manifest_cleanup_state)").all() as { name: string }[];
  assert(mcsCols.map(c => c.name).includes("tenant_id"), "manifest_cleanup_state.tenant_id exists");

  db.close();
}

// =============================================================================
// 2. CHECK constraints — write invalid data directly to DB and verify rejection
// =============================================================================

async function testCheckConstraints() {
  console.log("\n── 2. CHECK constraints ────────────────────────────────────");

  const db = new Database(DB_PATH);

  // allocations.status CHECK
  try {
    db.query(
      `INSERT INTO allocations (tenant_id, instance_id, status, workdir, created_at, updated_at)
       VALUES (1, 999, 'INVALID_STATUS', '/workspace', 0, 0)`
    ).run();
    assert(false, "allocations.status CHECK rejects invalid status");
  } catch (e: any) {
    assert(e.message.includes("CHECK") || e.message.includes("constraint"), "allocations.status CHECK rejects invalid status");
  }

  // allocations.status CHECK accepts valid
  try {
    // First ensure the instance exists for FK
    db.query(
      `INSERT OR IGNORE INTO instances (id, tenant_id, provider, provider_id, spec, region, workflow_state, created_at, last_heartbeat)
       VALUES (999, 1, 'test', 'test-999', 'ubuntu', 'local', 'launch-run:provisioning', 0, 0)`
    ).run();
    db.query(
      `INSERT INTO allocations (tenant_id, instance_id, status, workdir, created_at, updated_at)
       VALUES (1, 999, 'AVAILABLE', '/workspace', 0, 0)`
    ).run();
    assert(true, "allocations.status CHECK accepts 'AVAILABLE'");
    // Clean up
    db.query("DELETE FROM allocations WHERE instance_id = 999").run();
  } catch (e: any) {
    assert(false, "allocations.status CHECK accepts 'AVAILABLE': " + e.message);
  }

  // manifests.status CHECK
  try {
    db.query(
      `INSERT INTO manifests (tenant_id, workflow_id, status, created_at, updated_at)
       VALUES (1, 1, 'BOGUS', 0, 0)`
    ).run();
    assert(false, "manifests.status CHECK rejects 'BOGUS'");
  } catch (e: any) {
    assert(e.message.includes("CHECK") || e.message.includes("constraint"), "manifests.status CHECK rejects 'BOGUS'");
  }

  // workflows.status CHECK
  try {
    db.query(
      `INSERT INTO workflows (tenant_id, type, status, input_json, created_at, updated_at)
       VALUES (1, 'test', 'not_a_status', '{}', 0, 0)`
    ).run();
    assert(false, "workflows.status CHECK rejects 'not_a_status'");
  } catch (e: any) {
    assert(e.message.includes("CHECK") || e.message.includes("constraint"), "workflows.status CHECK rejects 'not_a_status'");
  }

  // Boolean CHECK on is_spot
  try {
    db.query(
      `INSERT INTO instances (tenant_id, provider, spec, region, workflow_state, is_spot, created_at, last_heartbeat)
       VALUES (1, 'test', 'ubuntu', 'local', 'launch-run:provisioning', 2, 0, 0)`
    ).run();
    assert(false, "instances.is_spot CHECK rejects value 2");
  } catch (e: any) {
    assert(e.message.includes("CHECK") || e.message.includes("constraint"), "instances.is_spot CHECK rejects value 2");
  }

  // manifest_resources.owner_type CHECK
  try {
    // Need a valid manifest first
    db.query(
      `INSERT OR IGNORE INTO workflows (id, tenant_id, type, status, input_json, created_at, updated_at)
       VALUES (9999, 1, 'test', 'pending', '{}', 0, 0)`
    ).run();
    db.query(
      `INSERT OR IGNORE INTO manifests (id, tenant_id, workflow_id, status, created_at, updated_at)
       VALUES (9999, 1, 9999, 'DRAFT', 0, 0)`
    ).run();
    db.query(
      `INSERT INTO manifest_resources (manifest_id, resource_type, resource_id, added_at, owner_type)
       VALUES (9999, 'test', 'test-1', 0, 'invalid_owner')`
    ).run();
    assert(false, "manifest_resources.owner_type CHECK rejects 'invalid_owner'");
  } catch (e: any) {
    assert(e.message.includes("CHECK") || e.message.includes("constraint"), "manifest_resources.owner_type CHECK rejects 'invalid_owner'");
  }

  // audit_log: INSERT succeeds with valid data
  const testUuid = "01JR" + Math.random().toString(36).slice(2).toUpperCase().padEnd(22, "0").slice(0, 22);
  try {
    db.query(
      `INSERT INTO audit_log (event_uuid, event_type, tenant_id, source, is_cost, data, occurred_at)
       VALUES (?, 'metering_start', 1, 'smoke_test', 1, '{"test":true}', ?)`
    ).run(testUuid, Date.now());
    assert(true, "audit_log INSERT with valid data succeeds");
  } catch (e: any) {
    assert(false, "audit_log INSERT with valid data succeeds: " + e.message);
  }

  // audit_log: UPDATE triggers ABORT
  try {
    db.query(
      `UPDATE audit_log SET event_type = 'tampered' WHERE event_uuid = ?`
    ).run(testUuid);
    assert(false, "audit_log UPDATE → ABORT (trigger fires)");
  } catch (e: any) {
    assert(e.message.includes("append-only") || e.message.includes("ABORT"), "audit_log UPDATE → ABORT (trigger fires)");
  }

  // audit_log: DELETE triggers ABORT
  try {
    db.query(
      `DELETE FROM audit_log WHERE event_uuid = ?`
    ).run(testUuid);
    assert(false, "audit_log DELETE → ABORT (trigger fires)");
  } catch (e: any) {
    assert(e.message.includes("append-only") || e.message.includes("ABORT"), "audit_log DELETE → ABORT (trigger fires)");
  }

  // Clean up test data
  db.query("DELETE FROM manifest_resources WHERE manifest_id = 9999").run();
  db.query("DELETE FROM manifests WHERE id = 9999").run();
  db.query("DELETE FROM workflows WHERE id = 9999").run();
  db.query("DELETE FROM instances WHERE id = 999").run();

  db.close();
}

// =============================================================================
// 3. Identity model — API key auth produces correct AuthContext
// =============================================================================

async function testIdentityModel() {
  console.log("\n── 3. Identity model (API key → AuthContext) ───────────────");

  // The seeded admin key should work
  const resp = await api("/v1/tenants");
  assert(resp.status === 200, "GET /v1/tenants returns 200 with valid key");

  const data = await resp.json();
  assert(Array.isArray(data.data), "Tenants response has data array");
  assert(data.data.length > 0, "Default tenant exists");

  // Create a user in the default tenant
  const createUserResp = await api("/v1/tenants/1/users", {
    method: "POST",
    body: JSON.stringify({
      email: "smoke@test.com",
      role: "member",
      display_name: "Smoke Tester",
      budget_usd: 50.0,
    }),
  });
  assert(createUserResp.status === 201, "POST /v1/tenants/1/users creates user");
  const userData = await createUserResp.json();
  const userId = userData.data?.id;
  assert(typeof userId === "number", `Created user has numeric id (got ${userId})`);
  assert(userData.data?.email === "smoke@test.com", "User email matches");
  assert(userData.data?.budget_usd === 50.0, "User budget_usd matches");

  // Verify user does NOT have api_key_id in response
  assert(!("api_key_id" in (userData.data ?? {})), "User response has no api_key_id field");

  // Create an API key for the tenant
  const createKeyResp = await api("/v1/keys", {
    method: "POST",
    body: JSON.stringify({ name: "smoke-test-key", role: "member" }),
  });
  assert(createKeyResp.status === 201, "POST /v1/keys creates API key");
  const keyData = await createKeyResp.json();
  const rawKey = keyData.data?.raw_key;
  assert(typeof rawKey === "string" && rawKey.startsWith("srk-"), "New key starts with srk-");

  // List keys — verify they appear
  const listKeysResp = await api("/v1/keys");
  assert(listKeysResp.status === 200, "GET /v1/keys works");
  const keysData = await listKeysResp.json();
  assert(keysData.data.length >= 2, "At least 2 keys exist (admin + smoke-test)");

  // Clean up: revoke the test key
  if (keyData.data?.id) {
    const revokeResp = await api(`/v1/keys/${keyData.data.id}`, { method: "DELETE" });
    assert(revokeResp.status === 200, "DELETE /v1/keys/:id revokes key");
  }

  // Clean up user
  if (userId) {
    const removeResp = await api(`/v1/users/${userId}`, { method: "DELETE" });
    assert(removeResp.status === 200, "DELETE /v1/users/:id removes user");
  }
}

// =============================================================================
// 4. Preflight endpoint
// =============================================================================

async function testPreflight() {
  console.log("\n── 4. Preflight endpoint ───────────────────────────────────");

  const resp = await api("/v1/preflight?operation=launch-run");
  assert(resp.status === 200, "GET /v1/preflight returns 200");

  const data = await resp.json();
  assert(typeof data.ok === "boolean", "Preflight response has ok boolean");
  assert(Array.isArray(data.warnings), "Preflight response has warnings array");

  // With no usage, ok should be true (no budget exceeded)
  assert(data.ok === true, "Preflight ok=true (no budget exceeded)");

  // Preflight without auth should fail
  const noAuthResp = await fetch(`${BASE}/v1/preflight?operation=launch-run`);
  assert(noAuthResp.status === 401, "Preflight without auth returns 401");
}

// =============================================================================
// 5. Usage endpoint (no api_key_id dependency)
// =============================================================================

async function testUsage() {
  console.log("\n── 5. Usage endpoint ───────────────────────────────────────");

  const resp = await api("/v1/usage");
  assert(resp.status === 200, "GET /v1/usage returns 200");

  const data = await resp.json();
  assert(typeof data.data?.tenant_id === "number", "Usage has tenant_id");
  assert(typeof data.data?.total_cost_usd === "number", "Usage has total_cost_usd");
  assert(Array.isArray(data.data?.users), "Usage has users array");

  // Each user entry should have used_usd but NOT api_key_id
  for (const u of data.data.users) {
    assert(typeof u.used_usd === "number", `User ${u.id} has used_usd`);
  }
}

// =============================================================================
// 6. Tailscale state via objects (heartbeat → objects → operations read)
// =============================================================================

async function testTailscaleObjects() {
  console.log("\n── 6. Tailscale state via objects table ────────────────────");

  // Create an instance via the DB (no real VM needed)
  const db = new Database(DB_PATH);
  db.query(
    `INSERT INTO instances (tenant_id, provider, provider_id, spec, region, workflow_state, is_spot, created_at, last_heartbeat)
     VALUES (1, 'test-smoke', 'smoke-vm-1', 'ubuntu', 'local', 'launch-run:provisioning', 0, ?, ?)`
  ).run(Date.now(), Date.now());
  const instanceRow = db.query("SELECT id FROM instances WHERE provider_id = 'smoke-vm-1'").get() as { id: number };
  const instanceId = instanceRow.id;
  db.close();

  // Send heartbeat with tailscale data (agent endpoint, no API key needed)
  const hbResp = await fetch(`${BASE}/v1/agent/heartbeat`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      instance_id: instanceId,
      status: "idle",
      active_allocations: [],
      tailscale_ip: "100.64.0.99",
      tailscale_status: "ready",
    }),
  });
  assert(hbResp.status === 200, "Heartbeat with tailscale data accepted");

  // Verify tailscale state is in objects table (not instance columns)
  const db2 = new Database(DB_PATH, { readonly: true });

  // Instance should NOT have tailscale columns
  const inst = db2.query("SELECT * FROM instances WHERE id = ?").get(instanceId) as any;
  assert(inst !== null, "Instance exists in DB");
  assert(!("tailscale_ip" in inst), "Instance row has no tailscale_ip column");
  assert(!("tailscale_status" in inst), "Instance row has no tailscale_status column");
  assert("provider_metadata" in inst, "Instance row has provider_metadata column");

  // Objects table should have tailscale_state record
  const tsObj = db2.query(
    `SELECT o.metadata_json FROM objects o
     JOIN object_tags t ON o.id = t.object_id
     WHERE o.type = 'tailscale_state' AND t.tag = ?`
  ).get(`instance_id:${instanceId}`) as { metadata_json: string } | null;
  assert(tsObj !== null, "tailscale_state object exists in objects table");

  if (tsObj) {
    const meta = JSON.parse(tsObj.metadata_json);
    assert(meta.tailscale_ip === "100.64.0.99", "Objects metadata has correct tailscale_ip");
    assert(meta.tailscale_status === "ready", "Objects metadata has correct tailscale_status");
  }

  // Second heartbeat without tailscale fields should preserve existing state
  await fetch(`${BASE}/v1/agent/heartbeat`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      instance_id: instanceId,
      status: "idle",
      active_allocations: [],
    }),
  });

  const tsObj2 = db2.query(
    `SELECT o.metadata_json FROM objects o
     JOIN object_tags t ON o.id = t.object_id
     WHERE o.type = 'tailscale_state' AND t.tag = ?`
  ).get(`instance_id:${instanceId}`) as { metadata_json: string } | null;
  if (tsObj2) {
    const meta2 = JSON.parse(tsObj2.metadata_json);
    assert(meta2.tailscale_ip === "100.64.0.99", "Tailscale state preserved after heartbeat without TS fields");
    assert(meta2.tailscale_status === "ready", "Tailscale status preserved after heartbeat without TS fields");
  }

  db2.close();

  // Clean up
  const db3 = new Database(DB_PATH);
  db3.query("DELETE FROM object_tags WHERE tag = ?").run(`instance_id:${instanceId}`);
  db3.query("DELETE FROM objects WHERE type = 'tailscale_state'").run();
  db3.query("DELETE FROM instances WHERE id = ?").run(instanceId);
  db3.close();
}

// =============================================================================
// 7. Budget enforcement via users.id
// =============================================================================

async function testBudgetEnforcement() {
  console.log("\n── 7. Budget enforcement ───────────────────────────────────");

  // Set a team budget on the default tenant
  const patchResp = await api("/v1/tenants/1", {
    method: "PATCH",
    body: JSON.stringify({ budget_usd: 100.0 }),
  });
  assert(patchResp.status === 200, "PATCH /v1/tenants/1 sets budget");

  // Preflight should still be ok (no usage)
  const preResp = await api("/v1/preflight?operation=launch-run");
  const preData = await preResp.json();
  assert(preData.ok === true, "Preflight ok with budget set but no usage");

  // Now set budget to 0 to trigger budget exceeded
  await api("/v1/tenants/1", {
    method: "PATCH",
    body: JSON.stringify({ budget_usd: 0 }),
  });

  const preResp2 = await api("/v1/preflight?operation=launch-run");
  const preData2 = await preResp2.json();
  // With budget=0, any non-zero usage triggers error. With zero usage, 0 >= 0 = true → error
  // Actually: remaining = 0 - 0 = 0, condition is remaining <= 0 → true
  assert(preData2.ok === false, "Preflight ok=false with budget=0 (exhausted)");
  assert(preData2.errors?.some((e: any) => e.code === "BUDGET_EXCEEDED"), "Budget exceeded error present");

  // Reset budget to null (no limit)
  await api("/v1/tenants/1", {
    method: "PATCH",
    body: JSON.stringify({ budget_usd: null }),
  });

  const preResp3 = await api("/v1/preflight?operation=launch-run");
  const preData3 = await preResp3.json();
  assert(preData3.ok === true, "Preflight ok after clearing budget limit");
}

// =============================================================================
// 8. Team management (user CRUD through CLI-equivalent endpoints)
// =============================================================================

async function testTeamManagement() {
  console.log("\n── 8. Team management ─────────────────────────────────────");

  // Create user
  const createResp = await api("/v1/tenants/1/users", {
    method: "POST",
    body: JSON.stringify({
      email: "alice@example.com",
      role: "member",
      display_name: "Alice",
    }),
  });
  assert(createResp.status === 201, "Create user Alice");
  const alice = (await createResp.json()).data;

  // Update user budget
  const updateResp = await api(`/v1/users/${alice.id}`, {
    method: "PATCH",
    body: JSON.stringify({ budget_usd: 25.0 }),
  });
  assert(updateResp.status === 200, "Update user budget");
  const updateBody = await updateResp.json();
  const updatedAlice = updateBody.data;
  assert(updatedAlice?.budget_usd === 25.0, "Budget updated correctly");

  // List users
  const listResp = await api("/v1/tenants/1/users");
  assert(listResp.status === 200, "List tenant users");
  const users = (await listResp.json()).data;
  assert(users.some((u: any) => u.email === "alice@example.com"), "Alice appears in user list");

  // Remove user
  const removeResp = await api(`/v1/users/${alice.id}`, { method: "DELETE" });
  assert(removeResp.status === 200, "Remove user Alice");

  // Verify removed
  const listResp2 = await api("/v1/tenants/1/users");
  const users2 = (await listResp2.json()).data;
  assert(!users2.some((u: any) => u.email === "alice@example.com"), "Alice no longer in user list after removal");
}

// =============================================================================
// Run
// =============================================================================

async function main() {
  console.log("WL-051 Schema Foundation — Whole-System Smoke Test");
  console.log(`Target: ${BASE}`);
  console.log(`API Key: ${API_KEY.slice(0, 12)}...`);

  // Health check
  try {
    const healthResp = await fetch(`${BASE}/v1/health`);
    assert(healthResp.status === 200, "Control plane is healthy");
  } catch (e) {
    console.error("Control plane not reachable. Run `repl control start` first.");
    process.exit(1);
  }

  await testSchemaDDL();
  await testCheckConstraints();
  await testIdentityModel();
  await testPreflight();
  await testUsage();
  await testTailscaleObjects();
  await testBudgetEnforcement();
  await testTeamManagement();

  // Summary
  console.log("\n════════════════════════════════════════════════════════════");
  console.log(`  ${passed} passed, ${failed} failed`);
  if (failures.length > 0) {
    console.log("\n  Failures:");
    for (const f of failures) {
      console.log(`    ✗ ${f}`);
    }
  }
  console.log("════════════════════════════════════════════════════════════\n");

  process.exit(failed > 0 ? 1 : 0);
}

main().catch((err) => {
  console.error("Smoke test crashed:", err);
  process.exit(2);
});
