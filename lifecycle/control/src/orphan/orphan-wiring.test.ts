// orphan/orphan-wiring.test.ts — Tests for BL-59, ORPH-03, ORPH-06, BL-60
//
// BL-59:   Background scan interval wiring
// ORPH-03: Whitelist CRUD API routes
// ORPH-06: Failed spawn detection + targeted scan
// BL-60:   Tag-based orphan adoption (A13)

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest } from "../../../tests/harness";
import {
  createInstance,
  addToWhitelist,
  getWhitelist,
  isWhitelisted,
  removeFromWhitelist,
  getRecentScans,
} from "../material/db";
import { getControlId, formatResourceName, _resetControlIdCache } from "../material/control-id";
import { getDatabase } from "../material/db/helpers";
import { scanProvider } from "./scanner";
import { classifySpawnError, scanProviderTargeted } from "./scanner";
import type { Provider, ProviderInstance, ProviderCapabilities } from "../provider/types";
import { registerProvider, clearAllProviders } from "../provider/registry";
import { createServer } from "../api/routes/server";
import { createHash } from "node:crypto";
import { getDatabase as getDb } from "../material/db/init";
import { resetRateLimits } from "../api/middleware/rate-limit";

// =============================================================================
// Shared helpers
// =============================================================================

function makeCapabilities(): ProviderCapabilities {
  return {
    snapshots: false,
    spot: false,
    gpu: false,
    multiRegion: false,
    persistentVolumes: false,
    warmVolumes: false,
    hibernation: false,
    costExplorer: false,
    tailscaleNative: false,
    idempotentSpawn: false,
    customNetworking: false,
  };
}

function makeInstance(
  id: string,
  name: string,
  createdAt: number = Date.now() - 2 * 3600 * 1000
): ProviderInstance {
  return {
    id,
    status: "running",
    spec: "2vcpu-4gb",
    region: "local",
    ip: null,
    createdAt,
    isSpot: false,
    metadata: { name },
  };
}

function buildMockProvider(
  name: string,
  cloudInstances: ProviderInstance[]
): Provider {
  return {
    name: name as any,
    capabilities: makeCapabilities(),
    async spawn() { throw new Error("not implemented"); },
    async terminate() {},
    async list() { return cloudInstances; },
    async get(id) { return cloudInstances.find(i => i.id === id) ?? null; },
    generateBootstrap() { throw new Error("not implemented"); },
  };
}

function makeAdminKey(tenantId: number): string {
  const rawKey = "srk-wiring-admin0000000000000000";
  const hash = createHash("sha256").update(rawKey).digest("hex");
  const db = getDb();
  db.prepare(
    `INSERT OR IGNORE INTO api_keys (key_hash, name, tenant_id, role, permissions, expires_at, created_at)
     VALUES (?, 'admin-key', ?, 'admin', 'all', ?, ?)`
  ).run(hash, tenantId, Date.now() + 86_400_000, Date.now());
  return rawKey;
}

let cleanup: () => Promise<void>;

beforeEach(() => {
  cleanup = setupTest();
  _resetControlIdCache();
  resetRateLimits();
});

afterEach(async () => {
  _resetControlIdCache();
  clearAllProviders();
  await cleanup();
});

// =============================================================================
// BL-59: Background scan wiring
// =============================================================================

describe("BL-59: background scan wiring", () => {
  test("scan runs and persists results to DB", async () => {
    const db = getDatabase();
    const controlId = getControlId(db);
    const orphanName = formatResourceName(controlId, 1, 999);

    const cloud = buildMockProvider("orbstack", [
      makeInstance("vm-bg-orphan-1", orphanName),
    ]);
    await registerProvider({ provider: cloud });

    await scanProvider("orbstack");

    const scans = getRecentScans("orbstack", 5);
    expect(scans.length).toBeGreaterThanOrEqual(1);
    expect(scans[0]!.orphans_found).toBeGreaterThanOrEqual(1);
  });

  test("scan results accessible via getRecentScans after multiple scans", async () => {
    const cloud = buildMockProvider("orbstack", []);
    await registerProvider({ provider: cloud });

    await scanProvider("orbstack");
    await scanProvider("orbstack");
    await scanProvider("orbstack");

    const scans = getRecentScans("orbstack", 10);
    expect(scans.length).toBe(3);
    // Results returned newest-first
    expect(scans[0]!.scanned_at).toBeGreaterThanOrEqual(scans[1]!.scanned_at);
    expect(scans[1]!.scanned_at).toBeGreaterThanOrEqual(scans[2]!.scanned_at);
  });

  test("scan with no orphans records zero orphans_found", async () => {
    const cloud = buildMockProvider("orbstack", []);
    await registerProvider({ provider: cloud });

    await scanProvider("orbstack");

    const scans = getRecentScans("orbstack", 1);
    expect(scans.length).toBe(1);
    expect(scans[0]!.orphans_found).toBe(0);
  });
});

// =============================================================================
// ORPH-03: Whitelist CRUD API routes
// =============================================================================

describe("ORPH-03: whitelist CRUD routes", () => {
  function makeServer() {
    const app = createServer({ port: 9999, corsOrigins: ["*"], maxBodySize: 1_000_000 });
    return app;
  }

  test("GET /v1/orphans/whitelist returns empty list initially", async () => {
    const app = makeServer();
    const key = makeAdminKey(1);

    const response = await app.handle(
      new Request("http://localhost/v1/orphans/whitelist", {
        headers: { Authorization: `Bearer ${key}` },
      })
    );

    expect(response.status).toBe(200);
    const body = await response.json() as any;
    expect(body.data).toEqual([]);
  });

  test("POST /v1/orphans/whitelist adds an entry", async () => {
    const app = makeServer();
    const key = makeAdminKey(1);

    const response = await app.handle(
      new Request("http://localhost/v1/orphans/whitelist", {
        method: "POST",
        headers: {
          Authorization: `Bearer ${key}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          provider: "orbstack",
          provider_id: "vm-test-whitelist-1",
          reason: "intentional test instance",
        }),
      })
    );

    expect(response.status).toBe(201);
    const body = await response.json() as any;
    expect(body.data.provider).toBe("orbstack");
    expect(body.data.provider_id).toBe("vm-test-whitelist-1");

    // Verify it's actually in the DB
    expect(isWhitelisted("orbstack", "vm-test-whitelist-1")).toBe(true);
  });

  test("GET /v1/orphans/whitelist returns added entries", async () => {
    const app = makeServer();
    const key = makeAdminKey(1);

    addToWhitelist("orbstack", "vm-listed-1", "instance", "reason a", "admin");
    addToWhitelist("aws", "i-abc123", "instance", "reason b", "admin");

    const response = await app.handle(
      new Request("http://localhost/v1/orphans/whitelist", {
        headers: { Authorization: `Bearer ${key}` },
      })
    );

    expect(response.status).toBe(200);
    const body = await response.json() as any;
    expect(body.data.length).toBe(2);
  });

  test("GET /v1/orphans/whitelist?provider= filters by provider", async () => {
    const app = makeServer();
    const key = makeAdminKey(1);

    addToWhitelist("orbstack", "vm-a", "instance", "reason", "admin");
    addToWhitelist("aws", "i-b", "instance", "reason", "admin");

    const response = await app.handle(
      new Request("http://localhost/v1/orphans/whitelist?provider=orbstack", {
        headers: { Authorization: `Bearer ${key}` },
      })
    );

    expect(response.status).toBe(200);
    const body = await response.json() as any;
    expect(body.data.length).toBe(1);
    expect(body.data[0].provider).toBe("orbstack");
  });

  test("DELETE /v1/orphans/whitelist/:provider/:provider_id removes entry", async () => {
    const app = makeServer();
    const key = makeAdminKey(1);

    addToWhitelist("orbstack", "vm-to-delete", "instance", "reason", "admin");
    expect(isWhitelisted("orbstack", "vm-to-delete")).toBe(true);

    const response = await app.handle(
      new Request("http://localhost/v1/orphans/whitelist/orbstack/vm-to-delete", {
        method: "DELETE",
        headers: { Authorization: `Bearer ${key}` },
      })
    );

    expect(response.status).toBe(200);
    const body = await response.json() as any;
    expect(body.data.removed).toBe(true);

    expect(isWhitelisted("orbstack", "vm-to-delete")).toBe(false);
  });

  test("DELETE /v1/orphans/whitelist/:provider/:provider_id returns 404 for missing entry", async () => {
    const app = makeServer();
    const key = makeAdminKey(1);

    const response = await app.handle(
      new Request("http://localhost/v1/orphans/whitelist/orbstack/nonexistent", {
        method: "DELETE",
        headers: { Authorization: `Bearer ${key}` },
      })
    );

    expect(response.status).toBe(404);
  });

  test("POST /v1/orphans/whitelist returns 403 for non-admin key", async () => {
    const app = makeServer();
    const rawKey = "srk-wiring-member000000000000000";
    const hash = createHash("sha256").update(rawKey).digest("hex");
    const db = getDb();
    db.prepare(
      `INSERT OR IGNORE INTO api_keys (key_hash, name, tenant_id, role, permissions, expires_at, created_at)
       VALUES (?, 'member-key', 1, 'member', 'view_resources', ?, ?)`
    ).run(hash, Date.now() + 86_400_000, Date.now());

    const response = await app.handle(
      new Request("http://localhost/v1/orphans/whitelist", {
        method: "POST",
        headers: {
          Authorization: `Bearer ${rawKey}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          provider: "orbstack",
          provider_id: "vm-nope",
          reason: "test",
        }),
      })
    );

    expect(response.status).toBe(403);
  });

  test("GET /v1/orphans/scans returns recent scan results", async () => {
    const app = makeServer();
    const key = makeAdminKey(1);

    // Seed some scans directly
    const { recordOrphanScan } = await import("../material/db");
    const t = Date.now();
    recordOrphanScan({ provider: "orbstack", scanned_at: t - 1000, orphans_found: 2, orphan_ids: ["a", "b"] });
    recordOrphanScan({ provider: "orbstack", scanned_at: t, orphans_found: 0, orphan_ids: [] });

    const response = await app.handle(
      new Request("http://localhost/v1/orphans/scans?provider=orbstack", {
        headers: { Authorization: `Bearer ${key}` },
      })
    );

    expect(response.status).toBe(200);
    const body = await response.json() as any;
    expect(body.data.length).toBe(2);
    // Most recent first
    expect(body.data[0].scanned_at).toBeGreaterThanOrEqual(body.data[1].scanned_at);
  });
});

// =============================================================================
// ORPH-06: Spawn error classification
// =============================================================================

describe("ORPH-06: spawn error classification", () => {
  test("TIMEOUT_ERROR → orphanRisk=true, scan_immediately", () => {
    const err = { code: "TIMEOUT_ERROR", message: "timeout" };
    const c = classifySpawnError(err);
    expect(c.orphanRisk).toBe(true);
    expect(c.action).toBe("scan_immediately");
  });

  test("NETWORK_ERROR → orphanRisk=true, scan_immediately", () => {
    const err = { code: "NETWORK_ERROR", message: "network failed" };
    const c = classifySpawnError(err);
    expect(c.orphanRisk).toBe(true);
    expect(c.action).toBe("scan_immediately");
  });

  test("PROVIDER_INTERNAL → orphanRisk=true, scan_immediately", () => {
    const err = { code: "PROVIDER_INTERNAL", message: "internal" };
    const c = classifySpawnError(err);
    expect(c.orphanRisk).toBe(true);
    expect(c.action).toBe("scan_immediately");
  });

  test("INVALID_SPEC → orphanRisk=false, none", () => {
    const err = { code: "INVALID_SPEC", message: "invalid" };
    const c = classifySpawnError(err);
    expect(c.orphanRisk).toBe(false);
    expect(c.action).toBe("none");
    expect(c.confidenceNoResource).toBe("high");
  });

  test("AUTH_ERROR → orphanRisk=false, none", () => {
    const err = { code: "AUTH_ERROR", message: "auth failed" };
    const c = classifySpawnError(err);
    expect(c.orphanRisk).toBe(false);
    expect(c.action).toBe("none");
  });

  test("CAPACITY_ERROR → orphanRisk=false, none", () => {
    const err = { code: "CAPACITY_ERROR", message: "no capacity" };
    const c = classifySpawnError(err);
    expect(c.orphanRisk).toBe(false);
    expect(c.action).toBe("none");
  });

  test("SPOT_INTERRUPTED → orphanRisk=false, none", () => {
    const err = { code: "SPOT_INTERRUPTED", message: "spot interrupted" };
    const c = classifySpawnError(err);
    expect(c.orphanRisk).toBe(false);
    expect(c.action).toBe("none");
  });

  test("Error message 'timeout' → orphanRisk=true", () => {
    const err = new Error("operation timed out");
    const c = classifySpawnError(err);
    expect(c.orphanRisk).toBe(true);
    expect(c.action).toBe("scan_immediately");
  });

  test("Error message 'network' → orphanRisk=true", () => {
    const err = new Error("network connection failed");
    const c = classifySpawnError(err);
    expect(c.orphanRisk).toBe(true);
  });

  test("Error message 'unauthorized' → orphanRisk=false", () => {
    const err = new Error("unauthorized access");
    const c = classifySpawnError(err);
    expect(c.orphanRisk).toBe(false);
    expect(c.action).toBe("none");
  });

  test("OPERATION_TIMEOUT (wait-for-boot scenario) → orphanRisk=true, scan_immediately", () => {
    // wait-for-boot.ts throws { code: "OPERATION_TIMEOUT" } on boot timeout.
    // Maps to spec §12.8 WAIT_FOR_BOOT_TIMEOUT row.
    const err = { code: "OPERATION_TIMEOUT", message: "Instance boot timed out" };
    const c = classifySpawnError(err);
    expect(c.orphanRisk).toBe(true);
    expect(c.action).toBe("scan_immediately");
    expect(c.confidenceNoResource).toBe("low");
  });

  test("RATE_LIMIT_ERROR → orphanRisk=true (provider may have partially processed)", () => {
    const err = { code: "RATE_LIMIT_ERROR", message: "rate limited" };
    const c = classifySpawnError(err);
    expect(c.orphanRisk).toBe(true);
    expect(c.action).toBe("scan_immediately");
  });

  test("unknown error object with no code → conservative orphanRisk=true", () => {
    const err = { message: "something unexpected happened" };
    const c = classifySpawnError(err);
    expect(c.orphanRisk).toBe(true);
    expect(c.action).toBe("scan_immediately");
  });

  test("AGENT_REGISTRATION_FAILED → orphanRisk=true (instance was spawned but agent failed to register)", () => {
    const err = { code: "AGENT_REGISTRATION_FAILED", message: "agent failed to connect" };
    const c = classifySpawnError(err);
    expect(c.orphanRisk).toBe(true);
    expect(c.action).toBe("scan_immediately");
  });

  test("code takes priority over message: AUTH_ERROR + timeout message → orphanRisk=false (code wins)", () => {
    // The message contains 'timeout' which would normally trigger orphanRisk=true,
    // but the code AUTH_ERROR is matched first and returns orphanRisk=false.
    const err = { code: "AUTH_ERROR", message: "operation timed out due to auth failure" };
    const c = classifySpawnError(err);
    expect(c.orphanRisk).toBe(false);
    expect(c.action).toBe("none");
  });
});

describe("ORPH-06: targeted scan", () => {
  test("targeted scan returns only orphans matching prefix", async () => {
    const db = getDatabase();
    const controlId = getControlId(db);
    const matchingName = formatResourceName(controlId, 1, 99);
    const otherName = formatResourceName(controlId, 2, 88);

    const cloud = buildMockProvider("orbstack", [
      makeInstance("vm-match-1", matchingName),
      makeInstance("vm-other-1", otherName),
    ]);
    await registerProvider({ provider: cloud });

    const orphans = await scanProviderTargeted("orbstack", matchingName);
    expect(orphans.length).toBe(1);
    expect(orphans[0]!.providerId).toBe("vm-match-1");
    expect(orphans[0]!.classification).toBe("known_orphan");
  });

  test("targeted scan returns empty when no prefix match", async () => {
    const cloud = buildMockProvider("orbstack", [
      makeInstance("vm-other-1", "some-random-name"),
    ]);
    await registerProvider({ provider: cloud });

    const orphans = await scanProviderTargeted("orbstack", "repl-xxxxxx-1-1");
    expect(orphans.length).toBe(0);
  });

  test("targeted scan excludes tracked instances", async () => {
    const db = getDatabase();
    const controlId = getControlId(db);
    const name = formatResourceName(controlId, 1, 1);

    createInstance({
      provider: "orbstack",
      provider_id: "vm-tracked-target",
      spec: "2vcpu-4gb",
      region: "local",
      ip: null,
      workflow_state: "launch-run:active",
      workflow_error: null,
      current_manifest_id: null,
      spawn_idempotency_key: null,
      is_spot: 0,
      spot_request_id: null,
      init_checksum: null,
      registration_token_hash: null,
      last_heartbeat: Date.now(),
    });

    const cloud = buildMockProvider("orbstack", [
      makeInstance("vm-tracked-target", name),
    ]);
    await registerProvider({ provider: cloud });

    const orphans = await scanProviderTargeted("orbstack", name);
    expect(orphans.length).toBe(0);
  });
});

// =============================================================================
// BL-60: Tag-based orphan adoption (A13)
// =============================================================================

describe("BL-60: tag-based orphan adoption (A13)", () => {
  test("known_orphan matching DB instance with empty provider_id gets adopted", async () => {
    const db = getDatabase();
    const controlId = getControlId(db);

    // Create an instance in spawn:pending state with no provider_id
    const inst = createInstance({
      provider: "orbstack",
      provider_id: "", // not yet assigned
      spec: "2vcpu-4gb",
      region: "local",
      ip: null,
      workflow_state: "spawn:pending",
      workflow_error: null,
      current_manifest_id: null,
      spawn_idempotency_key: "test-spawn-key",
      is_spot: 0,
      spot_request_id: null,
      init_checksum: null,
      registration_token_hash: null,
      last_heartbeat: Date.now(),
    });

    // Cloud has a resource named with this instance's ID
    const orphanName = formatResourceName(controlId, null, inst.id);

    const cloud = buildMockProvider("orbstack", [
      makeInstance("vm-adopted-1", orphanName),
    ]);
    await registerProvider({ provider: cloud });

    const result = await scanProvider("orbstack");

    // The orphan should NOT appear in the scan result orphans list
    const adoptedOrphan = result.orphans.find(o => o.providerId === "vm-adopted-1");
    expect(adoptedOrphan).toBeUndefined();

    // The DB instance should now have provider_id set
    const { getInstance } = await import("../material/db");
    const updated = getInstance(inst.id);
    expect(updated?.provider_id).toBe("vm-adopted-1");
    expect(updated?.workflow_state).toBe("launch-run:provisioning");
  });

  test("known_orphan with no matching DB instance → flagged as known_orphan", async () => {
    const db = getDatabase();
    const controlId = getControlId(db);

    // Use a resource ID that doesn't exist in DB
    const orphanName = formatResourceName(controlId, null, 99999);

    const cloud = buildMockProvider("orbstack", [
      makeInstance("vm-no-match-1", orphanName),
    ]);
    await registerProvider({ provider: cloud });

    const result = await scanProvider("orbstack");

    const orphan = result.orphans.find(o => o.providerId === "vm-no-match-1");
    expect(orphan).toBeDefined();
    expect(orphan?.classification).toBe("known_orphan");
  });

  test("known_orphan matching DB instance already in terminal state → NOT adopted", async () => {
    const db = getDatabase();
    const controlId = getControlId(db);

    // Instance in terminal state
    const inst = createInstance({
      provider: "orbstack",
      provider_id: "",
      spec: "2vcpu-4gb",
      region: "local",
      ip: null,
      workflow_state: "spawn:error",
      workflow_error: "spawn failed",
      current_manifest_id: null,
      spawn_idempotency_key: null,
      is_spot: 0,
      spot_request_id: null,
      init_checksum: null,
      registration_token_hash: null,
      last_heartbeat: Date.now(),
    });

    const orphanName = formatResourceName(controlId, null, inst.id);

    const cloud = buildMockProvider("orbstack", [
      makeInstance("vm-terminal-1", orphanName),
    ]);
    await registerProvider({ provider: cloud });

    const result = await scanProvider("orbstack");

    // Should still appear as orphan (not adopted)
    const orphan = result.orphans.find(o => o.providerId === "vm-terminal-1");
    expect(orphan).toBeDefined();
    expect(orphan?.classification).toBe("known_orphan");
  });

  test("known_orphan matching DB instance already with provider_id → NOT adopted", async () => {
    const db = getDatabase();
    const controlId = getControlId(db);

    // Instance already has a different provider_id
    const inst = createInstance({
      provider: "orbstack",
      provider_id: "vm-existing-provider",
      spec: "2vcpu-4gb",
      region: "local",
      ip: null,
      workflow_state: "launch-run:active",
      workflow_error: null,
      current_manifest_id: null,
      spawn_idempotency_key: null,
      is_spot: 0,
      spot_request_id: null,
      init_checksum: null,
      registration_token_hash: null,
      last_heartbeat: Date.now(),
    });

    const orphanName = formatResourceName(controlId, null, inst.id);

    // Cloud has TWO resources: the tracked one + a new untracked one with same name encoding
    const cloud = buildMockProvider("orbstack", [
      makeInstance("vm-existing-provider", "some-name"), // tracked
      makeInstance("vm-duplicate-1", orphanName),        // orphan
    ]);
    await registerProvider({ provider: cloud });

    const result = await scanProvider("orbstack");

    // vm-duplicate-1 should remain as orphan (existing provider_id blocks adoption)
    const orphan = result.orphans.find(o => o.providerId === "vm-duplicate-1");
    expect(orphan).toBeDefined();
    expect(orphan?.classification).toBe("known_orphan");
  });
});
