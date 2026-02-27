// orphan/scanner.test.ts - Tests for ORPH-01, ORPH-02, ORPH-04
//
// Uses mock providers — no real cloud calls.

import { describe, test, expect, beforeEach, afterEach, mock } from "bun:test";
import { setupTest } from "../../../tests/harness";
import {
  createInstance,
  addToWhitelist,
  isWhitelisted,
  getWhitelist,
  removeFromWhitelist,
  getRecentScans,
  recordOrphanScan,
} from "../material/db";
import { getControlId, formatResourceName, _resetControlIdCache } from "../material/control-id";
import { getDatabase } from "../material/db/helpers";
import { scanProvider } from "./scanner";
import { validateResourceName, enforceNamingConvention } from "./naming";
import { safeTerminateOrphan, safeTerminateBatch } from "./safety";
import type { Provider, ProviderInstance, ProviderCapabilities } from "../provider/types";
import { registerProvider, clearAllProviders } from "../provider/registry";

// =============================================================================
// Mock Provider Factory
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
  createdAt: number = Date.now() - 2 * 3600 * 1000,
  isSpot = false
): ProviderInstance {
  return {
    id,
    status: "running",
    spec: "2vcpu-4gb",
    region: "local",
    ip: null,
    createdAt,
    isSpot,
    metadata: { name },
  };
}

function buildMockProvider(
  name: string,
  cloudInstances: ProviderInstance[],
  terminateFn?: (id: string) => Promise<void>
): Provider {
  return {
    name: name as any,
    capabilities: makeCapabilities(),
    async spawn() { throw new Error("not implemented"); },
    async terminate(id) {
      if (terminateFn) return terminateFn(id);
    },
    async list() { return cloudInstances; },
    async get(id) { return cloudInstances.find(i => i.id === id) ?? null; },
    generateBootstrap() { throw new Error("not implemented"); },
  };
}

// =============================================================================
// Test Setup
// =============================================================================

let cleanup: () => Promise<void>;

beforeEach(() => {
  cleanup = setupTest();
  _resetControlIdCache();
});

afterEach(async () => {
  _resetControlIdCache();
  await cleanup();
});

// =============================================================================
// ORPH-01: Scanner + Classifier
// =============================================================================

describe("scanner", () => {
  test("empty cloud and DB → no orphans, no missing", async () => {
    const mock = buildMockProvider("orbstack", []);
    await registerProvider({ provider: mock });

    const result = await scanProvider("orbstack");
    expect(result.cloudCount).toBe(0);
    expect(result.trackedCount).toBe(0);
    expect(result.orphans).toHaveLength(0);
    expect(result.missing).toHaveLength(0);
  });

  test("cloud and DB agree → no orphans", async () => {
    const db = getDatabase();
    const controlId = getControlId(db);
    const name = formatResourceName(controlId, 1, 1);

    const inst = createInstance({
      provider: "orbstack",
      provider_id: "vm-tracked-1",
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
      provider_metadata: null,
      display_name: null,
    });

    const cloud = buildMockProvider("orbstack", [
      makeInstance("vm-tracked-1", name),
    ]);
    await registerProvider({ provider: cloud });

    const result = await scanProvider("orbstack");
    expect(result.cloudCount).toBe(1);
    expect(result.trackedCount).toBe(1);
    expect(result.orphans).toHaveLength(0);
    expect(result.missing).toHaveLength(0);
  });

  test("cloud has extra instance → classified as known_orphan", async () => {
    const db = getDatabase();
    const controlId = getControlId(db);
    const orphanName = formatResourceName(controlId, 2, 99);

    const cloud = buildMockProvider("orbstack", [
      makeInstance("vm-orphan-1", orphanName),
    ]);
    await registerProvider({ provider: cloud });

    const result = await scanProvider("orbstack");
    expect(result.orphans).toHaveLength(1);
    expect(result.orphans[0]!.classification).toBe("known_orphan");
    expect(result.orphans[0]!.providerId).toBe("vm-orphan-1");
    expect(result.orphans[0]!.parsedName?.controlId).toBe(controlId);
  });

  test("cloud has instance with foreign controlId → classified as foreign_orphan", async () => {
    const foreignName = formatResourceName("xxxxxx", 1, 1);
    const cloud = buildMockProvider("orbstack", [
      makeInstance("vm-foreign-1", foreignName),
    ]);
    await registerProvider({ provider: cloud });

    const result = await scanProvider("orbstack");
    expect(result.orphans).toHaveLength(1);
    expect(result.orphans[0]!.classification).toBe("foreign_orphan");
    expect(result.orphans[0]!.parsedName?.controlId).toBe("xxxxxx");
  });

  test("cloud has instance with unrecognised name → classified as unknown", async () => {
    const cloud = buildMockProvider("orbstack", [
      makeInstance("vm-random-1", "some-random-vm-name"),
    ]);
    await registerProvider({ provider: cloud });

    const result = await scanProvider("orbstack");
    expect(result.orphans).toHaveLength(1);
    expect(result.orphans[0]!.classification).toBe("unknown");
    expect(result.orphans[0]!.parsedName).toBeNull();
  });

  test("whitelisted instance → classified as whitelisted", async () => {
    const db = getDatabase();
    const controlId = getControlId(db);
    const orphanName = formatResourceName(controlId, 3, 50);

    addToWhitelist("orbstack", "vm-whitelisted-1", "instance", "intentional", "admin");

    const cloud = buildMockProvider("orbstack", [
      makeInstance("vm-whitelisted-1", orphanName),
    ]);
    await registerProvider({ provider: cloud });

    const result = await scanProvider("orbstack");
    expect(result.orphans).toHaveLength(1);
    expect(result.orphans[0]!.classification).toBe("whitelisted");
  });

  test("DB instance not in cloud → reported in missing", async () => {
    createInstance({
      provider: "orbstack",
      provider_id: "vm-gone-1",
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
      provider_metadata: null,
      display_name: null,
    });

    const cloud = buildMockProvider("orbstack", []); // nothing in cloud
    await registerProvider({ provider: cloud });

    const result = await scanProvider("orbstack");
    expect(result.missing).toContain("vm-gone-1");
    expect(result.orphans).toHaveLength(0);
  });

  test("terminating instances in DB are not counted as tracked (excluded from missing)", async () => {
    createInstance({
      provider: "orbstack",
      provider_id: "vm-terminating-1",
      spec: "2vcpu-4gb",
      region: "local",
      ip: null,
      workflow_state: "terminate:complete",
      workflow_error: null,
      current_manifest_id: null,
      spawn_idempotency_key: null,
      is_spot: 0,
      spot_request_id: null,
      init_checksum: null,
      registration_token_hash: null,
      last_heartbeat: Date.now(),
      provider_metadata: null,
      display_name: null,
    });

    const cloud = buildMockProvider("orbstack", []);
    await registerProvider({ provider: cloud });

    const result = await scanProvider("orbstack");
    expect(result.trackedCount).toBe(0);
    expect(result.missing).toHaveLength(0);
  });

  test("scan records are persisted to DB", async () => {
    const db = getDatabase();
    const controlId = getControlId(db);
    const orphanName = formatResourceName(controlId, 5, 200);

    const cloud = buildMockProvider("orbstack", [
      makeInstance("vm-scan-record-1", orphanName),
    ]);
    await registerProvider({ provider: cloud });

    await scanProvider("orbstack");

    const scans = getRecentScans("orbstack", 5);
    expect(scans.length).toBe(1);
    expect(scans[0]!.orphans_found).toBe(1);
    expect(scans[0]!.orphan_ids).toContain("vm-scan-record-1");
  });
});

// =============================================================================
// ORPH-02: Naming Convention Validation
// =============================================================================

describe("naming validation", () => {
  test("validates correct names", () => {
    const db = getDatabase();
    const controlId = getControlId(db);
    const name = formatResourceName(controlId, 42, 7);

    const result = validateResourceName(name);
    expect(result.valid).toBe(true);
    expect(result.errors).toHaveLength(0);
    expect(result.parsed?.controlId).toBe(controlId);
    expect(result.parsed?.manifestId).toBe(42);
    expect(result.parsed?.resourceId).toBe(7);
  });

  test("validates name with null manifest (none segment)", () => {
    const db = getDatabase();
    const controlId = getControlId(db);
    const name = formatResourceName(controlId, null, 1);

    const result = validateResourceName(name);
    expect(result.valid).toBe(true);
    expect(result.parsed?.manifestId).toBeNull();
  });

  test("rejects names that don't match convention", () => {
    const cases = [
      "some-random-vm",
      "repl-abc",
      "repl-abc123-1-",
      "",
      "repl-ABCDEF-1-1", // uppercase
    ];

    for (const name of cases) {
      const result = validateResourceName(name);
      expect(result.valid).toBe(false);
      expect(result.errors.length).toBeGreaterThan(0);
    }
  });

  test("enforceNamingConvention: compliant canonical name", () => {
    const db = getDatabase();
    const controlId = getControlId(db);
    const name = formatResourceName(controlId, 1, 1);

    const check = enforceNamingConvention(name);
    expect(check.compliant).toBe(true);
    expect(check.actual).toBe(name);
    expect(check.expected).toBe(name);
  });

  test("enforceNamingConvention: non-convention name is non-compliant", () => {
    const check = enforceNamingConvention("random-vm-name");
    expect(check.compliant).toBe(false);
  });
});

// =============================================================================
// ORPH-04: Safety Constraints
// =============================================================================

describe("safety constraints", () => {
  test("dry-run skips termination and returns skipped_dry_run", async () => {
    const terminated: string[] = [];
    const cloud = buildMockProvider("orbstack", [
      makeInstance("vm-safe-1", "some-name", Date.now() - 2 * 3600 * 1000),
    ], async (id) => { terminated.push(id); });
    await registerProvider({ provider: cloud });

    const result = await safeTerminateOrphan({
      provider: "orbstack",
      providerId: "vm-safe-1",
      dryRun: true,
      force: false,
    });

    expect(result.action).toBe("skipped_dry_run");
    expect(terminated).toHaveLength(0);
  });

  test("age threshold: skips resource younger than minAgeMs", async () => {
    const youngCreatedAt = Date.now() - 10 * 60 * 1000; // 10 minutes old
    const cloud = buildMockProvider("orbstack", [
      makeInstance("vm-young-1", "some-name", youngCreatedAt),
    ]);
    await registerProvider({ provider: cloud });

    const result = await safeTerminateOrphan({
      provider: "orbstack",
      providerId: "vm-young-1",
      dryRun: false,
      force: false,
      minAgeMs: 3600 * 1000, // 1 hour
    });

    expect(result.action).toBe("skipped_too_young");
    expect(result.reason).toMatch(/age/);
  });

  test("force: bypasses age threshold", async () => {
    const terminated: string[] = [];
    const youngCreatedAt = Date.now() - 10 * 60 * 1000; // 10 minutes
    const cloud = buildMockProvider("orbstack", [
      makeInstance("vm-young-2", "some-name", youngCreatedAt),
    ], async (id) => { terminated.push(id); });
    await registerProvider({ provider: cloud });

    const result = await safeTerminateOrphan({
      provider: "orbstack",
      providerId: "vm-young-2",
      dryRun: false,
      force: true,
    });

    expect(result.action).toBe("terminated");
    expect(terminated).toContain("vm-young-2");
  });

  test("whitelist: skips whitelisted resource", async () => {
    addToWhitelist("orbstack", "vm-wl-1", "instance", "intentional", "admin");

    const cloud = buildMockProvider("orbstack", [
      makeInstance("vm-wl-1", "some-name", Date.now() - 2 * 3600 * 1000),
    ]);
    await registerProvider({ provider: cloud });

    const result = await safeTerminateOrphan({
      provider: "orbstack",
      providerId: "vm-wl-1",
      dryRun: false,
      force: false,
    });

    expect(result.action).toBe("skipped_whitelisted");
  });

  test("force: bypasses whitelist", async () => {
    addToWhitelist("orbstack", "vm-wl-2", "instance", "intentional", "admin");

    const terminated: string[] = [];
    const cloud = buildMockProvider("orbstack", [
      makeInstance("vm-wl-2", "some-name", Date.now() - 2 * 3600 * 1000),
    ], async (id) => { terminated.push(id); });
    await registerProvider({ provider: cloud });

    const result = await safeTerminateOrphan({
      provider: "orbstack",
      providerId: "vm-wl-2",
      dryRun: false,
      force: true,
    });

    expect(result.action).toBe("terminated");
    expect(terminated).toContain("vm-wl-2");
  });

  test("successful termination of old-enough non-whitelisted resource", async () => {
    const terminated: string[] = [];
    const oldCreatedAt = Date.now() - 5 * 3600 * 1000; // 5 hours
    const cloud = buildMockProvider("orbstack", [
      makeInstance("vm-old-1", "some-name", oldCreatedAt),
    ], async (id) => { terminated.push(id); });
    await registerProvider({ provider: cloud });

    const result = await safeTerminateOrphan({
      provider: "orbstack",
      providerId: "vm-old-1",
      dryRun: false,
      force: false,
    });

    expect(result.action).toBe("terminated");
    expect(terminated).toContain("vm-old-1");
  });

  test("safeTerminateBatch processes all requests", async () => {
    const terminated: string[] = [];
    const oldCreatedAt = Date.now() - 5 * 3600 * 1000;
    const cloud = buildMockProvider("orbstack", [
      makeInstance("vm-batch-1", "n1", oldCreatedAt),
      makeInstance("vm-batch-2", "n2", oldCreatedAt),
    ], async (id) => { terminated.push(id); });
    await registerProvider({ provider: cloud });

    const results = await safeTerminateBatch([
      { provider: "orbstack", providerId: "vm-batch-1", dryRun: false, force: false },
      { provider: "orbstack", providerId: "vm-batch-2", dryRun: true, force: false },
    ]);

    expect(results).toHaveLength(2);
    expect(results[0]!.action).toBe("terminated");
    expect(results[1]!.action).toBe("skipped_dry_run");
    expect(terminated).toEqual(["vm-batch-1"]);
  });

  test("failed provider.get → returns failed action", async () => {
    const cloud: Provider = {
      name: "orbstack" as any,
      capabilities: makeCapabilities(),
      async spawn() { throw new Error("not impl"); },
      async terminate() {},
      async list() { return []; },
      async get() { throw new Error("API error"); },
      generateBootstrap() { throw new Error("not impl"); },
    };
    await registerProvider({ provider: cloud });

    const result = await safeTerminateOrphan({
      provider: "orbstack",
      providerId: "vm-error-1",
      dryRun: false,
      force: false,
    });

    expect(result.action).toBe("failed");
    expect(result.reason).toMatch(/provider\.get failed/);
  });
});

// =============================================================================
// DB: whitelist helpers
// =============================================================================

describe("whitelist db helpers", () => {
  test("getWhitelist returns all entries", () => {
    addToWhitelist("orbstack", "vm-a", "instance", "reason a", "admin");
    addToWhitelist("orbstack", "vm-b", "instance", "reason b", "admin");

    const entries = getWhitelist();
    expect(entries.length).toBe(2);
    expect(entries.map(e => e.provider_id).sort()).toEqual(["vm-a", "vm-b"]);
  });

  test("getWhitelist filtered by provider", () => {
    addToWhitelist("orbstack", "vm-a", "instance", "reason a", "admin");
    addToWhitelist("aws", "i-123", "instance", "reason b", "admin");

    const entries = getWhitelist("orbstack");
    expect(entries.length).toBe(1);
    expect(entries[0]!.provider).toBe("orbstack");
  });

  test("removeFromWhitelist removes and returns true", () => {
    addToWhitelist("orbstack", "vm-c", "instance", "reason", "admin");
    expect(isWhitelisted("orbstack", "vm-c")).toBe(true);

    const removed = removeFromWhitelist("orbstack", "vm-c");
    expect(removed).toBe(true);
    expect(isWhitelisted("orbstack", "vm-c")).toBe(false);
  });

  test("removeFromWhitelist returns false when entry absent", () => {
    const removed = removeFromWhitelist("orbstack", "nonexistent");
    expect(removed).toBe(false);
  });

  test("getRecentScans returns scans in desc order", () => {
    const t = Date.now();
    recordOrphanScan({ provider: "orbstack", scanned_at: t - 2000, orphans_found: 1, orphan_ids: ["a"] });
    recordOrphanScan({ provider: "orbstack", scanned_at: t - 1000, orphans_found: 0, orphan_ids: [] });

    const scans = getRecentScans("orbstack", 10);
    expect(scans.length).toBe(2);
    expect(scans[0]!.scanned_at).toBeGreaterThan(scans[1]!.scanned_at);
  });

  test("getRecentScans limit is honoured", () => {
    for (let i = 0; i < 5; i++) {
      recordOrphanScan({ provider: "orbstack", scanned_at: Date.now() - i * 1000, orphans_found: 0, orphan_ids: [] });
    }

    const scans = getRecentScans("orbstack", 3);
    expect(scans.length).toBe(3);
  });
});
