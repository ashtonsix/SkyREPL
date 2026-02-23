// tests/unit/resource-layer.test.ts - Resource Layer Tests
// Tests for allocation.ts, instance.ts, and run.ts resource modules

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest } from "../harness";
import {
  getDatabase,
  createAllocation,
  createInstance,
  getAllocation,
  createRun,
  type Allocation,
  type Instance,
  type Run,
} from "../../control/src/material/db";
import {
  claimAllocation,
  activateAllocation,
  completeAllocation,
} from "../../control/src/workflow/state-transitions";
import { TIMING } from "@skyrepl/contracts";
import { registerProvider } from "../../control/src/provider/registry";
import type { Provider, ProviderCapabilities, SpawnOptions, BootstrapConfig, BootstrapScript, ListFilter } from "../../control/src/provider/types";

// Resource layer modules under test
import {
  validateAllocationTransition,
  allowsSSHAccess,
  isInWarmPool,
  createAvailableAllocation,
  queryWarmPool,
  claimWarmPoolAllocation,
  transitionToComplete,
  checkReplenishmentEligibility,
  extendAllocationDebugHold,
  canTerminateInstance,
  reconcileClaimedAllocations,
  holdExpiryTask,
  ALLOCATION_TRANSITIONS,
} from "../../control/src/resource/allocation";

import {
  createInstanceRecord,
  getInstanceRecord,
  updateInstanceRecord,
  listInstanceRecords,
  isInstanceHealthy,
  getActiveInstances,
  detectStaleHeartbeats,
} from "../../control/src/resource/instance";

import {
  createRunRecord,
  getRunRecord,
  updateRunRecord,
  listRunRecords,
  isRunInProgress,
  getRunDuration,
} from "../../control/src/resource/run";

// =============================================================================
// Test Helpers
// =============================================================================

function createTestInstance(overrides?: Partial<{
  spec: string;
  region: string;
  init_checksum: string | null;
  workflow_state: string;
  last_heartbeat: number;
  provider: string;
  is_spot: number;
}>): Instance {
  const now = Date.now();
  return createInstance({
    provider: overrides?.provider ?? "orbstack",
    provider_id: `test-vm-${now}-${Math.random().toString(36).slice(2, 8)}`,
    spec: overrides?.spec ?? "gpu-small",
    region: overrides?.region ?? "us-east-1",
    ip: "10.0.0.1",
    workflow_state: overrides?.workflow_state ?? "launch-run:provisioning",
    workflow_error: null,
    current_manifest_id: null,
    spawn_idempotency_key: null,
    is_spot: overrides?.is_spot ?? 0,
    spot_request_id: null,
    init_checksum: overrides?.init_checksum !== undefined ? overrides.init_checksum : null,
    registration_token_hash: null,
    last_heartbeat: overrides?.last_heartbeat ?? now,
  });
}

function createTestRun(overrides?: Partial<{
  exit_code: number | null;
  workflow_state: string;
  started_at: number | null;
  finished_at: number | null;
  spot_interrupted: number;
}>): Run {
  return createRun({
    command: "echo test",
    workdir: "/workspace",
    max_duration_ms: 60000,
    workflow_state: overrides?.workflow_state ?? "launch-run:pending",
    workflow_error: null,
    current_manifest_id: null,
    exit_code: overrides?.exit_code ?? null,
    init_checksum: null,
    create_snapshot: 0,
    spot_interrupted: overrides?.spot_interrupted ?? 0,
    started_at: overrides?.started_at ?? null,
    finished_at: overrides?.finished_at ?? null,
  });
}

function createTestAllocation(instanceId: number, runId?: number | null): Allocation {
  return createAllocation({
    run_id: runId ?? null,
    instance_id: instanceId,
    status: "AVAILABLE",
    current_manifest_id: null,
    user: "default",
    workdir: "/home/user",
    debug_hold_until: null,
    completed_at: null,
  });
}

// =============================================================================
// Setup
// =============================================================================

let cleanup: () => Promise<void>;

beforeEach(() => {
  cleanup = setupTest();
});

afterEach(() => cleanup());

// =============================================================================
// Allocation: State Validation
// =============================================================================

describe("allocation: validateAllocationTransition", () => {
  test("validates all transitions against ALLOCATION_TRANSITIONS truth table", () => {
    const allStatuses = ["AVAILABLE", "CLAIMED", "ACTIVE", "COMPLETE", "FAILED"] as const;

    // Every valid transition (from ALLOCATION_TRANSITIONS) should pass
    for (const [from, allowed] of Object.entries(ALLOCATION_TRANSITIONS)) {
      for (const to of allowed) {
        const result = validateAllocationTransition(from as any, to);
        expect(result.valid).toBe(true);
        expect(result.error).toBeUndefined();
      }
    }

    // Every invalid transition should fail
    for (const from of allStatuses) {
      const allowed = ALLOCATION_TRANSITIONS[from];
      for (const to of allStatuses) {
        if (allowed.includes(to)) continue;
        const result = validateAllocationTransition(from, to);
        expect(result.valid).toBe(false);
        expect(result.error).toBeDefined();
      }
    }

    // Terminal states specifically mention "terminal state" in error
    for (const terminal of ["COMPLETE", "FAILED"] as const) {
      const result = validateAllocationTransition(terminal, "AVAILABLE");
      expect(result.error).toContain("terminal state");
    }
  });
});

describe("allocation: allowsSSHAccess", () => {
  test("returns true for ACTIVE allocation", () => {
    const instance = createTestInstance();
    const alloc = createTestAllocation(instance.id);
    const db = getDatabase();
    db.run("UPDATE allocations SET status = 'ACTIVE' WHERE id = ?", [alloc.id]);
    const updated = getAllocation(alloc.id)!;
    expect(allowsSSHAccess(updated)).toBe(true);
  });

  test("returns true for COMPLETE with active debug hold", () => {
    const instance = createTestInstance();
    const alloc = createTestAllocation(instance.id);
    const db = getDatabase();
    db.run(
      "UPDATE allocations SET status = 'COMPLETE', debug_hold_until = ?, completed_at = ? WHERE id = ?",
      [Date.now() + 300_000, Date.now(), alloc.id]
    );
    const updated = getAllocation(alloc.id)!;
    expect(allowsSSHAccess(updated)).toBe(true);
  });

  test("returns false for COMPLETE with expired debug hold", () => {
    const instance = createTestInstance();
    const alloc = createTestAllocation(instance.id);
    const db = getDatabase();
    db.run(
      "UPDATE allocations SET status = 'COMPLETE', debug_hold_until = ?, completed_at = ? WHERE id = ?",
      [Date.now() - 1000, Date.now(), alloc.id]
    );
    const updated = getAllocation(alloc.id)!;
    expect(allowsSSHAccess(updated)).toBe(false);
  });

  test("returns false for COMPLETE with no debug hold", () => {
    const instance = createTestInstance();
    const alloc = createTestAllocation(instance.id);
    const db = getDatabase();
    db.run(
      "UPDATE allocations SET status = 'COMPLETE', completed_at = ? WHERE id = ?",
      [Date.now(), alloc.id]
    );
    const updated = getAllocation(alloc.id)!;
    expect(allowsSSHAccess(updated)).toBe(false);
  });

  test("returns false for AVAILABLE allocation", () => {
    const instance = createTestInstance();
    const alloc = createTestAllocation(instance.id);
    expect(allowsSSHAccess(alloc)).toBe(false);
  });

  test("returns false for FAILED allocation", () => {
    const instance = createTestInstance();
    const alloc = createTestAllocation(instance.id);
    const db = getDatabase();
    db.run("UPDATE allocations SET status = 'FAILED', completed_at = ? WHERE id = ?", [Date.now(), alloc.id]);
    const updated = getAllocation(alloc.id)!;
    expect(allowsSSHAccess(updated)).toBe(false);
  });
});

describe("allocation: isInWarmPool", () => {
  test("only AVAILABLE is in warm pool", () => {
    const expected: Record<string, boolean> = {
      AVAILABLE: true,
      CLAIMED: false,
      ACTIVE: false,
      COMPLETE: false,
      FAILED: false,
    };
    for (const [status, inPool] of Object.entries(expected)) {
      expect(isInWarmPool(status as any)).toBe(inPool);
    }
  });
});

// =============================================================================
// Allocation: Warm Pool Operations
// =============================================================================

describe("allocation: createAvailableAllocation", () => {
  test("uses canonical /workspace workdir", async () => {
    const instance = createTestInstance();
    const alloc1 = await createAvailableAllocation(instance.id);
    expect(alloc1.workdir).toBe("/workspace");

    const alloc2 = await createAvailableAllocation(instance.id);
    expect(alloc2.workdir).toBe("/workspace");
  });
});

describe("allocation: queryWarmPool", () => {
  test("returns matches with checksum scores", async () => {
    const instance = createTestInstance({ spec: "gpu-small", init_checksum: "abc123" });
    await createAvailableAllocation(instance.id);

    const results = await queryWarmPool({ spec: "gpu-small", initChecksum: "abc123" });
    expect(results.length).toBe(1);
    expect(results[0].checksum_score).toBe(100);
    expect(results[0].spec).toBe("gpu-small");
  });

  test("scores vanilla instances at 50", async () => {
    const instance = createTestInstance({ spec: "gpu-small", init_checksum: null });
    await createAvailableAllocation(instance.id);

    const results = await queryWarmPool({ spec: "gpu-small", initChecksum: "abc123" });
    expect(results.length).toBe(1);
    expect(results[0].checksum_score).toBe(50);
  });

  test("excludes mismatched checksums", async () => {
    const instance = createTestInstance({ spec: "gpu-small", init_checksum: "different" });
    await createAvailableAllocation(instance.id);

    const results = await queryWarmPool({ spec: "gpu-small", initChecksum: "abc123" });
    expect(results.length).toBe(0);
  });

  test("filters by spec", async () => {
    const instance1 = createTestInstance({ spec: "gpu-small" });
    const instance2 = createTestInstance({ spec: "gpu-large" });
    await createAvailableAllocation(instance1.id);
    await createAvailableAllocation(instance2.id);

    const results = await queryWarmPool({ spec: "gpu-small" });
    expect(results.length).toBe(1);
    expect(results[0].spec).toBe("gpu-small");
  });

  test("filters by region", async () => {
    const instance1 = createTestInstance({ spec: "gpu-small", region: "us-east-1" });
    const instance2 = createTestInstance({ spec: "gpu-small", region: "eu-west-1" });
    await createAvailableAllocation(instance1.id);
    await createAvailableAllocation(instance2.id);

    const results = await queryWarmPool({ spec: "gpu-small", region: "eu-west-1" });
    expect(results.length).toBe(1);
    expect(results[0].region).toBe("eu-west-1");
  });

  test("respects limit parameter", async () => {
    const instance1 = createTestInstance({ spec: "gpu-small" });
    const instance2 = createTestInstance({ spec: "gpu-small" });
    const instance3 = createTestInstance({ spec: "gpu-small" });
    await createAvailableAllocation(instance1.id);
    await createAvailableAllocation(instance2.id);
    await createAvailableAllocation(instance3.id);

    const results = await queryWarmPool({ spec: "gpu-small" }, 2);
    expect(results.length).toBe(2);
  });

  test("orders exact match before vanilla", async () => {
    const vanillaInstance = createTestInstance({ spec: "gpu-small", init_checksum: null });
    const exactInstance = createTestInstance({ spec: "gpu-small", init_checksum: "abc123" });
    await createAvailableAllocation(vanillaInstance.id);
    await createAvailableAllocation(exactInstance.id);

    const results = await queryWarmPool({ spec: "gpu-small", initChecksum: "abc123" });
    expect(results.length).toBe(2);
    expect(results[0].checksum_score).toBe(100);
    expect(results[1].checksum_score).toBe(50);
  });
});

describe("allocation: claimWarmPoolAllocation", () => {
  test("claims a warm allocation successfully", async () => {
    const instance = createTestInstance({ spec: "gpu-small" });
    await createAvailableAllocation(instance.id);
    const run = createTestRun();

    // Register mock provider so materializeInstance confirms instance is alive
    await registerProvider({
      provider: {
        name: "orbstack" as any,
        capabilities: { snapshots: false, spot: false, gpu: false, multiRegion: false, persistentVolumes: false, warmVolumes: false, hibernation: false, costExplorer: false, tailscaleNative: false, idempotentSpawn: true, customNetworking: false } as ProviderCapabilities,
        async spawn(_opts: SpawnOptions) { return { id: "mock", status: "running", spec: "gpu-small", ip: "10.0.0.1", createdAt: Date.now(), isSpot: false }; },
        async terminate() {},
        async list() { return []; },
        async get(id: string) { return { id, status: "running", spec: "gpu-small", ip: "10.0.0.1", createdAt: Date.now(), isSpot: false }; },
        generateBootstrap() { return { content: "#!/bin/bash", format: "shell" as const, checksum: "mock" }; },
      } as Provider,
    });

    const result = await claimWarmPoolAllocation(
      { spec: "gpu-small", tenantId: 1 },
      run.id
    );
    expect(result.success).toBe(true);
    expect(result.allocation).toBeDefined();
    expect(result.allocation!.status).toBe("CLAIMED");
    expect(result.allocation!.run_id).toBe(run.id);
  });

  test("returns error when no warm allocation available", async () => {
    const run = createTestRun();
    const result = await claimWarmPoolAllocation(
      { spec: "gpu-small", tenantId: 1 },
      run.id
    );
    expect(result.success).toBe(false);
    expect(result.error).toContain("No warm allocation available");
  });
});

// =============================================================================
// Allocation: Debug Hold Extension
// =============================================================================

describe("allocation: extendAllocationDebugHold", () => {
  test("extends debug hold for COMPLETE allocation", async () => {
    const instance = createTestInstance();
    const alloc = createTestAllocation(instance.id);
    const run = createTestRun();

    // Transition through the lifecycle
    claimAllocation(alloc.id, run.id);
    activateAllocation(alloc.id);
    completeAllocation(alloc.id, { debugHoldUntil: Date.now() + 60_000 });

    const result = await extendAllocationDebugHold(alloc.id, {
      extensionMs: 300_000,
    });

    expect(result.success).toBe(true);
    expect(result.newDebugHoldUntil).toBeDefined();
    expect(result.newDebugHoldUntil!).toBeGreaterThan(Date.now());
  });

  test("caps extension at maxExtensionMs", async () => {
    const instance = createTestInstance();
    const alloc = createTestAllocation(instance.id);
    const run = createTestRun();

    claimAllocation(alloc.id, run.id);
    activateAllocation(alloc.id);
    completeAllocation(alloc.id);

    const now = Date.now();
    const result = await extendAllocationDebugHold(alloc.id, {
      extensionMs: 10_000_000, // very large
      maxExtensionMs: 60_000,  // cap at 1 minute
    });

    expect(result.success).toBe(true);
    // Should be capped near now + 60_000
    expect(result.newDebugHoldUntil!).toBeLessThanOrEqual(now + 60_000 + 1000);
  });

  test("caps at absolute max (24h from created_at)", async () => {
    const instance = createTestInstance();
    const alloc = createTestAllocation(instance.id);
    const run = createTestRun();

    claimAllocation(alloc.id, run.id);
    activateAllocation(alloc.id);
    completeAllocation(alloc.id);

    const allocation = getAllocation(alloc.id)!;
    const absoluteMax = allocation.created_at + TIMING.ABSOLUTE_MAX_HOLD_MS;

    const result = await extendAllocationDebugHold(alloc.id, {
      extensionMs: TIMING.ABSOLUTE_MAX_HOLD_MS + 1_000_000, // way past 24h
      maxExtensionMs: TIMING.ABSOLUTE_MAX_HOLD_MS + 1_000_000,
    });

    expect(result.success).toBe(true);
    expect(result.newDebugHoldUntil!).toBeLessThanOrEqual(absoluteMax);
  });

  test("fails for non-COMPLETE allocation", async () => {
    const instance = createTestInstance();
    const alloc = createTestAllocation(instance.id);

    const result = await extendAllocationDebugHold(alloc.id, {
      extensionMs: 300_000,
    });

    expect(result.success).toBe(false);
  });

  test("fails for non-existent allocation", async () => {
    const result = await extendAllocationDebugHold(99999, {
      extensionMs: 300_000,
    });

    expect(result.success).toBe(false);
  });
});

// =============================================================================
// Allocation: Termination Blocking
// =============================================================================

describe("allocation: canTerminateInstance", () => {
  test("returns true when no allocations exist", async () => {
    const instance = createTestInstance();
    expect(await canTerminateInstance(instance.id)).toBe(true);
  });

  test("returns false when debug hold is active", async () => {
    const instance = createTestInstance();
    const alloc = createTestAllocation(instance.id);
    const run = createTestRun();

    claimAllocation(alloc.id, run.id);
    activateAllocation(alloc.id);
    completeAllocation(alloc.id, { debugHoldUntil: Date.now() + 300_000 });

    expect(await canTerminateInstance(instance.id)).toBe(false);
  });

  test("returns true when debug hold has expired", async () => {
    const instance = createTestInstance();
    const alloc = createTestAllocation(instance.id);
    const run = createTestRun();

    claimAllocation(alloc.id, run.id);
    activateAllocation(alloc.id);
    completeAllocation(alloc.id, { debugHoldUntil: Date.now() - 1000 });

    expect(await canTerminateInstance(instance.id)).toBe(true);
  });

  test("returns false when non-terminal allocation exists", async () => {
    const instance = createTestInstance();
    createTestAllocation(instance.id); // AVAILABLE is non-terminal

    expect(await canTerminateInstance(instance.id)).toBe(false);
  });

  test("returns true when all allocations are terminal", async () => {
    const instance = createTestInstance();
    const alloc = createTestAllocation(instance.id);
    const run = createTestRun();

    claimAllocation(alloc.id, run.id);
    activateAllocation(alloc.id);
    completeAllocation(alloc.id);

    expect(await canTerminateInstance(instance.id)).toBe(true);
  });
});

// =============================================================================
// Allocation: Background Reconciliation
// =============================================================================

describe("allocation: reconcileClaimedAllocations", () => {
  test("fails stale CLAIMED allocations", async () => {
    const instance = createTestInstance();
    const alloc = createTestAllocation(instance.id);
    const run = createTestRun();

    claimAllocation(alloc.id, run.id);

    // Make it stale
    const db = getDatabase();
    db.run("UPDATE allocations SET updated_at = ? WHERE id = ?", [
      Date.now() - TIMING.CLAIMED_TIMEOUT_MS - 1000,
      alloc.id,
    ]);

    await reconcileClaimedAllocations();

    const updated = getAllocation(alloc.id)!;
    expect(updated.status).toBe("FAILED");
  });

  test("does not fail fresh CLAIMED allocations", async () => {
    const instance = createTestInstance();
    const alloc = createTestAllocation(instance.id);
    const run = createTestRun();

    claimAllocation(alloc.id, run.id);

    await reconcileClaimedAllocations();

    const updated = getAllocation(alloc.id)!;
    expect(updated.status).toBe("CLAIMED");
  });
});

describe("allocation: holdExpiryTask", () => {
  test("clears expired debug holds", async () => {
    const instance = createTestInstance();
    const alloc = createTestAllocation(instance.id);
    const run = createTestRun();

    claimAllocation(alloc.id, run.id);
    activateAllocation(alloc.id);
    completeAllocation(alloc.id, { debugHoldUntil: Date.now() - 1000 });

    await holdExpiryTask();

    const updated = getAllocation(alloc.id)!;
    expect(updated.debug_hold_until).toBeNull();
  });

  test("does not clear non-expired debug holds", async () => {
    const instance = createTestInstance();
    const alloc = createTestAllocation(instance.id);
    const run = createTestRun();

    claimAllocation(alloc.id, run.id);
    activateAllocation(alloc.id);
    const futureHold = Date.now() + 300_000;
    completeAllocation(alloc.id, { debugHoldUntil: futureHold });

    await holdExpiryTask();

    const updated = getAllocation(alloc.id)!;
    expect(updated.debug_hold_until).toBe(futureHold);
  });
});

// =============================================================================
// Allocation: transitionToComplete + replenishment
// =============================================================================

describe("allocation: transitionToComplete", () => {
  test("completes allocation with debug hold", async () => {
    const instance = createTestInstance();
    const run = createTestRun({ exit_code: 0 });
    const alloc = createTestAllocation(instance.id);

    // Transition through lifecycle: AVAILABLE -> CLAIMED -> ACTIVE
    claimAllocation(alloc.id, run.id);
    activateAllocation(alloc.id);

    await transitionToComplete(alloc.id, {
      exitCode: 0,
      debugHoldDurationMs: 300_000,
    });

    const updated = getAllocation(alloc.id)!;
    expect(updated.status).toBe("COMPLETE");
    expect(updated.debug_hold_until).not.toBeNull();
  });
});

describe("allocation: checkReplenishmentEligibility", () => {
  test("eligible when exit_code=0, fresh heartbeat, capacity available", async () => {
    const instance = createTestInstance();
    const run = createTestRun({ exit_code: 0 });
    const alloc = createTestAllocation(instance.id);

    // Move through lifecycle: AVAILABLE -> CLAIMED -> ACTIVE -> COMPLETE
    claimAllocation(alloc.id, run.id);
    activateAllocation(alloc.id);
    completeAllocation(alloc.id);

    const eligible = await checkReplenishmentEligibility(instance.id, alloc.id);
    expect(eligible).toBe(true);
  });

  test("eligible even when exit_code != 0", async () => {
    const instance = createTestInstance();
    const run = createTestRun({ exit_code: 1 });
    const alloc = createTestAllocation(instance.id);

    claimAllocation(alloc.id, run.id);
    activateAllocation(alloc.id);
    completeAllocation(alloc.id);

    // Non-zero exit code no longer blocks replenishment (A1 fix)
    const eligible = await checkReplenishmentEligibility(instance.id, alloc.id);
    expect(eligible).toBe(true);
  });

  test("not eligible when heartbeat is stale", async () => {
    const staleTime = Date.now() - TIMING.STALE_DETECTION_MS - 1000;
    const instance = createTestInstance({ last_heartbeat: staleTime });
    const run = createTestRun({ exit_code: 0 });
    const alloc = createTestAllocation(instance.id);

    claimAllocation(alloc.id, run.id);
    activateAllocation(alloc.id);
    completeAllocation(alloc.id);

    const eligible = await checkReplenishmentEligibility(instance.id, alloc.id);
    expect(eligible).toBe(false);
  });

  test("not eligible when spot interrupted", async () => {
    const instance = createTestInstance();
    const run = createTestRun({ exit_code: 0, spot_interrupted: 1 });
    const alloc = createTestAllocation(instance.id);

    claimAllocation(alloc.id, run.id);
    activateAllocation(alloc.id);
    completeAllocation(alloc.id);

    const eligible = await checkReplenishmentEligibility(instance.id, alloc.id);
    expect(eligible).toBe(false);
  });
});

// =============================================================================
// Instance: Lifecycle
// =============================================================================

describe("instance: CRUD schema smoke", () => {
  test("create -> read -> update -> list round-trip", () => {
    // Create
    const instance = createInstanceRecord({
      provider: "orbstack",
      provider_id: "test-vm-1",
      spec: "gpu-small",
      region: "us-east-1",
      ip: "10.0.0.1",
      workflow_state: "spawn:pending",
      workflow_error: null,
      current_manifest_id: null,
      spawn_idempotency_key: null,
      is_spot: 0,
      spot_request_id: null,
      init_checksum: null,
      registration_token_hash: null,
      last_heartbeat: Date.now(),
    });
    expect(instance.id).toBeDefined();

    // Read
    const fetched = getInstanceRecord(instance.id);
    expect(fetched).not.toBeNull();
    expect(fetched!.id).toBe(instance.id);
    expect(fetched!.provider).toBe("orbstack");

    // Read miss
    expect(getInstanceRecord(99999)).toBeNull();

    // Update
    const updated = updateInstanceRecord(instance.id, {
      workflow_state: "launch-run:provisioning",
      ip: "10.0.0.2",
    });
    expect(updated.workflow_state).toBe("launch-run:provisioning");
    expect(updated.ip).toBe("10.0.0.2");

    // List + filter
    createTestInstance({ provider: "aws", spec: "gpu-large" });
    const all = listInstanceRecords();
    expect(all.length).toBe(2);
    const filtered = listInstanceRecords({ provider: "orbstack" });
    expect(filtered.length).toBe(1);
    expect(filtered[0].provider).toBe("orbstack");
    const bySpec = listInstanceRecords({ spec: "gpu-small" });
    expect(bySpec.length).toBe(1);
  });
});

// =============================================================================
// Instance: State Queries
// =============================================================================

describe("instance: isInstanceHealthy", () => {
  test("healthy instance with fresh heartbeat and active state", () => {
    const instance = createTestInstance({
      workflow_state: "launch-run:provisioning",
      last_heartbeat: Date.now(),
    });
    expect(isInstanceHealthy(instance)).toBe(true);
  });

  test("unhealthy with stale heartbeat", () => {
    const instance = createTestInstance({
      workflow_state: "launch-run:provisioning",
      last_heartbeat: Date.now() - TIMING.STALE_DETECTION_MS - 1000,
    });
    expect(isInstanceHealthy(instance)).toBe(false);
  });

  test("unhealthy with error state", () => {
    const instance = createTestInstance({
      workflow_state: "spawn:error",
      last_heartbeat: Date.now(),
    });
    expect(isInstanceHealthy(instance)).toBe(false);
  });

  test("unhealthy with terminate:complete state", () => {
    const instance = createTestInstance({
      workflow_state: "terminate:complete",
      last_heartbeat: Date.now(),
    });
    expect(isInstanceHealthy(instance)).toBe(false);
  });
});

describe("instance: getActiveInstances", () => {
  test("returns non-terminal instances", () => {
    createTestInstance({ workflow_state: "spawn:pending" });
    createTestInstance({ workflow_state: "launch-run:provisioning" });
    createTestInstance({ workflow_state: "terminate:complete" });

    const active = getActiveInstances();
    expect(active.length).toBe(2);
  });

  test("excludes error states", () => {
    createTestInstance({ workflow_state: "spawn:pending" });
    createTestInstance({ workflow_state: "spawn:error" });
    createTestInstance({ workflow_state: "spawn:error" });

    const active = getActiveInstances();
    expect(active.length).toBe(1);
  });
});

describe("instance: detectStaleHeartbeats", () => {
  test("returns instances with old heartbeats", () => {
    createTestInstance({
      workflow_state: "launch-run:provisioning",
      last_heartbeat: Date.now() - 600_000,
    });
    createTestInstance({
      workflow_state: "launch-run:provisioning",
      last_heartbeat: Date.now(),
    });

    const stale = detectStaleHeartbeats(300_000);
    expect(stale.length).toBe(1);
  });

  test("detectStaleHeartbeats excludes terminal instances", () => {
    createTestInstance({
      workflow_state: "terminate:complete",
      last_heartbeat: Date.now() - 600_000,
    });

    const stale = detectStaleHeartbeats(300_000);
    expect(stale.length).toBe(0);
  });
});

// =============================================================================
// Run: Lifecycle
// =============================================================================

describe("run: CRUD schema smoke", () => {
  test("create -> read -> update -> list round-trip", () => {
    // Create
    const run = createRunRecord({
      command: "echo hello",
      workdir: "/workspace",
      max_duration_ms: 60000,
      workflow_state: "launch-run:pending",
      workflow_error: null,
      current_manifest_id: null,
      exit_code: null,
      init_checksum: null,
      create_snapshot: 0,
      spot_interrupted: 0,
      started_at: null,
      finished_at: null,
    });
    expect(run.id).toBeDefined();

    // Read
    const fetched = getRunRecord(run.id);
    expect(fetched).not.toBeNull();
    expect(fetched!.id).toBe(run.id);
    expect(fetched!.command).toBe("echo hello");

    // Read miss
    expect(getRunRecord(99999)).toBeNull();

    // Update
    const now = Date.now();
    const updated = updateRunRecord(run.id, {
      workflow_state: "launch-run:complete",
      exit_code: 0,
      finished_at: now,
    });
    expect(updated.workflow_state).toBe("launch-run:complete");
    expect(updated.exit_code).toBe(0);
    expect(updated.finished_at).toBe(now);

    // List + filter
    createTestRun({ workflow_state: "launch-run:pending" });
    const all = listRunRecords();
    expect(all.length).toBe(2);
    const filtered = listRunRecords({ workflow_state: "launch-run:pending" });
    expect(filtered.length).toBe(1);
  });
});

// =============================================================================
// Run: State Queries
// =============================================================================

describe("run: isRunInProgress", () => {
  test("truth table for workflow_state x finished_at", () => {
    const now = Date.now();
    const cases: Array<{ state: string; finished_at: number | null; expected: boolean }> = [
      { state: "launch-run:pending", finished_at: null, expected: true },
      { state: "launch-run:running", finished_at: null, expected: true },
      { state: "launch-run:complete", finished_at: now, expected: false },
      { state: "launch-run:error", finished_at: now, expected: false },
      { state: "launch-run:cancelled", finished_at: now, expected: false },
      // Edge: finished_at set overrides non-terminal state
      { state: "launch-run:running", finished_at: now, expected: false },
    ];
    for (const { state, finished_at, expected } of cases) {
      const run = createTestRun({ workflow_state: state, finished_at });
      expect(isRunInProgress(run)).toBe(expected);
    }
  });
});

describe("run: getRunDuration", () => {
  test("returns finished_at - started_at when both exist", () => {
    const now = Date.now();
    const run = createTestRun({ started_at: now - 5000, finished_at: now });
    expect(getRunDuration(run)).toBe(5000);
  });

  test("returns elapsed time when only started_at exists", () => {
    const started = Date.now() - 10000;
    const run = createTestRun({ started_at: started });
    const duration = getRunDuration(run);
    expect(duration).not.toBeNull();
    expect(duration!).toBeGreaterThanOrEqual(9900); // Allow some tolerance
    expect(duration!).toBeLessThanOrEqual(11000);
  });

  test("returns null when started_at is null", () => {
    const run = createTestRun();
    expect(getRunDuration(run)).toBeNull();
  });
});
