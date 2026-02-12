// tests/unit/resource-layer.test.ts - Resource Layer Tests
// Tests for allocation.ts, instance.ts, and run.ts resource modules

import { describe, test, expect, beforeEach } from "bun:test";
import {
  initDatabase,
  runMigrations,
  getDatabase,
  createAllocation,
  createInstance,
  getAllocation,
  getInstance,
  createRun,
  getRun,
  queryMany,
  execute,
  type Allocation,
  type Instance,
  type Run,
} from "../../control/src/material/db";
import {
  claimAllocation,
  activateAllocation,
  completeAllocation,
} from "../../control/src/workflow/state-transitions";
import { TIMING } from "../../shared/src/config/timing";

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
  VALID_TRANSITIONS,
} from "../../control/src/resource/allocation";

import {
  createInstanceRecord,
  getInstanceRecord,
  updateInstanceRecord,
  listInstanceRecords,
  isInstanceHealthy,
  getInstancesByProvider,
  getActiveInstances,
  getStaleInstances,
  updateHeartbeat,
  detectStaleHeartbeats,
} from "../../control/src/resource/instance";

import {
  createRunRecord,
  getRunRecord,
  updateRunRecord,
  listRunRecords,
  getActiveRuns,
  getRunsByInstance,
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
    workflow_state: overrides?.workflow_state ?? "spawn:complete",
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

beforeEach(() => {
  initDatabase(":memory:");
  runMigrations();
});

// =============================================================================
// Allocation: State Validation
// =============================================================================

describe("allocation: validateAllocationTransition", () => {
  test("AVAILABLE -> CLAIMED is valid", () => {
    const result = validateAllocationTransition("AVAILABLE", "CLAIMED");
    expect(result.valid).toBe(true);
    expect(result.error).toBeUndefined();
  });

  test("AVAILABLE -> ACTIVE is invalid", () => {
    const result = validateAllocationTransition("AVAILABLE", "ACTIVE");
    expect(result.valid).toBe(false);
    expect(result.error).toBeDefined();
    expect(result.error).toContain("Invalid transition");
  });

  test("CLAIMED -> ACTIVE is valid", () => {
    const result = validateAllocationTransition("CLAIMED", "ACTIVE");
    expect(result.valid).toBe(true);
  });

  test("CLAIMED -> FAILED is valid", () => {
    const result = validateAllocationTransition("CLAIMED", "FAILED");
    expect(result.valid).toBe(true);
  });

  test("ACTIVE -> COMPLETE is valid", () => {
    const result = validateAllocationTransition("ACTIVE", "COMPLETE");
    expect(result.valid).toBe(true);
  });

  test("COMPLETE -> anything is invalid (terminal state)", () => {
    const result = validateAllocationTransition("COMPLETE", "AVAILABLE");
    expect(result.valid).toBe(false);
    expect(result.error).toContain("terminal state");
  });

  test("FAILED -> anything is invalid (terminal state)", () => {
    const result = validateAllocationTransition("FAILED", "ACTIVE");
    expect(result.valid).toBe(false);
    expect(result.error).toContain("terminal state");
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
  test("AVAILABLE is in warm pool", () => {
    expect(isInWarmPool("AVAILABLE")).toBe(true);
  });

  test("CLAIMED is not in warm pool", () => {
    expect(isInWarmPool("CLAIMED")).toBe(false);
  });

  test("ACTIVE is not in warm pool", () => {
    expect(isInWarmPool("ACTIVE")).toBe(false);
  });

  test("COMPLETE is not in warm pool", () => {
    expect(isInWarmPool("COMPLETE")).toBe(false);
  });
});

// =============================================================================
// Allocation: Warm Pool Operations
// =============================================================================

describe("allocation: createAvailableAllocation", () => {
  test("creates AVAILABLE allocation with auto-generated workdir", async () => {
    const instance = createTestInstance();
    const alloc = await createAvailableAllocation(instance.id);

    expect(alloc.status).toBe("AVAILABLE");
    expect(alloc.run_id).toBeNull();
    expect(alloc.instance_id).toBe(instance.id);
    expect(alloc.user).toBe("default");
    expect(alloc.workdir).toBe("work_0");
    expect(alloc.debug_hold_until).toBeNull();
    expect(alloc.completed_at).toBeNull();
  });

  test("auto-increments workdir counter based on non-terminal count", async () => {
    const instance = createTestInstance();
    const alloc1 = await createAvailableAllocation(instance.id);
    expect(alloc1.workdir).toBe("work_0");

    // With one non-terminal allocation existing, next should be work_1
    const alloc2 = await createAvailableAllocation(instance.id);
    expect(alloc2.workdir).toBe("work_1");
  });

  test("respects user and workdir overrides", async () => {
    const instance = createTestInstance();
    const alloc = await createAvailableAllocation(instance.id, {
      user: "ubuntu",
      workdir: "/custom/path",
    });

    expect(alloc.user).toBe("ubuntu");
    expect(alloc.workdir).toBe("/custom/path");
  });

  test("sets current_manifest_id to null when not provided", async () => {
    const instance = createTestInstance();
    const alloc = await createAvailableAllocation(instance.id);
    expect(alloc.current_manifest_id).toBeNull();
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

    const result = await claimWarmPoolAllocation(
      { spec: "gpu-small" },
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
      { spec: "gpu-small" },
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

  test("not eligible when exit_code != 0", async () => {
    const instance = createTestInstance();
    const run = createTestRun({ exit_code: 1 });
    const alloc = createTestAllocation(instance.id);

    claimAllocation(alloc.id, run.id);
    activateAllocation(alloc.id);
    completeAllocation(alloc.id);

    const eligible = await checkReplenishmentEligibility(instance.id, alloc.id);
    expect(eligible).toBe(false);
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

describe("instance: CRUD operations", () => {
  test("createInstanceRecord creates and returns instance", () => {
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
    expect(instance.provider).toBe("orbstack");
    expect(instance.spec).toBe("gpu-small");
  });

  test("getInstanceRecord returns instance by id", () => {
    const created = createTestInstance();
    const fetched = getInstanceRecord(created.id);
    expect(fetched).not.toBeNull();
    expect(fetched!.id).toBe(created.id);
  });

  test("getInstanceRecord returns null for non-existent id", () => {
    const result = getInstanceRecord(99999);
    expect(result).toBeNull();
  });

  test("updateInstanceRecord updates fields", () => {
    const instance = createTestInstance();
    const updated = updateInstanceRecord(instance.id, {
      workflow_state: "spawn:complete",
      ip: "10.0.0.2",
    });
    expect(updated.workflow_state).toBe("spawn:complete");
    expect(updated.ip).toBe("10.0.0.2");
  });

  test("listInstanceRecords with no filter returns all", () => {
    createTestInstance({ provider: "orbstack" });
    createTestInstance({ provider: "aws" });
    const all = listInstanceRecords();
    expect(all.length).toBe(2);
  });

  test("listInstanceRecords filters by provider", () => {
    createTestInstance({ provider: "orbstack" });
    createTestInstance({ provider: "aws" });
    const filtered = listInstanceRecords({ provider: "orbstack" });
    expect(filtered.length).toBe(1);
    expect(filtered[0].provider).toBe("orbstack");
  });

  test("listInstanceRecords filters by spec", () => {
    createTestInstance({ spec: "gpu-small" });
    createTestInstance({ spec: "gpu-large" });
    const filtered = listInstanceRecords({ spec: "gpu-small" });
    expect(filtered.length).toBe(1);
    expect(filtered[0].spec).toBe("gpu-small");
  });
});

// =============================================================================
// Instance: State Queries
// =============================================================================

describe("instance: isInstanceHealthy", () => {
  test("healthy instance with fresh heartbeat and active state", () => {
    const instance = createTestInstance({
      workflow_state: "spawn:complete",
      last_heartbeat: Date.now(),
    });
    expect(isInstanceHealthy(instance)).toBe(true);
  });

  test("unhealthy with stale heartbeat", () => {
    const instance = createTestInstance({
      workflow_state: "spawn:complete",
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

  test("unhealthy with compensated state", () => {
    const instance = createTestInstance({
      workflow_state: "spawn:compensated",
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

describe("instance: getInstancesByProvider", () => {
  test("returns instances for a specific provider", () => {
    createTestInstance({ provider: "orbstack" });
    createTestInstance({ provider: "orbstack" });
    createTestInstance({ provider: "aws" });

    const orbstack = getInstancesByProvider("orbstack");
    expect(orbstack.length).toBe(2);

    const aws = getInstancesByProvider("aws");
    expect(aws.length).toBe(1);
  });
});

describe("instance: getActiveInstances", () => {
  test("returns non-terminal instances", () => {
    createTestInstance({ workflow_state: "spawn:pending" });
    createTestInstance({ workflow_state: "spawn:complete" });
    createTestInstance({ workflow_state: "terminate:complete" });

    const active = getActiveInstances();
    expect(active.length).toBe(2);
  });

  test("excludes error and compensated states", () => {
    createTestInstance({ workflow_state: "spawn:pending" });
    createTestInstance({ workflow_state: "spawn:error" });
    createTestInstance({ workflow_state: "spawn:compensated" });

    const active = getActiveInstances();
    expect(active.length).toBe(1);
  });
});

describe("instance: getStaleInstances", () => {
  test("returns instances with stale heartbeats", () => {
    createTestInstance({
      workflow_state: "spawn:complete",
      last_heartbeat: Date.now() - 600_000,
    });
    createTestInstance({
      workflow_state: "spawn:complete",
      last_heartbeat: Date.now(),
    });

    const stale = getStaleInstances(300_000);
    expect(stale.length).toBe(1);
  });

  test("excludes terminal instances even if stale", () => {
    createTestInstance({
      workflow_state: "terminate:complete",
      last_heartbeat: Date.now() - 600_000,
    });

    const stale = getStaleInstances(300_000);
    expect(stale.length).toBe(0);
  });
});

// =============================================================================
// Instance: Heartbeat
// =============================================================================

describe("instance: heartbeat operations", () => {
  test("updateHeartbeat updates the heartbeat timestamp", () => {
    const instance = createTestInstance({ last_heartbeat: 1000 });
    const newTimestamp = Date.now();
    updateHeartbeat(instance.id, newTimestamp);

    const updated = getInstance(instance.id)!;
    expect(updated.last_heartbeat).toBe(newTimestamp);
  });

  test("detectStaleHeartbeats returns instances with old heartbeats", () => {
    createTestInstance({
      workflow_state: "spawn:complete",
      last_heartbeat: Date.now() - 600_000,
    });
    createTestInstance({
      workflow_state: "spawn:complete",
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

describe("run: CRUD operations", () => {
  test("createRunRecord creates and returns run", () => {
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
    expect(run.command).toBe("echo hello");
    expect(run.workflow_state).toBe("launch-run:pending");
  });

  test("getRunRecord returns run by id", () => {
    const created = createTestRun();
    const fetched = getRunRecord(created.id);
    expect(fetched).not.toBeNull();
    expect(fetched!.id).toBe(created.id);
  });

  test("getRunRecord returns null for non-existent id", () => {
    const result = getRunRecord(99999);
    expect(result).toBeNull();
  });

  test("updateRunRecord updates fields", () => {
    const run = createTestRun();
    const now = Date.now();
    const updated = updateRunRecord(run.id, {
      workflow_state: "launch-run:complete",
      exit_code: 0,
      finished_at: now,
    });
    expect(updated.workflow_state).toBe("launch-run:complete");
    expect(updated.exit_code).toBe(0);
    expect(updated.finished_at).toBe(now);
  });

  test("listRunRecords with no filter returns all", () => {
    createTestRun();
    createTestRun();
    const all = listRunRecords();
    expect(all.length).toBe(2);
  });

  test("listRunRecords filters by workflow_state", () => {
    createTestRun({ workflow_state: "launch-run:pending" });
    createTestRun({ workflow_state: "launch-run:complete" });
    const filtered = listRunRecords({ workflow_state: "launch-run:pending" });
    expect(filtered.length).toBe(1);
    expect(filtered[0].workflow_state).toBe("launch-run:pending");
  });
});

// =============================================================================
// Run: State Queries
// =============================================================================

describe("run: getActiveRuns", () => {
  test("returns runs not in terminal states", () => {
    createTestRun({ workflow_state: "launch-run:pending" });
    createTestRun({ workflow_state: "launch-run:running" });
    createTestRun({ workflow_state: "launch-run:complete" });
    createTestRun({ workflow_state: "launch-run:error" });

    const active = getActiveRuns();
    expect(active.length).toBe(2);
  });
});

describe("run: getRunsByInstance", () => {
  test("returns runs associated with instance via allocations", () => {
    const instance = createTestInstance();
    const run1 = createTestRun();
    const run2 = createTestRun();

    // Create allocations linking runs to instance
    createAllocation({
      run_id: run1.id,
      instance_id: instance.id,
      status: "ACTIVE",
      current_manifest_id: null,
      user: "default",
      workdir: "/workspace",
      debug_hold_until: null,
      completed_at: null,
    });
    createAllocation({
      run_id: run2.id,
      instance_id: instance.id,
      status: "COMPLETE",
      current_manifest_id: null,
      user: "default",
      workdir: "/workspace",
      debug_hold_until: null,
      completed_at: Date.now(),
    });

    const runs = getRunsByInstance(instance.id);
    expect(runs.length).toBe(2);
  });

  test("returns empty array when no runs for instance", () => {
    const instance = createTestInstance();
    const runs = getRunsByInstance(instance.id);
    expect(runs.length).toBe(0);
  });
});

describe("run: isRunInProgress", () => {
  test("returns true for pending run", () => {
    const run = createTestRun({ workflow_state: "launch-run:pending" });
    expect(isRunInProgress(run)).toBe(true);
  });

  test("returns true for running run", () => {
    const run = createTestRun({ workflow_state: "launch-run:running" });
    expect(isRunInProgress(run)).toBe(true);
  });

  test("returns false for completed run", () => {
    const run = createTestRun({ workflow_state: "launch-run:complete", finished_at: Date.now() });
    expect(isRunInProgress(run)).toBe(false);
  });

  test("returns false for error run", () => {
    const run = createTestRun({ workflow_state: "launch-run:error", finished_at: Date.now() });
    expect(isRunInProgress(run)).toBe(false);
  });

  test("returns false for cancelled run", () => {
    const run = createTestRun({ workflow_state: "launch-run:cancelled", finished_at: Date.now() });
    expect(isRunInProgress(run)).toBe(false);
  });

  test("returns false if finished_at is set even with non-terminal state", () => {
    const run = createTestRun({ workflow_state: "launch-run:running", finished_at: Date.now() });
    expect(isRunInProgress(run)).toBe(false);
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
