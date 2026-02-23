// tests/unit/reconciliation.test.ts - Allocation Reconciliation Tests (#LIFE-11)
// Covers all 6 reconciliation subtasks per spec section 5.5.

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest } from "../harness";
import {
  createInstance,
  createAllocation,
  createRun,
  getAllocation,
  execute,
  type Allocation,
} from "../../control/src/material/db";
import { runReconciliation } from "../../control/src/background/reconciliation";
import { TIMING } from "@skyrepl/contracts";
import { registerProvider } from "../../control/src/provider/registry";
import type { Provider, ProviderCapabilities, SpawnOptions, BootstrapConfig, BootstrapScript, ListFilter } from "../../control/src/provider/types";

// =============================================================================
// Top-level test setup
// =============================================================================

let cleanup: () => Promise<void>;
beforeEach(() => { cleanup = setupTest(); });
afterEach(() => cleanup());

// =============================================================================
// Test Helpers
// =============================================================================

function createTestInstance(
  lastHeartbeat: number,
  workflowState = "launch-run:provisioning"
) {
  return createInstance({
    provider: "test",
    provider_id: `test-${Date.now()}-${Math.random()}`,
    spec: "test-spec",
    region: "local",
    ip: "10.0.0.1",
    workflow_state: workflowState,
    workflow_error: null,
    current_manifest_id: null,
    spawn_idempotency_key: null,
    is_spot: 0,
    spot_request_id: null,
    init_checksum: null,
    registration_token_hash: null,
    last_heartbeat: lastHeartbeat,
  });
}

function createTestAllocation(
  instanceId: number,
  status: "AVAILABLE" | "CLAIMED" | "ACTIVE" | "COMPLETE" | "FAILED",
  options?: {
    runId?: number | null;
    completedAt?: number | null;
  }
) {
  return createAllocation({
    run_id: options?.runId ?? null,
    instance_id: instanceId,
    status,
    current_manifest_id: null,
    user: "test",
    workdir: "/tmp/test",
    debug_hold_until: null,
    completed_at: options?.completedAt ?? null,
  });
}

function createTestRun(options?: {
  finishedAt?: number | null;
  workflowState?: string;
}) {
  return createRun({
    command: "echo test",
    workdir: "/tmp/test",
    max_duration_ms: 3600000,
    workflow_state: options?.workflowState ?? "launch-run:running",
    workflow_error: null,
    current_manifest_id: null,
    exit_code: null,
    init_checksum: null,
    create_snapshot: 0,
    spot_interrupted: 0,
    started_at: Date.now(),
    finished_at: options?.finishedAt ?? null,
  });
}

// =============================================================================
// Subtask 1: CLAIMED Timeout
// =============================================================================

describe("Reconciliation - CLAIMED Timeout", () => {
  test("CLAIMED allocation past timeout -> FAILED", async () => {
    const instance = createTestInstance(Date.now());
    const alloc = createTestAllocation(instance.id, "CLAIMED");

    // Backdate updated_at to be past the CLAIMED timeout
    const pastCutoff = Date.now() - TIMING.CLAIMED_TIMEOUT_MS - 1000;
    execute("UPDATE allocations SET updated_at = ? WHERE id = ?", [
      pastCutoff,
      alloc.id,
    ]);

    const result = await runReconciliation();

    expect(result.claimedTimeout).toBe(1);
    expect(getAllocation(alloc.id)?.status).toBe("FAILED");
  });

  test("recent CLAIMED allocation -> not touched", async () => {
    const instance = createTestInstance(Date.now());
    const alloc = createTestAllocation(instance.id, "CLAIMED");

    const result = await runReconciliation();

    expect(result.claimedTimeout).toBe(0);
    expect(getAllocation(alloc.id)?.status).toBe("CLAIMED");
  });
});

// =============================================================================
// Subtask 2: Warm Pool Health
// =============================================================================

describe("Reconciliation - Warm Pool Health", () => {
  test("AVAILABLE with stale heartbeat -> FAILED", async () => {
    const staleHeartbeat = Date.now() - TIMING.STALE_DETECTION_MS - 1000;
    const instance = createTestInstance(staleHeartbeat);
    const alloc = createTestAllocation(instance.id, "AVAILABLE");

    const result = await runReconciliation();

    expect(result.warmPoolHealth).toBe(1);
    expect(getAllocation(alloc.id)?.status).toBe("FAILED");
  });

  test("AVAILABLE with fresh heartbeat -> not touched", async () => {
    const freshHeartbeat = Date.now();
    const instance = createTestInstance(freshHeartbeat);
    const alloc = createTestAllocation(instance.id, "AVAILABLE");

    const result = await runReconciliation();

    expect(result.warmPoolHealth).toBe(0);
    expect(getAllocation(alloc.id)?.status).toBe("AVAILABLE");
  });
});

// =============================================================================
// Subtask 3: Orphaned Claims
// =============================================================================

describe("Reconciliation - Orphaned Claims", () => {
  test("CLAIMED with finished run -> FAILED", async () => {
    const instance = createTestInstance(Date.now());
    const run = createTestRun({ finishedAt: Date.now() - 5000 });
    const alloc = createTestAllocation(instance.id, "CLAIMED", {
      runId: run.id,
    });

    const result = await runReconciliation();

    expect(result.orphanedClaims).toBe(1);
    expect(getAllocation(alloc.id)?.status).toBe("FAILED");
  });

  test("CLAIMED with running run -> not touched", async () => {
    const instance = createTestInstance(Date.now());
    const run = createTestRun({ finishedAt: null });
    const alloc = createTestAllocation(instance.id, "CLAIMED", {
      runId: run.id,
    });

    const result = await runReconciliation();

    expect(result.orphanedClaims).toBe(0);
    expect(getAllocation(alloc.id)?.status).toBe("CLAIMED");
  });

  test("CLAIMED with no run_id -> not caught by orphaned claims", async () => {
    const instance = createTestInstance(Date.now());
    // CLAIMED but no run_id (no JOIN match)
    const alloc = createTestAllocation(instance.id, "CLAIMED", {
      runId: null,
    });

    const result = await runReconciliation();

    // Orphaned claims requires a run_id JOIN, so this won't match
    expect(result.orphanedClaims).toBe(0);
  });
});

// =============================================================================
// Subtask 4: Stalled Transitions
// =============================================================================

describe("Reconciliation - Stalled Transitions", () => {
  test("ACTIVE with finished run -> COMPLETE", async () => {
    const instance = createTestInstance(Date.now());
    const run = createTestRun({ finishedAt: Date.now() - 5000 });
    const alloc = createTestAllocation(instance.id, "ACTIVE", {
      runId: run.id,
    });

    const result = await runReconciliation();

    expect(result.stalledTransitions).toBe(1);
    expect(getAllocation(alloc.id)?.status).toBe("COMPLETE");
  });

  test("ACTIVE with running run -> not touched", async () => {
    const instance = createTestInstance(Date.now());
    const run = createTestRun({ finishedAt: null });
    const alloc = createTestAllocation(instance.id, "ACTIVE", {
      runId: run.id,
    });

    const result = await runReconciliation();

    expect(result.stalledTransitions).toBe(0);
    expect(getAllocation(alloc.id)?.status).toBe("ACTIVE");
  });
});

// =============================================================================
// Subtask 5: Provider State Sync
// =============================================================================

describe("Reconciliation - Provider State Sync", () => {
  test("allocation on terminated instance -> FAILED", async () => {
    const instance = createTestInstance(Date.now(), "terminate:complete");
    const alloc = createTestAllocation(instance.id, "ACTIVE");

    const result = await runReconciliation();

    expect(result.providerStateSync).toBe(1);
    expect(getAllocation(alloc.id)?.status).toBe("FAILED");
  });

  test("AVAILABLE on terminated instance -> FAILED", async () => {
    const instance = createTestInstance(Date.now(), "terminate:running");
    const alloc = createTestAllocation(instance.id, "AVAILABLE");

    const result = await runReconciliation();

    expect(result.providerStateSync).toBe(1);
    expect(getAllocation(alloc.id)?.status).toBe("FAILED");
  });

  test("allocation on healthy instance -> not touched", async () => {
    const instance = createTestInstance(Date.now(), "launch-run:provisioning");
    const alloc = createTestAllocation(instance.id, "ACTIVE");

    const result = await runReconciliation();

    expect(result.providerStateSync).toBe(0);
    expect(getAllocation(alloc.id)?.status).toBe("ACTIVE");
  });

  test("already terminal allocation on terminated instance -> not double-processed", async () => {
    const instance = createTestInstance(Date.now(), "terminate:complete");
    const alloc = createTestAllocation(instance.id, "FAILED", {
      completedAt: Date.now() - 5000,
    });

    const result = await runReconciliation();

    // Should not be counted since it's already FAILED
    expect(result.providerStateSync).toBe(0);
    expect(getAllocation(alloc.id)?.status).toBe("FAILED");
  });

  test("E1: discovers externally-terminated instance via materializer", async () => {
    // Instance appears alive in DB but provider says it's gone.
    // reconcileProviderStateSync materializes the instance, which triggers
    // mark_terminated. Then the allocation cleanup catches it.
    const instance = createTestInstance(Date.now(), "launch-run:provisioning");
    const alloc = createTestAllocation(instance.id, "ACTIVE");

    // Register a provider that says this instance doesn't exist
    await registerProvider({
      provider: {
        name: "test" as any,
        capabilities: { snapshots: false, spot: false, gpu: false, multiRegion: false, persistentVolumes: false, warmVolumes: false, hibernation: false, costExplorer: false, tailscaleNative: false, idempotentSpawn: true, customNetworking: false } as ProviderCapabilities,
        async spawn() { return { id: "mock", status: "running", spec: "test-spec", ip: "10.0.0.1", createdAt: Date.now(), isSpot: false }; },
        async terminate() {},
        async list() { return []; },
        async get() { return null; }, // instance gone from provider
        generateBootstrap() { return { content: "#!/bin/bash", format: "shell" as const, checksum: "mock" }; },
      } as Provider,
    });

    const result = await runReconciliation();

    // mark_terminated should have fired, then allocation cleanup catches it
    expect(result.providerStateSync).toBe(1);
    expect(getAllocation(alloc.id)?.status).toBe("FAILED");
  });
});

// =============================================================================
// Subtask 6: Allocation Aging
// =============================================================================

describe("Reconciliation - Allocation Aging", () => {
  test("old AVAILABLE -> FAILED", async () => {
    const instance = createTestInstance(Date.now());
    const alloc = createTestAllocation(instance.id, "AVAILABLE");

    // Backdate created_at to be past the warm pool expiry
    const pastExpiry = Date.now() - TIMING.WARM_POOL_EXPIRY_MS - 1000;
    execute("UPDATE allocations SET created_at = ? WHERE id = ?", [
      pastExpiry,
      alloc.id,
    ]);

    const result = await runReconciliation();

    expect(result.allocationAging).toBe(1);
    expect(getAllocation(alloc.id)?.status).toBe("FAILED");
  });

  test("fresh AVAILABLE -> not touched", async () => {
    const instance = createTestInstance(Date.now());
    const alloc = createTestAllocation(instance.id, "AVAILABLE");

    const result = await runReconciliation();

    expect(result.allocationAging).toBe(0);
    expect(getAllocation(alloc.id)?.status).toBe("AVAILABLE");
  });
});

// =============================================================================
// Full Reconciliation Integration
// =============================================================================

describe("Reconciliation - Full Run", () => {
  test("runReconciliation returns correct counts for mixed scenarios", async () => {
    const now = Date.now();

    // 1. CLAIMED timeout: stale claimed allocation
    const inst1 = createTestInstance(now);
    const claimedAlloc = createTestAllocation(inst1.id, "CLAIMED");
    execute("UPDATE allocations SET updated_at = ? WHERE id = ?", [
      now - TIMING.CLAIMED_TIMEOUT_MS - 1000,
      claimedAlloc.id,
    ]);

    // 2. Provider state sync: allocation on terminated instance
    const inst2 = createTestInstance(now, "terminate:complete");
    createTestAllocation(inst2.id, "ACTIVE");

    // 3. Stalled transition: ACTIVE with finished run
    const inst3 = createTestInstance(now);
    const finishedRun = createTestRun({ finishedAt: now - 5000 });
    createTestAllocation(inst3.id, "ACTIVE", {
      runId: finishedRun.id,
    });

    // 4. Healthy allocation that should not be touched
    const inst4 = createTestInstance(now);
    const runningRun = createTestRun({ finishedAt: null });
    const healthyAlloc = createTestAllocation(inst4.id, "ACTIVE", {
      runId: runningRun.id,
    });

    const result = await runReconciliation();

    // Verify counts
    expect(result.claimedTimeout).toBe(1);
    expect(result.providerStateSync).toBe(1);
    expect(result.stalledTransitions).toBe(1);
    expect(result.warmPoolHealth).toBe(0);
    expect(result.orphanedClaims).toBe(0);
    expect(result.allocationAging).toBe(0);

    // Healthy allocation untouched
    expect(getAllocation(healthyAlloc.id)?.status).toBe("ACTIVE");
  });

  test("already terminal allocations are not double-processed", async () => {
    const instance = createTestInstance(Date.now());

    // Create an allocation that's already COMPLETE
    const alloc = createTestAllocation(instance.id, "COMPLETE", {
      completedAt: Date.now() - 60000,
    });

    const result = await runReconciliation();

    // Nothing should be processed
    const total = Object.values(result).reduce((a, b) => a + b, 0);
    expect(total).toBe(0);
    expect(getAllocation(alloc.id)?.status).toBe("COMPLETE");
  });

  test("empty database produces zero counts", async () => {
    const result = await runReconciliation();

    expect(result.claimedTimeout).toBe(0);
    expect(result.warmPoolHealth).toBe(0);
    expect(result.orphanedClaims).toBe(0);
    expect(result.stalledTransitions).toBe(0);
    expect(result.providerStateSync).toBe(0);
    expect(result.allocationAging).toBe(0);
  });
});
