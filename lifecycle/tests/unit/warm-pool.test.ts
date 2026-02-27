// tests/unit/warm-pool.test.ts - Warm Pool Lifecycle Tests (EX6 + BT5)

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest } from "../harness";
import {
  getDatabase,
  createAllocation,
  createInstance,
  getAllocation,
  findWarmAllocation,
  countInstanceAllocations,
  createWorkflow,
  createWorkflowNode,
  createManifest,
  updateWorkflow,
  createRun,
  queryMany,
  type Allocation,
  type Instance,
} from "../../control/src/material/db";
import {
  claimAllocation,
  failAllocation,
  activateAllocation,
  completeAllocation,
} from "../../control/src/workflow/state-transitions";
import {
  registerProvider,
  clearProviderCache,
} from "../../control/src/provider/registry";
import type { Provider } from "../../control/src/provider/types";

// =============================================================================
// Test Helpers
// =============================================================================

function createTestInstance(overrides: Partial<{
  spec: string;
  region: string;
  init_checksum: string | null;
  workflow_state: string;
  last_heartbeat: number;
}>): Instance {
  const now = Date.now();
  return createInstance({
    provider: "orbstack",
    provider_id: `test-vm-${now}-${Math.random().toString(36).slice(2, 8)}`,
    spec: overrides.spec ?? "gpu-small",
    region: overrides.region ?? "us-east-1",
    ip: "10.0.0.1",
    workflow_state: overrides.workflow_state ?? "launch-run:provisioning",
    workflow_error: null,
    current_manifest_id: null,
    spawn_idempotency_key: null,
    is_spot: 0,
    spot_request_id: null,
    init_checksum: overrides.init_checksum !== undefined ? overrides.init_checksum : null,
    registration_token_hash: null,
    last_heartbeat: overrides.last_heartbeat ?? now,
    provider_metadata: null,
    display_name: null,
  });
}

function createAvailableAllocation(instanceId: number): Allocation {
  return createAllocation({
    run_id: null,
    instance_id: instanceId,
    status: "AVAILABLE",
    current_manifest_id: null,
    user: "default",
    workdir: "/home/user",
    debug_hold_until: null,
    completed_at: null,
  });
}

function createTestRun(): number {
  const run = createRun({
    command: "echo test",
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
  return run.id;
}

// =============================================================================
// Setup
// =============================================================================

let cleanup: () => Promise<void>;

// No-op provider to prevent finalize from calling real orbctl terminate
const mockOrbstack: Provider = {
  name: "orbstack" as any,
  capabilities: {} as any,
  spawn: async () => { throw new Error("not implemented"); },
  terminate: async () => {},
  list: async () => [],
  get: async (id: string) => ({ id, status: "running" as const, spec: "gpu-small", ip: "10.0.0.1", createdAt: Date.now(), isSpot: false }),
  generateBootstrap: () => ({ content: "", format: "shell" as const, checksum: "" }),
  createSnapshot: async () => ({ requestId: "", status: { status: "available" as const, providerSnapshotId: "" } }),
  getSnapshotStatus: async () => ({ status: "available" as const, providerSnapshotId: "" }),
  deleteSnapshot: async () => {},
  getSnapshotByName: async () => null,
};

beforeEach(async () => {
  cleanup = setupTest();
  clearProviderCache();
  await registerProvider({ provider: mockOrbstack });
});

afterEach(() => cleanup());

// =============================================================================
// BT5: findWarmAllocation Scoring
// =============================================================================

describe("BT5: findWarmAllocation scoring", () => {
  test("returns null when no AVAILABLE allocations exist", () => {
    const result = findWarmAllocation({ spec: "gpu-small", tenantId: 1 });
    expect(result).toBeNull();
  });

  test("returns allocation matching spec", () => {
    const instance = createTestInstance({ spec: "gpu-small" });
    const alloc = createAvailableAllocation(instance.id);

    const result = findWarmAllocation({ spec: "gpu-small", tenantId: 1 });
    expect(result).not.toBeNull();
    expect(result!.id).toBe(alloc.id);
  });

  test("does not return allocation with mismatching spec", () => {
    const instance = createTestInstance({ spec: "gpu-large" });
    createAvailableAllocation(instance.id);

    const result = findWarmAllocation({ spec: "gpu-small", tenantId: 1 });
    expect(result).toBeNull();
  });

  test("filters by region when specified", () => {
    const instance1 = createTestInstance({ spec: "gpu-small", region: "us-east-1" });
    const instance2 = createTestInstance({ spec: "gpu-small", region: "eu-west-1" });
    createAvailableAllocation(instance1.id);
    const alloc2 = createAvailableAllocation(instance2.id);

    const result = findWarmAllocation({ spec: "gpu-small", tenantId: 1, region: "eu-west-1" });
    expect(result).not.toBeNull();
    expect(result!.id).toBe(alloc2.id);
  });

  test("scores 100 for exact init_checksum match", () => {
    const instanceExact = createTestInstance({
      spec: "gpu-small",
      init_checksum: "abc123",
    });
    const instanceVanilla = createTestInstance({
      spec: "gpu-small",
      init_checksum: null,
    });
    // Create vanilla first (older FIFO) — but exact match should win via score
    const allocVanilla = createAvailableAllocation(instanceVanilla.id);
    const allocExact = createAvailableAllocation(instanceExact.id);

    const result = findWarmAllocation({ spec: "gpu-small", tenantId: 1 }, "abc123");
    expect(result).not.toBeNull();
    expect(result!.id).toBe(allocExact.id);
  });

  test("scores 50 for NULL init_checksum (vanilla instance)", () => {
    const instanceVanilla = createTestInstance({
      spec: "gpu-small",
      init_checksum: null,
    });
    const alloc = createAvailableAllocation(instanceVanilla.id);

    const result = findWarmAllocation({ spec: "gpu-small", tenantId: 1 }, "abc123");
    expect(result).not.toBeNull();
    expect(result!.id).toBe(alloc.id);
  });

  test("excludes mismatching init_checksum (score 0)", () => {
    const instanceMismatch = createTestInstance({
      spec: "gpu-small",
      init_checksum: "different789",
    });
    createAvailableAllocation(instanceMismatch.id);

    const result = findWarmAllocation({ spec: "gpu-small", tenantId: 1 }, "abc123");
    expect(result).toBeNull();
  });

  test("FIFO tiebreaker: oldest first among equal scores", () => {
    const instance1 = createTestInstance({
      spec: "gpu-small",
      init_checksum: "abc123",
    });
    const instance2 = createTestInstance({
      spec: "gpu-small",
      init_checksum: "abc123",
    });
    const alloc1 = createAvailableAllocation(instance1.id);
    const alloc2 = createAvailableAllocation(instance2.id);

    const result = findWarmAllocation({ spec: "gpu-small", tenantId: 1 }, "abc123");
    expect(result).not.toBeNull();
    expect(result!.id).toBe(alloc1.id); // First created = FIFO
  });

  test("prefers exact match over vanilla across different FIFO order", () => {
    // Vanilla created first (older), but exact match should rank higher (score 100 > 50)
    const instanceVanilla = createTestInstance({
      spec: "gpu-small",
      init_checksum: null,
    });
    const instanceExact = createTestInstance({
      spec: "gpu-small",
      init_checksum: "abc123",
    });
    createAvailableAllocation(instanceVanilla.id);
    const allocExact = createAvailableAllocation(instanceExact.id);

    const result = findWarmAllocation({ spec: "gpu-small", tenantId: 1 }, "abc123");
    expect(result).not.toBeNull();
    expect(result!.id).toBe(allocExact.id);
  });

  test("only considers AVAILABLE allocations (not CLAIMED/ACTIVE)", () => {
    const instance = createTestInstance({ spec: "gpu-small" });
    const alloc = createAvailableAllocation(instance.id);
    const runId = createTestRun();

    // Claim the allocation
    claimAllocation(alloc.id, runId);

    const result = findWarmAllocation({ spec: "gpu-small", tenantId: 1 });
    expect(result).toBeNull();
  });

  test("only considers allocations with run_id IS NULL", () => {
    const instance = createTestInstance({ spec: "gpu-small" });
    const runId = createTestRun();
    const db = getDatabase();
    // Insert allocation with run_id set (not a warm allocation)
    db.run(
      `INSERT INTO allocations (run_id, instance_id, status, user, workdir, created_at, updated_at)
       VALUES (?, ?, 'AVAILABLE', 'default', '/home/user', ?, ?)`,
      [runId, instance.id, Date.now(), Date.now()]
    );

    const result = findWarmAllocation({ spec: "gpu-small", tenantId: 1 });
    expect(result).toBeNull();
  });

  test("only considers instances with workflow_state ending in :complete or launch-run:provisioning", () => {
    const instance = createTestInstance({
      spec: "gpu-small",
      workflow_state: "spawn:pending",
    });
    createAvailableAllocation(instance.id);

    const result = findWarmAllocation({ spec: "gpu-small", tenantId: 1 });
    expect(result).toBeNull();
  });

  test("no initChecksum: returns any AVAILABLE allocation (FIFO)", () => {
    const instance1 = createTestInstance({
      spec: "gpu-small",
      init_checksum: "abc123",
    });
    const instance2 = createTestInstance({
      spec: "gpu-small",
      init_checksum: null,
    });
    const alloc1 = createAvailableAllocation(instance1.id);
    const alloc2 = createAvailableAllocation(instance2.id);

    // No initChecksum passed — any available allocation, FIFO order
    const result = findWarmAllocation({ spec: "gpu-small", tenantId: 1 });
    expect(result).not.toBeNull();
    expect(result!.id).toBe(alloc1.id);
  });
});

// =============================================================================
// resolve-instance: Warm vs Cold Path
// =============================================================================

describe("resolve-instance: warm vs cold path", () => {
  function setupWorkflowWithNodes() {
    const now = Date.now();
    const workflow = createWorkflow({
      type: "launch-run",
      parent_workflow_id: null,
      depth: 0,
      status: "running",
      current_node: null,
      input_json: JSON.stringify({
        runId: 1,
        command: "echo hi",
        spec: "gpu-small",
        provider: "orbstack",
      }),
      output_json: null,
      error_json: null,
      manifest_id: null,
      trace_id: null,
      idempotency_key: null,
      timeout_ms: 60000,
      timeout_at: null,
      started_at: now,
      finished_at: null,
      updated_at: now,
    });
    const manifest = createManifest(workflow.id);
    updateWorkflow(workflow.id, { manifest_id: manifest.id });

    // Create all relevant nodes
    const nodes = [
      { nodeId: "resolve-instance", type: "resolve-instance", deps: null },
      { nodeId: "claim-warm-allocation", type: "claim-allocation", deps: '["resolve-instance"]' },
      { nodeId: "spawn-instance", type: "spawn-instance", deps: '["resolve-instance"]' },
      { nodeId: "wait-for-boot", type: "wait-for-boot", deps: '["spawn-instance"]' },
    ];
    for (const n of nodes) {
      createWorkflowNode({
        workflow_id: workflow.id,
        node_id: n.nodeId,
        node_type: n.type,
        status: "pending",
        input_json: "{}",
        output_json: null,
        error_json: null,
        depends_on: n.deps,
        attempt: 0,
        retry_reason: null,
        started_at: null,
        finished_at: null,
        updated_at: now,
      });
    }

    return { workflowId: workflow.id, manifestId: manifest.id };
  }

  test("warm path: resolve-instance returns warmAvailable=true and skips spawn/boot nodes", async () => {
    const { workflowId, manifestId } = setupWorkflowWithNodes();

    // Create a warm instance + allocation
    const instance = createTestInstance({ spec: "gpu-small" });
    const alloc = createAvailableAllocation(instance.id);

    const { resolveInstanceExecutor } = await import(
      "../../control/src/workflow/nodes/resolve-instance"
    );

    const ctx = {
      workflowId,
      nodeId: "resolve-instance",
      manifestId,
      tenantId: 1,
      workflowInput: { runId: 1, command: "echo hi", spec: "gpu-small", provider: "orbstack" },
      getNodeOutput: () => null,
      log: () => {},
      input: {},
      emitResource: () => {},
      claimResource: async () => false,
      applyPattern: () => {},
      checkCancellation: () => {},
      sleep: async () => {},
      spawnSubworkflow: async () => ({ workflowId: 0, wait: async () => ({ status: "completed" as const }) }),
    };

    const output = await resolveInstanceExecutor.execute(ctx as any);
    expect(output.warmAvailable).toBe(true);
    expect(output.allocationId).toBe(alloc.id);
    expect(output.instanceId).toBe(instance.id);

    // Verify spawn and boot nodes are skipped
    const db = getDatabase();
    const spawnNode = db.query(
      "SELECT status FROM workflow_nodes WHERE workflow_id = ? AND node_id = 'spawn-instance'"
    ).get(workflowId) as any;
    expect(spawnNode.status).toBe("skipped");

    const bootNode = db.query(
      "SELECT status FROM workflow_nodes WHERE workflow_id = ? AND node_id = 'wait-for-boot'"
    ).get(workflowId) as any;
    expect(bootNode.status).toBe("skipped");

    // claim-warm-allocation should still be pending (not skipped)
    const claimNode = db.query(
      "SELECT status FROM workflow_nodes WHERE workflow_id = ? AND node_id = 'claim-warm-allocation'"
    ).get(workflowId) as any;
    expect(claimNode.status).toBe("pending");
  });

  test("warm→cold fallthrough: dead warm instance falls through to cold path (#2.06)", async () => {
    const { workflowId, manifestId } = setupWorkflowWithNodes();

    // Create a warm instance + allocation (warm pool has a candidate)
    const instance = createTestInstance({ spec: "gpu-small" });
    const alloc = createAvailableAllocation(instance.id);

    // Override provider.get to return null — simulates out-of-band termination
    clearProviderCache();
    const deadProvider: Provider = {
      ...mockOrbstack,
      get: async () => null,  // Provider says: instance doesn't exist
    };
    await registerProvider({ provider: deadProvider });

    const { resolveInstanceExecutor } = await import(
      "../../control/src/workflow/nodes/resolve-instance"
    );

    const logMessages: string[] = [];
    const ctx = {
      workflowId,
      nodeId: "resolve-instance",
      manifestId,
      tenantId: 1,
      workflowInput: { runId: 1, command: "echo hi", spec: "gpu-small", provider: "orbstack" },
      getNodeOutput: () => null,
      log: (_level: string, msg: string) => { logMessages.push(msg); },
      input: {},
      emitResource: () => {},
      claimResource: async () => false,
      applyPattern: () => {},
      checkCancellation: () => {},
      sleep: async () => {},
      spawnSubworkflow: async () => ({ workflowId: 0, wait: async () => ({ status: "completed" as const }) }),
    };

    const output = await resolveInstanceExecutor.execute(ctx as any);

    // Must fall through to cold path
    expect(output.warmAvailable).toBe(false);
    expect(output.allocationId).toBeUndefined();

    // Stale allocation should be failed
    const db = getDatabase();
    const failedAlloc = db.query(
      "SELECT status FROM allocations WHERE id = ?"
    ).get(alloc.id) as any;
    expect(failedAlloc.status).toBe("FAILED");

    // claim-warm-allocation should be skipped (cold path)
    const claimNode = db.query(
      "SELECT status FROM workflow_nodes WHERE workflow_id = ? AND node_id = 'claim-warm-allocation'"
    ).get(workflowId) as any;
    expect(claimNode.status).toBe("skipped");

    // spawn/boot should remain pending (cold path needs them)
    const spawnNode = db.query(
      "SELECT status FROM workflow_nodes WHERE workflow_id = ? AND node_id = 'spawn-instance'"
    ).get(workflowId) as any;
    expect(spawnNode.status).toBe("pending");

    // Log should mention the fallthrough
    expect(logMessages.some(m => m.includes("falling through to cold"))).toBe(true);
  });

  test("warm→cold fallthrough: terminating warm instance falls through to cold path (EC2 shutting-down)", async () => {
    const { workflowId, manifestId } = setupWorkflowWithNodes();

    // Create a warm instance + allocation
    const instance = createTestInstance({ spec: "gpu-small" });
    const alloc = createAvailableAllocation(instance.id);

    // Provider returns "terminating" — EC2 shutting-down intermediate state
    clearProviderCache();
    const dyingProvider: Provider = {
      ...mockOrbstack,
      get: async (id: string) => ({
        id, status: "terminating" as any, spec: "gpu-small",
        ip: "10.0.0.1", createdAt: Date.now(), isSpot: false,
      }),
    };
    await registerProvider({ provider: dyingProvider });

    const { resolveInstanceExecutor } = await import(
      "../../control/src/workflow/nodes/resolve-instance"
    );

    const ctx = {
      workflowId,
      nodeId: "resolve-instance",
      manifestId,
      tenantId: 1,
      workflowInput: { runId: 1, command: "echo hi", spec: "gpu-small", provider: "orbstack" },
      getNodeOutput: () => null,
      log: () => {},
      input: {},
      emitResource: () => {},
      claimResource: async () => false,
      applyPattern: () => {},
      checkCancellation: () => {},
      sleep: async () => {},
      spawnSubworkflow: async () => ({ workflowId: 0, wait: async () => ({ status: "completed" as const }) }),
    };

    const output = await resolveInstanceExecutor.execute(ctx as any);

    // Must fall through to cold path — "terminating" is effectively dead
    expect(output.warmAvailable).toBe(false);

    // Instance should be marked terminated in DB
    const db = getDatabase();
    const dbInstance = db.query("SELECT workflow_state FROM instances WHERE id = ?").get(instance.id) as any;
    expect(dbInstance.workflow_state).toBe("terminate:complete");
  });

  test("cold path: resolve-instance returns warmAvailable=false and skips claim node", async () => {
    const { workflowId, manifestId } = setupWorkflowWithNodes();

    // No warm allocations available
    const { resolveInstanceExecutor } = await import(
      "../../control/src/workflow/nodes/resolve-instance"
    );

    const ctx = {
      workflowId,
      nodeId: "resolve-instance",
      manifestId,
      tenantId: 1,
      workflowInput: { runId: 1, command: "echo hi", spec: "gpu-small", provider: "orbstack" },
      getNodeOutput: () => null,
      log: () => {},
      input: {},
      emitResource: () => {},
      claimResource: async () => false,
      applyPattern: () => {},
      checkCancellation: () => {},
      sleep: async () => {},
      spawnSubworkflow: async () => ({ workflowId: 0, wait: async () => ({ status: "completed" as const }) }),
    };

    const output = await resolveInstanceExecutor.execute(ctx as any);
    expect(output.warmAvailable).toBe(false);
    expect(output.allocationId).toBeUndefined();

    // Verify claim-warm-allocation is skipped
    const db = getDatabase();
    const claimNode = db.query(
      "SELECT status FROM workflow_nodes WHERE workflow_id = ? AND node_id = 'claim-warm-allocation'"
    ).get(workflowId) as any;
    expect(claimNode.status).toBe("skipped");

    // spawn and boot should remain pending
    const spawnNode = db.query(
      "SELECT status FROM workflow_nodes WHERE workflow_id = ? AND node_id = 'spawn-instance'"
    ).get(workflowId) as any;
    expect(spawnNode.status).toBe("pending");
  });
});

// =============================================================================
// Replenishment (in finalize)
// =============================================================================

describe("Replenishment in finalize", () => {
  function setupFinalizeForReplenishment(opts: {
    exitCode: number;
    spotInterrupted?: boolean;
    staleHeartbeat?: boolean;
    maxAllocations?: boolean;
  }) {
    const db = getDatabase();
    const ts = Date.now();

    const workflow = createWorkflow({
      type: "launch-run",
      parent_workflow_id: null,
      depth: 0,
      status: "running",
      current_node: null,
      input_json: "{}",
      output_json: null,
      error_json: null,
      manifest_id: null,
      trace_id: null,
      idempotency_key: null,
      timeout_ms: 60000,
      timeout_at: null,
      started_at: ts,
      finished_at: null,
      updated_at: ts,
    });
    const manifest = createManifest(workflow.id);
    updateWorkflow(workflow.id, { manifest_id: manifest.id });

    // Create instance
    const instance = createTestInstance({
      spec: "gpu-small",
      last_heartbeat: opts.staleHeartbeat ? ts - 2_000_000 : ts,
    });

    // Create run
    const run = createRun({
      command: "echo hi",
      workdir: "/workspace",
      max_duration_ms: 60000,
      workflow_state: "launch-run:running",
      workflow_error: null,
      current_manifest_id: manifest.id,
      exit_code: null,
      init_checksum: null,
      create_snapshot: 0,
      spot_interrupted: 0,
      started_at: ts,
      finished_at: null,
    });

    // Create allocation in ACTIVE state
    const alloc = createAllocation({
      run_id: run.id,
      instance_id: instance.id,
      status: "AVAILABLE",
      current_manifest_id: manifest.id,
      user: "ubuntu",
      workdir: "/workspace",
      debug_hold_until: null,
      completed_at: null,
    });
    // Transition: AVAILABLE -> CLAIMED -> ACTIVE
    db.run("UPDATE allocations SET status = 'CLAIMED', run_id = ?, updated_at = ? WHERE id = ?", [run.id, ts, alloc.id]);
    db.run("UPDATE allocations SET status = 'ACTIVE', updated_at = ? WHERE id = ?", [ts + 1, alloc.id]);

    // If testing max allocations, create many AVAILABLE allocations
    if (opts.maxAllocations) {
      for (let i = 0; i < 10; i++) {
        createAllocation({
          run_id: null,
          instance_id: instance.id,
          status: "AVAILABLE",
          current_manifest_id: null,
          user: "default",
          workdir: "/home/user",
          debug_hold_until: null,
          completed_at: null,
        });
      }
    }

    // Create predecessor nodes with outputs
    createWorkflowNode({
      workflow_id: workflow.id,
      node_id: "create-allocation",
      node_type: "create-allocation",
      status: "completed",
      input_json: "{}",
      output_json: JSON.stringify({
        instanceId: instance.id,
        allocationId: alloc.id,
        workdir: "/workspace",
      }),
      error_json: null,
      depends_on: null,
      attempt: 0,
      retry_reason: null,
      started_at: ts,
      finished_at: ts,
      updated_at: ts,
    });
    createWorkflowNode({
      workflow_id: workflow.id,
      node_id: "await-completion",
      node_type: "await-completion",
      status: "completed",
      input_json: "{}",
      output_json: JSON.stringify({
        exitCode: opts.exitCode,
        completedAt: ts,
        durationMs: 1000,
        spotInterrupted: opts.spotInterrupted ?? false,
        timedOut: false,
      }),
      error_json: null,
      depends_on: '["create-allocation"]',
      attempt: 0,
      retry_reason: null,
      started_at: ts,
      finished_at: ts,
      updated_at: ts,
    });

    const ctx = {
      workflowId: workflow.id,
      nodeId: "finalize",
      manifestId: manifest.id,
      workflowInput: { runId: run.id, command: "echo hi", spec: "gpu-small", provider: "orbstack" },
      getNodeOutput: (nodeId: string) => {
        if (nodeId === "create-allocation") return { instanceId: instance.id, allocationId: alloc.id, workdir: "/workspace" };
        if (nodeId === "await-completion") return { exitCode: opts.exitCode, completedAt: ts, durationMs: 1000, spotInterrupted: opts.spotInterrupted ?? false, timedOut: false };
        return null;
      },
      log: () => {},
      input: {},
      emitResource: () => {},
      claimResource: async () => false,
      applyPattern: () => {},
      checkCancellation: () => {},
      sleep: async () => {},
      spawnSubworkflow: async () => ({ workflowId: 0, wait: async () => ({ status: "completed" as const }) }),
    };

    return { workflowId: workflow.id, manifestId: manifest.id, instanceId: instance.id, runId: run.id, allocationId: alloc.id, ctx };
  }

  test("replenishes warm pool on clean exit (exitCode=0)", async () => {
    const { instanceId, ctx } = setupFinalizeForReplenishment({ exitCode: 0 });

    const { finalizeExecutor } = await import("../../control/src/workflow/nodes/finalize");
    await finalizeExecutor.execute(ctx as any);

    // Should have a new AVAILABLE allocation for this instance
    const newAllocs = queryMany<Allocation>(
      "SELECT * FROM allocations WHERE instance_id = ? AND status = 'AVAILABLE'",
      [instanceId]
    );
    expect(newAllocs.length).toBe(1);
    expect(newAllocs[0].run_id).toBeNull();
  });

  test("replenishes even on non-zero exit code", async () => {
    const { instanceId, ctx } = setupFinalizeForReplenishment({ exitCode: 1 });

    const { finalizeExecutor } = await import("../../control/src/workflow/nodes/finalize");
    await finalizeExecutor.execute(ctx as any);

    // Non-zero exit code no longer blocks replenishment (A1 fix)
    const newAllocs = queryMany<Allocation>(
      "SELECT * FROM allocations WHERE instance_id = ? AND status = 'AVAILABLE'",
      [instanceId]
    );
    expect(newAllocs.length).toBe(1);
    expect(newAllocs[0].run_id).toBeNull();
  });

  test("does NOT replenish when spot interrupted", async () => {
    const { instanceId, ctx } = setupFinalizeForReplenishment({ exitCode: 0, spotInterrupted: true });

    const { finalizeExecutor } = await import("../../control/src/workflow/nodes/finalize");
    await finalizeExecutor.execute(ctx as any);

    const newAllocs = queryMany<Allocation>(
      "SELECT * FROM allocations WHERE instance_id = ? AND status = 'AVAILABLE'",
      [instanceId]
    );
    expect(newAllocs.length).toBe(0);
  });

  test("does NOT replenish when heartbeat is stale", async () => {
    const { instanceId, ctx } = setupFinalizeForReplenishment({ exitCode: 0, staleHeartbeat: true });

    const { finalizeExecutor } = await import("../../control/src/workflow/nodes/finalize");
    await finalizeExecutor.execute(ctx as any);

    const newAllocs = queryMany<Allocation>(
      "SELECT * FROM allocations WHERE instance_id = ? AND status = 'AVAILABLE'",
      [instanceId]
    );
    expect(newAllocs.length).toBe(0);
  });

  test("does NOT replenish when max allocations reached", async () => {
    const { instanceId, ctx } = setupFinalizeForReplenishment({ exitCode: 0, maxAllocations: true });

    const { finalizeExecutor } = await import("../../control/src/workflow/nodes/finalize");
    await finalizeExecutor.execute(ctx as any);

    // Should NOT have created an additional AVAILABLE allocation beyond the existing 10
    const newAllocs = queryMany<Allocation>(
      "SELECT * FROM allocations WHERE instance_id = ? AND status = 'AVAILABLE'",
      [instanceId]
    );
    expect(newAllocs.length).toBe(10); // No new one added
  });
});

// =============================================================================
// countInstanceAllocations
// =============================================================================

describe("countInstanceAllocations", () => {
  test("counts non-terminal allocations for an instance", () => {
    const instance = createTestInstance({ spec: "gpu-small" });

    // Create 2 AVAILABLE + 1 COMPLETE
    createAvailableAllocation(instance.id);
    createAvailableAllocation(instance.id);
    const alloc3 = createAllocation({
      run_id: null,
      instance_id: instance.id,
      status: "AVAILABLE",
      current_manifest_id: null,
      user: "default",
      workdir: "/home/user",
      debug_hold_until: null,
      completed_at: null,
    });
    // Fail one allocation to make it terminal
    failAllocation(alloc3.id, "AVAILABLE");

    expect(countInstanceAllocations(instance.id)).toBe(2); // 2 AVAILABLE, not counting FAILED
  });

  test("returns 0 when no allocations exist", () => {
    const instance = createTestInstance({ spec: "gpu-small" });
    expect(countInstanceAllocations(instance.id)).toBe(0);
  });
});

// =============================================================================
// Full Lifecycle: Spawn -> Complete -> Recycle -> Claim
// =============================================================================

describe("Full warm pool lifecycle", () => {
  test("spawn -> active -> complete -> replenish -> find warm -> claim", () => {
    const runId1 = createTestRun();
    const runId2 = createTestRun();

    // 1. Create a "completed" instance (simulating post-spawn)
    const instance = createTestInstance({
      spec: "gpu-small",
      init_checksum: "abc123",
    });

    // 2. Create initial allocation (AVAILABLE, no run_id) and run it through lifecycle
    const initialAlloc = createAllocation({
      run_id: null,
      instance_id: instance.id,
      status: "AVAILABLE",
      current_manifest_id: null,
      user: "default",
      workdir: "/home/user",
      debug_hold_until: null,
      completed_at: null,
    });

    // AVAILABLE -> CLAIMED
    const claimResult = claimAllocation(initialAlloc.id, runId1);
    expect(claimResult.success).toBe(true);

    // CLAIMED -> ACTIVE
    const activateResult = activateAllocation(initialAlloc.id);
    expect(activateResult.success).toBe(true);

    // ACTIVE -> COMPLETE
    const completeResult = completeAllocation(initialAlloc.id);
    expect(completeResult.success).toBe(true);

    // 3. Replenish: create a new AVAILABLE allocation (simulating what finalize does)
    const newAlloc = createAllocation({
      run_id: null,
      instance_id: instance.id,
      status: "AVAILABLE",
      current_manifest_id: null,
      user: "default",
      workdir: "/home/user",
      debug_hold_until: null,
      completed_at: null,
    });

    // 4. Find warm allocation — should find the new one
    const found = findWarmAllocation({ spec: "gpu-small", tenantId: 1 }, "abc123");
    expect(found).not.toBeNull();
    expect(found!.id).toBe(newAlloc.id);
    expect(found!.instance_id).toBe(instance.id);

    // 5. Claim the warm allocation for a new run
    const warmClaim = claimAllocation(newAlloc.id, runId2);
    expect(warmClaim.success).toBe(true);
    if (warmClaim.success) {
      expect(warmClaim.data.run_id).toBe(runId2);
      expect(warmClaim.data.status).toBe("CLAIMED");
    }

    // 6. Verify original allocation is still COMPLETE (not affected)
    const original = getAllocation(initialAlloc.id);
    expect(original!.status).toBe("COMPLETE");
  });
});
