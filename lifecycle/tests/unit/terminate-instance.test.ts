// tests/unit/terminate-instance.test.ts - Terminate Instance Intent Tests
// Covers: blueprint registration, full workflow execution, API endpoint,
//         and select node executor edge cases not covered by E2E

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest } from "../harness";
import { seedTestApiKey, testFetch } from "../integration/helpers/test-auth";
import {
  createInstance,
  createAllocation,
  createRun,
  getInstance,
  getAllocation,
  getWorkflow,
  getWorkflowNodes,
  queryMany,
  queryOne,
  createUsageRecord,
  type Instance,
  type Allocation,
} from "../../control/src/material/db";

import {
  submit,
  getBlueprint,
  getNodeExecutor,
  createWorkflowEngine,
} from "../../control/src/workflow/engine";

import {
  registerTerminateInstance,
  terminateInstance,
} from "../../control/src/intent/terminate-instance";

import { registerLaunchRun } from "../../control/src/intent/launch-run";

import { validateInstanceExecutor } from "../../control/src/workflow/nodes/validate-instance";
import { drainAllocationsExecutor } from "../../control/src/workflow/nodes/drain-allocations";
import { terminateProviderExecutor } from "../../control/src/workflow/nodes/terminate-provider";
import { cleanupRecordsExecutor } from "../../control/src/workflow/nodes/cleanup-records";

import {
  registerProvider,
  unregisterProvider,
} from "../../control/src/provider/registry";
import type { Provider } from "../../control/src/provider/types";

import type { NodeContext } from "../../control/src/workflow/engine.types";

// =============================================================================
// Top-level harness setup
// =============================================================================

let cleanup: () => Promise<void>;
beforeEach(() => { cleanup = setupTest({ engine: true }); });
afterEach(() => cleanup());

// =============================================================================
// Test Helpers
// =============================================================================

function createTestInstance(overrides: Partial<Omit<Instance, "id" | "created_at">> = {}) {
  return createInstance({
    provider: "mock-provider",
    provider_id: `test-${Date.now()}-${Math.random().toString(36).slice(2)}`,
    spec: "cpu-1x",
    region: "local",
    ip: "127.0.0.1",
    workflow_state: "launch:complete",
    workflow_error: null,
    current_manifest_id: null,
    spawn_idempotency_key: null,
    is_spot: 0,
    spot_request_id: null,
    init_checksum: null,
    registration_token_hash: null,
    last_heartbeat: Date.now(),
    ...overrides,
  });
}

function createTestAllocation(
  instanceId: number,
  status: Allocation["status"] = "AVAILABLE",
  runId: number | null = null
) {
  return createAllocation({
    run_id: runId,
    instance_id: instanceId,
    status,
    current_manifest_id: null,
    user: "test-user",
    workdir: "/tmp/test",
    debug_hold_until: null,
    completed_at: null,
  });
}

function createTestRun() {
  return createRun({
    command: "echo test",
    workdir: "/tmp/test",
    max_duration_ms: 3600000,
    workflow_state: "launch:pending",
    workflow_error: null,
    current_manifest_id: null,
    exit_code: null,
    init_checksum: null,
    create_snapshot: 0,
    spot_interrupted: 0,
    started_at: null,
    finished_at: null,
  });
}

let terminateCalls: string[] = [];

function createMockProvider(): Provider {
  return {
    name: "mock-provider" as any,
    capabilities: {
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
    },
    async spawn() {
      return {
        id: "mock-vm-1",
        status: "running" as const,
        spec: "cpu-1x",
        createdAt: Date.now(),
        isSpot: false,
      };
    },
    async terminate(providerId: string) {
      terminateCalls.push(providerId);
    },
    async list() {
      return [];
    },
    async get() {
      return null;
    },
    generateBootstrap() {
      return { content: "", format: "shell" as const, checksum: "" };
    },
  };
}

function createMockNodeContext(
  overrides: Partial<NodeContext> & { workflowInput: unknown }
): NodeContext {
  const logs: Array<{ level: string; message: string; data?: Record<string, unknown> }> = [];

  return {
    workflowId: 1,
    nodeId: "test-node",
    input: {},
    manifestId: 1,
    controlId: "test",
    tenantId: 1,
    emitResource: () => {},
    claimResource: async () => true,
    applyPattern: () => {},
    getNodeOutput: () => undefined,
    log: (level, message, data) => {
      logs.push({ level, message, data });
    },
    checkCancellation: () => {},
    sleep: async () => {},
    spawnSubworkflow: async () => ({ workflowId: 1, wait: async () => ({ status: "completed" as const }) }),
    ...overrides,
  };
}

// =============================================================================
// Node Executor Edge Cases (unique failure modes not covered by E2E)
// =============================================================================

describe("Terminate Instance - Node Executor Edge Cases", () => {
  beforeEach(() => {
    terminateCalls = [];
  });

  test("validate-instance: blocked by active debug hold", async () => {
    const instance = createTestInstance();
    const run = createTestRun();
    const alloc = createTestAllocation(instance.id, "COMPLETE", run.id);

    // Set debug hold in the future
    const futureHold = Date.now() + 60000; // 1 minute from now
    queryMany(
      "UPDATE allocations SET debug_hold_until = ?, completed_at = ? WHERE id = ?",
      [futureHold, Date.now(), alloc.id]
    );

    const ctx = createMockNodeContext({
      workflowInput: { instanceId: instance.id },
    });

    await expect(validateInstanceExecutor.execute(ctx)).rejects.toThrow(
      /blocked by active debug holds/i
    );

    // State should NOT have changed to terminate:draining
    const updated = getInstance(instance.id);
    expect(updated?.workflow_state).toBe("launch:complete");
  });

  test("drain-allocations: no allocations is a no-op", async () => {
    const instance = createTestInstance();

    const ctx = createMockNodeContext({
      workflowInput: { instanceId: instance.id },
      getNodeOutput: (nodeId: string) => {
        if (nodeId === "validate-instance") {
          return { instanceId: instance.id };
        }
        return undefined;
      },
    });

    const result = await drainAllocationsExecutor.execute(ctx);

    expect(result.drained).toBe(0);
    expect(result.failed).toBe(0);
  });

  // ---------------------------------------------------------------------------
  // terminate-provider (unique: mocks provider differently from E2E)
  // ---------------------------------------------------------------------------

  describe("terminate-provider", () => {
    test("calls provider.terminate", async () => {
      const mockProvider = createMockProvider();
      await registerProvider({ provider: mockProvider });

      const ctx = createMockNodeContext({
        workflowInput: { instanceId: 1 },
        getNodeOutput: (nodeId: string) => {
          if (nodeId === "validate-instance") {
            return {
              instanceId: 1,
              provider: "mock-provider",
              providerId: "mock-vm-123",
              spec: "cpu-1x",
              region: "local",
            };
          }
          return undefined;
        },
      });

      const result = await terminateProviderExecutor.execute(ctx);

      expect(result.terminated).toBe(true);
      expect(result.providerId).toBe("mock-vm-123");
      expect(terminateCalls).toContain("mock-vm-123");

      await unregisterProvider("mock-provider");
    });

    test("handles already-terminated gracefully (not found)", async () => {
      const mockProvider = createMockProvider();
      mockProvider.terminate = async (providerId: string) => {
        throw new Error(`VM ${providerId} not found`);
      };
      await registerProvider({ provider: mockProvider });

      const ctx = createMockNodeContext({
        workflowInput: { instanceId: 1 },
        getNodeOutput: (nodeId: string) => {
          if (nodeId === "validate-instance") {
            return {
              instanceId: 1,
              provider: "mock-provider",
              providerId: "mock-vm-gone",
              spec: "cpu-1x",
              region: "local",
            };
          }
          return undefined;
        },
      });

      const result = await terminateProviderExecutor.execute(ctx);

      expect(result.terminated).toBe(true);
      expect(result.providerId).toBe("mock-vm-gone");

      await unregisterProvider("mock-provider");
    });

    test("handles empty providerId gracefully", async () => {
      const ctx = createMockNodeContext({
        workflowInput: { instanceId: 1 },
        getNodeOutput: (nodeId: string) => {
          if (nodeId === "validate-instance") {
            return {
              instanceId: 1,
              provider: "mock-provider",
              providerId: "",
              spec: "cpu-1x",
              region: "local",
            };
          }
          return undefined;
        },
      });

      const result = await terminateProviderExecutor.execute(ctx);

      expect(result.terminated).toBe(true);
      expect(result.providerId).toBe("");
    });

    test("re-throws genuine provider errors", async () => {
      const mockProvider = createMockProvider();
      mockProvider.terminate = async () => {
        throw new Error("Network timeout");
      };
      await registerProvider({ provider: mockProvider });

      const ctx = createMockNodeContext({
        workflowInput: { instanceId: 1 },
        getNodeOutput: (nodeId: string) => {
          if (nodeId === "validate-instance") {
            return {
              instanceId: 1,
              provider: "mock-provider",
              providerId: "mock-vm-123",
              spec: "cpu-1x",
              region: "local",
            };
          }
          return undefined;
        },
      });

      await expect(terminateProviderExecutor.execute(ctx)).rejects.toThrow("Network timeout");

      await unregisterProvider("mock-provider");
    });
  });

  test("cleanup-records: closes open usage records", async () => {
    const instance = createTestInstance();
    const usageRecord = createUsageRecord({
      instance_id: instance.id,
      allocation_id: null,
      run_id: null,
      provider: "mock-provider",
      spec: "cpu-1x",
      region: "local",
      is_spot: 0,
      started_at: Date.now() - 60000,
      finished_at: null,
      duration_ms: null,
      estimated_cost_usd: null,
    });

    const ctx = createMockNodeContext({
      workflowInput: { instanceId: instance.id },
      getNodeOutput: (nodeId: string) => {
        if (nodeId === "validate-instance") {
          return { instanceId: instance.id };
        }
        return undefined;
      },
    });

    const result = await cleanupRecordsExecutor.execute(ctx);

    expect(result.recordsCleaned).toBe(1);

    // Verify the usage record was closed
    const updated = queryOne<{ finished_at: number | null; duration_ms: number | null }>(
      "SELECT finished_at, duration_ms FROM usage_records WHERE id = ?",
      [usageRecord.id]
    );
    expect(updated?.finished_at).not.toBeNull();
    expect(updated?.duration_ms).toBeGreaterThan(0);
  });
});

// =============================================================================
// Registration Tests
// =============================================================================

describe("Terminate Instance - Registration", () => {
  beforeEach(() => {
    createWorkflowEngine();
  });

  test("registerTerminateInstance registers blueprint and all executors", () => {
    registerTerminateInstance();

    expect(getBlueprint("terminate-instance")).toBeDefined();
    expect(getNodeExecutor("validate-instance")).toBeDefined();
    expect(getNodeExecutor("drain-allocations")).toBeDefined();
    expect(getNodeExecutor("drain-ssh-sessions")).toBeDefined();
    expect(getNodeExecutor("cleanup-features")).toBeDefined();
    expect(getNodeExecutor("terminate-provider")).toBeDefined();
    expect(getNodeExecutor("cleanup-records")).toBeDefined();
  });
});

// =============================================================================
// Full Workflow Execution Tests
// =============================================================================

describe("Terminate Instance - Workflow Execution", () => {
  beforeEach(async () => {
    createWorkflowEngine();
    registerLaunchRun();
    registerTerminateInstance();
    terminateCalls = [];

    // Register mock provider
    const mockProvider = createMockProvider();
    await registerProvider({ provider: mockProvider });
  });

  afterEach(async () => {
    await unregisterProvider("mock-provider");
  });

  test("full workflow completes all 6 nodes", async () => {
    const instance = createTestInstance();
    const alloc = createTestAllocation(instance.id, "AVAILABLE");

    const result = await submit({
      type: "terminate-instance",
      input: { instanceId: instance.id },
    });

    expect(result.workflowId).toBeGreaterThan(0);

    // submit() fires executeLoop in the background. Poll for completion.
    const deadline = Date.now() + 10_000;
    while (Date.now() < deadline) {
      const wf = getWorkflow(result.workflowId)!;
      if (wf.status === "completed" || wf.status === "failed") break;
      await new Promise((r) => setTimeout(r, 50));
    }

    // Verify workflow completed
    const finalWorkflow = getWorkflow(result.workflowId)!;
    expect(finalWorkflow.status).toBe("completed");

    // Verify all nodes completed
    const nodes = getWorkflowNodes(result.workflowId);
    for (const node of nodes) {
      expect(node.status).toBe("completed");
    }

    // Verify instance is terminated
    const updated = getInstance(instance.id);
    expect(updated?.workflow_state).toBe("terminate:complete");

    // Verify allocation was drained
    const updatedAlloc = getAllocation(alloc.id);
    expect(updatedAlloc?.status).toBe("FAILED");

    // Verify provider.terminate was called
    expect(terminateCalls).toContain(instance.provider_id);
  });

  test("terminateInstance entry point validates instance exists", async () => {
    await expect(
      terminateInstance({ instanceId: 99999 })
    ).rejects.toThrow(/not found/i);
  });

  test("terminateInstance rejects already-terminated instance", async () => {
    const instance = createTestInstance({
      workflow_state: "terminate:complete",
    });

    await expect(
      terminateInstance({ instanceId: instance.id })
    ).rejects.toThrow(/already terminated/i);
  });

  test("terminateInstance rejects instance already being terminated", async () => {
    const instance = createTestInstance({
      workflow_state: "terminate:draining",
    });

    await expect(
      terminateInstance({ instanceId: instance.id })
    ).rejects.toThrow(/already being terminated/i);
  });
});

// =============================================================================
// API Endpoint Tests
// =============================================================================

describe("Terminate Instance - API", () => {
  let server: any = null;
  let baseUrl: string;

  beforeEach(async () => {
    createWorkflowEngine();
    registerLaunchRun();
    registerTerminateInstance();
    terminateCalls = [];
    seedTestApiKey();

    const mockProvider = createMockProvider();
    await registerProvider({ provider: mockProvider });

    const { createServer } = await import("../../control/src/api/routes");
    const app = createServer({ port: 0, corsOrigins: ["*"], maxBodySize: 10 * 1024 * 1024 });
    server = app.listen({ port: 0, idleTimeout: 0 });
    baseUrl = `http://localhost:${server.server!.port}`;
  });

  afterEach(async () => {
    if (server) {
      server.server!.stop(true);
      server = null;
    }
    await unregisterProvider("mock-provider");
  });

  test("POST /v1/workflows/terminate-instance returns 202", async () => {
    const instance = createTestInstance();

    const response = await testFetch(`${baseUrl}/v1/workflows/terminate-instance`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ instanceId: instance.id }),
    });

    expect(response.status).toBe(202);
    const body = await response.json() as any;
    expect(body.workflow_id).toBeGreaterThan(0);
    expect(body.instance_id).toBe(instance.id);
    expect(body.status).toBeDefined();
  });

  test("POST with invalid instanceId returns 400", async () => {
    const response = await testFetch(`${baseUrl}/v1/workflows/terminate-instance`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ instanceId: "not-a-number" }),
    });

    expect(response.status).toBe(400);
    const body = await response.json() as any;
    expect(body.error.code).toBe("INVALID_INPUT");
  });

  test("POST with non-existent instanceId returns 404", async () => {
    const response = await testFetch(`${baseUrl}/v1/workflows/terminate-instance`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ instanceId: 99999 }),
    });

    expect(response.status).toBe(404);
  });

  test("POST with already-terminated instance returns 409", async () => {
    const instance = createTestInstance({
      workflow_state: "terminate:complete",
    });

    const response = await testFetch(`${baseUrl}/v1/workflows/terminate-instance`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ instanceId: instance.id }),
    });

    expect(response.status).toBe(409);
  });

  test("POST with missing body returns 400", async () => {
    const response = await testFetch(`${baseUrl}/v1/workflows/terminate-instance`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({}),
    });

    expect(response.status).toBe(400);
  });

  test("POST supports snake_case instance_id", async () => {
    const instance = createTestInstance();

    const response = await testFetch(`${baseUrl}/v1/workflows/terminate-instance`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ instance_id: instance.id }),
    });

    expect(response.status).toBe(202);
    const body = await response.json() as any;
    expect(body.workflow_id).toBeGreaterThan(0);
  });
});
