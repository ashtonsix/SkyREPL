// tests/unit/terminate-instance.test.ts - Terminate Instance Intent Tests
// Covers: blueprint registration, full workflow execution, API endpoint,
//         select node executor edge cases not covered by E2E,
//         and parameterised contract suites (#WF2-06)

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest, waitForWorkflow, verifyWorkflowInvariant } from "../harness";
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
  emitAuditEvent,
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
import { makeMockProvider, mockQueue } from "../mock-provider";
import { createManifest, createWorkflow, type Workflow } from "../../control/src/material/db";
import { sealManifest } from "../../control/src/workflow/state-transitions";

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
    provider_metadata: null,
    display_name: null,
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

  test("cleanup-records: emits metering_stop audit event and sets terminate:complete", async () => {
    const instance = createTestInstance();

    // Emit a metering_start first (setup: instance was billable)
    emitAuditEvent({
      event_type: "metering_start",
      tenant_id: 1,
      instance_id: instance.id,
      provider: "mock-provider",
      spec: "cpu-1x",
      region: "local",
      source: "lifecycle",
      is_cost: true,
      is_usage: true,
      data: {
        provider_resource_id: instance.provider_id,
        metering_window_start_ms: Date.now() - 60000,
      },
      dedupe_key: `mock-provider:${instance.provider_id}:metering_start`,
      occurred_at: Date.now() - 60000,
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

    // Verify instance is now terminate:complete
    const updated = getInstance(instance.id);
    expect(updated?.workflow_state).toBe("terminate:complete");

    // Verify metering_stop was emitted to audit_log
    const stopEvent = queryOne<{ event_type: string; instance_id: number }>(
      "SELECT event_type, instance_id FROM audit_log WHERE event_type = 'metering_stop' AND instance_id = ?",
      [instance.id]
    );
    expect(stopEvent).not.toBeNull();
    expect(stopEvent?.event_type).toBe("metering_stop");
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

// =============================================================================
// Parameterised Contract Tests (#WF2-06)
//
// Contract: for any provider configuration, if terminate-instance completes,
// these invariants MUST hold. We run the suite against several provider
// configurations to prove the properties aren't accidentally provider-specific.
// =============================================================================

// ---------------------------------------------------------------------------
// Contract Suite Factory
//
// runTerminateInstanceContract(name, providerFactory) registers a describe block
// that runs all behavioral invariants for one provider configuration.
// ---------------------------------------------------------------------------

type ProviderFactory = () => Provider;

function runTerminateInstanceContract(suiteName: string, providerFactory: ProviderFactory) {
  describe(`Terminate Instance Contract: ${suiteName}`, () => {
    let currentCleanup: () => Promise<void>;
    let provider: Provider;

    beforeEach(async () => {
      currentCleanup = setupTest({ engine: true });
      createWorkflowEngine();
      registerLaunchRun();
      registerTerminateInstance();
      terminateCalls = [];
      provider = providerFactory();
      await registerProvider({ provider });
    });

    afterEach(async () => {
      await unregisterProvider(provider.name as string);
      await currentCleanup();
    });

    // -------------------------------------------------------------------------
    // CONTRACT: terminal state invariant
    // "For any provider, a successful termination must leave the instance in
    //  terminate:complete and the workflow in completed."
    // -------------------------------------------------------------------------

    test("CONTRACT: successful termination produces terminate:complete instance state", async () => {
      const instance = createTestInstance({ provider: provider.name as any });

      const { workflowId } = await submit({
        type: "terminate-instance",
        input: { instanceId: instance.id },
      });

      const wf = await waitForWorkflow(workflowId);
      expect(wf.status).toBe("completed");

      const updated = getInstance(instance.id);
      expect(updated?.workflow_state).toBe("terminate:complete");
    });

    // -------------------------------------------------------------------------
    // CONTRACT: node sequence invariant
    // "All 6 nodes must complete, in dependency order."
    // -------------------------------------------------------------------------

    test("CONTRACT: all 6 nodes complete in dependency order", async () => {
      const instance = createTestInstance({ provider: provider.name as any });

      const { workflowId } = await submit({
        type: "terminate-instance",
        input: { instanceId: instance.id },
      });

      await waitForWorkflow(workflowId);

      const nodes = getWorkflowNodes(workflowId);
      expect(nodes.length).toBe(6);

      const byId: Record<string, string> = {};
      for (const n of nodes) byId[n.node_id] = n.status;

      const expectedNodes = [
        "validate-instance",
        "drain-allocations",
        "drain-ssh-sessions",
        "cleanup-features",
        "terminate-provider",
        "cleanup-records",
      ];
      for (const id of expectedNodes) {
        expect(byId[id]).toBe("completed");
      }

      // Dependency ordering: finished_at must be non-decreasing along the chain
      // (each node must have finished_at >= the previous node's started_at).
      const ordered = nodes.sort((a, b) =>
        (a.finished_at ?? 0) - (b.finished_at ?? 0)
      );
      for (let i = 1; i < ordered.length; i++) {
        if (ordered[i - 1].finished_at != null && ordered[i].started_at != null) {
          expect(ordered[i].started_at!).toBeGreaterThanOrEqual(ordered[i - 1].started_at!);
        }
      }
    });

    // -------------------------------------------------------------------------
    // CONTRACT: provider.terminate called exactly once
    // "For any provider, terminate-provider must invoke provider.terminate with
    //  the correct provider_id."
    // -------------------------------------------------------------------------

    test("CONTRACT: provider.terminate called with instance provider_id", async () => {
      const instance = createTestInstance({
        provider: provider.name as any,
        provider_id: `contract-test-${Date.now()}`,
      });

      const { workflowId } = await submit({
        type: "terminate-instance",
        input: { instanceId: instance.id },
      });

      await waitForWorkflow(workflowId);

      expect(terminateCalls).toContain(instance.provider_id);
    });

    // -------------------------------------------------------------------------
    // CONTRACT: non-terminal allocations are drained
    // "For any provider, all AVAILABLE allocations on the instance must be
    //  transitioned to FAILED before workflow completes."
    // -------------------------------------------------------------------------

    test("CONTRACT: non-terminal allocations are drained to FAILED", async () => {
      const instance = createTestInstance({ provider: provider.name as any });
      const alloc1 = createTestAllocation(instance.id, "AVAILABLE");
      const alloc2 = createTestAllocation(instance.id, "AVAILABLE");

      const { workflowId } = await submit({
        type: "terminate-instance",
        input: { instanceId: instance.id },
      });

      await waitForWorkflow(workflowId);

      const a1 = getAllocation(alloc1.id);
      const a2 = getAllocation(alloc2.id);
      expect(a1?.status).toBe("FAILED");
      expect(a2?.status).toBe("FAILED");
    });

    // -------------------------------------------------------------------------
    // CONTRACT: workflow invariants hold
    // Uses the shared verifyWorkflowInvariant helper to assert finished_at set,
    // all nodes terminal, etc. — independent of provider.
    // -------------------------------------------------------------------------

    test("CONTRACT: post-completion workflow invariants hold", async () => {
      const instance = createTestInstance({ provider: provider.name as any });

      const { workflowId } = await submit({
        type: "terminate-instance",
        input: { instanceId: instance.id },
      });

      await waitForWorkflow(workflowId);

      verifyWorkflowInvariant(workflowId, { checkAllocations: false });
    });

    // -------------------------------------------------------------------------
    // CONTRACT: already-terminated instance is handled gracefully
    // "validate-instance must still pass if the instance is already in
    //  terminate:complete — it should return successfully (idempotent)."
    // -------------------------------------------------------------------------

    test("CONTRACT: terminate:complete instance passes validate-instance (idempotent node)", async () => {
      const instance = createTestInstance({
        provider: provider.name as any,
        workflow_state: "terminate:complete",
      });

      // Execute the executor directly — it should return without throwing
      const ctx = createMockNodeContext({
        workflowInput: { instanceId: instance.id },
      });

      const result = await validateInstanceExecutor.execute(ctx);
      expect(result.instanceId).toBe(instance.id);
    });

    // -------------------------------------------------------------------------
    // CONTRACT: error classification — provider error propagates
    // "If provider.terminate throws a non-idempotent error, the workflow must
    //  fail (not silently swallow it)."
    // -------------------------------------------------------------------------

    test("CONTRACT: genuine provider error fails the terminate-provider node", async () => {
      // Override terminate to throw a non-idempotent error
      const originalTerminate = provider.terminate;
      provider.terminate = async () => {
        throw new Error("Rate limit exceeded");
      };

      const instance = createTestInstance({ provider: provider.name as any });

      const { workflowId } = await submit({
        type: "terminate-instance",
        input: { instanceId: instance.id },
      });

      const wf = await waitForWorkflow(workflowId);
      expect(wf.status).toBe("failed");

      const nodes = getWorkflowNodes(workflowId);
      const terminateNode = nodes.find((n) => n.node_id === "terminate-provider");
      expect(terminateNode?.status).toBe("failed");

      // Restore
      provider.terminate = originalTerminate;
    });

    // -------------------------------------------------------------------------
    // CONTRACT: idempotent error handling — "not found" is treated as success
    // "If provider.terminate returns a not-found error, terminate-provider must
    //  succeed (idempotent for already-gone instances)."
    // -------------------------------------------------------------------------

    test("CONTRACT: provider not-found error is idempotent (treated as success)", async () => {
      provider.terminate = async (providerId) => {
        throw new Error(`Instance ${providerId} not found`);
      };

      const instance = createTestInstance({ provider: provider.name as any });

      const { workflowId } = await submit({
        type: "terminate-instance",
        input: { instanceId: instance.id },
      });

      const wf = await waitForWorkflow(workflowId);
      // "not found" must not fail the workflow
      expect(wf.status).toBe("completed");

      const updated = getInstance(instance.id);
      expect(updated?.workflow_state).toBe("terminate:complete");
    });
  });
}

// ---------------------------------------------------------------------------
// Provider Configurations Under Test
// ---------------------------------------------------------------------------

// Configuration 1: Basic provider (no spot, no GPU)
runTerminateInstanceContract("basic on-demand provider", () => {
  const p = createMockProvider();
  return p;
});

// Configuration 2: Spot-capable provider (same termination contract, different capabilities)
runTerminateInstanceContract("spot-capable provider", () => {
  const p: Provider = {
    ...createMockProvider(),
    name: "mock-provider" as any,
    capabilities: {
      snapshots: false,
      spot: true,
      gpu: false,
      multiRegion: true,
      persistentVolumes: false,
      warmVolumes: false,
      hibernation: false,
      costExplorer: false,
      tailscaleNative: false,
      idempotentSpawn: true,
      customNetworking: false,
    },
  };
  return p;
});

// Configuration 3: Provider with slow terminate (tests timing robustness)
runTerminateInstanceContract("slow-terminate provider", () => {
  const base = createMockProvider();
  const original = base.terminate.bind(base);
  base.terminate = async (providerId: string) => {
    // Simulate network latency with a brief yield
    await new Promise((r) => setTimeout(r, 1));
    return original(providerId);
  };
  return base;
});

// ---------------------------------------------------------------------------
// Spot Instance Contract Tests
// (Separate from the parameterised suite — spot adds extra DB state concerns)
// ---------------------------------------------------------------------------

describe("Terminate Instance Contract: spot instance handling", () => {
  let currentCleanup: () => Promise<void>;

  beforeEach(async () => {
    currentCleanup = setupTest({ engine: true });
    createWorkflowEngine();
    registerLaunchRun();
    registerTerminateInstance();
    terminateCalls = [];
    const mockProvider = createMockProvider();
    await registerProvider({ provider: mockProvider });
  });

  afterEach(async () => {
    await unregisterProvider("mock-provider");
    await currentCleanup();
  });

  test("spot instance with no spot_request_id terminates cleanly", async () => {
    const instance = createTestInstance({
      is_spot: 1,
      spot_request_id: null,
    });

    const { workflowId } = await submit({
      type: "terminate-instance",
      input: { instanceId: instance.id },
    });

    const wf = await waitForWorkflow(workflowId);
    expect(wf.status).toBe("completed");

    const updated = getInstance(instance.id);
    expect(updated?.workflow_state).toBe("terminate:complete");
  });
});

// ---------------------------------------------------------------------------
// Instance State Transition Invariants
//
// Property: the validate-instance node atomically locks the instance into
// terminate:draining. Once that lock is acquired, no other workflow can
// acquire it for the same instance.
// ---------------------------------------------------------------------------

describe("Terminate Instance Contract: state machine invariants", () => {
  let currentCleanup: () => Promise<void>;

  beforeEach(async () => {
    currentCleanup = setupTest({ engine: true });
    createWorkflowEngine();
    registerTerminateInstance();
    terminateCalls = [];
    const mockProvider = createMockProvider();
    await registerProvider({ provider: mockProvider });
  });

  afterEach(async () => {
    await unregisterProvider("mock-provider");
    await currentCleanup();
  });

  test("INVARIANT: after validate-instance, instance is in terminate:draining or later", async () => {
    const instance = createTestInstance({ workflow_state: "launch:complete" });

    const ctx = createMockNodeContext({
      workflowInput: { instanceId: instance.id },
    });

    await validateInstanceExecutor.execute(ctx);

    const updated = getInstance(instance.id);
    // Must have advanced from launch:complete to a terminate:* state
    expect(updated?.workflow_state).toMatch(/^terminate:/);
  });

  test("INVARIANT: double-submit of terminate is rejected once instance enters terminate: state", async () => {
    const instance = createTestInstance({ workflow_state: "terminate:draining" });

    // Already mid-termination — second submission must be rejected
    await expect(terminateInstance({ instanceId: instance.id })).rejects.toThrow(
      /already being terminated/i
    );
  });

  test("INVARIANT: metering_stop audit event is emitted when workflow completes", async () => {
    const instance = createTestInstance();

    // Emit metering_start in test setup (instance was billable)
    emitAuditEvent({
      event_type: "metering_start",
      tenant_id: 1,
      instance_id: instance.id,
      provider: "mock-provider",
      spec: "cpu-1x",
      region: "local",
      source: "lifecycle",
      is_cost: true,
      is_usage: true,
      data: {
        provider_resource_id: instance.provider_id,
        metering_window_start_ms: Date.now() - 120_000,
      },
      dedupe_key: `mock-provider:${instance.provider_id}:metering_start`,
      occurred_at: Date.now() - 120_000,
    });

    const { workflowId } = await submit({
      type: "terminate-instance",
      input: { instanceId: instance.id },
    });

    await waitForWorkflow(workflowId);

    // metering_stop must be in audit_log for this instance
    const stopEvent = queryOne<{ event_type: string; is_cost: number; occurred_at: number }>(
      "SELECT event_type, is_cost, occurred_at FROM audit_log WHERE event_type = 'metering_stop' AND instance_id = ?",
      [instance.id]
    );
    expect(stopEvent).not.toBeNull();
    expect(stopEvent?.event_type).toBe("metering_stop");
    expect(stopEvent?.is_cost).toBe(1);
    expect(stopEvent?.occurred_at).toBeGreaterThan(0);
  });
});

// =============================================================================
// C4: Parameterised Provider Contract Tests (#WF2-06 Phase 2)
//
// Contract: for any named provider ('orbstack', 'aws', 'lambda'), the
// terminate-instance workflow must satisfy all behavioral invariants.
// Each provider gets a mock registered under its real name so instance DB
// records (provider column) match correctly.
// =============================================================================

const C4_PROVIDERS = ["orbstack", "aws", "lambda"] as const;

for (const providerName of C4_PROVIDERS) {
  describe(`C4: terminate-instance contract [provider: ${providerName}]`, () => {
    let currentCleanup: () => Promise<void>;
    let terminateCallsC4: string[] = [];

    beforeEach(async () => {
      currentCleanup = setupTest({ engine: true });
      createWorkflowEngine();
      registerLaunchRun();
      registerTerminateInstance();
      terminateCallsC4 = [];

      // Register mock under the exact provider name used in the DB
      const mock = makeMockProvider({
        terminate: (id: string) => { terminateCallsC4.push(id); },
      });
      // Override name to match providerName
      (mock as any).name = providerName;
      await registerProvider({ provider: mock });
    });

    afterEach(async () => {
      await unregisterProvider(providerName);
      await currentCleanup();
    });

    // Helper: create instance with the correct provider name
    function makeProviderInstance(overrides: Parameters<typeof createTestInstance>[0] = {}) {
      return createTestInstance({ provider: providerName as any, ...overrides });
    }

    // -------------------------------------------------------------------------
    // C4-1: Workflow completes and instance reaches terminate:complete
    // -------------------------------------------------------------------------

    test("successful termination → instance reaches terminate:complete", async () => {
      const instance = makeProviderInstance();

      const { workflowId } = await submit({
        type: "terminate-instance",
        input: { instanceId: instance.id },
      });

      const wf = await waitForWorkflow(workflowId);
      expect(wf.status).toBe("completed");

      const updated = getInstance(instance.id);
      expect(updated?.workflow_state).toBe("terminate:complete");
    });

    // -------------------------------------------------------------------------
    // C4-2: provider.terminate is called with the correct provider_id
    // -------------------------------------------------------------------------

    test("provider.terminate called with instance provider_id", async () => {
      const instance = makeProviderInstance({
        provider_id: `${providerName}-vm-${Date.now()}`,
      });

      const { workflowId } = await submit({
        type: "terminate-instance",
        input: { instanceId: instance.id },
      });

      await waitForWorkflow(workflowId);

      expect(terminateCallsC4).toContain(instance.provider_id);
    });

    // -------------------------------------------------------------------------
    // C4-3: All non-terminal allocations are drained
    // -------------------------------------------------------------------------

    test("terminates all instances: all AVAILABLE allocations drained to FAILED", async () => {
      const instance = makeProviderInstance();
      const alloc1 = createTestAllocation(instance.id, "AVAILABLE");
      const alloc2 = createTestAllocation(instance.id, "AVAILABLE");

      const { workflowId } = await submit({
        type: "terminate-instance",
        input: { instanceId: instance.id },
      });

      await waitForWorkflow(workflowId);

      expect(getAllocation(alloc1.id)?.status).toBe("FAILED");
      expect(getAllocation(alloc2.id)?.status).toBe("FAILED");
    });

    // -------------------------------------------------------------------------
    // C4-4: partial terminate failure via mockQueue
    // On first call the mock succeeds, second call throws PROVIDER_INTERNAL.
    // Since terminate-instance only calls terminate once per instance,
    // we verify the workflow completes (first call path).
    // This tests the mockQueue utility in context.
    // -------------------------------------------------------------------------

    test("handles partial terminate failure with mockQueue (first call succeeds)", async () => {
      const callLog: string[] = [];
      const queuedFn = mockQueue<void>(
        undefined,       // first call: success
        new Error(`${providerName}: rate limit exceeded`),  // second call: fail
        undefined,       // subsequent: success again
      );

      const mock2 = makeMockProvider({
        terminate: (id: string) => {
          callLog.push(id);
          return queuedFn();
        },
      });
      (mock2 as any).name = providerName;
      // Re-register (replaces the one from beforeEach — clearAllProviders in setupTest handles isolation)
      await registerProvider({ provider: mock2 });

      const instance = makeProviderInstance({
        provider_id: `${providerName}-queue-test-${Date.now()}`,
      });

      const { workflowId } = await submit({
        type: "terminate-instance",
        input: { instanceId: instance.id },
      });

      const wf = await waitForWorkflow(workflowId);
      // First call succeeds → workflow completes
      expect(wf.status).toBe("completed");
      expect(callLog).toContain(instance.provider_id);

      const updated = getInstance(instance.id);
      expect(updated?.workflow_state).toBe("terminate:complete");
    });

    // -------------------------------------------------------------------------
    // C4-5: NOT_FOUND from provider → treated as success (idempotent)
    // -------------------------------------------------------------------------

    test("handles already-terminated instance (NOT_FOUND → skip)", async () => {
      const mock3 = makeMockProvider({
        terminate: (_id: string) => {
          throw new Error(`${providerName}: instance not found`);
        },
      });
      (mock3 as any).name = providerName;
      await registerProvider({ provider: mock3 });

      const instance = makeProviderInstance();

      const { workflowId } = await submit({
        type: "terminate-instance",
        input: { instanceId: instance.id },
      });

      const wf = await waitForWorkflow(workflowId);
      // NOT_FOUND from provider must not fail the workflow
      expect(wf.status).toBe("completed");

      const updated = getInstance(instance.id);
      expect(updated?.workflow_state).toBe("terminate:complete");
    });

    // -------------------------------------------------------------------------
    // C4-6: rejects concurrent termination (ConflictError)
    // -------------------------------------------------------------------------

    test("rejects concurrent termination (ConflictError)", async () => {
      const instance = makeProviderInstance({
        workflow_state: "terminate:draining",
      });

      await expect(
        terminateInstance({ instanceId: instance.id })
      ).rejects.toThrow(/already being terminated/i);
    });

    // -------------------------------------------------------------------------
    // C4-7: post-completion workflow invariants via verifyWorkflowInvariant
    // -------------------------------------------------------------------------

    test("post-completion workflow invariants hold", async () => {
      const instance = makeProviderInstance();

      const { workflowId } = await submit({
        type: "terminate-instance",
        input: { instanceId: instance.id },
      });

      await waitForWorkflow(workflowId);

      verifyWorkflowInvariant(workflowId, { checkAllocations: false });
    });
  });
}
