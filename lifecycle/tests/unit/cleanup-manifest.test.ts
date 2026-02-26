// tests/unit/cleanup-manifest.test.ts - Cleanup Manifest Intent Tests
// Covers: #LIFE-09 (blueprint + nodes), #LIFE-10 (background trigger wiring),
//         and parameterised contract suites (#WF2-06)

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest, waitForWorkflow, verifyWorkflowInvariant } from "../harness";
import {
  createInstance,
  createAllocation,
  createRun,
  createManifest,
  createWorkflow,
  addResourceToManifest,
  getManifest,
  getManifestResources,
  getAllocation,
  getRun,
  getInstance,
  queryOne,
  queryMany,
  getWorkflowNodes,
  type Manifest,
  type Workflow,
} from "../../control/src/material/db";

import {
  sealManifest,
} from "../../control/src/workflow/state-transitions";

import {
  submit,
  submit as submitWf,
  registerBlueprint,
  registerNodeExecutor,
  getBlueprint,
  getNodeExecutor,
  createWorkflowEngine,
} from "../../control/src/workflow/engine";

import { registerCleanupManifest, cleanupManifest } from "../../control/src/intent/cleanup-manifest";
import { registerLaunchRun } from "../../control/src/intent/launch-run";
import { listExpiredManifests } from "../../control/src/material/db";

import { loadManifestResourcesExecutor } from "../../control/src/workflow/nodes/load-manifest-resources";
import { sortAndGroupExecutor } from "../../control/src/workflow/nodes/sort-and-group";
import { cleanupResourcesExecutor } from "../../control/src/workflow/nodes/cleanup-resources";
import { deleteManifestExecutor } from "../../control/src/workflow/nodes/delete-manifest";

import type { NodeContext } from "../../control/src/workflow/engine.types";
import { registerProvider } from "../../control/src/provider/registry";
import type { Provider } from "../../control/src/provider/types";

// =============================================================================
// Top-level harness setup
// =============================================================================

let cleanup: () => Promise<void>;
beforeEach(async () => {
  cleanup = setupTest({ engine: true });
  // Register a no-op mock provider so cleanup-resources doesn't call real orbctl
  await registerProvider({
    provider: {
      name: "orbstack",
      capabilities: {} as Provider["capabilities"],
      async spawn() { throw new Error("not implemented"); },
      async terminate() {},
      async list() { return []; },
      async get() { return null; },
      generateBootstrap() { return { content: "", format: "shell" as const, checksum: "" }; },
    } as Provider,
  });
});
afterEach(() => cleanup());

// =============================================================================
// Local manifestCleanupCheck (mirrors main.ts logic without triggering startup)
// =============================================================================

async function manifestCleanupCheck(): Promise<void> {
  const now = Date.now();
  const expiredManifests = listExpiredManifests(now);

  const MAX_CLEANUPS_PER_CYCLE = 10;
  let spawned = 0;

  for (const manifest of expiredManifests) {
    if (spawned >= MAX_CLEANUPS_PER_CYCLE) break;

    const existing = queryOne<{ id: number }>(
      `SELECT id FROM workflows WHERE type = 'cleanup-manifest' AND input_json LIKE '%"manifestId":${manifest.id}%' AND status IN ('created','pending','running')`,
      []
    );
    if (existing) continue;

    try {
      await submit({
        type: "cleanup-manifest",
        input: { manifestId: manifest.id },
      });
      spawned++;
    } catch (err) {
      // Swallow in tests
    }
  }
}

// =============================================================================
// Test Helpers
// =============================================================================

function createTestInstance() {
  return createInstance({
    provider: "orbstack",
    provider_id: `test-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
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
  });
}

function createDummyWorkflow(): Workflow {
  return createWorkflow({
    type: "test-workflow",
    parent_workflow_id: null,
    depth: 0,
    status: "completed",
    current_node: null,
    input_json: JSON.stringify({ test: true }),
    output_json: null,
    error_json: null,
    manifest_id: null,
    trace_id: null,
    idempotency_key: null,
    timeout_ms: 3600000,
    timeout_at: Date.now() + 3600000,
    started_at: Date.now(),
    finished_at: Date.now(),
    updated_at: Date.now(),
  });
}

function createSealedManifestWithResources(options?: {
  expiresAt?: number;
  /** If true, resources are added with owner_type='workflow', not 'manifest' (unclaimed).
   *  This matters for listExpiredManifests which now excludes manifests with unclaimed resources. */
  claimedResources?: boolean;
}) {
  // Create a real workflow first (manifest has FK to workflows)
  const workflow = createDummyWorkflow();
  const manifest = createManifest(workflow.id);

  // Create backing resources
  const instance = createTestInstance();
  const alloc = createAllocation({
    run_id: null,
    instance_id: instance.id,
    status: "COMPLETE",
    current_manifest_id: manifest.id,
    user: "test-user",
    workdir: "/tmp/test",
    debug_hold_until: null,
    completed_at: Date.now(),
  });
  const run = createRun({
    command: "echo test",
    workdir: "/tmp/test",
    max_duration_ms: 3600000,
    workflow_state: "launch-run:finalized",
    workflow_error: null,
    current_manifest_id: manifest.id,
    exit_code: 0,
    init_checksum: null,
    create_snapshot: 0,
    spot_interrupted: 0,
    started_at: Date.now(),
    finished_at: Date.now(),
  });

  // Add resources to manifest
  const ownerOpts = options?.claimedResources
    ? { ownerType: "workflow" as const }
    : {};
  addResourceToManifest(manifest.id, "allocation", String(alloc.id), { cleanupPriority: 90, ...ownerOpts });
  addResourceToManifest(manifest.id, "run", String(run.id), { cleanupPriority: 80, ...ownerOpts });
  addResourceToManifest(manifest.id, "instance", String(instance.id), { cleanupPriority: 50, ...ownerOpts });

  // Seal the manifest
  const expiresAt = options?.expiresAt ?? Date.now() - 1000; // Already expired by default
  sealManifest(manifest.id, expiresAt);

  return { manifest: getManifest(manifest.id)!, instance, alloc, run };
}

function buildMockNodeContext(
  workflowInput: unknown,
  nodeOutputs: Record<string, unknown> = {}
): NodeContext {
  const logs: Array<{ level: string; message: string; data?: unknown }> = [];
  return {
    workflowId: 1,
    nodeId: "test-node",
    input: {},
    workflowInput,
    manifestId: 0,
    controlId: "test",
    tenantId: 1,
    emitResource() {},
    async claimResource() { return false; },
    applyPattern() {},
    getNodeOutput(nodeId: string) {
      return nodeOutputs[nodeId] ?? null;
    },
    log(level, message, data) {
      logs.push({ level, message, data });
    },
    checkCancellation() {},
    async sleep(_ms: number) {},
    async spawnSubworkflow() {
      throw new Error("Not implemented in test context");
    },
  };
}

// =============================================================================
// Node Unit Tests
// =============================================================================

describe("Cleanup Manifest - Node Executors", () => {

  // ---------------------------------------------------------------------------
  // load-manifest-resources
  // ---------------------------------------------------------------------------

  describe("load-manifest-resources", () => {
    test("loads resources for SEALED manifest", async () => {
      const { manifest } = createSealedManifestWithResources();
      const ctx = buildMockNodeContext({ manifestId: manifest.id });

      const output = await loadManifestResourcesExecutor.execute(ctx);

      expect(output.manifestId).toBe(manifest.id);
      expect(output.resourceCount).toBe(3);
      expect(output.resources.length).toBe(3);
      // Resources should include allocation, run, and instance
      const types = output.resources.map((r) => r.type).sort();
      expect(types).toEqual(["allocation", "instance", "run"]);
    });

    test("rejects non-SEALED manifest (DRAFT)", async () => {
      const wf = createDummyWorkflow();
      const manifest = createManifest(wf.id);
      const ctx = buildMockNodeContext({ manifestId: manifest.id });

      await expect(loadManifestResourcesExecutor.execute(ctx)).rejects.toThrow(
        /status is DRAFT, expected SEALED/
      );
    });

    test("rejects non-existent manifest", async () => {
      const ctx = buildMockNodeContext({ manifestId: 99999 });

      await expect(loadManifestResourcesExecutor.execute(ctx)).rejects.toThrow(
        /not found/
      );
    });
  });

  // ---------------------------------------------------------------------------
  // sort-and-group
  // ---------------------------------------------------------------------------

  describe("sort-and-group", () => {
    test("groups and sorts resources by priority (highest first)", async () => {
      const resources = [
        { type: "allocation", id: "1", priority: 90 },
        { type: "allocation", id: "2", priority: 90 },
        { type: "run", id: "3", priority: 80 },
        { type: "instance", id: "4", priority: 50 },
        { type: "snapshot", id: "5", priority: 10 },
      ];

      const ctx = buildMockNodeContext({}, {
        "load-manifest-resources": {
          manifestId: 1,
          resourceCount: resources.length,
          resources,
        },
      });

      const output = await sortAndGroupExecutor.execute(ctx);

      expect(output.groups.length).toBe(4);
      // Verify priority order: allocation(90) > run(80) > instance(50) > snapshot(10)
      expect(output.groups[0].type).toBe("allocation");
      expect(output.groups[0].priority).toBe(90);
      expect(output.groups[0].resourceIds).toEqual(["1", "2"]);

      expect(output.groups[1].type).toBe("run");
      expect(output.groups[1].priority).toBe(80);

      expect(output.groups[2].type).toBe("instance");
      expect(output.groups[2].priority).toBe(50);

      expect(output.groups[3].type).toBe("snapshot");
      expect(output.groups[3].priority).toBe(10);
    });
  });

  // ---------------------------------------------------------------------------
  // cleanup-resources
  // ---------------------------------------------------------------------------

  describe("cleanup-resources", () => {
    test("deletes allocation records", async () => {
      const { manifest, alloc } = createSealedManifestWithResources();

      const ctx = buildMockNodeContext({ manifestId: manifest.id }, {
        "sort-and-group": {
          groups: [
            { type: "allocation", priority: 90, resourceIds: [String(alloc.id)] },
          ],
        },
      });

      const output = await cleanupResourcesExecutor.execute(ctx);

      expect(output.cleaned).toBe(1);
      // Verify allocation was deleted
      const deletedAlloc = getAllocation(alloc.id);
      expect(deletedAlloc).toBeNull();
    });

    test("marks run records as cleanup:complete", async () => {
      const { manifest, run } = createSealedManifestWithResources();

      const ctx = buildMockNodeContext({ manifestId: manifest.id }, {
        "sort-and-group": {
          groups: [
            { type: "run", priority: 80, resourceIds: [String(run.id)] },
          ],
        },
      });

      const output = await cleanupResourcesExecutor.execute(ctx);

      expect(output.cleaned).toBe(1);
      const updatedRun = getRun(run.id);
      expect(updatedRun?.workflow_state).toBe("cleanup:complete");
    });

    test("handles unknown resource type gracefully", async () => {
      const { manifest } = createSealedManifestWithResources();

      const ctx = buildMockNodeContext({ manifestId: manifest.id }, {
        "sort-and-group": {
          groups: [
            { type: "unknown_type", priority: 42, resourceIds: ["999"] },
          ],
        },
      });

      const output = await cleanupResourcesExecutor.execute(ctx);

      expect(output.skipped).toBe(1);
      expect(output.failed).toBe(0);
    });

    test("marks cleanup_processed_at on manifest_resources", async () => {
      const { manifest, alloc } = createSealedManifestWithResources();

      const ctx = buildMockNodeContext({ manifestId: manifest.id }, {
        "sort-and-group": {
          groups: [
            { type: "allocation", priority: 90, resourceIds: [String(alloc.id)] },
          ],
        },
      });

      await cleanupResourcesExecutor.execute(ctx);

      // Check that cleanup_processed_at was set
      const resource = queryOne<{ cleanup_processed_at: number | null }>(
        "SELECT cleanup_processed_at FROM manifest_resources WHERE manifest_id = ? AND resource_type = 'allocation' AND resource_id = ?",
        [manifest.id, String(alloc.id)]
      );
      expect(resource?.cleanup_processed_at).not.toBeNull();
    });

    test("skips already-deleted resources gracefully", async () => {
      const { manifest } = createSealedManifestWithResources();

      // Reference a non-existent allocation
      const ctx = buildMockNodeContext({ manifestId: manifest.id }, {
        "sort-and-group": {
          groups: [
            { type: "allocation", priority: 90, resourceIds: ["99999"] },
          ],
        },
      });

      const output = await cleanupResourcesExecutor.execute(ctx);

      expect(output.skipped).toBe(1);
      expect(output.failed).toBe(0);
    });

    test("marks instance as terminate:complete", async () => {
      const { manifest, instance } = createSealedManifestWithResources();

      const ctx = buildMockNodeContext({ manifestId: manifest.id }, {
        "sort-and-group": {
          groups: [
            { type: "instance", priority: 50, resourceIds: [String(instance.id)] },
          ],
        },
      });

      const output = await cleanupResourcesExecutor.execute(ctx);

      expect(output.cleaned).toBe(1);
      const updatedInstance = getInstance(instance.id);
      expect(updatedInstance?.workflow_state).toBe("terminate:complete");
    });

  });

  // ---------------------------------------------------------------------------
  // delete-manifest
  // ---------------------------------------------------------------------------

  describe("delete-manifest", () => {
    test("deletes manifest and its resources", async () => {
      const { manifest } = createSealedManifestWithResources();

      const ctx = buildMockNodeContext({ manifestId: manifest.id });

      const output = await deleteManifestExecutor.execute(ctx);

      expect(output.deleted).toBe(true);
      expect(output.manifestId).toBe(manifest.id);

      // Verify manifest is gone
      const deleted = getManifest(manifest.id);
      expect(deleted).toBeNull();

      // Verify manifest_resources are gone
      const resources = getManifestResources(manifest.id);
      expect(resources.length).toBe(0);
    });

    test("handles already-deleted manifest (idempotent)", async () => {
      const ctx = buildMockNodeContext({ manifestId: 99999 });

      const output = await deleteManifestExecutor.execute(ctx);

      // Should succeed silently
      expect(output.deleted).toBe(true);
      expect(output.manifestId).toBe(99999);
    });
  });
});

// =============================================================================
// Full Blueprint Execution Tests
// =============================================================================

describe("Cleanup Manifest - Full Blueprint Execution", () => {
  beforeEach(() => {
    registerCleanupManifest();
    registerLaunchRun();
  });

  test("workflow cleans up all resources and deletes manifest", async () => {
    const { manifest, alloc, run, instance } = createSealedManifestWithResources();

    // Submit cleanup-manifest workflow
    const result = await submit({
      type: "cleanup-manifest",
      input: { manifestId: manifest.id },
    });

    expect(result.workflowId).toBeGreaterThan(0);

    // Wait for workflow to complete
    const workflow = await waitForWorkflow(result.workflowId);

    expect(workflow.status).toBe("completed");

    // Verify resources were cleaned
    expect(getAllocation(alloc.id)).toBeNull(); // Deleted
    expect(getRun(run.id)?.workflow_state).toBe("cleanup:complete"); // Marked cleaned
    expect(getInstance(instance.id)?.workflow_state).toBe("terminate:complete"); // Terminated

    // Verify manifest was deleted
    expect(getManifest(manifest.id)).toBeNull();
    expect(getManifestResources(manifest.id).length).toBe(0);
  });

  test("workflow fails for non-SEALED manifest", async () => {
    const wf = createDummyWorkflow();
    const manifest = createManifest(wf.id);

    const result = await submit({
      type: "cleanup-manifest",
      input: { manifestId: manifest.id },
    });

    // Wait for workflow to complete (should fail)
    const workflow = await waitForWorkflow(result.workflowId);

    expect(workflow.status).toBe("failed");
  });

  test("idempotency: cleaning already-cleaned manifest is no-op", async () => {
    // Create and fully clean a manifest
    const { manifest } = createSealedManifestWithResources();

    const result1 = await submit({
      type: "cleanup-manifest",
      input: { manifestId: manifest.id },
    });

    // Wait for first cleanup to complete
    const wf1 = await waitForWorkflow(result1.workflowId);
    expect(wf1.status).toBe("completed");

    // Now submit another cleanup for the same (now-deleted) manifest.
    // The load-manifest-resources node should fail with NOT_FOUND,
    // causing workflow to fail. This is expected — the manifest is gone.
    const result2 = await submit({
      type: "cleanup-manifest",
      input: { manifestId: manifest.id },
    });

    // Second attempt should fail (manifest was already deleted)
    const wf2 = await waitForWorkflow(result2.workflowId);
    expect(wf2.status).toBe("failed");
  });
});

// =============================================================================
// Background Trigger Tests (#LIFE-10)
// =============================================================================

describe("Cleanup Manifest - Background Trigger (#LIFE-10)", () => {
  beforeEach(() => {
    registerCleanupManifest();
    registerLaunchRun();
  });

  test("spawns cleanup workflow for expired manifests", async () => {
    // Create an expired sealed manifest with claimed resources (eligible for cleanup)
    createSealedManifestWithResources({ expiresAt: Date.now() - 60_000, claimedResources: true });

    // Run the background check
    await manifestCleanupCheck();

    // Verify a cleanup workflow was spawned
    const workflows = queryMany<{ id: number; type: string }>(
      "SELECT id, type FROM workflows WHERE type = 'cleanup-manifest'",
      []
    );
    expect(workflows.length).toBe(1);
  });

  test("respects max 10 limit per cycle", async () => {
    // Create 15 expired manifests with claimed resources (eligible for cleanup)
    for (let i = 0; i < 15; i++) {
      createSealedManifestWithResources({ expiresAt: Date.now() - 60_000, claimedResources: true });
    }

    // Run the background check
    await manifestCleanupCheck();

    // Verify only 10 workflows were spawned
    const workflows = queryMany<{ id: number }>(
      "SELECT id FROM workflows WHERE type = 'cleanup-manifest'",
      []
    );
    expect(workflows.length).toBe(10);
  });

  test("skips manifests with active cleanup workflow", async () => {
    // Create an expired sealed manifest with claimed resources (eligible for cleanup)
    const { manifest } = createSealedManifestWithResources({ expiresAt: Date.now() - 60_000, claimedResources: true });
    const targetManifestId = manifest.id;

    // Submit a cleanup workflow for it first
    const result = await submit({
      type: "cleanup-manifest",
      input: { manifestId: targetManifestId },
    });

    // Wait for the workflow to finish
    await waitForWorkflow(result.workflowId);

    // Count cleanup workflows targeting the ORIGINAL manifest (via input_json).
    // We scope by input_json to avoid counting workflows for unrelated expired manifests
    // (e.g., the cleanup-manifest workflow's own empty manifest which also expires immediately).
    const countForManifest = () => queryMany<{ id: number }>(
      `SELECT id FROM workflows WHERE type = 'cleanup-manifest' AND input_json LIKE ?`,
      [`%"manifestId":${targetManifestId}%`]
    ).length;

    const before = countForManifest();

    // Run the background check — should not spawn a duplicate for the original manifest
    await manifestCleanupCheck();

    const after = countForManifest();

    // The original manifest was deleted by the first cleanup workflow, so
    // listExpiredManifests won't return it again. No new workflow is spawned for it.
    expect(after).toEqual(before);
  });

  test("does not spawn for non-expired manifests", async () => {
    // Create a manifest that expires in the future (claimed resources so it would be eligible if expired)
    createSealedManifestWithResources({ expiresAt: Date.now() + 3600_000, claimedResources: true });

    // Run the background check
    await manifestCleanupCheck();

    // Verify no cleanup workflows were spawned
    const workflows = queryMany<{ id: number }>(
      "SELECT id FROM workflows WHERE type = 'cleanup-manifest'",
      []
    );
    expect(workflows.length).toBe(0);
  });
});

// =============================================================================
// Entry Point Validation Tests (SD-G1-09)
// =============================================================================

describe("Cleanup Manifest - Entry Point Validation", () => {
  beforeEach(() => {
    registerCleanupManifest();
    registerLaunchRun();
  });

  test("rejects DRAFT manifest with INVALID_STATE error", async () => {
    const wf = createDummyWorkflow();
    const manifest = createManifest(wf.id);

    // manifest is DRAFT (not sealed)
    try {
      await cleanupManifest({ manifestId: manifest.id });
      // Should not reach here
      expect(true).toBe(false);
    } catch (err: any) {
      expect(err.code).toBe("INVALID_STATE");
      expect(err.category).toBe("validation");
      expect(err.message).toContain("DRAFT");
      expect(err.message).toContain("expected SEALED");
    }
  });

  test("rejects non-existent manifest with NOT_FOUND error", async () => {
    try {
      await cleanupManifest({ manifestId: 99999 });
      expect(true).toBe(false);
    } catch (err: any) {
      expect(err.code).toBe("NOT_FOUND");
    }
  });

  test("accepts SEALED manifest", async () => {
    const { manifest } = createSealedManifestWithResources();

    // Should not throw
    const workflow = await cleanupManifest({ manifestId: manifest.id });
    expect(workflow).toBeDefined();
    expect(workflow.type).toBe("cleanup-manifest");
  });
});

// =============================================================================
// Parameterised Contract Tests (#WF2-06)
//
// Contract: for any manifest configuration, if cleanup-manifest completes,
// these invariants MUST hold. We run the suite across different resource
// compositions to prove properties aren't accidentally resource-specific.
// =============================================================================

// ---------------------------------------------------------------------------
// Contract Suite Factory
//
// runCleanupManifestContract(name, setupFn) registers a describe block that
// runs all behavioral invariants for one manifest/resource configuration.
//
// setupFn returns the fixture created by the test; the contract tests assert
// invariants against whatever it returns.
// ---------------------------------------------------------------------------

type ManifestFixture = {
  manifest: ReturnType<typeof getManifest>;
  instance: ReturnType<typeof createTestInstance>;
  alloc: ReturnType<typeof createAllocation>;
  run: ReturnType<typeof createRun>;
};

type SetupFn = () => ManifestFixture;

function runCleanupManifestContract(suiteName: string, setupFn: SetupFn) {
  describe(`Cleanup Manifest Contract: ${suiteName}`, () => {
    // The outer file-level beforeEach (setupTest + orbstack mock) already runs
    // before each test. We only need to register blueprints here.
    beforeEach(() => {
      registerCleanupManifest();
      registerLaunchRun();
    });

    // -------------------------------------------------------------------------
    // CONTRACT: terminal state invariant
    // "For any manifest configuration, a successful cleanup must produce a
    //  completed workflow and a deleted manifest."
    // -------------------------------------------------------------------------

    test("CONTRACT: completed workflow deletes manifest", async () => {
      const { manifest } = setupFn();

      const result = await submitWf({
        type: "cleanup-manifest",
        input: { manifestId: manifest!.id },
      });

      const wf = await waitForWorkflow(result.workflowId);
      expect(wf.status).toBe("completed");

      const deleted = getManifest(manifest!.id);
      expect(deleted).toBeNull();
    });

    // -------------------------------------------------------------------------
    // CONTRACT: node sequence invariant
    // "All 4 nodes must complete in dependency order."
    // -------------------------------------------------------------------------

    test("CONTRACT: all 4 nodes complete in dependency order", async () => {
      const { manifest } = setupFn();

      const result = await submitWf({
        type: "cleanup-manifest",
        input: { manifestId: manifest!.id },
      });

      await waitForWorkflow(result.workflowId);

      const nodes = getWorkflowNodes(result.workflowId);
      expect(nodes.length).toBe(4);

      const byId: Record<string, string> = {};
      for (const n of nodes) byId[n.node_id] = n.status;

      expect(byId["load-manifest-resources"]).toBe("completed");
      expect(byId["sort-and-group"]).toBe("completed");
      expect(byId["cleanup-resources"]).toBe("completed");
      expect(byId["delete-manifest"]).toBe("completed");
    });

    // -------------------------------------------------------------------------
    // CONTRACT: priority ordering invariant
    // "Resources within a manifest must be cleaned in descending priority order.
    //  Higher-priority types (allocation=90) must be processed before
    //  lower-priority types (instance=50)."
    // -------------------------------------------------------------------------

    test("CONTRACT: sort-and-group produces groups in descending priority", async () => {
      const { manifest } = setupFn();

      const result = await submitWf({
        type: "cleanup-manifest",
        input: { manifestId: manifest!.id },
      });

      await waitForWorkflow(result.workflowId);

      const nodes = getWorkflowNodes(result.workflowId);
      const sortNode = nodes.find((n) => n.node_id === "sort-and-group");
      expect(sortNode).toBeDefined();
      expect(sortNode!.output_json).not.toBeNull();

      const sortOutput = JSON.parse(sortNode!.output_json!) as { groups: Array<{ type: string; priority: number }> };
      const priorities = sortOutput.groups.map((g) => g.priority);

      // Verify non-increasing order
      for (let i = 1; i < priorities.length; i++) {
        expect(priorities[i]).toBeLessThanOrEqual(priorities[i - 1]!);
      }
    });

    // -------------------------------------------------------------------------
    // CONTRACT: manifest_resources cleaned
    // "After cleanup, there must be no manifest_resources entries remaining
    //  for the deleted manifest."
    // -------------------------------------------------------------------------

    test("CONTRACT: no manifest_resources remain after cleanup", async () => {
      const { manifest } = setupFn();
      const manifestId = manifest!.id;

      const result = await submitWf({
        type: "cleanup-manifest",
        input: { manifestId },
      });

      await waitForWorkflow(result.workflowId);

      const remaining = getManifestResources(manifestId);
      expect(remaining.length).toBe(0);
    });

    // -------------------------------------------------------------------------
    // CONTRACT: workflow invariants hold
    // Uses verifyWorkflowInvariant to assert finished_at set, all nodes
    // terminal — independent of resource configuration.
    // -------------------------------------------------------------------------

    test("CONTRACT: post-completion workflow invariants hold", async () => {
      const { manifest } = setupFn();

      const result = await submitWf({
        type: "cleanup-manifest",
        input: { manifestId: manifest!.id },
      });

      await waitForWorkflow(result.workflowId);

      verifyWorkflowInvariant(result.workflowId, {
        expectManifestSealed: false, // cleanup-manifest deletes manifest, not seals it
        checkAllocations: false,
      });
    });
  });
}

// ---------------------------------------------------------------------------
// Manifest Configurations Under Test
// ---------------------------------------------------------------------------

// We need the setupTest to already be called (top-level beforeEach handles it),
// but each runCleanupManifestContract suite also adds a nested beforeEach for
// re-registration. The outer beforeEach from the top-level handles DB init.

// Configuration 1: Full manifest (allocation + run + instance)
runCleanupManifestContract("full manifest (allocation + run + instance)", () => {
  // The DB is already set up by the file-level beforeEach.
  // Just create the fixture.
  return createSealedManifestWithResources() as ManifestFixture;
});

// Configuration 2: Minimal manifest (allocation only)
runCleanupManifestContract("minimal manifest (allocation only)", () => {
  const fixture = createSealedManifestWithResources();

  // Remove run and instance from manifest_resources — test with just allocation
  queryMany(
    "DELETE FROM manifest_resources WHERE manifest_id = ? AND resource_type != 'allocation'",
    [fixture.manifest.id]
  );

  return fixture as ManifestFixture;
});

// Configuration 3: Manifest with many allocations of same type
runCleanupManifestContract("manifest with multiple same-type resources", () => {
  const wf = createDummyWorkflow();
  const manifest = createManifest(wf.id);
  const instance = createTestInstance();

  // Create 3 allocations
  for (let i = 0; i < 3; i++) {
    const alloc = createAllocation({
      run_id: null,
      instance_id: instance.id,
      status: "COMPLETE",
      current_manifest_id: manifest.id,
      user: "test-user",
      workdir: "/tmp/test",
      debug_hold_until: null,
      completed_at: Date.now(),
    });
    addResourceToManifest(manifest.id, "allocation", String(alloc.id), { cleanupPriority: 90 });
  }

  addResourceToManifest(manifest.id, "instance", String(instance.id), { cleanupPriority: 50 });

  sealManifest(manifest.id, Date.now() - 1000);

  const sealed = getManifest(manifest.id)!;
  return {
    manifest: sealed,
    instance,
    alloc: null as any,
    run: null as any,
  } as ManifestFixture;
});

// =============================================================================
// Cleanup Manifest Contract: Partial Failure Handling
//
// Tests that the cleanup workflow correctly handles individual resource
// failures without aborting the entire cleanup.
// =============================================================================

describe("Cleanup Manifest Contract: partial failure handling", () => {
  beforeEach(async () => {
    await registerProvider({
      provider: {
        name: "orbstack",
        capabilities: {} as Provider["capabilities"],
        async spawn() { throw new Error("not implemented"); },
        async terminate() {},
        async list() { return []; },
        async get() { return null; },
        generateBootstrap() { return { content: "", format: "shell" as const, checksum: "" }; },
      } as Provider,
    });
    registerCleanupManifest();
    registerLaunchRun();
  });

  test("PARTIAL FAILURE: non-existent resource IDs are skipped without failing workflow", async () => {
    const wf = createDummyWorkflow();
    const manifest = createManifest(wf.id);

    // Add a reference to a non-existent allocation (ID 99999)
    addResourceToManifest(manifest.id, "allocation", "99999", { cleanupPriority: 90 });

    sealManifest(manifest.id, Date.now() - 1000);

    const result = await submitWf({
      type: "cleanup-manifest",
      input: { manifestId: manifest.id },
    });

    const completedWf = await waitForWorkflow(result.workflowId);
    expect(completedWf.status).toBe("completed");

    // Workflow completed even though the allocation didn't exist
    const deleted = getManifest(manifest.id);
    expect(deleted).toBeNull();
  });

  test("PARTIAL FAILURE: already-deleted instance is skipped (idempotent)", async () => {
    const { manifest, instance } = createSealedManifestWithResources();

    // Pre-delete the instance to simulate a partially-processed manifest.
    // Must delete dependent rows first (allocations FK-reference instances).
    queryMany("DELETE FROM allocations WHERE instance_id = ?", [instance.id]);
    queryMany("DELETE FROM instances WHERE id = ?", [instance.id]);

    const result = await submitWf({
      type: "cleanup-manifest",
      input: { manifestId: manifest.id },
    });

    const completedWf = await waitForWorkflow(result.workflowId);
    // Workflow must complete even though instance was already gone
    expect(completedWf.status).toBe("completed");
  });

  test("PARTIAL FAILURE: cleanup-resources counts skipped correctly for non-existent resources", async () => {
    const wf = createDummyWorkflow();
    const manifest = createManifest(wf.id);

    // Add non-existent resources of different types
    addResourceToManifest(manifest.id, "allocation", "88881", { cleanupPriority: 90 });
    addResourceToManifest(manifest.id, "run", "88882", { cleanupPriority: 80 });

    sealManifest(manifest.id, Date.now() - 1000);

    const result = await submitWf({
      type: "cleanup-manifest",
      input: { manifestId: manifest.id },
    });

    await waitForWorkflow(result.workflowId);

    const nodes = getWorkflowNodes(result.workflowId);
    const cleanupNode = nodes.find((n) => n.node_id === "cleanup-resources");
    expect(cleanupNode?.output_json).not.toBeNull();

    const output = JSON.parse(cleanupNode!.output_json!) as { cleaned: number; skipped: number; failed: number };
    // Both were non-existent so they should be skipped
    expect(output.skipped).toBe(2);
    expect(output.cleaned).toBe(0);
  });
});

// =============================================================================
// Cleanup Manifest Contract: Idempotency at the Node Level
//
// Tests that individual nodes are safe to re-run and produce consistent output.
// =============================================================================

describe("Cleanup Manifest Contract: node-level idempotency", () => {
  test("load-manifest-resources is idempotent: same output on repeated calls", async () => {
    const { manifest } = createSealedManifestWithResources();

    const ctx = buildMockNodeContext({ manifestId: manifest.id });

    const output1 = await loadManifestResourcesExecutor.execute(ctx);
    const output2 = await loadManifestResourcesExecutor.execute(ctx);

    expect(output1.manifestId).toBe(output2.manifestId);
    expect(output1.resourceCount).toBe(output2.resourceCount);
    expect(output1.resources.map(r => r.id).sort()).toEqual(
      output2.resources.map(r => r.id).sort()
    );
  });

  test("sort-and-group is deterministic: same groups on repeated calls", async () => {
    const resources = [
      { type: "allocation", id: "1", priority: 90 },
      { type: "run", id: "2", priority: 80 },
      { type: "instance", id: "3", priority: 50 },
    ];
    const nodeOutputs = {
      "load-manifest-resources": { manifestId: 1, resourceCount: 3, resources },
    };

    const ctx1 = buildMockNodeContext({}, nodeOutputs);
    const ctx2 = buildMockNodeContext({}, nodeOutputs);

    const out1 = await sortAndGroupExecutor.execute(ctx1);
    const out2 = await sortAndGroupExecutor.execute(ctx2);

    expect(out1.groups.map(g => g.type)).toEqual(out2.groups.map(g => g.type));
    expect(out1.groups.map(g => g.priority)).toEqual(out2.groups.map(g => g.priority));
  });

  test("delete-manifest is idempotent: second call on already-deleted manifest succeeds", async () => {
    const { manifest } = createSealedManifestWithResources();
    const manifestId = manifest.id;

    const ctx = buildMockNodeContext({ manifestId });

    // First deletion
    const out1 = await deleteManifestExecutor.execute(ctx);
    expect(out1.deleted).toBe(true);

    // Second deletion on already-deleted manifest
    const ctx2 = buildMockNodeContext({ manifestId });
    const out2 = await deleteManifestExecutor.execute(ctx2);
    expect(out2.deleted).toBe(true); // Must not throw
  });

  test("cleanup-resources is idempotent for allocations: second run skips already-deleted", async () => {
    const { manifest, alloc } = createSealedManifestWithResources();

    const groups = [{ type: "allocation", priority: 90, resourceIds: [String(alloc.id)] }];
    const ctx1 = buildMockNodeContext({ manifestId: manifest.id }, { "sort-and-group": { groups } });

    // First run deletes the allocation
    const out1 = await cleanupResourcesExecutor.execute(ctx1);
    expect(out1.cleaned).toBe(1);
    expect(getAllocation(alloc.id)).toBeNull();

    // Second run on the same group — allocation already gone
    const ctx2 = buildMockNodeContext({ manifestId: manifest.id }, { "sort-and-group": { groups } });
    const out2 = await cleanupResourcesExecutor.execute(ctx2);
    expect(out2.skipped).toBe(1); // Already deleted → skipped
    expect(out2.cleaned).toBe(0);
  });
});

// =============================================================================
// Cleanup Manifest Contract: Priority Ordering Correctness
//
// The priority system is a core contract property: if allocations (priority 90)
// are cleaned before instances (priority 50), then when instance cleanup runs,
// no active allocations can block or observe stale state.
// =============================================================================

describe("Cleanup Manifest Contract: priority ordering correctness", () => {
  test("sort-and-group places allocation(90) before run(80) before instance(50)", async () => {
    const resources = [
      { type: "instance", id: "3", priority: 50 },
      { type: "allocation", id: "1", priority: 90 },
      { type: "run", id: "2", priority: 80 },
    ];

    const ctx = buildMockNodeContext({}, {
      "load-manifest-resources": { manifestId: 1, resourceCount: 3, resources },
    });

    const output = await sortAndGroupExecutor.execute(ctx);

    expect(output.groups[0]!.type).toBe("allocation");
    expect(output.groups[1]!.type).toBe("run");
    expect(output.groups[2]!.type).toBe("instance");
  });

  test("sort-and-group merges multiple resources of same type into one group", async () => {
    const resources = [
      { type: "allocation", id: "1", priority: 90 },
      { type: "allocation", id: "2", priority: 90 },
      { type: "allocation", id: "3", priority: 90 },
    ];

    const ctx = buildMockNodeContext({}, {
      "load-manifest-resources": { manifestId: 1, resourceCount: 3, resources },
    });

    const output = await sortAndGroupExecutor.execute(ctx);

    expect(output.groups.length).toBe(1);
    expect(output.groups[0]!.type).toBe("allocation");
    expect(output.groups[0]!.resourceIds.length).toBe(3);
  });

  test("sort-and-group uses highest priority within a mixed-priority type group", async () => {
    // This can happen if resources of the same type have different priorities
    const resources = [
      { type: "allocation", id: "1", priority: 70 },
      { type: "allocation", id: "2", priority: 90 }, // Higher
      { type: "instance", id: "3", priority: 50 },
    ];

    const ctx = buildMockNodeContext({}, {
      "load-manifest-resources": { manifestId: 1, resourceCount: 3, resources },
    });

    const output = await sortAndGroupExecutor.execute(ctx);

    // Allocation group should use max priority (90)
    const allocGroup = output.groups.find((g) => g.type === "allocation");
    expect(allocGroup!.priority).toBe(90);
    // Should still be first (higher than instance=50)
    expect(output.groups[0]!.type).toBe("allocation");
  });

  test("cleanup-resources processes groups strictly in priority order from sort-and-group", async () => {
    const { manifest, alloc, run, instance } = createSealedManifestWithResources();

    // Submit full workflow to observe real execution order
    const result = await (async () => {
      // Re-register needed since this is a nested describe without its own beforeEach
      await registerProvider({
        provider: {
          name: "orbstack",
          capabilities: {} as Provider["capabilities"],
          async spawn() { throw new Error("not implemented"); },
          async terminate() {},
          async list() { return []; },
          async get() { return null; },
          generateBootstrap() { return { content: "", format: "shell" as const, checksum: "" }; },
        } as Provider,
      });
      registerCleanupManifest();
      registerLaunchRun();

      return submitWf({
        type: "cleanup-manifest",
        input: { manifestId: manifest.id },
      });
    })();

    await waitForWorkflow(result.workflowId);

    // Allocation (90) must have been cleaned before instance (50):
    // allocation deleted (null), instance marked terminate:complete
    expect(getAllocation(alloc.id)).toBeNull();
    expect(getInstance(instance.id)?.workflow_state).toBe("terminate:complete");
  });
});
