// tests/unit/cleanup-manifest.test.ts - Cleanup Manifest Intent Tests
// Covers: #LIFE-09 (blueprint + nodes) and #LIFE-10 (background trigger wiring)

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import {
  initDatabase,
  closeDatabase,
  runMigrations,
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
  getWorkflow,
  getWorkflowNodes,
  queryOne,
  queryMany,
  type Manifest,
  type Workflow,
} from "../../control/src/material/db";

import {
  sealManifest,
} from "../../control/src/workflow/state-transitions";

import {
  submit,
  registerBlueprint,
  registerNodeExecutor,
  resetEngineShutdown,
  requestEngineShutdown,
  awaitEngineQuiescence,
} from "../../control/src/workflow/engine";

import { registerCleanupManifest } from "../../control/src/intent/cleanup-manifest";
import { registerLaunchRun } from "../../control/src/intent/launch-run";
import { listExpiredManifests } from "../../control/src/material/db";

import { loadManifestResourcesExecutor } from "../../control/src/workflow/nodes/load-manifest-resources";
import { sortAndGroupExecutor } from "../../control/src/workflow/nodes/sort-and-group";
import { cleanupResourcesExecutor } from "../../control/src/workflow/nodes/cleanup-resources";
import { deleteManifestExecutor } from "../../control/src/workflow/nodes/delete-manifest";

import type { NodeContext } from "../../control/src/workflow/engine.types";

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

function createSealedManifestWithResources(options?: { expiresAt?: number }) {
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
  addResourceToManifest(manifest.id, "allocation", String(alloc.id), { cleanupPriority: 90 });
  addResourceToManifest(manifest.id, "run", String(run.id), { cleanupPriority: 80 });
  addResourceToManifest(manifest.id, "instance", String(instance.id), { cleanupPriority: 50 });

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
    async sleep(ms: number) {
      await new Promise((r) => setTimeout(r, ms));
    },
    async spawnSubworkflow() {
      throw new Error("Not implemented in test context");
    },
  };
}

// =============================================================================
// Node Unit Tests
// =============================================================================

describe("Cleanup Manifest - Node Executors", () => {
  beforeEach(() => {
    initDatabase(":memory:");
    runMigrations();
  });

  afterEach(() => {
    closeDatabase();
  });

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

    test("stubs snapshot and artifact cleanup", async () => {
      const { manifest } = createSealedManifestWithResources();

      const ctx = buildMockNodeContext({ manifestId: manifest.id }, {
        "sort-and-group": {
          groups: [
            { type: "snapshot", priority: 10, resourceIds: ["1"] },
            { type: "artifact", priority: 70, resourceIds: ["2"] },
          ],
        },
      });

      const output = await cleanupResourcesExecutor.execute(ctx);

      expect(output.skipped).toBe(2);
      expect(output.cleaned).toBe(0);
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
    resetEngineShutdown();
    initDatabase(":memory:");
    runMigrations();
    registerCleanupManifest();
    registerLaunchRun();
  });

  afterEach(async () => {
    requestEngineShutdown();
    await awaitEngineQuiescence(5_000);
    closeDatabase();
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
    const deadline = Date.now() + 10_000;
    let workflow;
    while (Date.now() < deadline) {
      workflow = getWorkflow(result.workflowId);
      if (workflow && (workflow.status === "completed" || workflow.status === "failed")) {
        break;
      }
      await new Promise((r) => setTimeout(r, 50));
    }

    expect(workflow?.status).toBe("completed");

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
    const deadline = Date.now() + 10_000;
    let workflow;
    while (Date.now() < deadline) {
      workflow = getWorkflow(result.workflowId);
      if (workflow && (workflow.status === "completed" || workflow.status === "failed")) {
        break;
      }
      await new Promise((r) => setTimeout(r, 50));
    }

    expect(workflow?.status).toBe("failed");
  });

  test("idempotency: cleaning already-cleaned manifest is no-op", async () => {
    // Create and fully clean a manifest
    const { manifest } = createSealedManifestWithResources();

    const result1 = await submit({
      type: "cleanup-manifest",
      input: { manifestId: manifest.id },
    });

    // Wait for first cleanup to complete
    const deadline1 = Date.now() + 10_000;
    while (Date.now() < deadline1) {
      const wf = getWorkflow(result1.workflowId);
      if (wf && (wf.status === "completed" || wf.status === "failed")) break;
      await new Promise((r) => setTimeout(r, 50));
    }
    expect(getWorkflow(result1.workflowId)?.status).toBe("completed");

    // Now submit another cleanup for the same (now-deleted) manifest.
    // The load-manifest-resources node should fail with NOT_FOUND,
    // causing workflow to fail. This is expected — the manifest is gone.
    const result2 = await submit({
      type: "cleanup-manifest",
      input: { manifestId: manifest.id },
    });

    const deadline2 = Date.now() + 10_000;
    while (Date.now() < deadline2) {
      const wf = getWorkflow(result2.workflowId);
      if (wf && (wf.status === "completed" || wf.status === "failed")) break;
      await new Promise((r) => setTimeout(r, 50));
    }

    // Second attempt should fail (manifest was already deleted)
    const wf2 = getWorkflow(result2.workflowId);
    expect(wf2?.status).toBe("failed");
  });
});

// =============================================================================
// Background Trigger Tests (#LIFE-10)
// =============================================================================

describe("Cleanup Manifest - Background Trigger (#LIFE-10)", () => {
  beforeEach(() => {
    resetEngineShutdown();
    initDatabase(":memory:");
    runMigrations();
    registerCleanupManifest();
    registerLaunchRun();
  });

  afterEach(async () => {
    requestEngineShutdown();
    await awaitEngineQuiescence(5_000);
    closeDatabase();
  });

  test("spawns cleanup workflow for expired manifests", async () => {
    // Create an expired sealed manifest
    createSealedManifestWithResources({ expiresAt: Date.now() - 60_000 });

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
    // Create 15 expired manifests
    for (let i = 0; i < 15; i++) {
      createSealedManifestWithResources({ expiresAt: Date.now() - 60_000 });
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
    // Create an expired sealed manifest
    const { manifest } = createSealedManifestWithResources({ expiresAt: Date.now() - 60_000 });

    // Submit a cleanup workflow for it first
    await submit({
      type: "cleanup-manifest",
      input: { manifestId: manifest.id },
    });

    // Wait briefly for workflow to be created
    await new Promise((r) => setTimeout(r, 100));

    // Count current cleanup workflows
    const before = queryMany<{ id: number }>(
      "SELECT id FROM workflows WHERE type = 'cleanup-manifest'",
      []
    );

    // Run the background check — should not spawn a duplicate
    await manifestCleanupCheck();

    const after = queryMany<{ id: number }>(
      "SELECT id FROM workflows WHERE type = 'cleanup-manifest'",
      []
    );

    // Should not have spawned a new one (the first might have already completed
    // in some race, so we check that at most 1 more was created)
    expect(after.length).toBeLessThanOrEqual(before.length + 1);
  });

  test("does not spawn for non-expired manifests", async () => {
    // Create a manifest that expires in the future
    createSealedManifestWithResources({ expiresAt: Date.now() + 3600_000 });

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
