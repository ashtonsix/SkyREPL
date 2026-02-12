// tests/unit/workflow-subworkflow.test.ts - Subworkflow Contract Tests (#WF-03)
// Covers: cancel propagation, timeout propagation, manifestId in result,
// cancellation-aware wait(), depth enforcement, manifest independence

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import {
  initDatabase,
  closeDatabase,
  runMigrations,
  getWorkflow,
  getWorkflowNodes,
  queryMany,
  type Workflow,
} from "../../control/src/material/db";
import {
  registerBlueprint,
  registerNodeExecutor,
  submit,
  cancelWorkflow,
  requestEngineShutdown,
  awaitEngineQuiescence,
  resetEngineShutdown,
  MAX_SUBWORKFLOW_DEPTH,
} from "../../control/src/workflow/engine";
import type { SubworkflowResult } from "../../control/src/workflow/engine.types";

// =============================================================================
// Test Setup
// =============================================================================

beforeEach(() => {
  resetEngineShutdown();
  initDatabase(":memory:");
  runMigrations();
});

afterEach(async () => {
  requestEngineShutdown();
  await awaitEngineQuiescence(5_000);
  closeDatabase();
});

// =============================================================================
// Helpers
// =============================================================================

/** Wait for a workflow to reach a terminal state */
async function waitForWorkflow(
  workflowId: number,
  timeoutMs = 5_000
): Promise<Workflow> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const wf = getWorkflow(workflowId);
    if (wf && ["completed", "failed", "cancelled"].includes(wf.status)) {
      return wf;
    }
    await Bun.sleep(50);
  }
  const wf = getWorkflow(workflowId);
  throw new Error(
    `Workflow ${workflowId} did not terminate within ${timeoutMs}ms (status: ${wf?.status})`
  );
}

// =============================================================================
// Tests
// =============================================================================

describe("Subworkflow Contract (#WF-03)", () => {
  test("basic: parent spawns child, child completes, parent continues", async () => {
    registerNodeExecutor({
      name: "sub-noop",
      idempotent: true,
      async execute() {
        return { ok: true };
      },
    });

    registerNodeExecutor({
      name: "sub-spawn-child",
      idempotent: false,
      async execute(ctx) {
        const handle = await ctx.spawnSubworkflow("sub-child-basic", {
          fromParent: true,
        });
        const result = await handle.wait();
        return { childResult: result };
      },
    });

    registerBlueprint({
      type: "sub-parent-basic",
      entryNode: "start",
      nodes: {
        start: { type: "sub-spawn-child" },
        done: { type: "sub-noop", dependsOn: ["start"] },
      },
    });

    registerBlueprint({
      type: "sub-child-basic",
      entryNode: "work",
      nodes: {
        work: { type: "sub-noop" },
      },
    });

    const result = await submit({
      type: "sub-parent-basic",
      input: {},
    });

    const wf = await waitForWorkflow(result.workflowId);
    expect(wf.status).toBe("completed");

    // Verify child also completed
    const children = queryMany<Workflow>(
      "SELECT * FROM workflows WHERE parent_workflow_id = ?",
      [result.workflowId]
    );
    expect(children.length).toBe(1);
    expect(children[0].status).toBe("completed");
  });

  test("child failure propagates: child fails, parent node receives failure", async () => {
    registerNodeExecutor({
      name: "sub-always-fail",
      idempotent: true,
      async execute() {
        throw Object.assign(new Error("child boom"), {
          code: "VALIDATION_ERROR",
          category: "validation",
        });
      },
    });

    registerNodeExecutor({
      name: "sub-spawn-failing-child",
      idempotent: false,
      async execute(ctx) {
        const handle = await ctx.spawnSubworkflow("sub-child-fail", {});
        const result = await handle.wait();
        if (result.status === "failed") {
          throw Object.assign(new Error("Child workflow failed"), {
            code: "VALIDATION_ERROR",
            category: "validation",
          });
        }
        return { childResult: result };
      },
    });

    registerBlueprint({
      type: "sub-parent-fail",
      entryNode: "start",
      nodes: {
        start: { type: "sub-spawn-failing-child" },
      },
    });

    registerBlueprint({
      type: "sub-child-fail",
      entryNode: "work",
      nodes: {
        work: { type: "sub-always-fail" },
      },
    });

    const result = await submit({
      type: "sub-parent-fail",
      input: {},
    });

    const wf = await waitForWorkflow(result.workflowId);
    expect(wf.status).toBe("failed");
  });

  test("cancel cascade: cancel parent cascades to child", async () => {
    let childStarted = false;

    registerNodeExecutor({
      name: "sub-slow-child",
      idempotent: true,
      async execute(ctx) {
        childStarted = true;
        // Spin until cancelled or shutdown
        for (let i = 0; i < 100; i++) {
          await ctx.sleep(50);
        }
        return { ok: true };
      },
    });

    registerNodeExecutor({
      name: "sub-spawn-slow-child",
      idempotent: false,
      async execute(ctx) {
        const handle = await ctx.spawnSubworkflow("sub-child-slow", {});
        const result = await handle.wait();
        return { childResult: result };
      },
    });

    registerBlueprint({
      type: "sub-parent-cancel",
      entryNode: "start",
      nodes: {
        start: { type: "sub-spawn-slow-child" },
      },
    });

    registerBlueprint({
      type: "sub-child-slow",
      entryNode: "work",
      nodes: {
        work: { type: "sub-slow-child" },
      },
    });

    const result = await submit({
      type: "sub-parent-cancel",
      input: {},
    });

    // Wait for child to start
    const deadline = Date.now() + 3_000;
    while (!childStarted && Date.now() < deadline) {
      await Bun.sleep(50);
    }
    expect(childStarted).toBe(true);

    // Cancel parent
    const cancelResult = await cancelWorkflow(result.workflowId, "user_request");
    expect(cancelResult.success).toBe(true);

    // Wait a bit for propagation
    await Bun.sleep(500);

    // Child should be cancelled
    const children = queryMany<Workflow>(
      "SELECT * FROM workflows WHERE parent_workflow_id = ?",
      [result.workflowId]
    );
    expect(children.length).toBe(1);
    expect(children[0].status).toBe("cancelled");
  });

  test("timeout cascade: parent timeout cancels child", async () => {
    registerNodeExecutor({
      name: "sub-slow-for-timeout",
      idempotent: true,
      async execute(ctx) {
        // Spin until cancelled or shutdown
        for (let i = 0; i < 100; i++) {
          await ctx.sleep(50);
        }
        return { ok: true };
      },
    });

    registerNodeExecutor({
      name: "sub-spawn-for-timeout",
      idempotent: false,
      async execute(ctx) {
        const handle = await ctx.spawnSubworkflow("sub-child-timeout", {});
        const result = await handle.wait();
        return { childResult: result };
      },
    });

    registerBlueprint({
      type: "sub-parent-timeout",
      entryNode: "start",
      nodes: {
        start: { type: "sub-spawn-for-timeout" },
      },
    });

    registerBlueprint({
      type: "sub-child-timeout",
      entryNode: "work",
      nodes: {
        work: { type: "sub-slow-for-timeout" },
      },
    });

    // Submit with a very short timeout
    const result = await submit({
      type: "sub-parent-timeout",
      input: {},
      timeout: 300, // 300ms timeout
    });

    // Wait for parent to time out and fail
    const wf = await waitForWorkflow(result.workflowId);
    expect(wf.status).toBe("failed");

    // Verify error is timeout
    const error = wf.error_json ? JSON.parse(wf.error_json) : null;
    expect(error?.code).toBe("WORKFLOW_TIMEOUT");

    // Wait for child cancellation to propagate
    await Bun.sleep(500);

    // Child should be cancelled
    const children = queryMany<Workflow>(
      "SELECT * FROM workflows WHERE parent_workflow_id = ?",
      [result.workflowId]
    );
    expect(children.length).toBe(1);
    expect(children[0].status).toBe("cancelled");
  });

  test("max depth enforcement: depth exceeding MAX_SUBWORKFLOW_DEPTH errors", async () => {
    // Register a recursive spawner that chains subworkflows
    registerNodeExecutor({
      name: "sub-recursive-spawn",
      idempotent: false,
      async execute(ctx) {
        const handle = await ctx.spawnSubworkflow("sub-recursive", {});
        const result = await handle.wait();
        if (result.status === "failed") {
          throw Object.assign(new Error(result.error ?? "Child workflow failed"), {
            code: "VALIDATION_ERROR",
            category: "validation",
          });
        }
        return { childResult: result };
      },
    });

    registerNodeExecutor({
      name: "sub-depth-noop",
      idempotent: true,
      async execute() {
        return { ok: true };
      },
    });

    registerBlueprint({
      type: "sub-recursive",
      entryNode: "spawn",
      nodes: {
        spawn: { type: "sub-recursive-spawn" },
      },
    });

    // Submit depth-0 workflow; it will spawn depth-1, which spawns depth-2,
    // which spawns depth-3, which spawns depth-4 (should fail at MAX_SUBWORKFLOW_DEPTH=3)
    const result = await submit({
      type: "sub-recursive",
      input: {},
    });

    // Eventually the chain should fail due to depth limit
    const wf = await waitForWorkflow(result.workflowId, 10_000);
    expect(wf.status).toBe("failed");
  });

  test("manifest independence: child and parent have separate manifests", async () => {
    registerNodeExecutor({
      name: "sub-emit-resource",
      idempotent: true,
      async execute(ctx) {
        ctx.emitResource("instance", "res-" + ctx.workflowId, 10);
        return { emitted: true };
      },
    });

    registerNodeExecutor({
      name: "sub-spawn-emitter",
      idempotent: false,
      async execute(ctx) {
        ctx.emitResource("instance", "parent-res-" + ctx.workflowId, 10);
        const handle = await ctx.spawnSubworkflow("sub-child-emitter", {});
        const result = await handle.wait();
        return { childResult: result };
      },
    });

    registerBlueprint({
      type: "sub-parent-emitter",
      entryNode: "start",
      nodes: {
        start: { type: "sub-spawn-emitter" },
      },
    });

    registerBlueprint({
      type: "sub-child-emitter",
      entryNode: "work",
      nodes: {
        work: { type: "sub-emit-resource" },
      },
    });

    const result = await submit({
      type: "sub-parent-emitter",
      input: {},
    });

    const wf = await waitForWorkflow(result.workflowId);
    expect(wf.status).toBe("completed");

    // Verify parent and child have different manifest_ids
    const parent = getWorkflow(result.workflowId)!;
    const children = queryMany<Workflow>(
      "SELECT * FROM workflows WHERE parent_workflow_id = ?",
      [result.workflowId]
    );
    expect(children.length).toBe(1);
    const child = children[0];

    expect(parent.manifest_id).not.toBeNull();
    expect(child.manifest_id).not.toBeNull();
    expect(parent.manifest_id).not.toBe(child.manifest_id);
  });

  test("SubworkflowResult includes manifestId", async () => {
    let capturedResult: SubworkflowResult | null = null;

    registerNodeExecutor({
      name: "sub-capture-noop",
      idempotent: true,
      async execute() {
        return { captured: true };
      },
    });

    registerNodeExecutor({
      name: "sub-spawn-capture",
      idempotent: false,
      async execute(ctx) {
        const handle = await ctx.spawnSubworkflow("sub-child-capture", {});
        const result = await handle.wait();
        capturedResult = result;
        return { childResult: result };
      },
    });

    registerBlueprint({
      type: "sub-parent-capture",
      entryNode: "start",
      nodes: {
        start: { type: "sub-spawn-capture" },
      },
    });

    registerBlueprint({
      type: "sub-child-capture",
      entryNode: "work",
      nodes: {
        work: { type: "sub-capture-noop" },
      },
    });

    const result = await submit({
      type: "sub-parent-capture",
      input: {},
    });

    await waitForWorkflow(result.workflowId);

    expect(capturedResult).not.toBeNull();
    expect(capturedResult!.status).toBe("completed");
    expect(capturedResult!.manifestId).toBeDefined();
    expect(typeof capturedResult!.manifestId).toBe("number");

    // Verify it matches the actual child manifest
    const children = queryMany<Workflow>(
      "SELECT * FROM workflows WHERE parent_workflow_id = ?",
      [result.workflowId]
    );
    expect(capturedResult!.manifestId).toBe(children[0].manifest_id);
  });

  test("multiple children: parent spawns 2 children, both complete", async () => {
    registerNodeExecutor({
      name: "sub-multi-noop",
      idempotent: true,
      async execute() {
        return { ok: true };
      },
    });

    registerNodeExecutor({
      name: "sub-spawn-two-children",
      idempotent: false,
      async execute(ctx) {
        const handle1 = await ctx.spawnSubworkflow("sub-child-multi", {
          child: 1,
        });
        const handle2 = await ctx.spawnSubworkflow("sub-child-multi", {
          child: 2,
        });
        const [r1, r2] = await Promise.all([handle1.wait(), handle2.wait()]);
        return { child1: r1, child2: r2 };
      },
    });

    registerBlueprint({
      type: "sub-parent-multi",
      entryNode: "start",
      nodes: {
        start: { type: "sub-spawn-two-children" },
      },
    });

    registerBlueprint({
      type: "sub-child-multi",
      entryNode: "work",
      nodes: {
        work: { type: "sub-multi-noop" },
      },
    });

    const result = await submit({
      type: "sub-parent-multi",
      input: {},
    });

    const wf = await waitForWorkflow(result.workflowId);
    expect(wf.status).toBe("completed");

    // Verify both children completed
    const children = queryMany<Workflow>(
      "SELECT * FROM workflows WHERE parent_workflow_id = ?",
      [result.workflowId]
    );
    expect(children.length).toBe(2);
    expect(children.every((c) => c.status === "completed")).toBe(true);
  });

  test("child cancellation does not kill parent (parent handles gracefully)", async () => {
    registerNodeExecutor({
      name: "sub-graceful-noop",
      idempotent: true,
      async execute() {
        return { ok: true };
      },
    });

    let childWorkflowId: number | null = null;

    registerNodeExecutor({
      name: "sub-spawn-then-cancel-child",
      idempotent: false,
      async execute(ctx) {
        const handle = await ctx.spawnSubworkflow("sub-child-graceful", {});
        childWorkflowId = handle.workflowId;
        // Cancel the child immediately
        await cancelWorkflow(handle.workflowId, "test_cancel");
        const result = await handle.wait();
        // Parent gracefully handles child cancellation
        return { childStatus: result.status, handled: true };
      },
    });

    registerBlueprint({
      type: "sub-parent-graceful",
      entryNode: "start",
      nodes: {
        start: { type: "sub-spawn-then-cancel-child" },
      },
    });

    registerBlueprint({
      type: "sub-child-graceful",
      entryNode: "work",
      nodes: {
        work: { type: "sub-graceful-noop" },
      },
    });

    const result = await submit({
      type: "sub-parent-graceful",
      input: {},
    });

    const wf = await waitForWorkflow(result.workflowId);
    // Parent should complete successfully since it handled the child cancellation
    expect(wf.status).toBe("completed");
  });

  test("subworkflow idempotency: submitting with same key returns existing", async () => {
    registerNodeExecutor({
      name: "sub-idem-noop",
      idempotent: true,
      async execute() {
        return { ok: true };
      },
    });

    registerBlueprint({
      type: "sub-idem-test",
      entryNode: "work",
      nodes: {
        work: { type: "sub-idem-noop" },
      },
    });

    const result1 = await submit({
      type: "sub-idem-test",
      input: {},
      idempotencyKey: "sub-idem-key-1",
    });

    const result2 = await submit({
      type: "sub-idem-test",
      input: {},
      idempotencyKey: "sub-idem-key-1",
    });

    expect(result1.workflowId).toBe(result2.workflowId);
    expect(result2.status).toBe("deduplicated");
  });

  test("depth tracking: child workflow has depth parent+1", async () => {
    registerNodeExecutor({
      name: "sub-depth-track-noop",
      idempotent: true,
      async execute() {
        return { ok: true };
      },
    });

    registerNodeExecutor({
      name: "sub-depth-track-spawn",
      idempotent: false,
      async execute(ctx) {
        const handle = await ctx.spawnSubworkflow("sub-depth-track-child", {});
        const result = await handle.wait();
        return { childResult: result };
      },
    });

    registerBlueprint({
      type: "sub-depth-track-parent",
      entryNode: "start",
      nodes: {
        start: { type: "sub-depth-track-spawn" },
      },
    });

    registerBlueprint({
      type: "sub-depth-track-child",
      entryNode: "work",
      nodes: {
        work: { type: "sub-depth-track-noop" },
      },
    });

    const result = await submit({
      type: "sub-depth-track-parent",
      input: {},
    });

    await waitForWorkflow(result.workflowId);

    const parent = getWorkflow(result.workflowId)!;
    expect(parent.depth).toBe(0);

    const children = queryMany<Workflow>(
      "SELECT * FROM workflows WHERE parent_workflow_id = ?",
      [result.workflowId]
    );
    expect(children.length).toBe(1);
    expect(children[0].depth).toBe(1);
  });
});
