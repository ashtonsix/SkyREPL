// tests/unit/workflow-cancel-patterns.test.ts - Cancel Propagation Across All Workflow Patterns (#WF-07)
// Covers: IAR cancel, PFO cancel, subworkflow cancel cascade, mixed patterns, idempotency

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import {
  initDatabase,
  closeDatabase,
  runMigrations,
  getWorkflow,
  getWorkflowNodes,
  queryOne,
  queryMany,
  type Workflow,
  type WorkflowNode,
} from "../../control/src/material/db";
import {
  submit,
  registerBlueprint,
  registerNodeExecutor,
  cancelWorkflow,
  requestEngineShutdown,
  awaitEngineQuiescence,
  resetEngineShutdown,
} from "../../control/src/workflow/engine";

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

/** Wait for a condition to be true */
async function waitFor(
  fn: () => boolean,
  timeoutMs = 3_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (fn()) return;
    await Bun.sleep(50);
  }
  throw new Error(`waitFor timed out after ${timeoutMs}ms`);
}

function getNodeByNodeId(
  workflowId: number,
  nodeId: string
): WorkflowNode | null {
  return queryOne<WorkflowNode>(
    "SELECT * FROM workflow_nodes WHERE workflow_id = ? AND node_id = ?",
    [workflowId, nodeId]
  );
}

// =============================================================================
// IAR Cancel Tests
// =============================================================================

describe("Cancel Propagation: IAR Pattern (#WF-07)", () => {
  test("IAR cancel: pending inserted node and target node get skipped", async () => {
    // Workflow: source -> [IAR inserts 'inserted' before 'target'] -> target -> done
    // Cancel after source completes but before inserted node runs.
    let sourceFinished = false;

    registerNodeExecutor({
      name: "iar-cancel-source",
      idempotent: true,
      async execute(ctx) {
        ctx.applyPattern("insert-and-reconverge", {
          insertedNode: {
            id: "inserted",
            type: "iar-cancel-slow",
            input: {},
          },
          beforeNode: "target",
        });
        sourceFinished = true;
        return { done: true };
      },
    });

    registerNodeExecutor({
      name: "iar-cancel-slow",
      idempotent: true,
      async execute(ctx) {
        // Slow node — gives us time to cancel
        for (let i = 0; i < 100; i++) {
          await ctx.sleep(50);
        }
        return { ok: true };
      },
    });

    registerNodeExecutor({
      name: "iar-cancel-target",
      idempotent: true,
      async execute() {
        return { target: true };
      },
    });

    registerNodeExecutor({
      name: "iar-cancel-done",
      idempotent: true,
      async execute() {
        return { done: true };
      },
    });

    registerBlueprint({
      type: "test-iar-cancel",
      entryNode: "source",
      nodes: {
        source: { type: "iar-cancel-source", dependsOn: [] },
        target: { type: "iar-cancel-target", dependsOn: ["source"] },
        done: { type: "iar-cancel-done", dependsOn: ["target"] },
      },
    });

    const result = await submit({
      type: "test-iar-cancel",
      input: {},
    });

    // Wait for source to finish (which applies IAR)
    await waitFor(() => sourceFinished);

    // Wait a moment for inserted node to become ready/start
    await Bun.sleep(100);

    // Cancel the workflow
    const cancelResult = await cancelWorkflow(result.workflowId, "test_cancel");
    expect(cancelResult.success).toBe(true);

    // Wait for propagation
    await Bun.sleep(500);

    // Verify workflow is cancelled
    const wf = getWorkflow(result.workflowId);
    expect(wf!.status).toBe("cancelled");

    // Verify: target and done nodes should be skipped (they were pending)
    const target = getNodeByNodeId(result.workflowId, "target");
    expect(target).not.toBeNull();
    expect(target!.status).toBe("skipped");

    const done = getNodeByNodeId(result.workflowId, "done");
    expect(done).not.toBeNull();
    expect(done!.status).toBe("skipped");
  });

  test("IAR cancel: running inserted node — workflow reaches cancelled", async () => {
    // Create a workflow where IAR inserts a slow node, then cancel while it runs
    let insertedNodeStarted = false;

    registerNodeExecutor({
      name: "iar-cancel2-source",
      idempotent: true,
      async execute(ctx) {
        ctx.applyPattern("insert-and-reconverge", {
          insertedNode: {
            id: "slow-inserted",
            type: "iar-cancel2-slow",
            input: {},
          },
          beforeNode: "target",
        });
        return { done: true };
      },
    });

    registerNodeExecutor({
      name: "iar-cancel2-slow",
      idempotent: true,
      async execute(ctx) {
        insertedNodeStarted = true;
        // Spin until cancelled or timeout
        for (let i = 0; i < 100; i++) {
          await ctx.sleep(50);
        }
        return { ok: true };
      },
    });

    registerNodeExecutor({
      name: "iar-cancel2-target",
      idempotent: true,
      async execute() {
        return { target: true };
      },
    });

    registerBlueprint({
      type: "test-iar-cancel2",
      entryNode: "source",
      nodes: {
        source: { type: "iar-cancel2-source", dependsOn: [] },
        target: { type: "iar-cancel2-target", dependsOn: ["source"] },
      },
    });

    const result = await submit({
      type: "test-iar-cancel2",
      input: {},
    });

    // Wait for the inserted node to start running
    await waitFor(() => insertedNodeStarted);

    // Cancel the workflow while inserted node is running
    const cancelResult = await cancelWorkflow(result.workflowId, "test_cancel");
    expect(cancelResult.success).toBe(true);

    // Wait for propagation
    await Bun.sleep(500);

    // Verify workflow status
    const wf = getWorkflow(result.workflowId);
    expect(wf!.status).toBe("cancelled");

    // Target node should be skipped (was pending, blocked on inserted node)
    const target = getNodeByNodeId(result.workflowId, "target");
    expect(target!.status).toBe("skipped");
  });
});

// =============================================================================
// PFO Cancel Tests
// =============================================================================

describe("Cancel Propagation: PFO Pattern (#WF-07)", () => {
  test("PFO cancel: all pending branches and join node get skipped", async () => {
    // Apply PFO, cancel immediately before branches start
    let sourceFinished = false;

    registerNodeExecutor({
      name: "pfo-cancel-source",
      idempotent: true,
      async execute(ctx) {
        ctx.applyPattern("parallel-fan-out", {
          branches: [
            { id: "branch-1", type: "pfo-cancel-slow", input: { idx: 1 } },
            { id: "branch-2", type: "pfo-cancel-slow", input: { idx: 2 } },
            { id: "branch-3", type: "pfo-cancel-slow", input: { idx: 3 } },
          ],
          joinNode: "join",
          sourceNodeId: "source",
        });
        sourceFinished = true;
        return { done: true };
      },
    });

    registerNodeExecutor({
      name: "pfo-cancel-slow",
      idempotent: true,
      async execute(ctx) {
        // Slow — gives time to cancel
        for (let i = 0; i < 100; i++) {
          await ctx.sleep(50);
        }
        return { ok: true };
      },
    });

    registerNodeExecutor({
      name: "pfo-cancel-join",
      idempotent: true,
      async execute() {
        return { joined: true };
      },
    });

    registerBlueprint({
      type: "test-pfo-cancel-all-pending",
      entryNode: "source",
      nodes: {
        source: { type: "pfo-cancel-source", dependsOn: [] },
        join: { type: "pfo-cancel-join", dependsOn: ["source"] },
      },
    });

    const result = await submit({
      type: "test-pfo-cancel-all-pending",
      input: {},
    });

    // Wait for source to finish (which creates PFO branches)
    await waitFor(() => sourceFinished);

    // Cancel immediately
    const cancelResult = await cancelWorkflow(result.workflowId, "test_cancel");
    expect(cancelResult.success).toBe(true);

    // Wait for propagation
    await Bun.sleep(500);

    const wf = getWorkflow(result.workflowId);
    expect(wf!.status).toBe("cancelled");

    // Check that all pending nodes got skipped
    const nodes = getWorkflowNodes(result.workflowId);
    const skippedNodes = nodes.filter((n) => n.status === "skipped");
    const skippedIds = skippedNodes.map((n) => n.node_id).sort();

    // At minimum, the join node should be skipped
    expect(skippedIds).toContain("join");

    // Branches that were still pending should be skipped
    // (some may have started running before cancel was processed)
    for (const n of nodes) {
      if (n.node_id.startsWith("branch-")) {
        expect(["skipped", "running", "completed"]).toContain(n.status);
      }
    }
  });

  test("PFO cancel: some branches running — workflow reaches cancelled", async () => {
    let branchesStarted = 0;

    registerNodeExecutor({
      name: "pfo-cancel2-source",
      idempotent: true,
      async execute(ctx) {
        ctx.applyPattern("parallel-fan-out", {
          branches: [
            { id: "b1", type: "pfo-cancel2-slow-branch", input: { idx: 1 } },
            { id: "b2", type: "pfo-cancel2-slow-branch", input: { idx: 2 } },
          ],
          joinNode: "join",
          sourceNodeId: "source",
        });
        return { done: true };
      },
    });

    registerNodeExecutor({
      name: "pfo-cancel2-slow-branch",
      idempotent: true,
      async execute(ctx) {
        branchesStarted++;
        for (let i = 0; i < 100; i++) {
          await ctx.sleep(50);
        }
        return { ok: true };
      },
    });

    registerNodeExecutor({
      name: "pfo-cancel2-join",
      idempotent: true,
      async execute() {
        return { joined: true };
      },
    });

    registerBlueprint({
      type: "test-pfo-cancel-running",
      entryNode: "source",
      nodes: {
        source: { type: "pfo-cancel2-source", dependsOn: [] },
        join: { type: "pfo-cancel2-join", dependsOn: ["source"] },
      },
    });

    const result = await submit({
      type: "test-pfo-cancel-running",
      input: {},
    });

    // Wait for at least one branch to start running
    await waitFor(() => branchesStarted >= 1);

    // Cancel while branches are running
    const cancelResult = await cancelWorkflow(result.workflowId, "test_cancel");
    expect(cancelResult.success).toBe(true);

    // Wait for propagation
    await Bun.sleep(500);

    const wf = getWorkflow(result.workflowId);
    expect(wf!.status).toBe("cancelled");

    // Join node should be skipped (was pending)
    const join = getNodeByNodeId(result.workflowId, "join");
    expect(join!.status).toBe("skipped");
  });

  test("PFO first-failure-cancels: branch failure skips pending siblings", async () => {
    const executionLog: string[] = [];

    registerNodeExecutor({
      name: "pfo-ffc-cancel-source",
      idempotent: true,
      async execute(ctx) {
        executionLog.push("source");
        ctx.applyPattern("parallel-fan-out", {
          branches: [
            { id: "fail-branch", type: "pfo-ffc-cancel-fail", input: {} },
            { id: "slow-branch", type: "pfo-ffc-cancel-slow", input: {} },
          ],
          joinNode: "join",
          sourceNodeId: "source",
          joinMode: "first-failure-cancels",
        });
        return { done: true };
      },
    });

    registerNodeExecutor({
      name: "pfo-ffc-cancel-fail",
      idempotent: true,
      async execute() {
        executionLog.push("fail-branch");
        throw Object.assign(new Error("branch failed"), {
          code: "VALIDATION_ERROR",
          category: "validation",
        });
      },
    });

    registerNodeExecutor({
      name: "pfo-ffc-cancel-slow",
      idempotent: true,
      async execute(ctx) {
        // This should get skipped if the engine processes the fast failure first
        await ctx.sleep(200);
        executionLog.push("slow-branch");
        return { ok: true };
      },
    });

    registerNodeExecutor({
      name: "pfo-ffc-cancel-join",
      idempotent: true,
      async execute() {
        executionLog.push("join");
        return { joined: true };
      },
    });

    registerBlueprint({
      type: "test-pfo-ffc-cancel",
      entryNode: "source",
      nodes: {
        source: { type: "pfo-ffc-cancel-source", dependsOn: [] },
        join: { type: "pfo-ffc-cancel-join", dependsOn: ["source"] },
      },
    });

    const result = await submit({
      type: "test-pfo-ffc-cancel",
      input: {},
    });

    // Wait for workflow to terminate
    const wf = await waitForWorkflow(result.workflowId);
    expect(wf.status).toBe("failed");

    // The failing branch should have failed
    const failBranch = getNodeByNodeId(result.workflowId, "fail-branch");
    expect(failBranch!.status).toBe("failed");

    // Join should not have run
    expect(executionLog).not.toContain("join");
  });
});

// =============================================================================
// Subworkflow Cancel Tests
// =============================================================================

describe("Cancel Propagation: Subworkflow (#WF-07)", () => {
  test("parent cancel cascades to child subworkflow", async () => {
    let childStarted = false;

    registerNodeExecutor({
      name: "sub-cancel-slow-child",
      idempotent: true,
      async execute(ctx) {
        childStarted = true;
        for (let i = 0; i < 100; i++) {
          await ctx.sleep(50);
        }
        return { ok: true };
      },
    });

    registerNodeExecutor({
      name: "sub-cancel-spawn",
      idempotent: false,
      async execute(ctx) {
        const handle = await ctx.spawnSubworkflow("sub-cancel-child", {});
        const result = await handle.wait();
        return { childResult: result };
      },
    });

    registerBlueprint({
      type: "sub-cancel-parent",
      entryNode: "start",
      nodes: {
        start: { type: "sub-cancel-spawn" },
      },
    });

    registerBlueprint({
      type: "sub-cancel-child",
      entryNode: "work",
      nodes: {
        work: { type: "sub-cancel-slow-child" },
      },
    });

    const result = await submit({
      type: "sub-cancel-parent",
      input: {},
    });

    // Wait for child to start
    await waitFor(() => childStarted);

    // Cancel parent
    const cancelResult = await cancelWorkflow(result.workflowId, "user_cancel");
    expect(cancelResult.success).toBe(true);

    // Wait for propagation
    await Bun.sleep(500);

    // Verify child was cancelled
    const children = queryMany<Workflow>(
      "SELECT * FROM workflows WHERE parent_workflow_id = ?",
      [result.workflowId]
    );
    expect(children.length).toBe(1);
    expect(children[0].status).toBe("cancelled");
  });

  test("nested subworkflow cancel (depth 2): parent -> child -> grandchild", async () => {
    let grandchildStarted = false;

    registerNodeExecutor({
      name: "nested-cancel-slow",
      idempotent: true,
      async execute(ctx) {
        grandchildStarted = true;
        for (let i = 0; i < 100; i++) {
          await ctx.sleep(50);
        }
        return { ok: true };
      },
    });

    registerNodeExecutor({
      name: "nested-cancel-spawn-grandchild",
      idempotent: false,
      async execute(ctx) {
        const handle = await ctx.spawnSubworkflow("nested-cancel-grandchild", {});
        const result = await handle.wait();
        return { grandchildResult: result };
      },
    });

    registerNodeExecutor({
      name: "nested-cancel-spawn-child",
      idempotent: false,
      async execute(ctx) {
        const handle = await ctx.spawnSubworkflow("nested-cancel-child", {});
        const result = await handle.wait();
        return { childResult: result };
      },
    });

    registerBlueprint({
      type: "nested-cancel-parent",
      entryNode: "start",
      nodes: {
        start: { type: "nested-cancel-spawn-child" },
      },
    });

    registerBlueprint({
      type: "nested-cancel-child",
      entryNode: "spawn",
      nodes: {
        spawn: { type: "nested-cancel-spawn-grandchild" },
      },
    });

    registerBlueprint({
      type: "nested-cancel-grandchild",
      entryNode: "work",
      nodes: {
        work: { type: "nested-cancel-slow" },
      },
    });

    const result = await submit({
      type: "nested-cancel-parent",
      input: {},
    });

    // Wait for grandchild to start
    await waitFor(() => grandchildStarted, 5_000);

    // Cancel top-level parent
    const cancelResult = await cancelWorkflow(result.workflowId, "user_cancel");
    expect(cancelResult.success).toBe(true);

    // Wait for cascading cancel propagation
    await Bun.sleep(1000);

    // Verify child cancelled
    const children = queryMany<Workflow>(
      "SELECT * FROM workflows WHERE parent_workflow_id = ?",
      [result.workflowId]
    );
    expect(children.length).toBe(1);
    expect(children[0].status).toBe("cancelled");

    // Verify grandchild cancelled
    const grandchildren = queryMany<Workflow>(
      "SELECT * FROM workflows WHERE parent_workflow_id = ?",
      [children[0].id]
    );
    expect(grandchildren.length).toBe(1);
    expect(grandchildren[0].status).toBe("cancelled");
  });

  test("child cancel does not error when parent is already cancelled", async () => {
    let childWorkflowId: number | null = null;
    let childStarted = false;

    registerNodeExecutor({
      name: "sub-noerr-slow",
      idempotent: true,
      async execute(ctx) {
        childStarted = true;
        for (let i = 0; i < 100; i++) {
          await ctx.sleep(50);
        }
        return { ok: true };
      },
    });

    registerNodeExecutor({
      name: "sub-noerr-spawn",
      idempotent: false,
      async execute(ctx) {
        const handle = await ctx.spawnSubworkflow("sub-noerr-child", {});
        childWorkflowId = handle.workflowId;
        const result = await handle.wait();
        return { childResult: result };
      },
    });

    registerBlueprint({
      type: "sub-noerr-parent",
      entryNode: "start",
      nodes: {
        start: { type: "sub-noerr-spawn" },
      },
    });

    registerBlueprint({
      type: "sub-noerr-child",
      entryNode: "work",
      nodes: {
        work: { type: "sub-noerr-slow" },
      },
    });

    const result = await submit({
      type: "sub-noerr-parent",
      input: {},
    });

    // Wait for child to start
    await waitFor(() => childStarted);

    // Cancel parent (cascades to child)
    const cancelResult = await cancelWorkflow(result.workflowId, "user_cancel");
    expect(cancelResult.success).toBe(true);

    // Wait for propagation
    await Bun.sleep(500);

    // Now try to cancel child again — should not throw, should be idempotent
    expect(childWorkflowId).not.toBeNull();
    const childCancelResult = await cancelWorkflow(childWorkflowId!, "double_cancel");
    // Should succeed (idempotent — already cancelled)
    expect(childCancelResult.success).toBe(true);
    expect(childCancelResult.status).toBe("cancelled");
  });
});

// =============================================================================
// Mixed Pattern Cancel Tests
// =============================================================================

describe("Cancel Propagation: Mixed Patterns (#WF-07)", () => {
  test("PFO + subworkflow cancel: PFO branches with subworkflows cascade-cancel", async () => {
    let subworkflowsStarted = 0;

    registerNodeExecutor({
      name: "mixed-pfo-source",
      idempotent: true,
      async execute(ctx) {
        ctx.applyPattern("parallel-fan-out", {
          branches: [
            { id: "branch-a", type: "mixed-pfo-branch-spawn", input: { idx: "a" } },
            { id: "branch-b", type: "mixed-pfo-branch-spawn", input: { idx: "b" } },
          ],
          joinNode: "join",
          sourceNodeId: "source",
        });
        return { done: true };
      },
    });

    registerNodeExecutor({
      name: "mixed-pfo-branch-spawn",
      idempotent: false,
      async execute(ctx) {
        const input = ctx.input as { idx: string };
        const handle = await ctx.spawnSubworkflow("mixed-pfo-sub-child", {
          branch: input.idx,
        });
        const result = await handle.wait();
        return { childResult: result };
      },
    });

    registerNodeExecutor({
      name: "mixed-pfo-sub-slow",
      idempotent: true,
      async execute(ctx) {
        subworkflowsStarted++;
        for (let i = 0; i < 100; i++) {
          await ctx.sleep(50);
        }
        return { ok: true };
      },
    });

    registerNodeExecutor({
      name: "mixed-pfo-join",
      idempotent: true,
      async execute() {
        return { joined: true };
      },
    });

    registerBlueprint({
      type: "mixed-pfo-parent",
      entryNode: "source",
      nodes: {
        source: { type: "mixed-pfo-source", dependsOn: [] },
        join: { type: "mixed-pfo-join", dependsOn: ["source"] },
      },
    });

    registerBlueprint({
      type: "mixed-pfo-sub-child",
      entryNode: "work",
      nodes: {
        work: { type: "mixed-pfo-sub-slow" },
      },
    });

    const result = await submit({
      type: "mixed-pfo-parent",
      input: {},
    });

    // Wait for at least one subworkflow to start
    await waitFor(() => subworkflowsStarted >= 1, 5_000);

    // Cancel the parent
    const cancelResult = await cancelWorkflow(result.workflowId, "user_cancel");
    expect(cancelResult.success).toBe(true);

    // Wait for cascading propagation
    await Bun.sleep(1000);

    const wf = getWorkflow(result.workflowId);
    expect(wf!.status).toBe("cancelled");

    // Check all subworkflows spawned by branches are cancelled
    // Subworkflows are children of the parent workflow (spawned from within branch node executors)
    const subWorkflows = queryMany<Workflow>(
      "SELECT * FROM workflows WHERE parent_workflow_id = ?",
      [result.workflowId]
    );

    for (const sub of subWorkflows) {
      expect(sub.status).toBe("cancelled");
    }
  });
});

// =============================================================================
// Cancel Idempotency Tests
// =============================================================================

describe("Cancel Idempotency (#WF-07)", () => {
  test("cancel already-cancelled workflow returns success without error", async () => {
    registerNodeExecutor({
      name: "idem-cancel-slow",
      idempotent: true,
      async execute(ctx) {
        for (let i = 0; i < 100; i++) {
          await ctx.sleep(50);
        }
        return { ok: true };
      },
    });

    registerBlueprint({
      type: "idem-cancel-test",
      entryNode: "work",
      nodes: {
        work: { type: "idem-cancel-slow" },
      },
    });

    const result = await submit({
      type: "idem-cancel-test",
      input: {},
    });

    // Wait for workflow to start running
    await waitFor(() => {
      const wf = getWorkflow(result.workflowId);
      return wf?.status === "running";
    });

    // First cancel
    const cancel1 = await cancelWorkflow(result.workflowId, "first_cancel");
    expect(cancel1.success).toBe(true);
    expect(cancel1.status).toBe("cancelled");

    // Second cancel — should be idempotent
    const cancel2 = await cancelWorkflow(result.workflowId, "second_cancel");
    expect(cancel2.success).toBe(true);
    expect(cancel2.status).toBe("cancelled");
  });

  test("cancel completed workflow returns success (terminal state)", async () => {
    registerNodeExecutor({
      name: "idem-cancel-fast",
      idempotent: true,
      async execute() {
        return { ok: true };
      },
    });

    registerBlueprint({
      type: "idem-cancel-completed",
      entryNode: "work",
      nodes: {
        work: { type: "idem-cancel-fast" },
      },
    });

    const result = await submit({
      type: "idem-cancel-completed",
      input: {},
    });

    // Wait for workflow to complete
    const wf = await waitForWorkflow(result.workflowId);
    expect(wf.status).toBe("completed");

    // Cancel a completed workflow — should be idempotent success
    const cancelResult = await cancelWorkflow(result.workflowId, "late_cancel");
    expect(cancelResult.success).toBe(true);
    expect(cancelResult.status).toBe("completed");
  });

  test("cancel non-existent workflow returns not_found", async () => {
    const cancelResult = await cancelWorkflow(99999, "ghost_cancel");
    expect(cancelResult.success).toBe(false);
    expect(cancelResult.status).toBe("not_found");
  });
});
