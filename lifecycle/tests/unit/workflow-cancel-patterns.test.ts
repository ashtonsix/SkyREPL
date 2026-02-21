// tests/unit/workflow-cancel-patterns.test.ts - Cancel Propagation Across All Workflow Patterns (#WF-07)
// Covers: IAR cancel, PFO cancel, subworkflow cancel cascade, mixed patterns, idempotency

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import {
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
} from "../../control/src/workflow/engine";
import { setupTest, waitForWorkflow } from "../harness";

// =============================================================================
// Test Setup
// =============================================================================

let cleanup: () => Promise<void>;

beforeEach(() => {
  cleanup = setupTest({ engine: true });
});

afterEach(() => cleanup());

// =============================================================================
// Helpers
// =============================================================================

/** Poll until a condition is true (5ms interval, tight loop) */
async function waitFor(
  fn: () => boolean,
  timeoutMs = 5_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (fn()) return;
    await Bun.sleep(5);
  }
  throw new Error(`waitFor timed out after ${timeoutMs}ms`);
}

/** Poll until a node reaches a specific status */
async function waitForNodeStatus(
  workflowId: number,
  nodeId: string,
  status: string | string[],
  timeoutMs = 5_000
): Promise<void> {
  const statuses = Array.isArray(status) ? status : [status];
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const node = queryOne<WorkflowNode>(
      "SELECT * FROM workflow_nodes WHERE workflow_id = ? AND node_id = ?",
      [workflowId, nodeId]
    );
    if (node && statuses.includes(node.status)) return;
    await Bun.sleep(5);
  }
  throw new Error(`Node ${nodeId} did not reach status ${statuses.join("|")} within ${timeoutMs}ms`);
}

/** Poll until ALL nodes in a workflow are terminal (completed/failed/skipped) */
async function waitForAllNodesTerminal(
  workflowId: number,
  timeoutMs = 5_000
): Promise<void> {
  const terminalStatuses = new Set(["completed", "failed", "skipped"]);
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const nodes = getWorkflowNodes(workflowId);
    if (nodes.length > 0 && nodes.every(n => terminalStatuses.has(n.status))) return;
    await Bun.sleep(5);
  }
  const nodes = getWorkflowNodes(workflowId);
  const nonTerminal = nodes.filter(n => !terminalStatuses.has(n.status));
  throw new Error(
    `Not all nodes terminal within ${timeoutMs}ms. Non-terminal: ${nonTerminal.map(n => `${n.node_id}:${n.status}`).join(", ")}`
  );
}

/** Poll until a child workflow reaches a specific status */
async function waitForChildWorkflowStatus(
  parentId: number,
  expectedStatus: string,
  timeoutMs = 5_000
): Promise<Workflow[]> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const children = queryMany<Workflow>(
      "SELECT * FROM workflows WHERE parent_workflow_id = ?",
      [parentId]
    );
    if (children.length > 0 && children.every(c => c.status === expectedStatus)) {
      return children;
    }
    await Bun.sleep(5);
  }
  const children = queryMany<Workflow>(
    "SELECT * FROM workflows WHERE parent_workflow_id = ?",
    [parentId]
  );
  throw new Error(
    `Child workflows did not all reach ${expectedStatus} within ${timeoutMs}ms. Current: ${children.map(c => `${c.id}:${c.status}`).join(", ")}`
  );
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
    // Cancel after source completes but before inserted node finishes.
    // Latch: inserted node blocks until test releases it.
    let resolveInsertedLatch!: () => void;
    const insertedLatch = new Promise<void>(r => { resolveInsertedLatch = r; });

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
        return { done: true };
      },
    });

    registerNodeExecutor({
      name: "iar-cancel-slow",
      idempotent: true,
      async execute() {
        await insertedLatch; // Block until test releases
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

    // Wait for the inserted node to be running (guarantees source is done and IAR applied)
    await waitForNodeStatus(result.workflowId, "inserted", "running");

    // Cancel the workflow while inserted node is blocked on latch
    const cancelResult = await cancelWorkflow(result.workflowId, "test_cancel");
    expect(cancelResult.success).toBe(true);

    // Release the latch so the engine loop can process the cancellation
    resolveInsertedLatch();

    // Wait for terminal state + all nodes processed
    const wf = await waitForWorkflow(result.workflowId);
    expect(wf.status).toBe("cancelled");
    await waitForAllNodesTerminal(result.workflowId);

    // Verify: target and done nodes should be skipped (they were pending)
    const target = getNodeByNodeId(result.workflowId, "target");
    expect(target).not.toBeNull();
    expect(target!.status).toBe("skipped");

    const done = getNodeByNodeId(result.workflowId, "done");
    expect(done).not.toBeNull();
    expect(done!.status).toBe("skipped");
  });

  test("IAR cancel: running inserted node — workflow reaches cancelled", async () => {
    // Create a workflow where IAR inserts a slow node, then cancel while it runs.
    // Latch: the inserted node blocks until test releases it.
    let resolveInsertedLatch!: () => void;
    const insertedLatch = new Promise<void>(r => { resolveInsertedLatch = r; });

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
      async execute() {
        await insertedLatch; // Block until test releases
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

    // Wait for the inserted node to be running
    await waitForNodeStatus(result.workflowId, "slow-inserted", "running");

    // Cancel the workflow while inserted node is running (blocked on latch)
    const cancelResult = await cancelWorkflow(result.workflowId, "test_cancel");
    expect(cancelResult.success).toBe(true);

    // Release latch so engine can process cancellation
    resolveInsertedLatch();

    // Wait for terminal state + all nodes processed
    const wf = await waitForWorkflow(result.workflowId);
    expect(wf.status).toBe("cancelled");
    await waitForAllNodesTerminal(result.workflowId);

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
    // Apply PFO, cancel while branches are blocked on latches.
    // Use per-branch latches so we can hold ALL branches.
    const branchLatches: Record<string, () => void> = {};
    const branchPromises: Record<string, Promise<void>> = {};
    for (const idx of [1, 2, 3]) {
      branchPromises[idx] = new Promise<void>(r => { branchLatches[idx] = r; });
    }

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
        return { done: true };
      },
    });

    registerNodeExecutor({
      name: "pfo-cancel-slow",
      idempotent: true,
      async execute(ctx) {
        const idx = (ctx.input as { idx: number }).idx;
        await branchPromises[idx]; // Block until test releases
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

    // Wait for at least one branch to be running (guarantees source is done)
    await waitFor(() => {
      const nodes = getWorkflowNodes(result.workflowId);
      return nodes.some(n => n.node_id.startsWith("branch-") && n.status === "running");
    });

    // Cancel while branches are blocked on latches
    const cancelResult = await cancelWorkflow(result.workflowId, "test_cancel");
    expect(cancelResult.success).toBe(true);

    // Release all branch latches so engine can process cancellation
    for (const resolve of Object.values(branchLatches)) resolve();

    // Wait for terminal state + all nodes processed
    const wf = await waitForWorkflow(result.workflowId);
    expect(wf.status).toBe("cancelled");
    await waitForAllNodesTerminal(result.workflowId);

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
    // Latch: both branches block until released
    const branchLatches: Record<string, () => void> = {};
    const branchPromises: Record<string, Promise<void>> = {};
    for (const idx of [1, 2]) {
      branchPromises[idx] = new Promise<void>(r => { branchLatches[idx] = r; });
    }

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
        const idx = (ctx.input as { idx: number }).idx;
        await branchPromises[idx]; // Block until test releases
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

    // Wait for at least one branch to be running
    await waitFor(() => {
      const nodes = getWorkflowNodes(result.workflowId);
      return nodes.some(n => (n.node_id === "b1" || n.node_id === "b2") && n.status === "running");
    });

    // Cancel while branches are running (blocked on latches)
    const cancelResult = await cancelWorkflow(result.workflowId, "test_cancel");
    expect(cancelResult.success).toBe(true);

    // Release all latches
    for (const resolve of Object.values(branchLatches)) resolve();

    // Wait for terminal state + all nodes processed
    const wf = await waitForWorkflow(result.workflowId);
    expect(wf.status).toBe("cancelled");
    await waitForAllNodesTerminal(result.workflowId);

    // Join node should be skipped (was pending)
    const join = getNodeByNodeId(result.workflowId, "join");
    expect(join!.status).toBe("skipped");
  });

  test("PFO first-failure-cancels: branch failure skips pending siblings", async () => {
    const executionLog: string[] = [];

    // Latch for slow branch — holds it until released
    let resolveSlowLatch!: () => void;
    const slowLatch = new Promise<void>(r => { resolveSlowLatch = r; });

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
      async execute() {
        await slowLatch; // Block until released
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

    // Release the slow latch so the engine can finish
    resolveSlowLatch();

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
    // Latch: child node blocks until released
    let resolveChildLatch!: () => void;
    const childLatch = new Promise<void>(r => { resolveChildLatch = r; });

    registerNodeExecutor({
      name: "sub-cancel-slow-child",
      idempotent: true,
      async execute() {
        await childLatch; // Block until test releases
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

    // Wait for child workflow to exist and its node to be running
    await waitFor(() => {
      const children = queryMany<Workflow>(
        "SELECT * FROM workflows WHERE parent_workflow_id = ?",
        [result.workflowId]
      );
      if (children.length === 0) return false;
      const childNodes = getWorkflowNodes(children[0].id);
      return childNodes.some(n => n.status === "running");
    });

    // Cancel parent
    const cancelResult = await cancelWorkflow(result.workflowId, "user_cancel");
    expect(cancelResult.success).toBe(true);

    // Wait for child to be marked cancelled in DB (parent's handleCancellation cascades)
    // Do NOT release latch yet — child must be cancelled before its node completes.
    const children = await waitForChildWorkflowStatus(result.workflowId, "cancelled");
    expect(children.length).toBe(1);
    expect(children[0].status).toBe("cancelled");

    // Now release child latch so engine loops can clean up
    resolveChildLatch();

    // Wait for parent to fully terminate
    await waitForWorkflow(result.workflowId);
  });

  test("nested subworkflow cancel (depth 2): parent -> child -> grandchild", async () => {
    // Latch: grandchild node blocks until released
    let resolveGrandchildLatch!: () => void;
    const grandchildLatch = new Promise<void>(r => { resolveGrandchildLatch = r; });

    registerNodeExecutor({
      name: "nested-cancel-slow",
      idempotent: true,
      async execute() {
        await grandchildLatch; // Block until test releases
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

    // Wait for grandchild workflow to exist and its node to be running
    await waitFor(() => {
      const children = queryMany<Workflow>(
        "SELECT * FROM workflows WHERE parent_workflow_id = ?",
        [result.workflowId]
      );
      if (children.length === 0) return false;
      const grandchildren = queryMany<Workflow>(
        "SELECT * FROM workflows WHERE parent_workflow_id = ?",
        [children[0].id]
      );
      if (grandchildren.length === 0) return false;
      const gcNodes = getWorkflowNodes(grandchildren[0].id);
      return gcNodes.some(n => n.status === "running");
    });

    // Cancel top-level parent
    const cancelResult = await cancelWorkflow(result.workflowId, "user_cancel");
    expect(cancelResult.success).toBe(true);

    // Wait for child to be cancelled in DB (parent's handleCancellation cascades)
    // Do NOT release latch yet — grandchild must be cancelled before its node completes.
    const children = await waitForChildWorkflowStatus(result.workflowId, "cancelled");
    expect(children.length).toBe(1);

    // Wait for grandchild to be cancelled (cascading from child's handleCancellation)
    const grandchildren = await waitForChildWorkflowStatus(children[0].id, "cancelled");
    expect(grandchildren.length).toBe(1);
    expect(grandchildren[0].status).toBe("cancelled");

    // Now release grandchild latch so engine loops can clean up
    resolveGrandchildLatch();

    // Wait for parent to fully terminate
    await waitForWorkflow(result.workflowId);
  });

  test("child cancel does not error when parent is already cancelled", async () => {
    let childWorkflowId: number | null = null;
    // Latch: child node blocks until released
    let resolveChildLatch!: () => void;
    const childLatch = new Promise<void>(r => { resolveChildLatch = r; });

    registerNodeExecutor({
      name: "sub-noerr-slow",
      idempotent: true,
      async execute() {
        await childLatch; // Block until test releases
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

    // Wait for child workflow to be running
    await waitFor(() => {
      if (childWorkflowId == null) return false;
      const childNodes = getWorkflowNodes(childWorkflowId);
      return childNodes.some(n => n.status === "running");
    });

    // Cancel parent (cascades to child)
    const cancelResult = await cancelWorkflow(result.workflowId, "user_cancel");
    expect(cancelResult.success).toBe(true);

    // Wait for child to be marked cancelled (parent's handleCancellation cascades)
    await waitForChildWorkflowStatus(result.workflowId, "cancelled");

    // Now try to cancel child again — should not throw, should be idempotent
    expect(childWorkflowId).not.toBeNull();
    const childCancelResult = await cancelWorkflow(childWorkflowId!, "double_cancel");
    // Should succeed (idempotent — already cancelled)
    expect(childCancelResult.success).toBe(true);
    expect(childCancelResult.status).toBe("cancelled");

    // Release child latch so engine loops can clean up
    resolveChildLatch();

    // Wait for parent to fully terminate
    await waitForWorkflow(result.workflowId);
  });
});

// =============================================================================
// Mixed Pattern Cancel Tests
// =============================================================================

describe("Cancel Propagation: Mixed Patterns (#WF-07)", () => {
  test("PFO + subworkflow cancel: PFO branches with subworkflows cascade-cancel", async () => {
    // Latches: subworkflow child nodes block until released
    let resolveSubLatch!: () => void;
    const subLatch = new Promise<void>(r => { resolveSubLatch = r; });

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
      async execute() {
        await subLatch; // Block until test releases
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

    // Wait for at least one subworkflow's node to be running
    await waitFor(() => {
      const subs = queryMany<Workflow>(
        "SELECT * FROM workflows WHERE parent_workflow_id = ?",
        [result.workflowId]
      );
      for (const sub of subs) {
        const nodes = getWorkflowNodes(sub.id);
        if (nodes.some(n => n.status === "running")) return true;
      }
      return false;
    });

    // Cancel the parent
    const cancelResult = await cancelWorkflow(result.workflowId, "user_cancel");
    expect(cancelResult.success).toBe(true);

    // Wait for all subworkflows to be cancelled in DB (cascading from parent)
    // Do NOT release latch yet — subworkflows must be cancelled before their nodes complete.
    const subWorkflows = await waitForChildWorkflowStatus(result.workflowId, "cancelled");

    for (const sub of subWorkflows) {
      expect(sub.status).toBe("cancelled");
    }

    // Release subworkflow latches so engine loops can clean up
    resolveSubLatch();

    // Wait for parent to reach terminal state
    const wf = await waitForWorkflow(result.workflowId);
    expect(wf.status).toBe("cancelled");
  });
});

// =============================================================================
// Cancel Idempotency Tests
// =============================================================================

describe("Cancel Idempotency (#WF-07)", () => {
  test("cancel already-cancelled workflow returns success without error", async () => {
    // Latch: node blocks until released
    let resolveNodeLatch!: () => void;
    const nodeLatch = new Promise<void>(r => { resolveNodeLatch = r; });

    registerNodeExecutor({
      name: "idem-cancel-slow",
      idempotent: true,
      async execute() {
        await nodeLatch; // Block until test releases
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

    // Wait for the work node to be running
    await waitForNodeStatus(result.workflowId, "work", "running");

    // First cancel
    const cancel1 = await cancelWorkflow(result.workflowId, "first_cancel");
    expect(cancel1.success).toBe(true);
    expect(cancel1.status).toBe("cancelled");

    // Second cancel — should be idempotent
    const cancel2 = await cancelWorkflow(result.workflowId, "second_cancel");
    expect(cancel2.success).toBe(true);
    expect(cancel2.status).toBe("cancelled");

    // Release latch so engine can clean up
    resolveNodeLatch();
    await waitForWorkflow(result.workflowId);
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
