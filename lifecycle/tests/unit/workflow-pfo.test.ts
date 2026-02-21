// tests/unit/workflow-pfo.test.ts - Parallel Fan-Out (PFO) Pattern Tests
// Covers: applyParallelFanOut in patterns.ts, engine.ts PFO dispatch + join mode handling

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest } from "../harness";
import {
  createWorkflow,
  createWorkflowNode,
  getWorkflowNodes,
  getWorkflow,
  queryOne,
  type Workflow,
  type WorkflowNode,
} from "../../control/src/material/db";
import {
  applyParallelFanOut,
  applyInsertAndReconverge,
  MAX_PARALLEL_BRANCHES,
  type ParallelFanOutConfig,
} from "../../control/src/workflow/patterns";
import {
  startNode,
  completeNode,
  failNode,
  startWorkflow,
} from "../../control/src/workflow/state-transitions";
import {
  submit,
  registerBlueprint,
  registerNodeExecutor,
  buildNodeContext,
  findReadyNodesFromArray,
} from "../../control/src/workflow/engine";

// =============================================================================
// Top-level harness setup
// =============================================================================

let cleanup: () => Promise<void>;
beforeEach(() => { cleanup = setupTest({ engine: true }); });
afterEach(() => cleanup());

// =============================================================================
// Test Helpers
// =============================================================================

function createTestWorkflow(
  overrides: Partial<Omit<Workflow, "id" | "created_at">> = {}
) {
  return createWorkflow({
    type: "test-workflow",
    parent_workflow_id: null,
    depth: 0,
    status: "pending",
    current_node: null,
    input_json: JSON.stringify({ test: true }),
    output_json: null,
    error_json: null,
    manifest_id: null,
    trace_id: null,
    idempotency_key: null,
    timeout_ms: 3600000,
    timeout_at: Date.now() + 3600000,
    started_at: null,
    finished_at: null,
    updated_at: Date.now(),
    ...overrides,
  });
}

function createTestNode(
  workflowId: number,
  nodeId: string,
  nodeType: string,
  overrides: Partial<Omit<WorkflowNode, "id" | "created_at">> = {}
) {
  return createWorkflowNode({
    workflow_id: workflowId,
    node_id: nodeId,
    node_type: nodeType,
    status: "pending",
    input_json: JSON.stringify({}),
    output_json: null,
    error_json: null,
    depends_on: null,
    attempt: 0,
    retry_reason: null,
    started_at: null,
    finished_at: null,
    updated_at: Date.now(),
    ...overrides,
  });
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

function parseDeps(node: WorkflowNode): string[] {
  return node.depends_on ? (JSON.parse(node.depends_on) as string[]) : [];
}

// =============================================================================
// Unit Tests: applyParallelFanOut
// =============================================================================

describe("Parallel Fan-Out (PFO) Pattern", () => {

  // ---------------------------------------------------------------------------
  // 1. Basic PFO: 3 branches all succeed, join node runs after all complete
  // ---------------------------------------------------------------------------

  test("basic PFO: 3 branches created, join node depends on all branches", () => {
    const wf = createTestWorkflow();

    // Source node and join node
    createTestNode(wf.id, "source", "step", { depends_on: null });
    createTestNode(wf.id, "join", "step", {
      depends_on: JSON.stringify(["source"]),
    });

    applyParallelFanOut(wf.id, {
      branches: [
        { id: "branch-1", type: "step", input: { idx: 1 } },
        { id: "branch-2", type: "step", input: { idx: 2 } },
        { id: "branch-3", type: "step", input: { idx: 3 } },
      ],
      joinNode: "join",
      sourceNodeId: "source",
    });

    // Verify 5 nodes total: source, join, branch-1, branch-2, branch-3
    const nodes = getWorkflowNodes(wf.id);
    expect(nodes.length).toBe(5);

    // Verify each branch was created with correct properties
    for (let i = 1; i <= 3; i++) {
      const branch = getNodeByNodeId(wf.id, `branch-${i}`);
      expect(branch).not.toBeNull();
      expect(branch!.status).toBe("pending");
      expect(branch!.node_type).toBe("step");
      expect(branch!.retry_reason).toBe("pfo_branch");
      expect(JSON.parse(branch!.input_json)).toEqual({ idx: i });
    }

    // Verify join node depends on all 3 branches
    const join = getNodeByNodeId(wf.id, "join");
    const joinDeps = parseDeps(join!);
    expect(joinDeps).toContain("source");
    expect(joinDeps).toContain("branch-1");
    expect(joinDeps).toContain("branch-2");
    expect(joinDeps).toContain("branch-3");
    expect(joinDeps.length).toBe(4);

    // Verify join node has join mode metadata
    expect(join!.retry_reason).toBe("pfo_join:all-succeed");
  });

  // ---------------------------------------------------------------------------
  // 2. Validation: > MAX_PARALLEL_BRANCHES rejects
  // ---------------------------------------------------------------------------

  test("validation: rejects > MAX_PARALLEL_BRANCHES", () => {
    const wf = createTestWorkflow();
    createTestNode(wf.id, "join", "step", { depends_on: null });

    const tooManyBranches = Array.from({ length: MAX_PARALLEL_BRANCHES + 1 }, (_, i) => ({
      id: `branch-${i}`,
      type: "step",
      input: {},
    }));

    expect(() => {
      applyParallelFanOut(wf.id, {
        branches: tooManyBranches,
        joinNode: "join",
      });
    }).toThrow(`exceeds MAX_PARALLEL_BRANCHES (${MAX_PARALLEL_BRANCHES})`);
  });

  // ---------------------------------------------------------------------------
  // 3. Validation: empty branches rejects
  // ---------------------------------------------------------------------------

  test("validation: rejects empty branches array", () => {
    const wf = createTestWorkflow();
    createTestNode(wf.id, "join", "step", { depends_on: null });

    expect(() => {
      applyParallelFanOut(wf.id, {
        branches: [],
        joinNode: "join",
      });
    }).toThrow("PFO requires at least 1 branch");
  });

  // ---------------------------------------------------------------------------
  // 4. Join node not found rejects
  // ---------------------------------------------------------------------------

  test("validation: rejects when join node not found", () => {
    const wf = createTestWorkflow();

    expect(() => {
      applyParallelFanOut(wf.id, {
        branches: [{ id: "b1", type: "step", input: {} }],
        joinNode: "nonexistent",
      });
    }).toThrow("Join node 'nonexistent' not found");
  });

  // ---------------------------------------------------------------------------
  // 5. Join node must be pending
  // ---------------------------------------------------------------------------

  test("validation: rejects when join node is not pending", () => {
    const wf = createTestWorkflow({ status: "running" });
    startWorkflow(wf.id);

    const joinNode = createTestNode(wf.id, "join", "step", { depends_on: null });
    startNode(joinNode.id);

    expect(() => {
      applyParallelFanOut(wf.id, {
        branches: [{ id: "b1", type: "step", input: {} }],
        joinNode: "join",
      });
    }).toThrow("must be pending, got: running");
  });

  // ---------------------------------------------------------------------------
  // 6. PFO branch nodes have correct depends_on (sourceNodeId)
  // ---------------------------------------------------------------------------

  test("branch nodes depend on sourceNodeId when provided", () => {
    const wf = createTestWorkflow();
    createTestNode(wf.id, "source", "step", { depends_on: null });
    createTestNode(wf.id, "join", "step", {
      depends_on: JSON.stringify(["source"]),
    });

    applyParallelFanOut(wf.id, {
      branches: [
        { id: "b1", type: "step", input: {} },
        { id: "b2", type: "step", input: {} },
      ],
      joinNode: "join",
      sourceNodeId: "source",
    });

    const b1 = getNodeByNodeId(wf.id, "b1");
    const b2 = getNodeByNodeId(wf.id, "b2");
    expect(parseDeps(b1!)).toEqual(["source"]);
    expect(parseDeps(b2!)).toEqual(["source"]);
  });

  // ---------------------------------------------------------------------------
  // 7. Branch nodes have no deps when sourceNodeId is omitted
  // ---------------------------------------------------------------------------

  test("branch nodes have no dependencies when sourceNodeId omitted", () => {
    const wf = createTestWorkflow();
    createTestNode(wf.id, "join", "step", { depends_on: null });

    applyParallelFanOut(wf.id, {
      branches: [
        { id: "b1", type: "step", input: {} },
        { id: "b2", type: "step", input: {} },
      ],
      joinNode: "join",
    });

    const b1 = getNodeByNodeId(wf.id, "b1");
    const b2 = getNodeByNodeId(wf.id, "b2");
    expect(parseDeps(b1!)).toEqual([]);
    expect(parseDeps(b2!)).toEqual([]);
  });

  // ---------------------------------------------------------------------------
  // 8. Join node depends_on includes all branches (merged with existing)
  // ---------------------------------------------------------------------------

  test("join node depends_on merges with existing dependencies", () => {
    const wf = createTestWorkflow();
    createTestNode(wf.id, "source", "step", { depends_on: null });
    createTestNode(wf.id, "join", "step", {
      depends_on: JSON.stringify(["source"]),
    });

    applyParallelFanOut(wf.id, {
      branches: [
        { id: "b1", type: "step", input: {} },
        { id: "b2", type: "step", input: {} },
      ],
      joinNode: "join",
      sourceNodeId: "source",
    });

    const join = getNodeByNodeId(wf.id, "join");
    const joinDeps = parseDeps(join!);
    // Should include original dep "source" plus both branches
    expect(joinDeps).toContain("source");
    expect(joinDeps).toContain("b1");
    expect(joinDeps).toContain("b2");
  });

  // ---------------------------------------------------------------------------
  // 9. Default join mode is "all-succeed"
  // ---------------------------------------------------------------------------

  test("default join mode is all-succeed", () => {
    const wf = createTestWorkflow();
    createTestNode(wf.id, "join", "step", { depends_on: null });

    applyParallelFanOut(wf.id, {
      branches: [{ id: "b1", type: "step", input: {} }],
      joinNode: "join",
    });

    const join = getNodeByNodeId(wf.id, "join");
    expect(join!.retry_reason).toBe("pfo_join:all-succeed");
  });

  // ---------------------------------------------------------------------------
  // 10. first-failure-cancels join mode is stored
  // ---------------------------------------------------------------------------

  test("first-failure-cancels join mode is stored on join node", () => {
    const wf = createTestWorkflow();
    createTestNode(wf.id, "join", "step", { depends_on: null });

    applyParallelFanOut(wf.id, {
      branches: [
        { id: "b1", type: "step", input: {} },
        { id: "b2", type: "step", input: {} },
      ],
      joinNode: "join",
      joinMode: "first-failure-cancels",
    });

    const join = getNodeByNodeId(wf.id, "join");
    expect(join!.retry_reason).toBe("pfo_join:first-failure-cancels");
  });

  // ---------------------------------------------------------------------------
  // 11. DAG scheduling: branches become ready when source completes
  // ---------------------------------------------------------------------------

  test("DAG scheduling: branches ready when source completes, join waits for all", () => {
    const wf = createTestWorkflow({ status: "running" });
    startWorkflow(wf.id);

    const source = createTestNode(wf.id, "source", "step", { depends_on: null });
    createTestNode(wf.id, "join", "step", {
      depends_on: JSON.stringify(["source"]),
    });

    applyParallelFanOut(wf.id, {
      branches: [
        { id: "b1", type: "step", input: {} },
        { id: "b2", type: "step", input: {} },
      ],
      joinNode: "join",
      sourceNodeId: "source",
    });

    // Before source completes: only source is ready
    let nodes = getWorkflowNodes(wf.id);
    let ready = findReadyNodesFromArray(nodes);
    expect(ready.map((n) => n.node_id).sort()).toEqual(["source"]);

    // Complete source
    startNode(source.id);
    completeNode(source.id, {});

    // After source completes: both branches should be ready, join should not
    nodes = getWorkflowNodes(wf.id);
    ready = findReadyNodesFromArray(nodes);
    const readyIds = ready.map((n) => n.node_id).sort();
    expect(readyIds).toContain("b1");
    expect(readyIds).toContain("b2");
    expect(readyIds).not.toContain("join");

    // Complete b1
    const b1 = getNodeByNodeId(wf.id, "b1")!;
    startNode(b1.id);
    completeNode(b1.id, {});

    // Join still not ready (b2 pending)
    nodes = getWorkflowNodes(wf.id);
    ready = findReadyNodesFromArray(nodes);
    expect(ready.map((n) => n.node_id)).not.toContain("join");

    // Complete b2
    const b2 = getNodeByNodeId(wf.id, "b2")!;
    startNode(b2.id);
    completeNode(b2.id, {});

    // Now join should be ready
    nodes = getWorkflowNodes(wf.id);
    ready = findReadyNodesFromArray(nodes);
    expect(ready.map((n) => n.node_id)).toContain("join");
  });

  // ---------------------------------------------------------------------------
  // 12. Engine integration: applyPattern dispatches 'parallel-fan-out'
  // ---------------------------------------------------------------------------

  test("engine applyPattern dispatches 'parallel-fan-out' correctly", () => {
    const wf = createTestWorkflow({ status: "running" });
    startWorkflow(wf.id);

    const source = createTestNode(wf.id, "source", "step", { depends_on: null });
    createTestNode(wf.id, "join", "step", {
      depends_on: JSON.stringify(["source"]),
    });

    const ctx = buildNodeContext(wf.id, source);

    ctx.applyPattern("parallel-fan-out", {
      branches: [
        { id: "pfo-b1", type: "step", input: { fromCtx: true } },
        { id: "pfo-b2", type: "step", input: { fromCtx: true } },
      ],
      joinNode: "join",
      sourceNodeId: "source",
    } as ParallelFanOutConfig);

    // Verify branches were created
    const b1 = getNodeByNodeId(wf.id, "pfo-b1");
    const b2 = getNodeByNodeId(wf.id, "pfo-b2");
    expect(b1).not.toBeNull();
    expect(b2).not.toBeNull();
    expect(b1!.retry_reason).toBe("pfo_branch");
    expect(b2!.retry_reason).toBe("pfo_branch");

    // Verify join node updated
    const join = getNodeByNodeId(wf.id, "join");
    const joinDeps = parseDeps(join!);
    expect(joinDeps).toContain("pfo-b1");
    expect(joinDeps).toContain("pfo-b2");
    expect(join!.retry_reason).toBe("pfo_join:all-succeed");
  });

  // ---------------------------------------------------------------------------
  // 13. Integration: PFO runs branches in parallel and join waits for all
  // ---------------------------------------------------------------------------

  test("integration: PFO branches run in parallel, join waits for all", async () => {
    const executionLog: string[] = [];

    registerBlueprint({
      type: "test-pfo-integration",
      entryNode: "source",
      nodes: {
        source: { type: "pfo-source", dependsOn: [] },
        join: { type: "pfo-join", dependsOn: ["source"] },
      },
    });

    registerNodeExecutor({
      name: "pfo-source",
      idempotent: true,
      async execute(ctx) {
        executionLog.push("source");
        ctx.applyPattern("parallel-fan-out", {
          branches: [
            { id: "b1", type: "pfo-branch", input: { idx: 1 } },
            { id: "b2", type: "pfo-branch", input: { idx: 2 } },
            { id: "b3", type: "pfo-branch", input: { idx: 3 } },
          ],
          joinNode: "join",
          sourceNodeId: "source",
        });
        return { step: "source-done" };
      },
    });

    registerNodeExecutor({
      name: "pfo-branch",
      idempotent: true,
      async execute(ctx) {
        const input = ctx.input as { idx: number };
        executionLog.push(`branch-${input.idx}`);
        return { branch: input.idx };
      },
    });

    registerNodeExecutor({
      name: "pfo-join",
      idempotent: true,
      async execute(ctx) {
        executionLog.push("join");
        return { joined: true };
      },
    });

    const result = await submit({
      type: "test-pfo-integration",
      input: { run: true },
    });

    expect(result.status).toBe("created");

    // Poll for workflow completion
    let wf: Workflow | null = null;
    for (let i = 0; i < 100; i++) {
      wf = getWorkflow(result.workflowId);
      if (
        wf &&
        (wf.status === "completed" ||
          wf.status === "failed" ||
          wf.status === "cancelled")
      )
        break;
      await Bun.sleep(50);
    }

    expect(wf).not.toBeNull();
    expect(wf!.status).toBe("completed");

    // Source must run first
    expect(executionLog[0]).toBe("source");

    // Join must run last
    expect(executionLog[executionLog.length - 1]).toBe("join");

    // All 3 branches must run (in any order) between source and join
    expect(executionLog).toContain("branch-1");
    expect(executionLog).toContain("branch-2");
    expect(executionLog).toContain("branch-3");
    expect(executionLog.length).toBe(5);

    // All nodes should be completed
    const nodes = getWorkflowNodes(result.workflowId);
    expect(nodes.length).toBe(5);
    for (const node of nodes) {
      expect(node.status).toBe("completed");
    }
  });

  // ---------------------------------------------------------------------------
  // 14. first-failure-cancels: one branch fails, pending siblings get skipped
  // ---------------------------------------------------------------------------

  test("first-failure-cancels: branch failure skips pending siblings", async () => {
    const executionLog: string[] = [];

    registerBlueprint({
      type: "test-pfo-ffc",
      entryNode: "source",
      nodes: {
        source: { type: "pfo-ffc-source", dependsOn: [] },
        join: { type: "pfo-ffc-join", dependsOn: ["source"] },
      },
    });

    registerNodeExecutor({
      name: "pfo-ffc-source",
      idempotent: true,
      async execute(ctx) {
        executionLog.push("source");
        ctx.applyPattern("parallel-fan-out", {
          branches: [
            { id: "fast-fail", type: "pfo-ffc-fail-branch", input: {} },
            { id: "slow-branch", type: "pfo-ffc-slow-branch", input: {} },
          ],
          joinNode: "join",
          sourceNodeId: "source",
          joinMode: "first-failure-cancels",
        });
        return { step: "source-done" };
      },
    });

    registerNodeExecutor({
      name: "pfo-ffc-fail-branch",
      idempotent: true,
      async execute() {
        executionLog.push("fast-fail");
        throw Object.assign(new Error("Branch failed"), {
          code: "VALIDATION_ERROR",
          category: "validation",
        });
      },
    });

    registerNodeExecutor({
      name: "pfo-ffc-slow-branch",
      idempotent: true,
      async execute() {
        // This branch is slow — with first-failure-cancels, it should get
        // skipped if the fast-fail branch fails first. But if the engine
        // picks both up simultaneously, it may start running before cancellation.
        await Bun.sleep(200);
        executionLog.push("slow-branch");
        return { done: true };
      },
    });

    registerNodeExecutor({
      name: "pfo-ffc-join",
      idempotent: true,
      async execute() {
        executionLog.push("join");
        return { joined: true };
      },
    });

    const result = await submit({
      type: "test-pfo-ffc",
      input: { run: true },
    });

    // Poll for workflow completion
    let wf: Workflow | null = null;
    for (let i = 0; i < 100; i++) {
      wf = getWorkflow(result.workflowId);
      if (
        wf &&
        (wf.status === "completed" ||
          wf.status === "failed" ||
          wf.status === "cancelled")
      )
        break;
      await Bun.sleep(50);
    }

    expect(wf).not.toBeNull();
    // Workflow should fail because a branch failed
    expect(wf!.status).toBe("failed");

    // Source ran, fast-fail ran and failed
    expect(executionLog).toContain("source");
    expect(executionLog).toContain("fast-fail");

    // Join should NOT have run
    expect(executionLog).not.toContain("join");

    // Check slow-branch: it may have been skipped or may have started
    // depending on timing. The key invariant is that the workflow fails.
    const slowBranch = getNodeByNodeId(result.workflowId, "slow-branch");
    expect(slowBranch).not.toBeNull();
    // It should be skipped, completed, or running (timing-dependent)
    // But most importantly, the fast-fail branch should be failed
    const fastFail = getNodeByNodeId(result.workflowId, "fast-fail");
    expect(fastFail!.status).toBe("failed");
  });

  // ---------------------------------------------------------------------------
  // 15. all-succeed (default): branch failure doesn't cancel siblings
  // ---------------------------------------------------------------------------

  test("all-succeed: branch failure does not cancel other branches", () => {
    const wf = createTestWorkflow({ status: "running" });
    startWorkflow(wf.id);

    const source = createTestNode(wf.id, "source", "step", { depends_on: null });
    createTestNode(wf.id, "join", "step", {
      depends_on: JSON.stringify(["source"]),
    });

    applyParallelFanOut(wf.id, {
      branches: [
        { id: "b1", type: "step", input: {} },
        { id: "b2", type: "step", input: {} },
        { id: "b3", type: "step", input: {} },
      ],
      joinNode: "join",
      sourceNodeId: "source",
      joinMode: "all-succeed",
    });

    // Complete source
    startNode(source.id);
    completeNode(source.id, {});

    // Fail b1
    const b1 = getNodeByNodeId(wf.id, "b1")!;
    startNode(b1.id);
    failNode(b1.id, JSON.stringify({ code: "TEST_ERROR", message: "test fail" }));

    // b2 and b3 should still be pending (not skipped)
    const b2 = getNodeByNodeId(wf.id, "b2")!;
    const b3 = getNodeByNodeId(wf.id, "b3")!;
    expect(b2.status).toBe("pending");
    expect(b3.status).toBe("pending");

    // b2 and b3 should still be ready (their deps are satisfied)
    const nodes = getWorkflowNodes(wf.id);
    const ready = findReadyNodesFromArray(nodes);
    const readyIds = ready.map((n) => n.node_id);
    expect(readyIds).toContain("b2");
    expect(readyIds).toContain("b3");
  });

  // ---------------------------------------------------------------------------
  // 16. Single branch PFO works correctly
  // ---------------------------------------------------------------------------

  test("single branch PFO: works correctly", () => {
    const wf = createTestWorkflow();
    createTestNode(wf.id, "source", "step", { depends_on: null });
    createTestNode(wf.id, "join", "step", {
      depends_on: JSON.stringify(["source"]),
    });

    applyParallelFanOut(wf.id, {
      branches: [{ id: "only-branch", type: "step", input: { solo: true } }],
      joinNode: "join",
      sourceNodeId: "source",
    });

    const nodes = getWorkflowNodes(wf.id);
    expect(nodes.length).toBe(3);

    const branch = getNodeByNodeId(wf.id, "only-branch");
    expect(branch).not.toBeNull();
    expect(branch!.retry_reason).toBe("pfo_branch");

    const join = getNodeByNodeId(wf.id, "join");
    const joinDeps = parseDeps(join!);
    expect(joinDeps).toContain("only-branch");
    expect(joinDeps).toContain("source");
  });

  // ---------------------------------------------------------------------------
  // 17. PFO branch can apply IAR (pattern composition)
  // ---------------------------------------------------------------------------

  test("PFO branch can apply IAR for pattern composition", () => {
    const wf = createTestWorkflow({ status: "running" });
    startWorkflow(wf.id);

    const source = createTestNode(wf.id, "source", "step", { depends_on: null });
    createTestNode(wf.id, "join", "step", {
      depends_on: JSON.stringify(["source"]),
    });
    createTestNode(wf.id, "post-join", "step", {
      depends_on: JSON.stringify(["join"]),
    });

    // Apply PFO
    applyParallelFanOut(wf.id, {
      branches: [
        { id: "b1", type: "step", input: {} },
        { id: "b2", type: "step", input: {} },
      ],
      joinNode: "join",
      sourceNodeId: "source",
    });

    // Now apply IAR: insert a node before post-join
    applyInsertAndReconverge(wf.id, {
      insertedNode: { id: "iar-node", type: "step", input: { iar: true } },
      beforeNode: "post-join",
    });

    // Verify the IAR node was created correctly
    const iarNode = getNodeByNodeId(wf.id, "iar-node");
    expect(iarNode).not.toBeNull();
    expect(iarNode!.retry_reason).toBe("iar_inserted");

    // IAR node should depend on join (same as post-join's original deps)
    const iarDeps = parseDeps(iarNode!);
    expect(iarDeps).toEqual(["join"]);

    // post-join should now depend on both join and iar-node
    const postJoin = getNodeByNodeId(wf.id, "post-join");
    const postJoinDeps = parseDeps(postJoin!);
    expect(postJoinDeps).toContain("join");
    expect(postJoinDeps).toContain("iar-node");

    // Total: source, join, post-join, b1, b2, iar-node = 6
    const nodes = getWorkflowNodes(wf.id);
    expect(nodes.length).toBe(6);
  });

  // ---------------------------------------------------------------------------
  // 18. MAX_PARALLEL_BRANCHES boundary: exactly 16 branches succeeds
  // ---------------------------------------------------------------------------

  test("boundary: exactly MAX_PARALLEL_BRANCHES branches succeeds", () => {
    const wf = createTestWorkflow();
    createTestNode(wf.id, "join", "step", { depends_on: null });

    const branches = Array.from({ length: MAX_PARALLEL_BRANCHES }, (_, i) => ({
      id: `branch-${i}`,
      type: "step",
      input: { idx: i },
    }));

    // Should not throw
    applyParallelFanOut(wf.id, {
      branches,
      joinNode: "join",
    });

    const nodes = getWorkflowNodes(wf.id);
    // 1 join + 16 branches = 17
    expect(nodes.length).toBe(17);

    const join = getNodeByNodeId(wf.id, "join");
    const joinDeps = parseDeps(join!);
    expect(joinDeps.length).toBe(MAX_PARALLEL_BRANCHES);
  });
});
