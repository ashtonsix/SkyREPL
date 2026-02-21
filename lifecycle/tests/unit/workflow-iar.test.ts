// tests/unit/workflow-iar.test.ts - Insert-and-Reconverge (IAR) Pattern Tests
// Covers: applyInsertAndReconverge in patterns.ts, engine.ts IAR dispatch

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
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
  applyInsertAndReconverge,
  type InsertAndReconvergeConfig,
} from "../../control/src/workflow/patterns";
import {
  startNode,
  completeNode,
  startWorkflow,
} from "../../control/src/workflow/state-transitions";
import {
  submit,
  registerBlueprint,
  registerNodeExecutor,
  buildNodeContext,
  findReadyNodesFromArray,
} from "../../control/src/workflow/engine";
import { setupTest } from "../harness";

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
// Unit Tests: applyInsertAndReconverge
// =============================================================================

let cleanup: () => Promise<void>;

beforeEach(() => {
  cleanup = setupTest({ engine: true });
});

afterEach(() => cleanup());

describe("Insert-and-Reconverge (IAR) Pattern", () => {

  // ---------------------------------------------------------------------------
  // Basic IAR
  // ---------------------------------------------------------------------------

  test("basic IAR: A->B->C inserts A' before B with correct dependencies", () => {
    const wf = createTestWorkflow();

    // Create a simple A -> B -> C chain
    createTestNode(wf.id, "A", "step", { depends_on: null });
    createTestNode(wf.id, "B", "step", {
      depends_on: JSON.stringify(["A"]),
    });
    createTestNode(wf.id, "C", "step", {
      depends_on: JSON.stringify(["B"]),
    });

    // Insert A' before B
    applyInsertAndReconverge(wf.id, {
      insertedNode: { id: "A-prime", type: "step", input: { extra: true } },
      beforeNode: "B",
    });

    // Verify inserted node was created
    const nodes = getWorkflowNodes(wf.id);
    expect(nodes.length).toBe(4);

    const aPrime = getNodeByNodeId(wf.id, "A-prime");
    expect(aPrime).not.toBeNull();
    expect(aPrime!.status).toBe("pending");
    expect(aPrime!.node_type).toBe("step");
    expect(aPrime!.retry_reason).toBe("iar_inserted");

    // A' should have the same deps as B originally had (i.e., ["A"])
    const aPrimeDeps = parseDeps(aPrime!);
    expect(aPrimeDeps).toEqual(["A"]);

    // B should now depend on both A and A'
    const b = getNodeByNodeId(wf.id, "B");
    const bDeps = parseDeps(b!);
    expect(bDeps).toContain("A");
    expect(bDeps).toContain("A-prime");
    expect(bDeps.length).toBe(2);

    // C should still depend only on B (unchanged)
    const c = getNodeByNodeId(wf.id, "C");
    const cDeps = parseDeps(c!);
    expect(cDeps).toEqual(["B"]);
  });

  // ---------------------------------------------------------------------------
  // IAR with no predecessors (insert before entry node)
  // ---------------------------------------------------------------------------

  test("IAR with no predecessors: insert before entry node", () => {
    const wf = createTestWorkflow();

    // Entry node with no dependencies
    createTestNode(wf.id, "entry", "step", { depends_on: null });
    createTestNode(wf.id, "next", "step", {
      depends_on: JSON.stringify(["entry"]),
    });

    // Insert before entry node
    applyInsertAndReconverge(wf.id, {
      insertedNode: { id: "pre-entry", type: "step", input: { init: true } },
      beforeNode: "entry",
    });

    // pre-entry should have no dependencies (same as entry's original: null)
    const preEntry = getNodeByNodeId(wf.id, "pre-entry");
    expect(preEntry).not.toBeNull();
    const preEntryDeps = parseDeps(preEntry!);
    expect(preEntryDeps).toEqual([]);

    // entry should now depend on pre-entry
    const entry = getNodeByNodeId(wf.id, "entry");
    const entryDeps = parseDeps(entry!);
    expect(entryDeps).toEqual(["pre-entry"]);
  });

  // ---------------------------------------------------------------------------
  // IAR preserves original dependencies
  // ---------------------------------------------------------------------------

  test("IAR preserves original dependencies of target node", () => {
    const wf = createTestWorkflow();

    // D depends on both A and B
    createTestNode(wf.id, "A", "step", { depends_on: null });
    createTestNode(wf.id, "B", "step", { depends_on: null });
    createTestNode(wf.id, "D", "step", {
      depends_on: JSON.stringify(["A", "B"]),
    });

    // Insert X before D
    applyInsertAndReconverge(wf.id, {
      insertedNode: { id: "X", type: "step", input: {} },
      beforeNode: "D",
    });

    // X should depend on A and B (same as D's original deps)
    const x = getNodeByNodeId(wf.id, "X");
    const xDeps = parseDeps(x!);
    expect(xDeps).toContain("A");
    expect(xDeps).toContain("B");
    expect(xDeps.length).toBe(2);

    // D should now depend on A, B, and X
    const d = getNodeByNodeId(wf.id, "D");
    const dDeps = parseDeps(d!);
    expect(dDeps).toContain("A");
    expect(dDeps).toContain("B");
    expect(dDeps).toContain("X");
    expect(dDeps.length).toBe(3);
  });

  // ---------------------------------------------------------------------------
  // Target must be pending
  // ---------------------------------------------------------------------------

  test("IAR target must be pending — fails if running", () => {
    const wf = createTestWorkflow({ status: "running" });
    startWorkflow(wf.id);

    const node = createTestNode(wf.id, "B", "step", { depends_on: null });
    // Transition to running
    startNode(node.id);

    expect(() => {
      applyInsertAndReconverge(wf.id, {
        insertedNode: { id: "X", type: "step", input: {} },
        beforeNode: "B",
      });
    }).toThrow("must be pending, got: running");
  });

  // ---------------------------------------------------------------------------
  // Target must exist
  // ---------------------------------------------------------------------------

  test("IAR target must exist — fails if not found", () => {
    const wf = createTestWorkflow();

    expect(() => {
      applyInsertAndReconverge(wf.id, {
        insertedNode: { id: "X", type: "step", input: {} },
        beforeNode: "nonexistent",
      });
    }).toThrow("Target node 'nonexistent' not found");
  });

  // ---------------------------------------------------------------------------
  // IAR depth limit
  // ---------------------------------------------------------------------------

  test("IAR depth limit: cannot insert before an IAR-inserted node", () => {
    const wf = createTestWorkflow();

    createTestNode(wf.id, "A", "step", { depends_on: null });
    createTestNode(wf.id, "B", "step", {
      depends_on: JSON.stringify(["A"]),
    });

    // First IAR: insert X before B (succeeds)
    applyInsertAndReconverge(wf.id, {
      insertedNode: { id: "X", type: "step", input: {} },
      beforeNode: "B",
    });

    // Second IAR: try to insert Y before X (X was IAR-inserted, should fail)
    expect(() => {
      applyInsertAndReconverge(wf.id, {
        insertedNode: { id: "Y", type: "step", input: {} },
        beforeNode: "X",
      });
    }).toThrow("already IAR-inserted (max depth 1)");
  });

  // ---------------------------------------------------------------------------
  // Inserted node gets correct input
  // ---------------------------------------------------------------------------

  test("IAR node gets correct input from config", () => {
    const wf = createTestWorkflow();

    createTestNode(wf.id, "B", "step", { depends_on: null });

    const inputData = { key: "value", nested: { arr: [1, 2, 3] } };
    applyInsertAndReconverge(wf.id, {
      insertedNode: { id: "inserted", type: "custom-type", input: inputData },
      beforeNode: "B",
    });

    const inserted = getNodeByNodeId(wf.id, "inserted");
    expect(inserted).not.toBeNull();
    expect(inserted!.node_type).toBe("custom-type");
    expect(JSON.parse(inserted!.input_json!)).toEqual(inputData);
  });

  // ---------------------------------------------------------------------------
  // IAR in a diamond DAG
  // ---------------------------------------------------------------------------

  test("IAR in a diamond: A->B->D, A->C->D, insert X before D", () => {
    const wf = createTestWorkflow();

    // Diamond: A -> B -> D, A -> C -> D
    createTestNode(wf.id, "A", "step", { depends_on: null });
    createTestNode(wf.id, "B", "step", {
      depends_on: JSON.stringify(["A"]),
    });
    createTestNode(wf.id, "C", "step", {
      depends_on: JSON.stringify(["A"]),
    });
    createTestNode(wf.id, "D", "step", {
      depends_on: JSON.stringify(["B", "C"]),
    });

    // Insert X before D
    applyInsertAndReconverge(wf.id, {
      insertedNode: { id: "X", type: "step", input: { diamond: true } },
      beforeNode: "D",
    });

    // X should depend on B and C (same as D's original deps)
    const x = getNodeByNodeId(wf.id, "X");
    const xDeps = parseDeps(x!);
    expect(xDeps).toContain("B");
    expect(xDeps).toContain("C");
    expect(xDeps.length).toBe(2);

    // D should now depend on B, C, and X
    const d = getNodeByNodeId(wf.id, "D");
    const dDeps = parseDeps(d!);
    expect(dDeps).toContain("B");
    expect(dDeps).toContain("C");
    expect(dDeps).toContain("X");
    expect(dDeps.length).toBe(3);

    // Verify total node count
    const nodes = getWorkflowNodes(wf.id);
    expect(nodes.length).toBe(5);
  });

  // ---------------------------------------------------------------------------
  // IAR scheduling correctness (ready node analysis)
  // ---------------------------------------------------------------------------

  test("IAR inserted node becomes ready when predecessors complete", () => {
    const wf = createTestWorkflow({ status: "running" });
    startWorkflow(wf.id);

    // A -> B chain
    const nodeA = createTestNode(wf.id, "A", "step", { depends_on: null });
    createTestNode(wf.id, "B", "step", {
      depends_on: JSON.stringify(["A"]),
    });

    // Insert X before B
    applyInsertAndReconverge(wf.id, {
      insertedNode: { id: "X", type: "step", input: {} },
      beforeNode: "B",
    });

    // Before A completes: only A should be ready (no deps)
    let nodes = getWorkflowNodes(wf.id);
    let ready = findReadyNodesFromArray(nodes);
    expect(ready.map((n) => n.node_id).sort()).toEqual(["A"]);

    // Complete A
    startNode(nodeA.id);
    completeNode(nodeA.id, {});

    // After A completes: X should be ready (depends on A, which is complete)
    // B should NOT be ready (depends on A + X, and X is still pending)
    nodes = getWorkflowNodes(wf.id);
    ready = findReadyNodesFromArray(nodes);
    const readyIds = ready.map((n) => n.node_id).sort();
    expect(readyIds).toContain("X");
    expect(readyIds).not.toContain("B");

    // Complete X
    const xNode = getNodeByNodeId(wf.id, "X")!;
    startNode(xNode.id);
    completeNode(xNode.id, {});

    // After X completes: B should now be ready
    nodes = getWorkflowNodes(wf.id);
    ready = findReadyNodesFromArray(nodes);
    expect(ready.map((n) => n.node_id)).toContain("B");
  });

  // ---------------------------------------------------------------------------
  // Engine integration: applyPattern dispatch
  // ---------------------------------------------------------------------------

  test("engine applyPattern dispatches 'insert-and-reconverge' correctly", () => {
    const wf = createTestWorkflow({ status: "running" });
    startWorkflow(wf.id);

    createTestNode(wf.id, "A", "step", { depends_on: null });
    const nodeB = createTestNode(wf.id, "B", "step", {
      depends_on: JSON.stringify(["A"]),
    });

    // Build node context for node B (needs workflow to exist in DB)
    const ctx = buildNodeContext(wf.id, nodeB);

    // Call applyPattern through the context (this is how executors use it)
    ctx.applyPattern("insert-and-reconverge", {
      insertedNode: { id: "via-ctx", type: "step", input: { fromCtx: true } },
      beforeNode: "B",
    } as InsertAndReconvergeConfig);

    // Verify the IAR was applied
    const inserted = getNodeByNodeId(wf.id, "via-ctx");
    expect(inserted).not.toBeNull();
    expect(inserted!.retry_reason).toBe("iar_inserted");

    const b = getNodeByNodeId(wf.id, "B");
    const bDeps = parseDeps(b!);
    expect(bDeps).toContain("via-ctx");
    expect(bDeps).toContain("A");
  });

  // ---------------------------------------------------------------------------
  // Integration: IAR'd node runs in correct order in engine
  // ---------------------------------------------------------------------------

  test("integration: IAR'd node runs in correct execution order", async () => {
    const executionOrder: string[] = [];

    registerBlueprint({
      type: "test-iar-integration",
      entryNode: "node-a",
      nodes: {
        "node-a": { type: "iar-step-a", dependsOn: [] },
        "node-b": { type: "iar-step-b", dependsOn: ["node-a"] },
        "node-c": { type: "iar-step-c", dependsOn: ["node-b"] },
      },
    });

    registerNodeExecutor({
      name: "iar-step-a",
      idempotent: true,
      async execute(ctx) {
        executionOrder.push("a");
        // Dynamically insert a node before node-b via IAR
        ctx.applyPattern("insert-and-reconverge", {
          insertedNode: {
            id: "node-a-prime",
            type: "iar-step-a-prime",
            input: { inserted: true },
          },
          beforeNode: "node-b",
        });
        return { step: "a-done" };
      },
    });

    registerNodeExecutor({
      name: "iar-step-a-prime",
      idempotent: true,
      async execute() {
        executionOrder.push("a-prime");
        return { step: "a-prime-done" };
      },
    });

    registerNodeExecutor({
      name: "iar-step-b",
      idempotent: true,
      async execute() {
        executionOrder.push("b");
        return { step: "b-done" };
      },
    });

    registerNodeExecutor({
      name: "iar-step-c",
      idempotent: true,
      async execute() {
        executionOrder.push("c");
        return { step: "c-done" };
      },
    });

    const result = await submit({
      type: "test-iar-integration",
      input: { run: true },
    });

    expect(result.status).toBe("created");

    // Poll for workflow completion instead of blind sleep
    let wf: Workflow | null = null;
    for (let i = 0; i < 100; i++) {
      wf = getWorkflow(result.workflowId);
      if (wf && (wf.status === "completed" || wf.status === "failed" || wf.status === "cancelled")) break;
      await Bun.sleep(50);
    }

    // Verify workflow completed successfully
    expect(wf).not.toBeNull();
    expect(wf!.status).toBe("completed");

    // Verify execution order: a must come first, then a-prime, then b, then c
    expect(executionOrder.indexOf("a")).toBe(0);
    expect(executionOrder.indexOf("a-prime")).toBeLessThan(
      executionOrder.indexOf("b")
    );
    expect(executionOrder.indexOf("b")).toBeLessThan(
      executionOrder.indexOf("c")
    );
    expect(executionOrder.length).toBe(4);

    // Verify all 4 nodes completed
    const nodes = getWorkflowNodes(result.workflowId);
    expect(nodes.length).toBe(4);
    for (const node of nodes) {
      expect(node.status).toBe("completed");
    }
  });
});
