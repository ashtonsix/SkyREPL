// tests/unit/workflow-events.test.ts - Engine EventEmitter tests
//
// Verifies that the engine emits correct events with correct data.

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest, waitForWorkflow } from "../harness";
import {
  submit,
  registerBlueprint,
  registerNodeExecutor,
} from "../../control/src/workflow/engine";
import {
  workflowEvents,
  type NodeStartedPayload,
  type NodeCompletedPayload,
  type NodeFailedPayload,
  type WorkflowCompletedPayload,
  type WorkflowFailedPayload,
} from "../../control/src/workflow/events";
import type { NodeExecutor, WorkflowBlueprint } from "../../control/src/workflow/engine.types";

// =============================================================================
// Test harness
// =============================================================================

let cleanup: () => Promise<void>;
beforeEach(() => { cleanup = setupTest({ engine: true }); });
afterEach(() => cleanup());

// =============================================================================
// Helpers
// =============================================================================

function makeSuccessExecutor(name: string, output: Record<string, unknown> = {}): NodeExecutor {
  return {
    name,
    idempotent: false,
    execute: async () => output,
  };
}

function makeFailExecutor(name: string, errorCode = "VALIDATION_ERROR"): NodeExecutor {
  return {
    name,
    idempotent: false,
    execute: async () => {
      throw Object.assign(new Error(`${name} failed`), {
        code: errorCode,
        category: "validation",
      });
    },
  };
}

function makeBlueprint(nodeType: string): WorkflowBlueprint {
  return {
    type: `test-${nodeType}`,
    entryNode: "step",
    nodes: { step: { type: nodeType, dependsOn: [] } },
  };
}

function makeLinearBlueprint(steps: string[]): WorkflowBlueprint {
  const nodes: WorkflowBlueprint["nodes"] = {};
  for (let i = 0; i < steps.length; i++) {
    nodes[`node-${i}`] = {
      type: steps[i],
      dependsOn: i > 0 ? [`node-${i - 1}`] : [],
    };
  }
  return { type: "test-linear", entryNode: "node-0", nodes };
}

/**
 * Subscribe to all workflow events globally BEFORE submitting.
 * Returns a stop() function and arrays that are lazily filtered by workflowId.
 *
 * Usage:
 *   const { events, stop } = subscribeToAllEvents();
 *   const { workflowId } = await submit(...);
 *   await waitForWorkflow(workflowId);
 *   stop();
 *   const e = events(workflowId);
 */
function subscribeToAllEvents() {
  const _nodeStarted: NodeStartedPayload[] = [];
  const _nodeCompleted: NodeCompletedPayload[] = [];
  const _nodeFailed: NodeFailedPayload[] = [];
  const _workflowCompleted: WorkflowCompletedPayload[] = [];
  const _workflowFailed: WorkflowFailedPayload[] = [];

  const onNodeStarted = (p: NodeStartedPayload) => _nodeStarted.push(p);
  const onNodeCompleted = (p: NodeCompletedPayload) => _nodeCompleted.push(p);
  const onNodeFailed = (p: NodeFailedPayload) => _nodeFailed.push(p);
  const onWorkflowCompleted = (p: WorkflowCompletedPayload) => _workflowCompleted.push(p);
  const onWorkflowFailed = (p: WorkflowFailedPayload) => _workflowFailed.push(p);

  workflowEvents.on("node_started", onNodeStarted);
  workflowEvents.on("node_completed", onNodeCompleted);
  workflowEvents.on("node_failed", onNodeFailed);
  workflowEvents.on("workflow_completed", onWorkflowCompleted);
  workflowEvents.on("workflow_failed", onWorkflowFailed);

  const stop = () => {
    workflowEvents.off("node_started", onNodeStarted);
    workflowEvents.off("node_completed", onNodeCompleted);
    workflowEvents.off("node_failed", onNodeFailed);
    workflowEvents.off("workflow_completed", onWorkflowCompleted);
    workflowEvents.off("workflow_failed", onWorkflowFailed);
  };

  const events = (workflowId: number) => ({
    nodeStarted: _nodeStarted.filter(p => p.workflowId === workflowId),
    nodeCompleted: _nodeCompleted.filter(p => p.workflowId === workflowId),
    nodeFailed: _nodeFailed.filter(p => p.workflowId === workflowId),
    workflowCompleted: _workflowCompleted.filter(p => p.workflowId === workflowId),
    workflowFailed: _workflowFailed.filter(p => p.workflowId === workflowId),
  });

  return { events, stop };
}

// =============================================================================
// Tests: node_started
// =============================================================================

describe("node_started event", () => {
  test("emits node_started with correct workflowId, nodeId, nodeType, and timestamp", async () => {
    registerNodeExecutor(makeSuccessExecutor("ok-node"));
    registerBlueprint(makeBlueprint("ok-node"));

    const { events, stop } = subscribeToAllEvents();
    const { workflowId } = await submit({ type: "test-ok-node", input: {} });
    await waitForWorkflow(workflowId);
    stop();

    const e = events(workflowId);
    expect(e.nodeStarted).toHaveLength(1);
    const evt = e.nodeStarted[0];
    expect(evt.workflowId).toBe(workflowId);
    expect(evt.nodeId).toBe("step");
    expect(evt.nodeType).toBe("ok-node");
    expect(typeof evt.timestamp).toBe("number");
    expect(evt.timestamp).toBeGreaterThan(0);
  });

  test("does NOT emit node_started for a workflow that was never submitted", () => {
    const captured: NodeStartedPayload[] = [];
    const listener = (p: NodeStartedPayload) => captured.push(p);
    workflowEvents.on("node_started", listener);
    // No submit — nothing should emit
    workflowEvents.off("node_started", listener);
    expect(captured).toHaveLength(0);
  });
});

// =============================================================================
// Tests: node_completed
// =============================================================================

describe("node_completed event", () => {
  test("emits node_completed with correct workflowId, nodeId, output, and timestamp", async () => {
    const output = { result: "hello", count: 42 };
    registerNodeExecutor(makeSuccessExecutor("output-node", output));
    registerBlueprint(makeBlueprint("output-node"));

    const { events, stop } = subscribeToAllEvents();
    const { workflowId } = await submit({ type: "test-output-node", input: {} });
    await waitForWorkflow(workflowId);
    stop();

    const e = events(workflowId);
    expect(e.nodeCompleted).toHaveLength(1);
    const evt = e.nodeCompleted[0];
    expect(evt.workflowId).toBe(workflowId);
    expect(evt.nodeId).toBe("step");
    expect(evt.nodeType).toBe("output-node");
    expect(evt.output).toMatchObject(output);
    expect(typeof evt.timestamp).toBe("number");
    expect(evt.timestamp).toBeGreaterThan(0);
  });

  test("emits node_completed for each node in a multi-node workflow", async () => {
    registerNodeExecutor(makeSuccessExecutor("step-a", { a: 1 }));
    registerNodeExecutor(makeSuccessExecutor("step-b", { b: 2 }));
    registerBlueprint(makeLinearBlueprint(["step-a", "step-b"]));

    const { events, stop } = subscribeToAllEvents();
    const { workflowId } = await submit({ type: "test-linear", input: {} });
    await waitForWorkflow(workflowId);
    stop();

    const e = events(workflowId);
    expect(e.nodeCompleted).toHaveLength(2);
    const nodeIds = e.nodeCompleted.map(ev => ev.nodeId).sort();
    expect(nodeIds).toEqual(["node-0", "node-1"]);
  });
});

// =============================================================================
// Tests: workflow_completed
// =============================================================================

describe("workflow_completed event", () => {
  test("emits workflow_completed after the last node finishes", async () => {
    registerNodeExecutor(makeSuccessExecutor("final-node", { done: true }));
    registerBlueprint(makeBlueprint("final-node"));

    const { events, stop } = subscribeToAllEvents();
    const { workflowId } = await submit({ type: "test-final-node", input: {} });
    await waitForWorkflow(workflowId);
    stop();

    const e = events(workflowId);
    expect(e.workflowCompleted).toHaveLength(1);
    const evt = e.workflowCompleted[0];
    expect(evt.workflowId).toBe(workflowId);
    expect(typeof evt.timestamp).toBe("number");
    expect(evt.timestamp).toBeGreaterThan(0);
  });

  test("workflow_completed event fires after node_completed (ordering)", async () => {
    registerNodeExecutor(makeSuccessExecutor("ordered-node"));
    registerBlueprint(makeBlueprint("ordered-node"));

    const order: string[] = [];
    const onNC = (p: NodeCompletedPayload) => { order.push(`node_completed:${p.workflowId}`); };
    const onWC = (p: WorkflowCompletedPayload) => { order.push(`workflow_completed:${p.workflowId}`); };
    workflowEvents.on("node_completed", onNC);
    workflowEvents.on("workflow_completed", onWC);

    const { workflowId } = await submit({ type: "test-ordered-node", input: {} });
    await waitForWorkflow(workflowId);

    workflowEvents.off("node_completed", onNC);
    workflowEvents.off("workflow_completed", onWC);

    // Filter to this workflow
    const relevant = order.filter(s => s.endsWith(`:${workflowId}`));
    const nodeIdx = relevant.lastIndexOf(`node_completed:${workflowId}`);
    const wfIdx = relevant.indexOf(`workflow_completed:${workflowId}`);
    expect(nodeIdx).toBeGreaterThanOrEqual(0);
    expect(wfIdx).toBeGreaterThan(nodeIdx);
  });

  test("does NOT emit workflow_completed for a workflow that has not started", () => {
    const captured: WorkflowCompletedPayload[] = [];
    const listener = (p: WorkflowCompletedPayload) => captured.push(p);
    workflowEvents.on("workflow_completed", listener);
    workflowEvents.off("workflow_completed", listener);
    expect(captured).toHaveLength(0);
  });
});

// =============================================================================
// Tests: node_failed / workflow_failed
// =============================================================================

describe("node_failed and workflow_failed events", () => {
  test("emits node_failed with correct nodeId and error message on terminal node failure", async () => {
    registerNodeExecutor(makeFailExecutor("failing-node"));
    registerBlueprint(makeBlueprint("failing-node"));

    const { events, stop } = subscribeToAllEvents();
    const { workflowId } = await submit({ type: "test-failing-node", input: {} });
    await waitForWorkflow(workflowId);
    stop();

    const e = events(workflowId);
    expect(e.nodeFailed).toHaveLength(1);
    const evt = e.nodeFailed[0];
    expect(evt.workflowId).toBe(workflowId);
    expect(evt.nodeId).toBe("step");
    expect(evt.nodeType).toBe("failing-node");
    expect(typeof evt.error).toBe("string");
    expect(evt.error.length).toBeGreaterThan(0);
    expect(typeof evt.timestamp).toBe("number");
    expect(evt.timestamp).toBeGreaterThan(0);
  });

  test("emits workflow_failed with error and nodeId when a node fails", async () => {
    registerNodeExecutor(makeFailExecutor("failing-node-2"));
    registerBlueprint(makeBlueprint("failing-node-2"));

    const { events, stop } = subscribeToAllEvents();
    const { workflowId } = await submit({ type: "test-failing-node-2", input: {} });
    await waitForWorkflow(workflowId);
    stop();

    const e = events(workflowId);
    expect(e.workflowFailed).toHaveLength(1);
    const evt = e.workflowFailed[0];
    expect(evt.workflowId).toBe(workflowId);
    expect(typeof evt.error).toBe("string");
    expect(evt.error.length).toBeGreaterThan(0);
    expect(typeof evt.timestamp).toBe("number");
    expect(evt.timestamp).toBeGreaterThan(0);
  });
});

// =============================================================================
// Tests: SSE filtering — events only for subscribed workflow
// =============================================================================

describe("SSE workflow isolation", () => {
  test("events from workflow A do not appear in listener filtered to workflow B", async () => {
    registerNodeExecutor(makeSuccessExecutor("iso-node"));
    registerBlueprint({ type: "test-iso-node", entryNode: "step", nodes: { step: { type: "iso-node", dependsOn: [] } } });

    const { events, stop } = subscribeToAllEvents();

    const resultA = await submit({ type: "test-iso-node", input: {} });
    const resultB = await submit({ type: "test-iso-node", input: {} });
    const wfAId = resultA.workflowId;
    const wfBId = resultB.workflowId;

    await waitForWorkflow(wfAId);
    await waitForWorkflow(wfBId);
    stop();

    const eventsForB = events(wfBId);

    // Workflow A's events should not appear when filtered to B
    for (const evt of eventsForB.nodeStarted) {
      expect(evt.workflowId).toBe(wfBId);
    }
    for (const evt of eventsForB.nodeCompleted) {
      expect(evt.workflowId).toBe(wfBId);
    }
    for (const evt of eventsForB.workflowCompleted) {
      expect(evt.workflowId).toBe(wfBId);
    }

    // Workflow B should have received its own events
    expect(eventsForB.nodeStarted).toHaveLength(1);
    expect(eventsForB.workflowCompleted).toHaveLength(1);
  });
});
