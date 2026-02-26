// tests/unit/workflow-engine.test.ts - Workflow Engine Comprehensive Tests
// Covers: state-transitions.ts, patterns.ts, engine.ts

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest, waitForWorkflow } from "../harness";
import {
  createWorkflow,
  createWorkflowNode,
  createManifest,
  getWorkflow,
  getWorkflowNodes,
  getManifest,
  updateWorkflow,
  queryOne,
  type Workflow,
  type WorkflowNode,
} from "../../control/src/material/db";

import {
  atomicTransition,
} from "../../control/src/workflow/state-transitions";

import {
  applyConditionalBranch,
  getPredecessors,
  getDownstreamNodes,
  updateJoinDependencies,
} from "../../control/src/workflow/patterns";

import {
  submit,
  registerBlueprint,
  registerNodeExecutor,
  getBlueprint,
  getNodeExecutor,
  determineRetryStrategy,
  handleRetry,
  executeNode,
  mapErrorToHttpStatus,
  calculateTopologySignature,
  findReadyNodesFromArray,
  buildNodeContext,
  executeLoop,
} from "../../control/src/workflow/engine";

import type {
  NodeExecutor,
  WorkflowBlueprint,
  NodeError,
  WorkflowError,
  RetryDecision,
} from "../../control/src/workflow/engine.types";

// =============================================================================
// Top-level harness setup
// =============================================================================

let cleanup: () => Promise<void>;
beforeEach(() => { cleanup = setupTest({ engine: true }); });
afterEach(() => cleanup());

// =============================================================================
// Test Helpers
// =============================================================================

function createTestWorkflow(overrides: Partial<Omit<Workflow, "id" | "created_at">> = {}) {
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

function createTestWorkflowNode(
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

function createTestManifest(workflowId: number) {
  return createManifest(workflowId);
}

// =============================================================================
// State Transitions Tests
// =============================================================================

describe("State Transitions", () => {

  // ---------------------------------------------------------------------------
  // atomicTransition
  // ---------------------------------------------------------------------------

  describe("atomicTransition", () => {
    test("succeeds for valid transition", () => {
      const wf = createTestWorkflow({ status: "pending" });
      const result = atomicTransition<Workflow>("workflows", wf.id, "pending", "running", { started_at: Date.now() });

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.status).toBe("running");
        expect(result.data.started_at).not.toBeNull();
      }
    });

    test("returns WRONG_STATE when current status does not match expected", () => {
      const wf = createTestWorkflow({ status: "pending" });
      const result = atomicTransition<Workflow>("workflows", wf.id, "running", "completed");

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.reason).toBe("WRONG_STATE");
        expect(result.current?.status).toBe("pending");
      }
    });

    test("returns NOT_FOUND for non-existent row", () => {
      const result = atomicTransition<Workflow>("workflows", 99999, "pending", "running");

      expect(result.success).toBe(false);
      if (!result.success) {
        expect(result.reason).toBe("NOT_FOUND");
        expect(result.current).toBeUndefined();
      }
    });

    test("supports array of valid source statuses", () => {
      const wf = createTestWorkflow({ status: "running" });
      const result = atomicTransition<Workflow>(
        "workflows",
        wf.id,
        ["running", "rolling_back"],
        "failed",
        { finished_at: Date.now() }
      );

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.status).toBe("failed");
      }
    });

    test("rejects invalid table names", () => {
      expect(() => {
        atomicTransition<Workflow>("evil_table", 1, "pending", "running");
      }).toThrow("Invalid table name");
    });

    test("applies additional updates alongside status change", () => {
      const wf = createTestWorkflow({ status: "running" });
      const now = Date.now();
      const result = atomicTransition<Workflow>(
        "workflows",
        wf.id,
        "running",
        "completed",
        { output_json: JSON.stringify({ result: "ok" }), finished_at: now }
      );

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.data.status).toBe("completed");
        expect(result.data.output_json).toBe(JSON.stringify({ result: "ok" }));
        expect(result.data.finished_at).toBe(now);
      }
    });
  });
});

// =============================================================================
// Engine Core Tests
// =============================================================================

describe("Engine Core", () => {

  // ---------------------------------------------------------------------------
  // determineRetryStrategy
  // ---------------------------------------------------------------------------

  describe("determineRetryStrategy", () => {
    test("retryable error returns shouldRetry true with strategy", () => {
      const error: NodeError = {
        code: "PROVIDER_INTERNAL",
        message: "API failed",
        category: "provider",
      };

      const decision = determineRetryStrategy(error, 0);
      expect(decision.shouldRetry).toBe(true);
      expect(decision.strategy).toBe("same_params");
      expect(decision.maxAttempts).toBe(3);
      expect(decision.delayMs).toBeGreaterThan(0);
    });

    test("non-retryable error returns shouldRetry false", () => {
      const error: NodeError = {
        code: "VALIDATION_ERROR",
        message: "Invalid input",
        category: "validation",
      };

      const decision = determineRetryStrategy(error, 0);
      expect(decision.shouldRetry).toBe(false);
      expect(decision.strategy).toBeNull();
    });

    test("unknown error code returns shouldRetry false", () => {
      const error: NodeError = {
        code: "TOTALLY_UNKNOWN_ERROR",
        message: "wat",
        category: "internal",
      };

      const decision = determineRetryStrategy(error, 0);
      expect(decision.shouldRetry).toBe(false);
      expect(decision.strategy).toBeNull();
    });

    test("max retries exceeded returns shouldRetry false", () => {
      const error: NodeError = {
        code: "PROVIDER_INTERNAL",
        message: "API failed",
        category: "provider",
      };

      // maxRetries for PROVIDER_INTERNAL is 3, so attempt 3 should not retry
      const decision = determineRetryStrategy(error, 3);
      expect(decision.shouldRetry).toBe(false);
      expect(decision.maxAttempts).toBe(3);
    });

    test("exponential backoff increases delay with attempt", () => {
      const error: NodeError = {
        code: "PROVIDER_INTERNAL",
        message: "API failed",
        category: "provider",
      };

      const d0 = determineRetryStrategy(error, 0);
      const d1 = determineRetryStrategy(error, 1);
      const d2 = determineRetryStrategy(error, 2);

      // Exponential: 2^0 * base, 2^1 * base, 2^2 * base
      expect(d1.delayMs).toBeGreaterThan(d0.delayMs);
      expect(d2.delayMs).toBeGreaterThan(d1.delayMs);
    });

    test("CAPACITY_ERROR uses alternative strategy", () => {
      const error: NodeError = {
        code: "CAPACITY_ERROR",
        message: "No capacity",
        category: "capacity",
      };

      const decision = determineRetryStrategy(error, 0);
      expect(decision.shouldRetry).toBe(true);
      expect(decision.strategy).toBe("alternative");
    });

    test("SPOT_INTERRUPTED uses fallback strategy", () => {
      const error: NodeError = {
        code: "SPOT_INTERRUPTED",
        message: "Spot reclaimed",
        category: "provider",
      };

      const decision = determineRetryStrategy(error, 0);
      expect(decision.shouldRetry).toBe(true);
      expect(decision.strategy).toBe("fallback");
    });

    test("RATE_LIMITED allows up to 5 retries", () => {
      const error: NodeError = {
        code: "RATE_LIMITED",
        message: "Too many requests",
        category: "rate_limit",
      };

      const d4 = determineRetryStrategy(error, 4);
      expect(d4.shouldRetry).toBe(true);

      const d5 = determineRetryStrategy(error, 5);
      expect(d5.shouldRetry).toBe(false);
    });

    test("INSUFFICIENT_PERMISSIONS is non-retryable", () => {
      const error: NodeError = {
        code: "INSUFFICIENT_PERMISSIONS",
        message: "Forbidden",
        category: "auth",
      };

      const decision = determineRetryStrategy(error, 0);
      expect(decision.shouldRetry).toBe(false);
    });

    test("DATABASE_ERROR is non-retryable", () => {
      const error: NodeError = {
        code: "DATABASE_ERROR",
        message: "DB error",
        category: "internal",
      };

      const decision = determineRetryStrategy(error, 0);
      expect(decision.shouldRetry).toBe(false);
    });
  });

  // ---------------------------------------------------------------------------
  // handleRetry - fallback path (SM-01)
  // ---------------------------------------------------------------------------

  describe("handleRetry fallback path (SM-01)", () => {
    test("fallback retry at attempt=0 transitions through failed->pending correctly", async () => {
      const wf = createTestWorkflow({ status: "running", started_at: Date.now() });
      const node = createTestWorkflowNode(wf.id, "spot-node", "spot-type", {
        status: "running",
        attempt: 1,
        started_at: Date.now(),
      });

      const error: NodeError = {
        code: "SPOT_INTERRUPTED",
        message: "Spot reclaimed",
        category: "provider",
      };

      const decision: RetryDecision = {
        shouldRetry: true,
        strategy: "fallback",
        delayMs: 0,
        maxAttempts: 1,
      };

      // The node is in running state; handleRetry for fallback with attempt===0
      // should call failNode first (running->failed), then resetNodeForRetry (failed->pending).
      const nodeWithAttempt0 = { ...node, attempt: 0 };

      await handleRetry(wf.id, nodeWithAttempt0, error, decision);

      // After handleRetry, the node should be back to pending (failed -> pending via resetNodeForRetry)
      const nodes = getWorkflowNodes(wf.id);
      const updatedNode = nodes.find(n => n.node_id === "spot-node");
      expect(updatedNode!.status).toBe("pending");
      expect(updatedNode!.retry_reason).toBe("spot_retry");
    });

    test("fallback retry at attempt>1 fails node and creates on-demand fallback via RWA", async () => {
      const wf = createTestWorkflow({ status: "running", started_at: Date.now() });
      const node = createTestWorkflowNode(wf.id, "spot-node-2", "spot-type", {
        status: "running",
        attempt: 2,
        input_json: JSON.stringify({ region: "us-east-1", is_spot: true }),
        started_at: Date.now(),
      });

      const error: NodeError = {
        code: "SPOT_INTERRUPTED",
        message: "Spot reclaimed",
        category: "provider",
      };

      const decision: RetryDecision = {
        shouldRetry: true,
        strategy: "fallback",
        delayMs: 0,
        maxAttempts: 3,
      };

      // When attempt > 1 (second execution), should fail the node and create on-demand fallback
      const nodeWithAttempt2 = { ...node, attempt: 2 };
      await handleRetry(wf.id, nodeWithAttempt2, error, decision);

      const nodes = getWorkflowNodes(wf.id);
      const updatedNode = nodes.find(n => n.node_id === "spot-node-2");
      expect(updatedNode!.status).toBe("failed");
      const errorJson = JSON.parse(updatedNode!.error_json!);
      expect(errorJson.retried_with).toBe("fallback_on_demand");

      // On-demand fallback node should have been created via RWA
      const fallbackNode = nodes.find(n => n.node_id === "spot-node-2-ondemand");
      expect(fallbackNode).not.toBeNull();
      expect(fallbackNode!.status).toBe("pending");
      const fallbackInput = JSON.parse(fallbackNode!.input_json);
      expect(fallbackInput.is_spot).toBe(false);
      expect(fallbackInput.region).toBe("us-east-1");
    });
  });

  // ---------------------------------------------------------------------------
  // mapErrorToHttpStatus
  // ---------------------------------------------------------------------------

  describe("mapErrorToHttpStatus", () => {
    test("maps all error categories to correct HTTP status codes", () => {
      const truthTable: Array<{ category: string; code: string; expected: number }> = [
        { category: "validation", code: "TEST", expected: 400 },
        { category: "auth", code: "UNAUTHORIZED", expected: 401 },
        { category: "auth", code: "FORBIDDEN", expected: 403 },
        { category: "not_found", code: "TEST", expected: 404 },
        { category: "conflict", code: "TEST", expected: 409 },
        { category: "rate_limit", code: "TEST", expected: 429 },
        { category: "timeout", code: "TEST", expected: 504 },
        { category: "provider", code: "TEST", expected: 502 },
        { category: "internal", code: "TEST", expected: 500 },
        { category: "garbage", code: "TEST", expected: 500 }, // unknown -> 500
      ];
      for (const { category, code, expected } of truthTable) {
        const err: WorkflowError = { code, message: "test", category };
        expect(mapErrorToHttpStatus(err)).toBe(expected);
      }
    });
  });

  // ---------------------------------------------------------------------------
  // calculateTopologySignature
  // ---------------------------------------------------------------------------

  describe("calculateTopologySignature", () => {
    test("returns deterministic hash for same structure", () => {
      const wf = createTestWorkflow();
      createTestWorkflowNode(wf.id, "spawn", "spawn-instance");
      createTestWorkflowNode(wf.id, "boot", "wait-for-boot", {
        depends_on: JSON.stringify(["spawn"]),
      });

      const sig1 = calculateTopologySignature(wf.id);
      const sig2 = calculateTopologySignature(wf.id);

      expect(sig1).toBe(sig2);
      expect(sig1.length).toBe(16);
    });

    test("different structures produce different hashes", () => {
      const wf1 = createTestWorkflow();
      createTestWorkflowNode(wf1.id, "step-a", "type-a");

      const wf2 = createTestWorkflow();
      createTestWorkflowNode(wf2.id, "step-b", "type-b");

      const sig1 = calculateTopologySignature(wf1.id);
      const sig2 = calculateTopologySignature(wf2.id);

      expect(sig1).not.toBe(sig2);
    });

    test("skipped nodes are excluded from signature", () => {
      const wf1 = createTestWorkflow();
      createTestWorkflowNode(wf1.id, "step-a", "type-a");

      const wf2 = createTestWorkflow();
      createTestWorkflowNode(wf2.id, "step-a", "type-a");
      createTestWorkflowNode(wf2.id, "step-skip", "type-skip", { status: "skipped" });

      const sig1 = calculateTopologySignature(wf1.id);
      const sig2 = calculateTopologySignature(wf2.id);

      expect(sig1).toBe(sig2);
    });
  });

  // ---------------------------------------------------------------------------
  // findReadyNodesFromArray
  // ---------------------------------------------------------------------------

  describe("findReadyNodesFromArray", () => {
    test("returns pending nodes with no dependencies", () => {
      const nodes: WorkflowNode[] = [
        { id: 1, workflow_id: 1, node_id: "a", node_type: "t", status: "pending", input_json: "{}", output_json: null, error_json: null, depends_on: null, attempt: 0, retry_reason: null, created_at: 0, started_at: null, finished_at: null, updated_at: 0 },
        { id: 2, workflow_id: 1, node_id: "b", node_type: "t", status: "pending", input_json: "{}", output_json: null, error_json: null, depends_on: JSON.stringify(["a"]), attempt: 0, retry_reason: null, created_at: 0, started_at: null, finished_at: null, updated_at: 0 },
      ];

      const ready = findReadyNodesFromArray(nodes);
      expect(ready.length).toBe(1);
      expect(ready[0].node_id).toBe("a");
    });

    test("returns nodes whose dependencies are all completed", () => {
      const nodes: WorkflowNode[] = [
        { id: 1, workflow_id: 1, node_id: "a", node_type: "t", status: "completed", input_json: "{}", output_json: "{}", error_json: null, depends_on: null, attempt: 1, retry_reason: null, created_at: 0, started_at: 0, finished_at: 0, updated_at: 0 },
        { id: 2, workflow_id: 1, node_id: "b", node_type: "t", status: "pending", input_json: "{}", output_json: null, error_json: null, depends_on: JSON.stringify(["a"]), attempt: 0, retry_reason: null, created_at: 0, started_at: null, finished_at: null, updated_at: 0 },
      ];

      const ready = findReadyNodesFromArray(nodes);
      expect(ready.length).toBe(1);
      expect(ready[0].node_id).toBe("b");
    });

    test("skipped dependencies count as satisfied", () => {
      const nodes: WorkflowNode[] = [
        { id: 1, workflow_id: 1, node_id: "a", node_type: "t", status: "skipped", input_json: "{}", output_json: null, error_json: null, depends_on: null, attempt: 0, retry_reason: null, created_at: 0, started_at: null, finished_at: 0, updated_at: 0 },
        { id: 2, workflow_id: 1, node_id: "b", node_type: "t", status: "pending", input_json: "{}", output_json: null, error_json: null, depends_on: JSON.stringify(["a"]), attempt: 0, retry_reason: null, created_at: 0, started_at: null, finished_at: null, updated_at: 0 },
      ];

      const ready = findReadyNodesFromArray(nodes);
      expect(ready.length).toBe(1);
      expect(ready[0].node_id).toBe("b");
    });

    test("returns empty when deps are not met (running)", () => {
      const nodes: WorkflowNode[] = [
        { id: 1, workflow_id: 1, node_id: "a", node_type: "t", status: "running", input_json: "{}", output_json: null, error_json: null, depends_on: null, attempt: 1, retry_reason: null, created_at: 0, started_at: 0, finished_at: null, updated_at: 0 },
        { id: 2, workflow_id: 1, node_id: "b", node_type: "t", status: "pending", input_json: "{}", output_json: null, error_json: null, depends_on: JSON.stringify(["a"]), attempt: 0, retry_reason: null, created_at: 0, started_at: null, finished_at: null, updated_at: 0 },
      ];

      const ready = findReadyNodesFromArray(nodes);
      expect(ready.length).toBe(0);
    });

    test("returns empty when deps are not met (failed)", () => {
      const nodes: WorkflowNode[] = [
        { id: 1, workflow_id: 1, node_id: "a", node_type: "t", status: "failed", input_json: "{}", output_json: null, error_json: "err", depends_on: null, attempt: 1, retry_reason: null, created_at: 0, started_at: 0, finished_at: 0, updated_at: 0 },
        { id: 2, workflow_id: 1, node_id: "b", node_type: "t", status: "pending", input_json: "{}", output_json: null, error_json: null, depends_on: JSON.stringify(["a"]), attempt: 0, retry_reason: null, created_at: 0, started_at: null, finished_at: null, updated_at: 0 },
      ];

      const ready = findReadyNodesFromArray(nodes);
      expect(ready.length).toBe(0);
    });

    test("handles multiple ready nodes in parallel", () => {
      const nodes: WorkflowNode[] = [
        { id: 1, workflow_id: 1, node_id: "a", node_type: "t", status: "pending", input_json: "{}", output_json: null, error_json: null, depends_on: null, attempt: 0, retry_reason: null, created_at: 0, started_at: null, finished_at: null, updated_at: 0 },
        { id: 2, workflow_id: 1, node_id: "b", node_type: "t", status: "pending", input_json: "{}", output_json: null, error_json: null, depends_on: null, attempt: 0, retry_reason: null, created_at: 0, started_at: null, finished_at: null, updated_at: 0 },
        { id: 3, workflow_id: 1, node_id: "c", node_type: "t", status: "pending", input_json: "{}", output_json: null, error_json: null, depends_on: JSON.stringify(["a", "b"]), attempt: 0, retry_reason: null, created_at: 0, started_at: null, finished_at: null, updated_at: 0 },
      ];

      const ready = findReadyNodesFromArray(nodes);
      expect(ready.length).toBe(2);
      const ids = ready.map((n) => n.node_id).sort();
      expect(ids).toEqual(["a", "b"]);
    });

    test("excludes running and completed nodes from ready set", () => {
      const nodes: WorkflowNode[] = [
        { id: 1, workflow_id: 1, node_id: "a", node_type: "t", status: "running", input_json: "{}", output_json: null, error_json: null, depends_on: null, attempt: 1, retry_reason: null, created_at: 0, started_at: 0, finished_at: null, updated_at: 0 },
        { id: 2, workflow_id: 1, node_id: "b", node_type: "t", status: "completed", input_json: "{}", output_json: "{}", error_json: null, depends_on: null, attempt: 1, retry_reason: null, created_at: 0, started_at: 0, finished_at: 0, updated_at: 0 },
      ];

      const ready = findReadyNodesFromArray(nodes);
      expect(ready.length).toBe(0);
    });
  });

  // ---------------------------------------------------------------------------
  // submit
  // ---------------------------------------------------------------------------

  describe("submit", () => {
    test("creates workflow + manifest + nodes from blueprint and returns workflowId", async () => {
      registerBlueprint({
        type: "test-submit",
        entryNode: "step-1",
        nodes: {
          "step-1": { type: "mock-exec", dependsOn: [] },
          "step-2": { type: "mock-exec", dependsOn: ["step-1"] },
        },
      });

      // Register executor so the executeLoop doesn't fail on unknown type
      registerNodeExecutor({
        name: "mock-exec",
        idempotent: true,
        async execute() {
          return { ok: true };
        },
      });

      const result = await submit({
        type: "test-submit",
        input: { foo: "bar" },
      });

      expect(result.status).toBe("created");
      expect(typeof result.workflowId).toBe("number");

      // Verify workflow was created in DB
      const wf = getWorkflow(result.workflowId);
      expect(wf).not.toBeNull();
      expect(wf!.type).toBe("test-submit");
      expect(wf!.input_json).toBe(JSON.stringify({ foo: "bar" }));

      // Verify manifest was created
      expect(wf!.manifest_id).not.toBeNull();
      const manifest = getManifest(wf!.manifest_id!);
      expect(manifest).not.toBeNull();
      expect(manifest!.status).toBe("DRAFT");

      // Verify nodes were created
      const nodes = getWorkflowNodes(result.workflowId);
      expect(nodes.length).toBe(2);

      const nodeIds = nodes.map((n) => n.node_id).sort();
      expect(nodeIds).toEqual(["step-1", "step-2"]);

      const step2 = nodes.find((n) => n.node_id === "step-2");
      expect(step2!.depends_on).toBe(JSON.stringify(["step-1"]));

      // Wait for fire-and-forget executeLoop to complete
      await waitForWorkflow(result.workflowId);
    });

    test("idempotency key returns deduplicated on second submit", async () => {
      registerBlueprint({
        type: "test-idemp",
        entryNode: "step-1",
        nodes: {
          "step-1": { type: "mock-idemp", dependsOn: [] },
        },
      });

      registerNodeExecutor({
        name: "mock-idemp",
        idempotent: true,
        async execute() {
          return {};
        },
      });

      const r1 = await submit({
        type: "test-idemp",
        input: {},
        idempotencyKey: "unique-key-123",
      });
      expect(r1.status).toBe("created");

      const r2 = await submit({
        type: "test-idemp",
        input: {},
        idempotencyKey: "unique-key-123",
      });
      expect(r2.status).toBe("deduplicated");
      expect(r2.existingWorkflowId).toBe(r1.workflowId);

      await waitForWorkflow(r1.workflowId);
    });

    test("throws when blueprint not found", async () => {
      try {
        await submit({
          type: "non-existent-blueprint",
          input: {},
        });
        expect(true).toBe(false); // Should not reach here
      } catch (err) {
        expect((err as Error).message).toContain("Blueprint not found");
      }
    });
  });

  // ---------------------------------------------------------------------------
  // buildNodeContext
  // ---------------------------------------------------------------------------

  describe("buildNodeContext", () => {
    test("builds context with correct fields", () => {
      const wf = createTestWorkflow();
      const manifest = createTestManifest(wf.id);

      // Update workflow with manifest_id
      updateWorkflow(wf.id, { manifest_id: manifest.id });

      const node = createTestWorkflowNode(wf.id, "step-1", "test-type", {
        input_json: JSON.stringify({ key: "value" }),
      });

      const ctx = buildNodeContext(wf.id, node);

      expect(ctx.workflowId).toBe(wf.id);
      expect(ctx.nodeId).toBe("step-1");
      expect(ctx.input).toEqual({ key: "value" });
      expect(ctx.manifestId).toBe(manifest.id);
      expect(typeof ctx.emitResource).toBe("function");
      expect(typeof ctx.getNodeOutput).toBe("function");
      expect(typeof ctx.log).toBe("function");
      expect(typeof ctx.checkCancellation).toBe("function");
      expect(typeof ctx.applyPattern).toBe("function");
    });

    test("getNodeOutput returns parsed output of dependency node", () => {
      const wf = createTestWorkflow();
      createTestWorkflowNode(wf.id, "dep-1", "test-type", {
        status: "completed",
        output_json: JSON.stringify({ result: 42 }),
      });

      const node = createTestWorkflowNode(wf.id, "step-2", "test-type", {
        depends_on: JSON.stringify(["dep-1"]),
      });

      const ctx = buildNodeContext(wf.id, node);
      const output = ctx.getNodeOutput("dep-1");

      expect(output).toEqual({ result: 42 });
    });

    test("getNodeOutput returns null for non-existent node", () => {
      const wf = createTestWorkflow();
      const node = createTestWorkflowNode(wf.id, "step-1", "test-type");

      const ctx = buildNodeContext(wf.id, node);
      const output = ctx.getNodeOutput("non-existent");

      expect(output).toBeNull();
    });
  });

  // ---------------------------------------------------------------------------
  // Simple DAG Execution (end-to-end)
  // ---------------------------------------------------------------------------

  describe("DAG Execution", () => {
    test("sequential 2-node DAG completes successfully", async () => {
      const executionOrder: string[] = [];

      registerBlueprint({
        type: "test-dag-seq",
        entryNode: "node-a",
        nodes: {
          "node-a": { type: "dag-step-a", dependsOn: [] },
          "node-b": { type: "dag-step-b", dependsOn: ["node-a"] },
        },
      });

      registerNodeExecutor({
        name: "dag-step-a",
        idempotent: true,
        async execute(ctx) {
          executionOrder.push("a");
          return { step: "a-done" };
        },
      });

      registerNodeExecutor({
        name: "dag-step-b",
        idempotent: true,
        async execute(ctx) {
          executionOrder.push("b");
          // Verify we can read node-a's output
          const aOutput = ctx.getNodeOutput("node-a");
          return { step: "b-done", aResult: aOutput };
        },
      });

      const result = await submit({
        type: "test-dag-seq",
        input: { run: true },
      });

      expect(result.status).toBe("created");

      // Wait for the fire-and-forget executeLoop
      await waitForWorkflow(result.workflowId);

      // Verify execution order
      expect(executionOrder).toEqual(["a", "b"]);

      // Verify workflow completed
      const wf = getWorkflow(result.workflowId);
      expect(wf).not.toBeNull();
      expect(wf!.status).toBe("completed");
      expect(wf!.finished_at).not.toBeNull();

      // Verify output contains both node outputs
      const output = JSON.parse(wf!.output_json!);
      expect(output["node-a"]).toEqual({ step: "a-done" });
      expect(output["node-b"]).toEqual({ step: "b-done", aResult: { step: "a-done" } });

      // Verify all nodes completed
      const nodes = getWorkflowNodes(result.workflowId);
      for (const node of nodes) {
        expect(node.status).toBe("completed");
        expect(node.finished_at).not.toBeNull();
      }

      // Verify manifest was sealed
      const manifest = getManifest(wf!.manifest_id!);
      expect(manifest!.status).toBe("SEALED");
    });

    test("parallel nodes execute independently", async () => {
      const executionOrder: string[] = [];

      registerBlueprint({
        type: "test-dag-parallel",
        entryNode: "node-a",
        nodes: {
          "node-a": { type: "par-step-a", dependsOn: [] },
          "node-b": { type: "par-step-b", dependsOn: [] },
          "node-join": { type: "par-step-join", dependsOn: ["node-a", "node-b"] },
        },
      });

      registerNodeExecutor({
        name: "par-step-a",
        idempotent: true,
        async execute() {
          executionOrder.push("a");
          return { from: "a" };
        },
      });

      registerNodeExecutor({
        name: "par-step-b",
        idempotent: true,
        async execute() {
          executionOrder.push("b");
          return { from: "b" };
        },
      });

      registerNodeExecutor({
        name: "par-step-join",
        idempotent: true,
        async execute() {
          executionOrder.push("join");
          return { from: "join" };
        },
      });

      const result = await submit({
        type: "test-dag-parallel",
        input: {},
      });

      await waitForWorkflow(result.workflowId);

      // Both a and b should execute before join
      expect(executionOrder.indexOf("join")).toBeGreaterThan(executionOrder.indexOf("a"));
      expect(executionOrder.indexOf("join")).toBeGreaterThan(executionOrder.indexOf("b"));

      const wf = getWorkflow(result.workflowId);
      expect(wf!.status).toBe("completed");
    });

    test("node failure causes workflow failure", async () => {
      registerBlueprint({
        type: "test-dag-fail",
        entryNode: "node-ok",
        nodes: {
          "node-ok": { type: "fail-step-ok", dependsOn: [] },
          "node-bad": { type: "fail-step-bad", dependsOn: ["node-ok"] },
        },
      });

      registerNodeExecutor({
        name: "fail-step-ok",
        idempotent: true,
        async execute() {
          return { ok: true };
        },
      });

      registerNodeExecutor({
        name: "fail-step-bad",
        idempotent: true,
        async execute() {
          throw Object.assign(new Error("Intentional failure"), {
            code: "VALIDATION_ERROR",
            category: "validation",
          });
        },
      });

      const result = await submit({
        type: "test-dag-fail",
        input: {},
      });

      await waitForWorkflow(result.workflowId);

      const wf = getWorkflow(result.workflowId);
      expect(wf!.status).toBe("failed");
      expect(wf!.error_json).not.toBeNull();
    });

  });
});

// =============================================================================
// Patterns Tests
// =============================================================================

describe("Patterns", () => {

  // ---------------------------------------------------------------------------
  // applyConditionalBranch
  // ---------------------------------------------------------------------------

  describe("applyConditionalBranch", () => {
    test("deterministic mode (condition=true): chosen=option1 pending, option2 skipped", () => {
      const wf = createTestWorkflow();
      // Create the join node that CB references
      createTestWorkflowNode(wf.id, "join", "join-type");

      applyConditionalBranch(wf.id, {
        condition: true,
        option1: { id: "branch-a", type: "type-a", input: { x: 1 } },
        option2: { id: "branch-b", type: "type-b", input: { y: 2 } },
        joinNode: "join",
      });

      const nodes = getWorkflowNodes(wf.id);
      const branchA = nodes.find((n) => n.node_id === "branch-a");
      const branchB = nodes.find((n) => n.node_id === "branch-b");

      expect(branchA).not.toBeUndefined();
      expect(branchA!.status).toBe("pending");
      expect(branchA!.node_type).toBe("type-a");

      expect(branchB).not.toBeUndefined();
      expect(branchB!.status).toBe("skipped");
      expect(branchB!.node_type).toBe("type-b");
    });

    test("deterministic mode (condition=false): chosen=option2 pending, option1 skipped", () => {
      const wf = createTestWorkflow();
      createTestWorkflowNode(wf.id, "join", "join-type");

      applyConditionalBranch(wf.id, {
        condition: false,
        option1: { id: "branch-a", type: "type-a", input: {} },
        option2: { id: "branch-b", type: "type-b", input: {} },
        joinNode: "join",
      });

      const nodes = getWorkflowNodes(wf.id);
      const branchA = nodes.find((n) => n.node_id === "branch-a");
      const branchB = nodes.find((n) => n.node_id === "branch-b");

      expect(branchA!.status).toBe("skipped");
      expect(branchB!.status).toBe("pending");
    });

    test("deterministic mode updates join node dependencies", () => {
      const wf = createTestWorkflow();
      createTestWorkflowNode(wf.id, "join", "join-type", {
        depends_on: JSON.stringify(["existing-dep"]),
      });

      applyConditionalBranch(wf.id, {
        condition: true,
        option1: { id: "branch-a", type: "type-a", input: {} },
        option2: { id: "branch-b", type: "type-b", input: {} },
        joinNode: "join",
      });

      const joinNode = queryOne<WorkflowNode>(
        "SELECT * FROM workflow_nodes WHERE workflow_id = ? AND node_id = ?",
        [wf.id, "join"]
      );
      const deps = JSON.parse(joinNode!.depends_on!) as string[];
      expect(deps).toContain("existing-dep");
      expect(deps).toContain("branch-a");
      expect(deps).toContain("branch-b");
    });

    test("triggerOnError mode: both options created as pending, fallback gated on primary", () => {
      const wf = createTestWorkflow();
      createTestWorkflowNode(wf.id, "join", "join-type");

      applyConditionalBranch(wf.id, {
        triggerOnError: true,
        option1: { id: "primary", type: "type-p", input: {} },
        option2: { id: "fallback", type: "type-f", input: {} },
        joinNode: "join",
      });

      const nodes = getWorkflowNodes(wf.id);
      const primary = nodes.find((n) => n.node_id === "primary");
      const fallback = nodes.find((n) => n.node_id === "fallback");

      expect(primary!.status).toBe("pending");
      expect(fallback!.status).toBe("pending");
      // Fallback depends on primary and carries the cb_fallback marker
      const fallbackDeps = JSON.parse(fallback!.depends_on!) as string[];
      expect(fallbackDeps).toContain("primary");
      expect(fallback!.retry_reason).toBe("cb_fallback_for:primary");
    });

    test("triggerOnError mode: join depends only on fallback branch", () => {
      const wf = createTestWorkflow();
      createTestWorkflowNode(wf.id, "join", "join-type");

      applyConditionalBranch(wf.id, {
        triggerOnError: true,
        option1: { id: "primary", type: "type-p", input: {} },
        option2: { id: "fallback", type: "type-f", input: {} },
        joinNode: "join",
      });

      const joinNode = queryOne<WorkflowNode>(
        "SELECT * FROM workflow_nodes WHERE workflow_id = ? AND node_id = ?",
        [wf.id, "join"]
      );
      const deps = JSON.parse(joinNode!.depends_on!) as string[];
      // Join depends only on fallback (which is skipped on success, or runs on failure)
      expect(deps).toContain("fallback");
      expect(deps).not.toContain("primary");
    });

    test("throws when join node does not exist", () => {
      const wf = createTestWorkflow();

      expect(() => {
        applyConditionalBranch(wf.id, {
          condition: true,
          option1: { id: "branch-a", type: "type-a", input: {} },
          option2: { id: "branch-b", type: "type-b", input: {} },
          joinNode: "nonexistent",
        });
      }).toThrow("Join node 'nonexistent' not found");
    });

    test("throws when condition is missing in deterministic mode", () => {
      const wf = createTestWorkflow();
      createTestWorkflowNode(wf.id, "join", "join-type");

      expect(() => {
        applyConditionalBranch(wf.id, {
          // No condition, no triggerOnError
          option1: { id: "branch-a", type: "type-a", input: {} },
          option2: { id: "branch-b", type: "type-b", input: {} },
          joinNode: "join",
        });
      }).toThrow("condition is required");
    });
  });

  // ---------------------------------------------------------------------------
  // getPredecessors
  // ---------------------------------------------------------------------------

  describe("getPredecessors", () => {
    test("returns predecessor nodes based on depends_on", () => {
      const wf = createTestWorkflow();
      createTestWorkflowNode(wf.id, "a", "type-a");
      createTestWorkflowNode(wf.id, "b", "type-b");
      createTestWorkflowNode(wf.id, "c", "type-c", {
        depends_on: JSON.stringify(["a", "b"]),
      });

      const preds = getPredecessors(wf.id, "c");
      const predIds = preds.map((p) => p.node_id).sort();
      expect(predIds).toEqual(["a", "b"]);
    });

    test("returns empty array for node with no dependencies", () => {
      const wf = createTestWorkflow();
      createTestWorkflowNode(wf.id, "a", "type-a");

      const preds = getPredecessors(wf.id, "a");
      expect(preds).toEqual([]);
    });

    test("returns empty array for non-existent node", () => {
      const wf = createTestWorkflow();
      const preds = getPredecessors(wf.id, "nonexistent");
      expect(preds).toEqual([]);
    });
  });

  // ---------------------------------------------------------------------------
  // getDownstreamNodes
  // ---------------------------------------------------------------------------

  describe("getDownstreamNodes", () => {
    test("returns nodes that depend on the given node", () => {
      const wf = createTestWorkflow();
      createTestWorkflowNode(wf.id, "a", "type-a");
      createTestWorkflowNode(wf.id, "b", "type-b", {
        depends_on: JSON.stringify(["a"]),
      });
      createTestWorkflowNode(wf.id, "c", "type-c", {
        depends_on: JSON.stringify(["a"]),
      });
      createTestWorkflowNode(wf.id, "d", "type-d", {
        depends_on: JSON.stringify(["b"]),
      });

      const downstream = getDownstreamNodes(wf.id, "a");
      const downIds = downstream.map((d) => d.node_id).sort();
      expect(downIds).toEqual(["b", "c"]);
    });

    test("returns empty array when no downstream nodes", () => {
      const wf = createTestWorkflow();
      createTestWorkflowNode(wf.id, "leaf", "type-leaf");

      const downstream = getDownstreamNodes(wf.id, "leaf");
      expect(downstream).toEqual([]);
    });
  });

  // ---------------------------------------------------------------------------
  // updateJoinDependencies
  // ---------------------------------------------------------------------------

  describe("updateJoinDependencies", () => {
    test("merges new deps into existing deps", () => {
      const wf = createTestWorkflow();
      createTestWorkflowNode(wf.id, "join", "join-type", {
        depends_on: JSON.stringify(["existing"]),
      });

      updateJoinDependencies(wf.id, "join", ["new-a", "new-b"]);

      const joinNode = queryOne<WorkflowNode>(
        "SELECT * FROM workflow_nodes WHERE workflow_id = ? AND node_id = ?",
        [wf.id, "join"]
      );
      const deps = JSON.parse(joinNode!.depends_on!) as string[];
      expect(deps).toContain("existing");
      expect(deps).toContain("new-a");
      expect(deps).toContain("new-b");
    });

    test("deduplicates dependencies", () => {
      const wf = createTestWorkflow();
      createTestWorkflowNode(wf.id, "join", "join-type", {
        depends_on: JSON.stringify(["dep-a"]),
      });

      updateJoinDependencies(wf.id, "join", ["dep-a", "dep-b"]);

      const joinNode = queryOne<WorkflowNode>(
        "SELECT * FROM workflow_nodes WHERE workflow_id = ? AND node_id = ?",
        [wf.id, "join"]
      );
      const deps = JSON.parse(joinNode!.depends_on!) as string[];
      // dep-a should appear only once
      expect(deps.filter((d) => d === "dep-a").length).toBe(1);
      expect(deps).toContain("dep-b");
    });

    test("handles node with null depends_on", () => {
      const wf = createTestWorkflow();
      createTestWorkflowNode(wf.id, "join", "join-type", {
        depends_on: null,
      });

      updateJoinDependencies(wf.id, "join", ["new-dep"]);

      const joinNode = queryOne<WorkflowNode>(
        "SELECT * FROM workflow_nodes WHERE workflow_id = ? AND node_id = ?",
        [wf.id, "join"]
      );
      const deps = JSON.parse(joinNode!.depends_on!) as string[];
      expect(deps).toEqual(["new-dep"]);
    });

    test("is a no-op when join node does not exist", () => {
      const wf = createTestWorkflow();

      // Should not throw
      updateJoinDependencies(wf.id, "nonexistent", ["dep-a"]);
    });
  });
});

// =============================================================================
// NodeExecutor.validate() Hook (#WF2-08)
// =============================================================================

describe("NodeExecutor.validate() Hook (#WF2-08)", () => {
  test("node with failing validate never executes", async () => {
    const executeCalls: string[] = [];

    registerNodeExecutor({
      name: "validate-fail-type",
      idempotent: true,
      async validate() {
        throw Object.assign(new Error("Input is invalid"), {
          code: "VALIDATION_ERROR",
          category: "validation",
        });
      },
      async execute() {
        executeCalls.push("should-not-run");
        return {};
      },
    });

    registerBlueprint({
      type: "test-validate-fail",
      entryNode: "step",
      nodes: {
        step: { type: "validate-fail-type", dependsOn: [] },
      },
    });

    const result = await submit({ type: "test-validate-fail", input: {} });
    const wf = await waitForWorkflow(result.workflowId);

    expect(wf.status).toBe("failed");
    expect(executeCalls).toEqual([]); // execute never called

    // Verify node failed with VALIDATION_ERROR
    const nodes = getWorkflowNodes(result.workflowId);
    const step = nodes.find(n => n.node_id === "step");
    expect(step!.status).toBe("failed");
    const err = JSON.parse(step!.error_json!);
    expect(err.code).toBe("VALIDATION_ERROR");
  });

  test("validate failure produces VALIDATION_ERROR even if error code differs", async () => {
    registerNodeExecutor({
      name: "validate-generic-err-type",
      idempotent: true,
      async validate() {
        throw new Error("bad input");
      },
      async execute() {
        return {};
      },
    });

    registerBlueprint({
      type: "test-validate-generic-err",
      entryNode: "step",
      nodes: {
        step: { type: "validate-generic-err-type", dependsOn: [] },
      },
    });

    const result = await submit({ type: "test-validate-generic-err", input: {} });
    const wf = await waitForWorkflow(result.workflowId);

    expect(wf.status).toBe("failed");
    const nodes = getWorkflowNodes(result.workflowId);
    const step = nodes.find(n => n.node_id === "step");
    const err = JSON.parse(step!.error_json!);
    expect(err.code).toBe("VALIDATION_ERROR");
  });

  test("validate failure does not trigger compensation", async () => {
    const compensateCalls: string[] = [];

    registerNodeExecutor({
      name: "validate-no-comp-type",
      idempotent: true,
      async validate() {
        throw Object.assign(new Error("invalid"), {
          code: "VALIDATION_ERROR",
        });
      },
      async execute() {
        return {};
      },
      async compensate(ctx) {
        compensateCalls.push(ctx.nodeId);
      },
    });

    registerBlueprint({
      type: "test-validate-no-comp",
      entryNode: "step",
      nodes: {
        step: { type: "validate-no-comp-type", dependsOn: [] },
      },
    });

    const result = await submit({ type: "test-validate-no-comp", input: {} });
    await waitForWorkflow(result.workflowId);

    // Compensation should NOT be called since execute never ran
    expect(compensateCalls).toEqual([]);
  });

  test("node with passing validate proceeds to execute", async () => {
    const executeCalls: string[] = [];
    const validateCalls: string[] = [];

    registerNodeExecutor({
      name: "validate-pass-type",
      idempotent: true,
      async validate() {
        validateCalls.push("validated");
      },
      async execute() {
        executeCalls.push("executed");
        return { ok: true };
      },
    });

    registerBlueprint({
      type: "test-validate-pass",
      entryNode: "step",
      nodes: {
        step: { type: "validate-pass-type", dependsOn: [] },
      },
    });

    const result = await submit({ type: "test-validate-pass", input: {} });
    const wf = await waitForWorkflow(result.workflowId);

    expect(wf.status).toBe("completed");
    expect(validateCalls).toEqual(["validated"]);
    expect(executeCalls).toEqual(["executed"]);
  });
});

// =============================================================================
// CB Try-Fallback (#WF2-02)
// =============================================================================

describe("CB Try-Fallback (#WF2-02)", () => {
  test("CB triggerOnError: option1 succeeds -> option2 skipped, join runs", async () => {
    const executionLog: string[] = [];

    registerNodeExecutor({
      name: "cb-source-success",
      idempotent: true,
      async execute(ctx) {
        ctx.applyPattern("conditional-branch", {
          triggerOnError: true,
          option1: { id: "try-node", type: "cb-try-fast", input: {} },
          option2: { id: "fallback-node", type: "cb-fallback-alt", input: {} },
          joinNode: "join",
        });
        executionLog.push("source");
        return { done: true };
      },
    });

    registerNodeExecutor({
      name: "cb-try-fast",
      idempotent: true,
      async execute() {
        executionLog.push("try");
        return { ok: true };
      },
    });

    registerNodeExecutor({
      name: "cb-fallback-alt",
      idempotent: true,
      async execute() {
        executionLog.push("fallback");
        return { fallback: true };
      },
    });

    registerNodeExecutor({
      name: "cb-join-type",
      idempotent: true,
      async execute() {
        executionLog.push("join");
        return { joined: true };
      },
    });

    registerBlueprint({
      type: "test-cb-success-path",
      entryNode: "source",
      nodes: {
        source: { type: "cb-source-success", dependsOn: [] },
        join: { type: "cb-join-type", dependsOn: ["source"] },
      },
    });

    const result = await submit({ type: "test-cb-success-path", input: {} });
    const wf = await waitForWorkflow(result.workflowId);

    expect(wf.status).toBe("completed");
    // try-node ran, fallback was skipped, join ran
    expect(executionLog).toContain("source");
    expect(executionLog).toContain("try");
    expect(executionLog).not.toContain("fallback");
    expect(executionLog).toContain("join");

    // Verify node states
    const nodes = getWorkflowNodes(result.workflowId);
    const tryNode = nodes.find(n => n.node_id === "try-node");
    const fallbackNode = nodes.find(n => n.node_id === "fallback-node");
    expect(tryNode!.status).toBe("completed");
    expect(fallbackNode!.status).toBe("skipped");
  });

  test("CB triggerOnError: option1 fails -> option2 runs, join runs", async () => {
    const executionLog: string[] = [];

    registerNodeExecutor({
      name: "cb-source-fail",
      idempotent: true,
      async execute(ctx) {
        ctx.applyPattern("conditional-branch", {
          triggerOnError: true,
          option1: { id: "try-fail", type: "cb-try-fails", input: {} },
          option2: { id: "fallback-runs", type: "cb-fallback-runs", input: {} },
          joinNode: "join",
        });
        executionLog.push("source");
        return { done: true };
      },
    });

    registerNodeExecutor({
      name: "cb-try-fails",
      idempotent: true,
      async execute() {
        executionLog.push("try");
        throw Object.assign(new Error("primary failed"), {
          code: "VALIDATION_ERROR",
          category: "validation",
        });
      },
    });

    registerNodeExecutor({
      name: "cb-fallback-runs",
      idempotent: true,
      async execute() {
        executionLog.push("fallback");
        return { fallback: true };
      },
    });

    registerNodeExecutor({
      name: "cb-join-type-2",
      idempotent: true,
      async execute() {
        executionLog.push("join");
        return { joined: true };
      },
    });

    registerBlueprint({
      type: "test-cb-failure-path",
      entryNode: "source",
      nodes: {
        source: { type: "cb-source-fail", dependsOn: [] },
        join: { type: "cb-join-type-2", dependsOn: ["source"] },
      },
    });

    const result = await submit({ type: "test-cb-failure-path", input: {} });
    const wf = await waitForWorkflow(result.workflowId);

    expect(wf.status).toBe("completed");
    // try-fail ran and failed, fallback ran, join ran
    expect(executionLog).toContain("source");
    expect(executionLog).toContain("try");
    expect(executionLog).toContain("fallback");
    expect(executionLog).toContain("join");

    // Verify node states
    const nodes = getWorkflowNodes(result.workflowId);
    const tryNode = nodes.find(n => n.node_id === "try-fail");
    const fallbackNode = nodes.find(n => n.node_id === "fallback-runs");
    expect(tryNode!.status).toBe("failed");
    expect(fallbackNode!.status).toBe("completed");
  });

  test("SPOT_INTERRUPTED: first attempt retries spot, second attempt creates on-demand fallback", async () => {
    let spawnAttempt = 0;
    const executionLog: string[] = [];

    registerNodeExecutor({
      name: "spot-spawn-test",
      idempotent: true,
      async execute() {
        spawnAttempt++;
        executionLog.push(`spawn-attempt-${spawnAttempt}`);
        // Always fail with SPOT_INTERRUPTED
        throw Object.assign(new Error("Spot instance reclaimed"), {
          code: "SPOT_INTERRUPTED",
          category: "provider",
        });
      },
    });

    registerNodeExecutor({
      name: "spot-join-type",
      idempotent: true,
      async execute() {
        executionLog.push("join");
        return { joined: true };
      },
    });

    registerBlueprint({
      type: "test-spot-fallback",
      entryNode: "spawn",
      nodes: {
        spawn: { type: "spot-spawn-test", dependsOn: [] },
        join: { type: "spot-join-type", dependsOn: ["spawn"] },
      },
    });

    const result = await submit({
      type: "test-spot-fallback",
      input: { is_spot: true, region: "us-east-1" },
    });

    const wf = await waitForWorkflow(result.workflowId);

    // The workflow should eventually fail (both spot attempts fail, on-demand fallback also fails)
    // or it creates the fallback node.
    // Since the same executor is used for both spot and on-demand, both fail.
    expect(wf.status).toBe("failed");

    // Verify the spot node was retried once (attempt 0 -> attempt 1)
    expect(spawnAttempt).toBeGreaterThanOrEqual(2);

    // Verify an on-demand fallback node was created
    const nodes = getWorkflowNodes(result.workflowId);
    const fallbackNode = nodes.find(n => n.node_id === "spawn-ondemand");
    expect(fallbackNode).not.toBeUndefined();
    if (fallbackNode) {
      const fallbackInput = JSON.parse(fallbackNode.input_json);
      expect(fallbackInput.is_spot).toBe(false);
    }
  });
});
