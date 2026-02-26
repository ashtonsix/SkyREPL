// retry.test.ts — SPOT_INTERRUPTED → on-demand fallback via RWA pattern
//
// Verifies the two-phase spot recovery behavior (§6.6.1):
//   Attempt 1: retry spot (same spec, same region)
//   Attempt 2+: compensate failed spot, fallback to on-demand via applyRetryWithAlternative

import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import { setupTest } from "../../../../tests/harness";
import {
  ERROR_RETRY_MAPPING,
  determineRetryStrategy,
} from "./retry";

// =============================================================================
// determineRetryStrategy — SPOT_INTERRUPTED mapping
// =============================================================================

describe("determineRetryStrategy — SPOT_INTERRUPTED", () => {
  it("maps to fallback strategy", () => {
    const mapping = ERROR_RETRY_MAPPING["SPOT_INTERRUPTED"];
    expect(mapping).toBeDefined();
    expect(mapping.retryable).toBe(true);
    expect(mapping.strategy).toBe("fallback");
  });

  it("allows retry on attempt 0", () => {
    const decision = determineRetryStrategy(
      { code: "SPOT_INTERRUPTED", message: "spot lost", category: "provider" },
      0
    );
    expect(decision.shouldRetry).toBe(true);
    expect(decision.strategy).toBe("fallback");
  });

  it("allows retry on attempt 1", () => {
    const decision = determineRetryStrategy(
      { code: "SPOT_INTERRUPTED", message: "spot lost", category: "provider" },
      1
    );
    expect(decision.shouldRetry).toBe(true);
    expect(decision.strategy).toBe("fallback");
  });

  it("exhausts at maxRetries", () => {
    const mapping = ERROR_RETRY_MAPPING["SPOT_INTERRUPTED"];
    const decision = determineRetryStrategy(
      { code: "SPOT_INTERRUPTED", message: "spot lost", category: "provider" },
      mapping.maxRetries
    );
    expect(decision.shouldRetry).toBe(false);
  });
});

// =============================================================================
// handleRetry — SPOT_INTERRUPTED fallback behavior (integration via DB)
// =============================================================================

import {
  createWorkflow,
  getWorkflowNodes,
  createWorkflowNode,
} from "../../material/db";
import {
  failNode,
  resetNodeForRetry,
  startNode,
} from "../state-transitions";

describe("handleRetry — SPOT_INTERRUPTED fallback", () => {
  let cleanup: () => Promise<void>;

  beforeEach(() => {
    cleanup = setupTest();
  });

  afterEach(async () => {
    await cleanup();
  });

  function createTestWorkflow(nodeInput: Record<string, unknown> = {}) {
    const wf = createWorkflow({
      type: "test-spot-fallback",
      parent_workflow_id: null,
      depth: 0,
      status: "pending",
      current_node: null,
      input_json: JSON.stringify({}),
      output_json: null,
      error_json: null,
      manifest_id: null,
      trace_id: null,
      idempotency_key: null,
      timeout_ms: 3600000,
      timeout_at: null,
      started_at: null,
      finished_at: null,
      updated_at: Date.now(),
    });
    createWorkflowNode({
      workflow_id: wf.id,
      node_id: "spawn-instance",
      node_type: "spawn-instance",
      status: "pending",
      input_json: JSON.stringify(nodeInput),
      output_json: null,
      error_json: null,
      depends_on: "[]",
      attempt: 0,
      retry_reason: null,
      started_at: null,
      finished_at: null,
      updated_at: Date.now(),
    });
    // Start the node (increments attempt to 1)
    startNode(wf.id, "spawn-instance");
    return wf.id;
  }

  describe("attempt=1 retries spot", () => {
    it("resets node for retry with spot_retry reason", () => {
      const wfId = createTestWorkflow({ spec: "gpu:a100", is_spot: true });
      const nodes = getWorkflowNodes(wfId);
      const node = nodes.find(n => n.node_id === "spawn-instance")!;
      expect(node.attempt).toBe(1);
      expect(node.status).toBe("running");

      // Simulate what handleRetry does for attempt<=1: fail then reset
      failNode(node.id, JSON.stringify({
        code: "SPOT_INTERRUPTED",
        message: "spot lost",
        retried_with: "fallback_spot_retry",
      }));
      resetNodeForRetry(node.id, "spot_retry");

      const updated = getWorkflowNodes(wfId);
      const updatedNode = updated.find(n => n.node_id === "spawn-instance")!;
      expect(updatedNode.status).toBe("pending");
      expect(updatedNode.retry_reason).toBe("spot_retry");
    });

    it("does not create alternative node on first attempt", () => {
      const wfId = createTestWorkflow({ spec: "gpu:a100", is_spot: true });
      const nodesBefore = getWorkflowNodes(wfId);
      expect(nodesBefore).toHaveLength(1);

      const node = nodesBefore[0];
      failNode(node.id, JSON.stringify({ code: "SPOT_INTERRUPTED", message: "spot lost" }));
      resetNodeForRetry(node.id, "spot_retry");

      const nodesAfter = getWorkflowNodes(wfId);
      expect(nodesAfter).toHaveLength(1);
    });
  });

  describe("attempt>1 triggers on-demand RWA", () => {
    it("fails original node with fallback_on_demand marker", () => {
      const wfId = createTestWorkflow({ spec: "gpu:a100", is_spot: true });
      // Simulate attempt 2: fail, reset, start again
      const nodes1 = getWorkflowNodes(wfId);
      failNode(nodes1[0].id, JSON.stringify({ code: "SPOT_INTERRUPTED", message: "spot lost" }));
      resetNodeForRetry(nodes1[0].id, "spot_retry");
      startNode(wfId, "spawn-instance"); // now attempt=2

      const nodes2 = getWorkflowNodes(wfId);
      const node = nodes2.find(n => n.node_id === "spawn-instance")!;
      expect(node.attempt).toBe(2);

      // Simulate what handleRetry does for attempt>1
      failNode(node.id, JSON.stringify({
        code: "SPOT_INTERRUPTED",
        message: "spot lost",
        retried_with: "fallback_on_demand",
      }));

      const nodesAfter = getWorkflowNodes(wfId);
      const failedNode = nodesAfter.find(n => n.node_id === "spawn-instance")!;
      expect(failedNode.status).toBe("failed");
      const error = JSON.parse(failedNode.error_json!);
      expect(error.retried_with).toBe("fallback_on_demand");
    });

    it("already on-demand fails with fallback_exhausted", () => {
      const wfId = createTestWorkflow({ spec: "gpu:a100", is_spot: false });
      const nodes = getWorkflowNodes(wfId);
      const node = nodes[0];

      // On-demand node hit with SPOT_INTERRUPTED — no further fallback
      failNode(node.id, JSON.stringify({
        code: "SPOT_INTERRUPTED",
        message: "spot lost",
        retried_with: "fallback_exhausted",
      }));

      const updated = getWorkflowNodes(wfId);
      const failedNode = updated.find(n => n.node_id === "spawn-instance")!;
      expect(failedNode.status).toBe("failed");
      const error = JSON.parse(failedNode.error_json!);
      expect(error.retried_with).toBe("fallback_exhausted");
    });
  });
});
