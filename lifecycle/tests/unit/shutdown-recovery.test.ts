// tests/unit/shutdown-recovery.test.ts - Shutdown and Crash Recovery Tests

import { describe, it, expect, beforeEach, afterEach } from "bun:test";
import {
  walCheckpoint,
  createWorkflow,
  createWorkflowNode,
  getWorkflow,
  getWorkflowNodes,
  updateWorkflowNode,
  findActiveWorkflows,
} from "../../control/src/material/db";
import { recoverWorkflows, registerNodeExecutor } from "../../control/src/workflow/engine";
import type { NodeExecutor, NodeContext } from "../../control/src/workflow/engine.types";
import { setupTest } from "../harness";

// =============================================================================
// Setup
// =============================================================================

let cleanup: () => Promise<void>;

beforeEach(() => {
  cleanup = setupTest({ engine: true });
});

afterEach(() => cleanup());

// =============================================================================
// Mock Node Executors
// =============================================================================

const idempotentExecutor: NodeExecutor = {
  name: "test-idempotent",
  idempotent: true,
  execute: async (_ctx: NodeContext) => {
    return { result: "ok" };
  },
};

const nonIdempotentExecutor: NodeExecutor = {
  name: "test-non-idempotent",
  idempotent: false,
  execute: async (_ctx: NodeContext) => {
    return { result: "ok" };
  },
};

// =============================================================================
// Crash Recovery Tests
// =============================================================================

describe("Crash Recovery", () => {
  it("should reset running idempotent node to pending", async () => {
    registerNodeExecutor(idempotentExecutor);

    // Create a running workflow with a running idempotent node
    const workflow = createWorkflow({
      type: "test-workflow",
      parent_workflow_id: null,
      depth: 0,
      status: "running",
      current_node: null,
      input_json: "{}",
      output_json: null,
      error_json: null,
      manifest_id: null,
      trace_id: null,
      idempotency_key: null,
      timeout_ms: 60000,
      timeout_at: Date.now() + 60000,
      started_at: Date.now(),
      finished_at: null,
      updated_at: Date.now(),
    });

    const node = createWorkflowNode({
      workflow_id: workflow.id,
      node_id: "test-node",
      node_type: "test-idempotent",
      status: "running",
      input_json: "{}",
      output_json: null,
      error_json: null,
      depends_on: null,
      attempt: 0,
      retry_reason: null,
      started_at: Date.now(),
      finished_at: null,
      updated_at: Date.now(),
    });

    // Run recovery
    await recoverWorkflows();

    // Wait a very short time just for the atomic transitions to complete
    await new Promise(r => setTimeout(r, 10));

    // Check node was reset to pending (immediately after recovery, before executeLoop runs)
    const nodes = getWorkflowNodes(workflow.id);
    const recoveredNode = nodes.find(n => n.id === node.id);
    expect(recoveredNode).toBeDefined();
    // Node should be pending (with retry_reason) or already completed (if executeLoop ran fast)
    // Since this is idempotent, both are acceptable outcomes
    expect(["pending", "completed"]).toContain(recoveredNode!.status);
    if (recoveredNode!.status === "pending") {
      expect(recoveredNode!.retry_reason).toBe("crash_recovery");
    }
  });

  it("should fail running non-idempotent node", async () => {
    registerNodeExecutor(nonIdempotentExecutor);

    // Create a running workflow with a running non-idempotent node
    const workflow = createWorkflow({
      type: "test-workflow",
      parent_workflow_id: null,
      depth: 0,
      status: "running",
      current_node: null,
      input_json: "{}",
      output_json: null,
      error_json: null,
      manifest_id: null,
      trace_id: null,
      idempotency_key: null,
      timeout_ms: 60000,
      timeout_at: Date.now() + 60000,
      started_at: Date.now(),
      finished_at: null,
      updated_at: Date.now(),
    });

    const node = createWorkflowNode({
      workflow_id: workflow.id,
      node_id: "test-node",
      node_type: "test-non-idempotent",
      status: "running",
      input_json: "{}",
      output_json: null,
      error_json: null,
      depends_on: null,
      attempt: 0,
      retry_reason: null,
      started_at: Date.now(),
      finished_at: null,
      updated_at: Date.now(),
    });

    // Run recovery
    await recoverWorkflows();

    // Wait a bit for async operations
    await new Promise(r => setTimeout(r, 100));

    // Check node was failed
    const nodes = getWorkflowNodes(workflow.id);
    const recoveredNode = nodes.find(n => n.id === node.id);
    expect(recoveredNode).toBeDefined();
    expect(recoveredNode?.status).toBe("failed");
    expect(recoveredNode?.error_json).toContain("CRASH_RECOVERY");
  });

  it("should be a no-op when no stale workflows exist", async () => {
    // No workflows created
    const activeWorkflowsBefore = findActiveWorkflows();
    expect(activeWorkflowsBefore).toHaveLength(0);

    // Run recovery
    await recoverWorkflows();

    // Should complete without error
    const activeWorkflowsAfter = findActiveWorkflows();
    expect(activeWorkflowsAfter).toHaveLength(0);
  });
});

// =============================================================================
// Shutdown Tests
// =============================================================================

describe("Shutdown", () => {
  it("should checkpoint WAL without throwing on active DB", () => {
    // Create some data
    createWorkflow({
      type: "test-workflow",
      parent_workflow_id: null,
      depth: 0,
      status: "pending",
      current_node: null,
      input_json: "{}",
      output_json: null,
      error_json: null,
      manifest_id: null,
      trace_id: null,
      idempotency_key: null,
      timeout_ms: 60000,
      timeout_at: Date.now() + 60000,
      started_at: null,
      finished_at: null,
      updated_at: Date.now(),
    });

    // WAL checkpoint should not throw
    expect(() => walCheckpoint()).not.toThrow();
  });

  // DELETED: placeholder test (expect(true).toBe(true)) for terminateOrphanedVMs.
  // Real coverage should come from integration tests with provider mocking.
});

// =============================================================================
// Cancel API Tests (logic tests - HTTP tests would go in integration)
// =============================================================================

describe("Cancel Workflow Logic", () => {
  it("should handle workflow not found gracefully", async () => {
    const { cancelWorkflow } = await import("../../control/src/workflow/engine");

    // Try to cancel non-existent workflow
    const result = await cancelWorkflow(999999, "test");

    expect(result.success).toBe(false);
    expect(result.status).toBe("not_found");
  });

  it("should be idempotent for already-completed workflows", async () => {
    const { cancelWorkflow } = await import("../../control/src/workflow/engine");

    // Create a completed workflow
    const workflow = createWorkflow({
      type: "test-workflow",
      parent_workflow_id: null,
      depth: 0,
      status: "completed",
      current_node: null,
      input_json: "{}",
      output_json: "{}",
      error_json: null,
      manifest_id: null,
      trace_id: null,
      idempotency_key: null,
      timeout_ms: 60000,
      timeout_at: Date.now() + 60000,
      started_at: Date.now() - 5000,
      finished_at: Date.now(),
      updated_at: Date.now(),
    });

    // Try to cancel - should succeed (idempotent)
    const result = await cancelWorkflow(workflow.id, "test");

    expect(result.success).toBe(true);
    expect(result.status).toBe("completed");

    // Workflow should still be completed
    const updated = getWorkflow(workflow.id);
    expect(updated?.status).toBe("completed");
  });

  it("should cancel running workflow successfully", async () => {
    const { cancelWorkflow } = await import("../../control/src/workflow/engine");

    // Create a running workflow
    const workflow = createWorkflow({
      type: "test-workflow",
      parent_workflow_id: null,
      depth: 0,
      status: "running",
      current_node: null,
      input_json: "{}",
      output_json: null,
      error_json: null,
      manifest_id: null,
      trace_id: null,
      idempotency_key: null,
      timeout_ms: 60000,
      timeout_at: Date.now() + 60000,
      started_at: Date.now(),
      finished_at: null,
      updated_at: Date.now(),
    });

    // Cancel the workflow
    const result = await cancelWorkflow(workflow.id, "user_requested");

    expect(result.success).toBe(true);
    expect(result.status).toBe("cancelled");

    // Workflow should be cancelled
    const updated = getWorkflow(workflow.id);
    expect(updated?.status).toBe("cancelled");
    expect(updated?.error_json).toContain("CANCELLED");
  });
});
