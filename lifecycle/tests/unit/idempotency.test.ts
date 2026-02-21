// tests/unit/idempotency.test.ts - Idempotency Tests (EX2, EX7, EX8)

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest } from "../harness";
import { getDatabase } from "../../control/src/material/db";
import {
  cancelWorkflow,
} from "../../control/src/workflow/engine";
import {
  getWorkflow,
  getManifest,
  getAllocation,
  getRun,
  createManifest,
  createAllocation,
  createWorkflow,
  createWorkflowNode,
  updateWorkflow,
} from "../../control/src/material/db";
import { sealManifest } from "../../control/src/workflow/state-transitions";
import type { LaunchRunWorkflowInput } from "../../control/src/intent/launch-run.schema";

let cleanup: () => Promise<void>;

beforeEach(() => {
  cleanup = setupTest();
});

afterEach(() => cleanup());

// =============================================================================
// EX2: Double-seal Tests
// =============================================================================

describe("EX2: Double-seal idempotency", () => {
  test("sealManifest on already-sealed manifest should be idempotent", async () => {
    // Create a workflow and manifest
    const workflow = createWorkflow({
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
      timeout_at: null,
      started_at: null,
      finished_at: null,
      updated_at: Date.now(),
    });
    const manifest = createManifest(workflow.id);
    const manifestId = manifest.id;
    const manifest1 = getManifest(manifestId);
    expect(manifest1?.status).toBe("DRAFT");

    // Seal it once
    const expiresAt = Date.now() + 1000000;
    const result1 = sealManifest(manifestId, expiresAt);
    expect(result1.success).toBe(true);

    const manifest2 = getManifest(manifestId);
    expect(manifest2?.status).toBe("SEALED");

    // Try to seal it again — should fail with WRONG_STATE but not crash
    const result2 = sealManifest(manifestId, expiresAt);
    expect(result2.success).toBe(false);
    if (!result2.success) {
      expect(result2.reason).toBe("WRONG_STATE");
    }

    // Manifest should still be sealed
    const manifest3 = getManifest(manifestId);
    expect(manifest3?.status).toBe("SEALED");
  });

  // DELETED: "sealManifestSafe handles already-sealed manifest without error"
  // Self-described as "implicitly covered by the first test above" with no additional assertions.
});

// =============================================================================
// EX7: Double-cancel Tests
// =============================================================================

describe("EX7: Double-cancel idempotency", () => {
  test("cancel a running workflow should succeed", async () => {
    // Create a running workflow
    const workflow = createWorkflow({
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
      timeout_at: null,
      started_at: null,
      finished_at: null,
      updated_at: Date.now(),
    });
    const workflowId = workflow.id;
    const manifest = createManifest(workflowId);
    const manifestId = manifest.id;

    // Transition to running
    updateWorkflow(workflowId, { status: "running", started_at: Date.now() });

    const result = await cancelWorkflow(workflowId, "test cancellation");
    expect(result.success).toBe(true);
    expect(result.status).toBe("cancelled");

    const updatedWorkflow = getWorkflow(workflowId);
    expect(updatedWorkflow?.status).toBe("cancelled");
  });

  test("cancel an already-cancelled workflow should be idempotent", async () => {
    // Create a cancelled workflow
    const workflow = createWorkflow({
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
      timeout_at: null,
      started_at: null,
      finished_at: null,
      updated_at: Date.now(),
    });
    const workflowId = workflow.id;
    const manifest = createManifest(workflowId);
    const manifestId = manifest.id;

    updateWorkflow(workflowId, {
      status: "cancelled",
      started_at: Date.now(),
      finished_at: Date.now(),
    });

    const result = await cancelWorkflow(workflowId, "test cancellation");
    expect(result.success).toBe(true);
    expect(result.status).toBe("cancelled");

    const updatedWorkflow = getWorkflow(workflowId);
    expect(updatedWorkflow?.status).toBe("cancelled");
  });

  test("cancel a completed workflow should be idempotent (returns completed status)", async () => {
    // Create a completed workflow
    const workflow = createWorkflow({
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
      timeout_at: null,
      started_at: null,
      finished_at: null,
      updated_at: Date.now(),
    });
    const workflowId = workflow.id;
    const manifest = createManifest(workflowId);
    const manifestId = manifest.id;

    updateWorkflow(workflowId, {
      status: "completed",
      started_at: Date.now(),
      finished_at: Date.now(),
    });

    const result = await cancelWorkflow(workflowId, "test cancellation");
    expect(result.success).toBe(true);
    expect(result.status).toBe("completed");

    const updatedWorkflow = getWorkflow(workflowId);
    expect(updatedWorkflow?.status).toBe("completed");
  });

  test("cancel a non-existent workflow should return not_found", async () => {
    const result = await cancelWorkflow(99999, "test cancellation");
    expect(result.success).toBe(false);
    expect(result.status).toBe("not_found");
  });
});

// =============================================================================
// EX8: Finalize-already-complete Tests
// =============================================================================

describe("EX8: Finalize-already-complete idempotency", () => {
  function setupFinalizeTest(allocationStatus: "ACTIVE" | "COMPLETE") {
    const db = getDatabase();
    const ts = Date.now();

    const workflow = createWorkflow({
      type: "launch-run",
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
      timeout_at: null,
      started_at: ts,
      finished_at: null,
      updated_at: ts,
    });
    const manifest = createManifest(workflow.id);
    updateWorkflow(workflow.id, { manifest_id: manifest.id });

    // Create instance
    db.run(
      `INSERT INTO instances (provider, provider_id, spec, region, workflow_state, created_at, last_heartbeat)
       VALUES (?, ?, ?, ?, ?, ?, ?)`,
      ["local", `test-inst-${ts}`, "small", "local", "ready", ts, ts]
    );
    const instanceId = Number((db.query("SELECT last_insert_rowid() as id").get() as { id: number }).id);

    // Create run
    db.run(
      `INSERT INTO runs (command, workdir, max_duration_ms, workflow_state, created_at)
       VALUES (?, ?, ?, ?, ?)`,
      ["echo hi", "/workspace", 60000, "launch-run:running", ts]
    );
    const runId = Number((db.query("SELECT last_insert_rowid() as id").get() as { id: number }).id);

    // Create allocation
    const alloc = createAllocation({
      run_id: runId,
      instance_id: instanceId,
      status: "AVAILABLE",
      current_manifest_id: manifest.id,
      user: "ubuntu",
      workdir: "/workspace",
      debug_hold_until: null,
      completed_at: null,
    });
    // Transition to target status
    if (allocationStatus === "ACTIVE") {
      db.run(
        "UPDATE allocations SET status = 'ACTIVE', updated_at = ? WHERE id = ?",
        [ts, alloc.id]
      );
    } else {
      db.run(
        "UPDATE allocations SET status = 'COMPLETE', completed_at = ?, updated_at = ? WHERE id = ?",
        [ts, ts, alloc.id]
      );
    }

    // Create predecessor nodes with outputs
    const allocNode = createWorkflowNode({
      workflow_id: workflow.id, node_id: "create-allocation", node_type: "create-allocation",
      status: "completed", input_json: "{}", output_json: JSON.stringify({ instanceId, allocationId: alloc.id, workdir: "/workspace" }),
      error_json: null, depends_on: null, attempt: 0, retry_reason: null,
      started_at: ts, finished_at: ts, updated_at: ts,
    });
    const waitNode = createWorkflowNode({
      workflow_id: workflow.id, node_id: "await-completion", node_type: "await-completion",
      status: "completed", input_json: "{}", output_json: JSON.stringify({ exitCode: 0, spotInterrupted: false }),
      error_json: null, depends_on: "create-allocation", attempt: 0, retry_reason: null,
      started_at: ts, finished_at: ts, updated_at: ts,
    });

    const ctx = {
      workflowId: workflow.id,
      nodeId: "finalize",
      manifestId: manifest.id,
      workflowInput: { runId, command: "echo hi", spec: "small", provider: "local" },
      getNodeOutput: (nodeId: string) => {
        if (nodeId === "create-allocation") return { instanceId, allocationId: alloc.id, workdir: "/workspace" };
        if (nodeId === "await-completion") return { exitCode: 0, spotInterrupted: false };
        return null;
      },
      log: () => {},
    };

    return { workflowId: workflow.id, manifestId: manifest.id, instanceId, runId, allocationId: alloc.id, ctx };
  }

  test("finalize with ACTIVE allocation should succeed", async () => {
    const { allocationId, runId, ctx } = setupFinalizeTest("ACTIVE");
    const { finalizeExecutor } = await import("../../control/src/workflow/nodes/finalize");

    const output = await finalizeExecutor.execute(ctx as any);

    // Verify output shape
    expect(output.allocationStatus).toBe("COMPLETE");
    expect(output.runStatus).toBe("finalized");

    // Verify exitCode (0) and spotInterrupted (false) from await-completion were
    // consumed correctly: spotInterrupted=false allows replenishment logic to run,
    // and the run is moved to finalized state
    const updatedRun = getRun(runId);
    expect(updatedRun).not.toBeNull();
    expect(updatedRun!.workflow_state).toBe("launch-run:finalized");

    // Allocation is now COMPLETE in DB
    expect(getAllocation(allocationId)?.status).toBe("COMPLETE");
  });

  test("finalize with already-COMPLETE allocation should be idempotent", async () => {
    const { allocationId, runId, ctx } = setupFinalizeTest("COMPLETE");
    const { finalizeExecutor } = await import("../../control/src/workflow/nodes/finalize");

    const output = await finalizeExecutor.execute(ctx as any);

    // Idempotent: same result whether ACTIVE→COMPLETE or already COMPLETE
    expect(output.allocationStatus).toBe("COMPLETE");
    expect(output.runStatus).toBe("finalized");

    // exitCode=0 from await-completion output means spotInterrupted=false,
    // so the run transitions to finalized state on retry too
    const updatedRun = getRun(runId);
    expect(updatedRun).not.toBeNull();
    expect(updatedRun!.workflow_state).toBe("launch-run:finalized");

    // Allocation remains COMPLETE (no regression to previous status)
    expect(getAllocation(allocationId)?.status).toBe("COMPLETE");
  });
});
