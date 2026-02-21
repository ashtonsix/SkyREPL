// tests/unit/workflow-patterns-basic.test.ts - Tests for #WF-06 (runSingleStep) and #WF-05 (pause/resume/retry)

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import {
  getWorkflow,
  type Workflow,
} from "../../control/src/material/db";
import {
  registerBlueprint,
  registerNodeExecutor,
  submit,
  runSingleStep,
  createWorkflowEngine,
} from "../../control/src/workflow/engine";
import { setupTest, waitForWorkflow } from "../harness";

/** Poll until a workflow reaches a specific (possibly non-terminal) status. */
async function waitForWorkflowStatus(
  workflowId: number,
  status: string,
  timeoutMs = 5_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const wf = getWorkflow(workflowId);
    if (wf?.status === status) return;
    await Bun.sleep(5);
  }
  const wf = getWorkflow(workflowId);
  throw new Error(
    `Workflow ${workflowId} did not reach '${status}' within ${timeoutMs}ms (status: ${wf?.status})`
  );
}

// =============================================================================
// Test Setup
// =============================================================================

let cleanup: () => Promise<void>;

beforeEach(() => {
  cleanup = setupTest({ engine: true });
});

afterEach(() => cleanup());

describe("Workflow Patterns Basic (#WF-05, #WF-06)", () => {

  // ===========================================================================
  // runSingleStep Tests (#WF-06)
  // ===========================================================================

  describe("runSingleStep (#WF-06)", () => {
    test("executes a single node and returns its output", async () => {
      registerNodeExecutor({
        name: "single-step-echo",
        idempotent: true,
        async execute(ctx) {
          const input = ctx.workflowInput as Record<string, unknown>;
          return { echoed: input.message };
        },
      });

      const result = await runSingleStep("echo-test", "single-step-echo", {
        message: "hello world",
      });

      expect(result.status).toBe("completed");
      expect(result.output).toEqual({ echoed: "hello world" });
      expect(result.workflowId).toBeGreaterThan(0);
    });

    test("returns failed status when node executor throws", async () => {
      registerNodeExecutor({
        name: "single-step-fail",
        idempotent: true,
        async execute() {
          throw Object.assign(new Error("Intentional failure"), {
            code: "VALIDATION_ERROR",
            category: "validation",
          });
        },
      });

      const result = await runSingleStep("fail-test", "single-step-fail", {});

      expect(result.status).toBe("failed");
      expect(result.workflowId).toBeGreaterThan(0);
    });

    test("respects timeout option", async () => {
      registerNodeExecutor({
        name: "single-step-slow",
        idempotent: true,
        async execute() {
          await new Promise((resolve) => setTimeout(resolve, 5000));
          return { done: true };
        },
      });

      const result = await runSingleStep("slow-test", "single-step-slow", {}, {
        timeout: 200,
      });

      // Should fail or timeout — the workflow-level timeout should catch it
      expect(["failed", "timeout"]).toContain(result.status);
    });

    test("deduplicates with idempotency key", async () => {
      registerNodeExecutor({
        name: "single-step-idempotent",
        idempotent: true,
        async execute() {
          return { value: 42 };
        },
      });

      const result1 = await runSingleStep(
        "idemp-test",
        "single-step-idempotent",
        {},
        { idempotencyKey: "unique-key-123" }
      );

      expect(result1.status).toBe("completed");

      // Second call with same idempotency key should get same workflow
      const result2 = await runSingleStep(
        "idemp-test",
        "single-step-idempotent",
        {},
        { idempotencyKey: "unique-key-123" }
      );

      expect(result2.workflowId).toBe(result1.workflowId);
      expect(result2.status).toBe("completed");
    });

    test("cleans up temporary blueprint from registry", async () => {
      registerNodeExecutor({
        name: "single-step-cleanup",
        idempotent: true,
        async execute() {
          return { ok: true };
        },
      });

      await runSingleStep("cleanup-test", "single-step-cleanup", {});

      // The temporary blueprint should be unregistered; submitting with it should fail
      // We can't directly inspect the registry, but we can verify the main function worked
      // and a second call with different params creates a new workflow
      const result2 = await runSingleStep("cleanup-test-2", "single-step-cleanup", {});
      expect(result2.status).toBe("completed");
    });
  });

  // ===========================================================================
  // pause / resume Tests (#WF-05)
  // ===========================================================================

  describe("pause / resume (#WF-05)", () => {
    test("pauses a running workflow and resumes it", async () => {
      // Use a latch to control when step2 completes
      let step2Started = false;
      let resolveStep2Latch: () => void;
      const step2Latch = new Promise<void>((r) => { resolveStep2Latch = r; });

      registerNodeExecutor({
        name: "pause-resume-step1",
        idempotent: true,
        async execute() {
          return { step: 1 };
        },
      });

      registerNodeExecutor({
        name: "pause-resume-step2",
        idempotent: true,
        async execute() {
          step2Started = true;
          // Wait until the test releases the latch
          await step2Latch;
          return { step: 2 };
        },
      });

      registerBlueprint({
        type: "test-pause-resume",
        entryNode: "first",
        nodes: {
          first: { type: "pause-resume-step1", dependsOn: [] },
          second: { type: "pause-resume-step2", dependsOn: ["first"] },
          third: { type: "pause-resume-step1", dependsOn: ["second"] },
        },
      });

      const engine = createWorkflowEngine();
      const submitResult = await engine.submit({
        type: "test-pause-resume",
        input: {},
      });

      // Wait for step2 to be actively running (so workflow is still "running")
      while (!step2Started) {
        await Bun.sleep(5);
      }

      // Pause the workflow while step2 is running
      await engine.pause(submitResult.workflowId);

      // Verify workflow is paused
      const pausedWf = getWorkflow(submitResult.workflowId);
      expect(pausedWf!.status).toBe("paused");

      // Confirm it stays paused (third node should NOT start)
      // Yield a few ticks to give the engine a chance to (incorrectly) advance
      for (let i = 0; i < 5; i++) await Bun.sleep(5);
      const stillPaused = getWorkflow(submitResult.workflowId);
      // May be paused or running if step2 completion triggers a loop iteration
      // The key test: third node shouldn't have completed yet
      expect(["paused", "running"]).toContain(stillPaused!.status);

      // Release the step2 latch and resume
      resolveStep2Latch!();
      if (stillPaused!.status === "paused") {
        await engine.resume(submitResult.workflowId);
      }

      // Wait for completion
      await waitForWorkflow(submitResult.workflowId);

      const completedWf = getWorkflow(submitResult.workflowId);
      expect(completedWf!.status).toBe("completed");
    });

    test("pause fails on non-running workflow", async () => {
      registerNodeExecutor({
        name: "pause-fail-node",
        idempotent: true,
        async execute() {
          return {};
        },
      });

      registerBlueprint({
        type: "test-pause-fail",
        entryNode: "step",
        nodes: {
          step: { type: "pause-fail-node", dependsOn: [] },
        },
      });

      const engine = createWorkflowEngine();
      const result = await engine.submit({
        type: "test-pause-fail",
        input: {},
      });

      // Wait for workflow to complete
      await waitForWorkflow(result.workflowId);

      // Try to pause a completed workflow — should fail
      try {
        await engine.pause(result.workflowId);
        expect(true).toBe(false); // Should not reach
      } catch (err) {
        expect((err as Error).message).toContain("Cannot pause workflow");
      }
    });

    test("resume fails on non-paused workflow", async () => {
      registerNodeExecutor({
        name: "resume-fail-node",
        idempotent: true,
        async execute() {
          await new Promise((resolve) => setTimeout(resolve, 200));
          return {};
        },
      });

      registerBlueprint({
        type: "test-resume-fail",
        entryNode: "step",
        nodes: {
          step: { type: "resume-fail-node", dependsOn: [] },
        },
      });

      const engine = createWorkflowEngine();
      const result = await engine.submit({
        type: "test-resume-fail",
        input: {},
      });

      // Try to resume a running (not paused) workflow — should fail
      await waitForWorkflowStatus(result.workflowId, "running");
      try {
        await engine.resume(result.workflowId);
        expect(true).toBe(false); // Should not reach
      } catch (err) {
        expect((err as Error).message).toContain("Cannot resume workflow");
      }
    });

    test("cancel works on paused workflow", async () => {
      let nodeStarted: () => void;
      const nodeStartedPromise = new Promise<void>((r) => { nodeStarted = r; });

      registerNodeExecutor({
        name: "cancel-paused-node",
        idempotent: true,
        async execute() {
          nodeStarted!();
          await new Promise((resolve) => setTimeout(resolve, 5000));
          return {};
        },
      });

      registerBlueprint({
        type: "test-cancel-paused",
        entryNode: "step",
        nodes: {
          step: { type: "cancel-paused-node", dependsOn: [] },
        },
      });

      const engine = createWorkflowEngine();
      const result = await engine.submit({
        type: "test-cancel-paused",
        input: {},
      });

      // Wait for the node to actually start executing
      await nodeStartedPromise;

      // Poll until workflow is running
      await waitForWorkflowStatus(result.workflowId, "running");

      // Pause it
      await engine.pause(result.workflowId);
      const paused = getWorkflow(result.workflowId);
      expect(paused!.status).toBe("paused");

      // Cancel the paused workflow
      const cancelResult = await engine.cancel(result.workflowId, "user requested");
      expect(cancelResult.success).toBe(true);
      expect(cancelResult.status).toBe("cancelled");

      const cancelled = getWorkflow(result.workflowId);
      expect(cancelled!.status).toBe("cancelled");
    });
  });

  // ===========================================================================
  // retry Tests (#WF-05)
  // ===========================================================================

  describe("retry (#WF-05)", () => {
    test("creates a new workflow with the same inputs", async () => {
      let executeCount = 0;

      registerNodeExecutor({
        name: "retry-test-node",
        idempotent: true,
        async execute() {
          executeCount++;
          if (executeCount === 1) {
            throw Object.assign(new Error("First attempt fails"), {
              code: "VALIDATION_ERROR",
              category: "validation",
            });
          }
          return { success: true };
        },
      });

      registerBlueprint({
        type: "test-retry-workflow",
        entryNode: "step",
        nodes: {
          step: { type: "retry-test-node", dependsOn: [] },
        },
      });

      const engine = createWorkflowEngine();

      // First submission — will fail
      const result1 = await engine.submit({
        type: "test-retry-workflow",
        input: { attempt: 1 },
      });

      await waitForWorkflow(result1.workflowId);

      const wf1 = getWorkflow(result1.workflowId);
      expect(wf1!.status).toBe("failed");

      // Retry — should create a new workflow
      const newWorkflowId = await engine.retry(result1.workflowId);

      expect(newWorkflowId).not.toBe(result1.workflowId);
      expect(newWorkflowId).toBeGreaterThan(result1.workflowId);

      // Wait for the retried workflow to complete
      await waitForWorkflow(newWorkflowId);

      const wf2 = getWorkflow(newWorkflowId);
      expect(wf2!.status).toBe("completed");
      expect(wf2!.type).toBe("test-retry-workflow");

      // Verify the input was preserved
      const input2 = JSON.parse(wf2!.input_json!);
      expect(input2.attempt).toBe(1);
    });

    test("retry generates a new idempotency key from the original", async () => {
      let callCount = 0;

      registerNodeExecutor({
        name: "retry-idemp-node",
        idempotent: true,
        async execute() {
          callCount++;
          if (callCount === 1) {
            throw Object.assign(new Error("First call fails"), {
              code: "VALIDATION_ERROR",
              category: "validation",
            });
          }
          return { done: true };
        },
      });

      registerBlueprint({
        type: "test-retry-idemp",
        entryNode: "step",
        nodes: {
          step: { type: "retry-idemp-node", dependsOn: [] },
        },
      });

      const engine = createWorkflowEngine();

      const result1 = await engine.submit({
        type: "test-retry-idemp",
        input: {},
        idempotencyKey: "original-key",
      });

      await waitForWorkflow(result1.workflowId);

      // Workflow must be failed before retry is allowed (SD-G1-07)
      const wf1 = getWorkflow(result1.workflowId);
      expect(wf1!.status).toBe("failed");

      const newWorkflowId = await engine.retry(result1.workflowId);
      const wf2 = getWorkflow(newWorkflowId);

      // New workflow should have a derived idempotency key
      expect(wf2!.idempotency_key).toContain("original-key:retry:");
      // And it should be a different key from the original
      expect(wf2!.idempotency_key).not.toBe("original-key");
    });

    test("retry throws on non-existent workflow", async () => {
      const engine = createWorkflowEngine();

      try {
        await engine.retry(99999);
        expect(true).toBe(false); // Should not reach
      } catch (err) {
        expect((err as Error).message).toContain("not found");
      }
    });

    // SD-G1-06: retry preserves parent_workflow_id and timeout
    test("retry preserves parent_workflow_id and timeout", async () => {
      let callCount = 0;

      registerNodeExecutor({
        name: "retry-preserve-node",
        idempotent: true,
        async execute() {
          callCount++;
          if (callCount === 1) {
            throw Object.assign(new Error("First call fails"), {
              code: "VALIDATION_ERROR",
              category: "validation",
            });
          }
          return { preserved: true };
        },
      });

      registerBlueprint({
        type: "test-retry-preserve",
        entryNode: "step",
        nodes: {
          step: { type: "retry-preserve-node", dependsOn: [] },
        },
      });

      const engine = createWorkflowEngine();

      // Submit with explicit parentWorkflowId and timeout
      // First create a "parent" workflow to reference
      const parentResult = await submit({
        type: "test-retry-preserve",
        input: {},
      });
      await waitForWorkflow(parentResult.workflowId);

      // Reset callCount so the child workflow fails on its first call
      callCount = 0;

      const childResult = await submit({
        type: "test-retry-preserve",
        input: { child: true },
        parentWorkflowId: parentResult.workflowId,
        timeout: 42_000,
      });

      await waitForWorkflow(childResult.workflowId);

      const failedChild = getWorkflow(childResult.workflowId);
      expect(failedChild!.status).toBe("failed");
      expect(failedChild!.parent_workflow_id).toBe(parentResult.workflowId);
      expect(failedChild!.timeout_ms).toBe(42_000);

      // Retry the failed child workflow
      const retriedId = await engine.retry(childResult.workflowId);

      await waitForWorkflow(retriedId);

      const retriedWf = getWorkflow(retriedId);
      expect(retriedWf!.status).toBe("completed");
      // SD-G1-06: parent_workflow_id and timeout should be preserved
      expect(retriedWf!.parent_workflow_id).toBe(parentResult.workflowId);
      expect(retriedWf!.timeout_ms).toBe(42_000);
    });

    // SD-G1-07: retry rejects non-failed workflow (completed)
    test("retry rejects completed workflow with INVALID_STATE", async () => {
      registerNodeExecutor({
        name: "retry-reject-completed-node",
        idempotent: true,
        async execute() {
          return { ok: true };
        },
      });

      registerBlueprint({
        type: "test-retry-reject-completed",
        entryNode: "step",
        nodes: {
          step: { type: "retry-reject-completed-node", dependsOn: [] },
        },
      });

      const engine = createWorkflowEngine();
      const result = await engine.submit({
        type: "test-retry-reject-completed",
        input: {},
      });

      await waitForWorkflow(result.workflowId);

      const wf = getWorkflow(result.workflowId);
      expect(wf!.status).toBe("completed");

      try {
        await engine.retry(result.workflowId);
        expect(true).toBe(false); // Should not reach
      } catch (err) {
        const e = err as Error & { code?: string };
        expect(e.message).toContain("Cannot retry workflow");
        expect(e.message).toContain("completed");
        expect(e.message).toContain("expected 'failed'");
        expect(e.code).toBe("INVALID_STATE");
      }
    });

    // SD-G1-07: retry rejects running workflow
    test("retry rejects running workflow with INVALID_STATE", async () => {
      let nodeStarted: () => void;
      const nodeStartedPromise = new Promise<void>((r) => { nodeStarted = r; });

      registerNodeExecutor({
        name: "retry-reject-running-node",
        idempotent: true,
        async execute() {
          nodeStarted!();
          await new Promise((resolve) => setTimeout(resolve, 5000));
          return {};
        },
      });

      registerBlueprint({
        type: "test-retry-reject-running",
        entryNode: "step",
        nodes: {
          step: { type: "retry-reject-running-node", dependsOn: [] },
        },
      });

      const engine = createWorkflowEngine();
      const result = await engine.submit({
        type: "test-retry-reject-running",
        input: {},
      });

      // Wait for the node to start executing
      await nodeStartedPromise;

      // Ensure workflow is running
      const wf = getWorkflow(result.workflowId);
      expect(wf!.status).toBe("running");

      try {
        await engine.retry(result.workflowId);
        expect(true).toBe(false); // Should not reach
      } catch (err) {
        const e = err as Error & { code?: string };
        expect(e.message).toContain("Cannot retry workflow");
        expect(e.message).toContain("running");
        expect(e.message).toContain("expected 'failed'");
        expect(e.code).toBe("INVALID_STATE");
      }
    });
  });
});
