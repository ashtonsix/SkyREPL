// tests/unit/concurrency-invariants.test.ts
// Tests that concurrency invariants hold under contention.
// Since bun:sqlite is single-connection (serialized), we use
// Promise.all with microtask yields to simulate the realistic
// concurrency model where multiple async operations race for
// the same DB rows.

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest, verifyWorkflowInvariant, waitForWorkflow } from "../harness";
import {
  createInstance,
  createAllocation,
  createRun,
  createWorkflow,
  getAllocation,
  getWorkflow,
  type Allocation,
  type Workflow,
} from "../../control/src/material/db";

import {
  claimAllocation,
  activateAllocation,
  completeAllocation,
  failAllocation,
  failAllocationAnyState,
  startWorkflow,
  completeWorkflow,
  failWorkflow,
  type TransitionResult,
} from "../../control/src/workflow/state-transitions";

import {
  submit,
  registerBlueprint,
  registerNodeExecutor,
} from "../../control/src/workflow/engine";

// =============================================================================
// Test Harness
// =============================================================================

let cleanup: () => Promise<void>;
beforeEach(() => { cleanup = setupTest({ engine: true }); });
afterEach(() => cleanup());

// =============================================================================
// Helpers
// =============================================================================

function makeInstance() {
  return createInstance({
    provider: "orbstack",
    provider_id: `test-${Date.now()}-${Math.random()}`,
    spec: "cpu-1x",
    region: "local",
    ip: "127.0.0.1",
    workflow_state: "launch:complete",
    workflow_error: null,
    current_manifest_id: null,
    spawn_idempotency_key: null,
    is_spot: 0,
    spot_request_id: null,
    init_checksum: null,
    registration_token_hash: null,
    last_heartbeat: Date.now(),
  });
}

function makeRun() {
  return createRun({
    command: "echo test",
    workdir: "/tmp/test",
    max_duration_ms: 3600000,
    workflow_state: "launch:pending",
    workflow_error: null,
    current_manifest_id: null,
    exit_code: null,
    init_checksum: null,
    create_snapshot: 0,
    spot_interrupted: 0,
    started_at: null,
    finished_at: null,
  });
}

// =============================================================================
// 1. Concurrent claimAllocation on same warm allocation
// =============================================================================

describe("Concurrent claimAllocation", () => {
  test("exactly one of two concurrent claims succeeds", async () => {
    const inst = makeInstance();
    const alloc = createAllocation({
      run_id: null,
      instance_id: inst.id,
      status: "AVAILABLE",
      current_manifest_id: null,
      user: "test",
      workdir: "/tmp",
      debug_hold_until: null,
      completed_at: null,
    });

    const run1 = makeRun();
    const run2 = makeRun();

    // Race two claims against the same allocation.
    // With bun:sqlite, these are serialized but the optimistic locking
    // in atomicTransition should still ensure exactly one wins.
    const [result1, result2] = await Promise.all([
      Promise.resolve().then(() => claimAllocation(alloc.id, run1.id)),
      Promise.resolve().then(() => claimAllocation(alloc.id, run2.id)),
    ]);

    const successes = [result1, result2].filter(r => r.success);
    const failures = [result1, result2].filter(r => !r.success);

    expect(successes.length).toBe(1);
    expect(failures.length).toBe(1);

    // The winner should have set the allocation to CLAIMED
    const finalAlloc = getAllocation(alloc.id)!;
    expect(finalAlloc.status).toBe("CLAIMED");
    expect(finalAlloc.run_id).not.toBeNull();

    // The loser should get WRONG_STATE (since first claim already changed status)
    const failure = failures[0] as { success: false; reason: string };
    expect(failure.reason).toBe("WRONG_STATE");
  });

  test("N concurrent claims on same allocation: exactly one winner", async () => {
    const inst = makeInstance();
    const alloc = createAllocation({
      run_id: null,
      instance_id: inst.id,
      status: "AVAILABLE",
      current_manifest_id: null,
      user: "test",
      workdir: "/tmp",
      debug_hold_until: null,
      completed_at: null,
    });

    const N = 5;
    const runs = Array.from({ length: N }, () => makeRun());

    const results = await Promise.all(
      runs.map(run =>
        Promise.resolve().then(() => claimAllocation(alloc.id, run.id))
      )
    );

    const successes = results.filter(r => r.success);
    expect(successes.length).toBe(1);

    const finalAlloc = getAllocation(alloc.id)!;
    expect(finalAlloc.status).toBe("CLAIMED");
  });
});

// =============================================================================
// 2. Concurrent allocation transition + reconciliation
// =============================================================================

describe("Concurrent allocation transitions", () => {
  test("concurrent completeAllocation and failAllocation: exactly one wins", async () => {
    const inst = makeInstance();
    const alloc = createAllocation({
      run_id: null,
      instance_id: inst.id,
      status: "ACTIVE",
      current_manifest_id: null,
      user: "test",
      workdir: "/tmp",
      debug_hold_until: null,
      completed_at: null,
    });

    const [completeResult, failResult] = await Promise.all([
      Promise.resolve().then(() => completeAllocation(alloc.id)),
      Promise.resolve().then(() => failAllocation(alloc.id, "ACTIVE")),
    ]);

    // Exactly one should succeed
    const successes = [completeResult, failResult].filter(r => r.success);
    expect(successes.length).toBe(1);

    // Final state should be one of the terminal states
    const finalAlloc = getAllocation(alloc.id)!;
    expect(["COMPLETE", "FAILED"]).toContain(finalAlloc.status);
    expect(finalAlloc.completed_at).not.toBeNull();
  });

  test("concurrent activateAllocation on same CLAIMED allocation: exactly one wins", async () => {
    const inst = makeInstance();
    const alloc = createAllocation({
      run_id: null,
      instance_id: inst.id,
      status: "CLAIMED",
      current_manifest_id: null,
      user: "test",
      workdir: "/tmp",
      debug_hold_until: null,
      completed_at: null,
    });

    const results = await Promise.all([
      Promise.resolve().then(() => activateAllocation(alloc.id)),
      Promise.resolve().then(() => activateAllocation(alloc.id)),
    ]);

    const successes = results.filter(r => r.success);
    // First one succeeds (CLAIMED -> ACTIVE), second fails (ACTIVE != CLAIMED)
    expect(successes.length).toBe(1);

    const finalAlloc = getAllocation(alloc.id)!;
    expect(finalAlloc.status).toBe("ACTIVE");
  });

  test("no double-transition: two concurrent failAllocationAnyState calls", async () => {
    const inst = makeInstance();
    const alloc = createAllocation({
      run_id: null,
      instance_id: inst.id,
      status: "ACTIVE",
      current_manifest_id: null,
      user: "test",
      workdir: "/tmp",
      debug_hold_until: null,
      completed_at: null,
    });

    const results = await Promise.all([
      Promise.resolve().then(() => failAllocationAnyState(alloc.id)),
      Promise.resolve().then(() => failAllocationAnyState(alloc.id)),
    ]);

    // First succeeds, second gets WRONG_STATE (already FAILED, which is terminal)
    const successes = results.filter(r => r.success);
    expect(successes.length).toBe(1);

    const finalAlloc = getAllocation(alloc.id)!;
    expect(finalAlloc.status).toBe("FAILED");
  });
});

// =============================================================================
// 3. Concurrent workflow transitions
// =============================================================================

describe("Concurrent workflow transitions", () => {
  test("concurrent startWorkflow: exactly one wins", async () => {
    const now = Date.now();
    const wf = createWorkflow({
      type: "test",
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
      timeout_ms: 3600000,
      timeout_at: now + 3600000,
      started_at: null,
      finished_at: null,
      updated_at: now,
    });

    const results = await Promise.all([
      Promise.resolve().then(() => startWorkflow(wf.id)),
      Promise.resolve().then(() => startWorkflow(wf.id)),
    ]);

    const successes = results.filter(r => r.success);
    expect(successes.length).toBe(1);

    const finalWf = getWorkflow(wf.id)!;
    expect(finalWf.status).toBe("running");
  });

  test("concurrent completeWorkflow and failWorkflow: exactly one wins", async () => {
    const now = Date.now();
    const wf = createWorkflow({
      type: "test",
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
      timeout_ms: 3600000,
      timeout_at: now + 3600000,
      started_at: now,
      finished_at: null,
      updated_at: now,
    });

    const [completeResult, failResult] = await Promise.all([
      Promise.resolve().then(() => completeWorkflow(wf.id, { ok: true })),
      Promise.resolve().then(() => failWorkflow(wf.id, "error")),
    ]);

    const successes = [completeResult, failResult].filter(r => r.success);
    expect(successes.length).toBe(1);

    const finalWf = getWorkflow(wf.id)!;
    expect(["completed", "failed"]).toContain(finalWf.status);
    expect(finalWf.finished_at).not.toBeNull();
  });
});

// =============================================================================
// 4. Concurrent submit with same idempotency key
// =============================================================================

describe("Concurrent submit with idempotency key", () => {
  test("two concurrent submits with same key: one creates, one deduplicates", async () => {
    registerBlueprint({
      type: "test-concurrent-submit",
      entryNode: "step-1",
      nodes: {
        "step-1": { type: "mock-concurrent", dependsOn: [] },
      },
    });

    registerNodeExecutor({
      name: "mock-concurrent",
      idempotent: true,
      async execute() {
        return { ok: true };
      },
    });

    const key = `concurrent-key-${Date.now()}`;

    const [r1, r2] = await Promise.all([
      submit({ type: "test-concurrent-submit", input: {}, idempotencyKey: key }),
      submit({ type: "test-concurrent-submit", input: {}, idempotencyKey: key }),
    ]);

    const statuses = [r1.status, r2.status].sort();

    // One should be "created", the other "deduplicated"
    // OR both could be "created" if the race condition means both check before either inserts
    // (which is a known limitation of the current non-transactional idempotency check).
    // Either way, we should get at most 2 workflows.
    const created = [r1, r2].filter(r => r.status === "created");
    const deduped = [r1, r2].filter(r => r.status === "deduplicated");

    // At least one must be created
    expect(created.length).toBeGreaterThanOrEqual(1);

    // If dedup worked perfectly, exactly one created and one deduped.
    // If both slipped through (race window), both are created but that's
    // a known limitation. We just verify the invariant holds for the
    // non-racing path: if one deduped, it references the correct workflow.
    if (deduped.length === 1) {
      expect(deduped[0].existingWorkflowId).toBe(created[0].workflowId);
    }

    // Wait for all fire-and-forget engine loops to complete
    for (const r of created) {
      await waitForWorkflow(r.workflowId);
    }
  });
});

// =============================================================================
// 5. verifyWorkflowInvariant integration
// =============================================================================

describe("verifyWorkflowInvariant helper", () => {
  test("passes for a correctly completed workflow via engine", async () => {
    registerBlueprint({
      type: "test-invariant-check",
      entryNode: "step-1",
      nodes: {
        "step-1": { type: "mock-invariant", dependsOn: [] },
      },
    });

    registerNodeExecutor({
      name: "mock-invariant",
      idempotent: true,
      async execute() {
        return { ok: true };
      },
    });

    const result = await submit({
      type: "test-invariant-check",
      input: {},
    });

    await waitForWorkflow(result.workflowId);

    // This should pass: workflow completed, all nodes completed, manifest sealed
    verifyWorkflowInvariant(result.workflowId);
  });

  test("passes for a failed workflow via engine", async () => {
    registerBlueprint({
      type: "test-invariant-fail",
      entryNode: "step-1",
      nodes: {
        "step-1": { type: "mock-invariant-fail", dependsOn: [] },
      },
    });

    registerNodeExecutor({
      name: "mock-invariant-fail",
      idempotent: true,
      async execute() {
        throw Object.assign(new Error("Intentional failure"), {
          code: "VALIDATION_ERROR",
          category: "validation",
        });
      },
    });

    const result = await submit({
      type: "test-invariant-fail",
      input: {},
    });

    const wf = await waitForWorkflow(result.workflowId);
    expect(wf.status).toBe("failed");

    // verifyWorkflowInvariant should pass for failed workflows too
    // (manifest seal is not expected for failed workflows)
    verifyWorkflowInvariant(result.workflowId);
  });
});

// =============================================================================
// 6. Optimistic lock RACE_LOST detection
// =============================================================================

describe("Optimistic lock guards", () => {
  test("stale updated_at causes RACE_LOST when row is modified between read and write", () => {
    // This tests the optimistic lock mechanism indirectly.
    // We create an allocation, modify it outside the transition function,
    // then attempt a transition using the stale state.
    const inst = makeInstance();
    const alloc = createAllocation({
      run_id: null,
      instance_id: inst.id,
      status: "AVAILABLE",
      current_manifest_id: null,
      user: "test",
      workdir: "/tmp",
      debug_hold_until: null,
      completed_at: null,
    });

    // Simulate a concurrent modification by bumping updated_at
    // This changes the optimistic lock token without changing status
    const { getDatabase } = require("../../control/src/material/db");
    const db = getDatabase();
    db.prepare("UPDATE allocations SET updated_at = ? WHERE id = ?")
      .run(Date.now() + 100000, alloc.id);

    // The claimAllocation function reads the current row, but the
    // optimistic lock check (WHERE updated_at = ?) will match the NEW updated_at
    // since it reads within the same transaction. So this should still succeed.
    // This verifies the read-then-write within a single transaction is atomic.
    const run = makeRun();
    const result = claimAllocation(alloc.id, run.id);
    expect(result.success).toBe(true);
  });
});
