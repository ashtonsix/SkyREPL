// tests/unit/state-machine-exhaustiveness.test.ts
// Full N*N transition matrix tests for ALL state machines.
// Each matrix is a single test that verifies every (from, to) pair.
// Adding a state without updating the truth table will cause failures.

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest } from "../harness";
import {
  createInstance,
  createAllocation,
  createRun,
  createWorkflow,
  createWorkflowNode,
  createManifest,
  type Allocation,
  type Workflow,
  type WorkflowNode,
  type Manifest,
} from "../../control/src/material/db";

import {
  atomicTransition,
  claimAllocation,
  releaseAllocation,
  activateAllocation,
  completeAllocation,
  failAllocation,
  failAllocationAnyState,
  sealManifest,
  startWorkflow,
  completeWorkflow,
  failWorkflow,
  cancelWorkflow as cancelWorkflowTransition,
  finalizeCancellation,
  pauseWorkflow,
  resumeWorkflow,
  startRollback,
  startNode,
  completeNode,
  failNode,
  resetNodeForRetry,
  skipNode,
  ALLOCATION_TRANSITIONS,
  type TransitionResult,
} from "../../control/src/workflow/state-transitions";

// =============================================================================
// Test Harness
// =============================================================================

let cleanup: () => Promise<void>;
beforeEach(() => { cleanup = setupTest(); });
afterEach(() => cleanup());

// =============================================================================
// Helpers: create records in a specific state
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
    provider_metadata: null,
    display_name: null,
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

type AllocStatus = "AVAILABLE" | "CLAIMED" | "ACTIVE" | "COMPLETE" | "FAILED";

function makeAllocation(status: AllocStatus): Allocation {
  const inst = makeInstance();
  return createAllocation({
    run_id: null,
    instance_id: inst.id,
    status,
    current_manifest_id: null,
    user: "test",
    workdir: "/tmp",
    debug_hold_until: null,
    completed_at: status === "COMPLETE" || status === "FAILED" ? Date.now() : null,
  });
}

type WfStatus = "pending" | "running" | "paused" | "cancelling" | "completed" | "failed" | "cancelled" | "rolling_back";

function makeWorkflow(status: WfStatus): Workflow {
  const now = Date.now();
  const isActive = !["pending"].includes(status);
  const isTerminal = ["completed", "failed", "cancelled"].includes(status);
  // cancelling is active but not terminal
  return createWorkflow({
    type: "test-sm",
    parent_workflow_id: null,
    depth: 0,
    status,
    current_node: null,
    input_json: "{}",
    output_json: isTerminal && status === "completed" ? '{"ok":true}' : null,
    error_json: isTerminal && status === "failed" ? '"error"' : null,
    manifest_id: null,
    trace_id: null,
    idempotency_key: null,
    timeout_ms: 3600000,
    timeout_at: now + 3600000,
    started_at: isActive ? now : null,
    finished_at: isTerminal ? now : null,
    updated_at: now,
  });
}

type NodeStatus = "pending" | "running" | "completed" | "failed" | "skipped";

function makeNode(status: NodeStatus): WorkflowNode {
  const wf = makeWorkflow("running");
  const now = Date.now();
  return createWorkflowNode({
    workflow_id: wf.id,
    node_id: `node-${Math.random().toString(36).slice(2, 8)}`,
    node_type: "test-type",
    status,
    input_json: "{}",
    output_json: status === "completed" ? '{"ok":true}' : null,
    error_json: status === "failed" ? '"err"' : null,
    depends_on: null,
    attempt: status === "pending" ? 0 : 1,
    retry_reason: null,
    started_at: status !== "pending" && status !== "skipped" ? now : null,
    finished_at: ["completed", "failed", "skipped"].includes(status) ? now : null,
    updated_at: now,
  });
}

function makeManifest(status: "DRAFT" | "SEALED"): Manifest {
  const wf = makeWorkflow("running");
  const manifest = createManifest(wf.id);
  if (status === "SEALED") {
    const result = sealManifest(manifest.id, Date.now() + 86400000);
    if (!result.success) throw new Error("Failed to seal manifest in setup");
    return result.data;
  }
  return manifest;
}

// =============================================================================
// 1. ALLOCATION STATE MACHINE
// =============================================================================

describe("Allocation State Machine", () => {
  const ALL_STATUSES: AllocStatus[] = ["AVAILABLE", "CLAIMED", "ACTIVE", "COMPLETE", "FAILED"];

  const VALID_TRANSITIONS: Record<AllocStatus, AllocStatus[]> = {
    AVAILABLE: ["CLAIMED", "FAILED"],
    CLAIMED: ["AVAILABLE", "ACTIVE", "FAILED"],
    ACTIVE: ["COMPLETE", "FAILED"],
    COMPLETE: [],
    FAILED: [],
  };

  function attemptAllocationTransition(
    fromStatus: AllocStatus,
    toStatus: AllocStatus
  ): TransitionResult<Allocation> {
    const alloc = makeAllocation(fromStatus);
    switch (toStatus) {
      case "CLAIMED": { const run = makeRun(); return claimAllocation(alloc.id, run.id); }
      case "AVAILABLE": return releaseAllocation(alloc.id);
      case "ACTIVE": return activateAllocation(alloc.id);
      case "COMPLETE": return completeAllocation(alloc.id);
      case "FAILED": return failAllocationAnyState(alloc.id);
      default: throw new Error(`Unknown target status: ${toStatus}`);
    }
  }

  test("truth table matches ALLOCATION_TRANSITIONS constant", () => {
    expect(ALL_STATUSES.sort()).toEqual(
      Object.keys(ALLOCATION_TRANSITIONS).sort() as AllocStatus[]
    );
    for (const from of ALL_STATUSES) {
      expect(VALID_TRANSITIONS[from].sort()).toEqual(
        [...ALLOCATION_TRANSITIONS[from]].sort()
      );
    }
  });

  test("N*N matrix: valid transitions succeed, invalid rejected", () => {
    const SPECIAL_CASES = new Set(["FAILED->COMPLETE"]);
    const failures: string[] = [];
    for (const from of ALL_STATUSES) {
      for (const to of ALL_STATUSES) {
        if (from === to) continue;
        if (SPECIAL_CASES.has(`${from}->${to}`)) continue;

        const isValid = VALID_TRANSITIONS[from].includes(to);
        const result = attemptAllocationTransition(from, to);
        if (isValid && !result.success) {
          failures.push(`${from} -> ${to}: expected success but got failure (${result.reason})`);
        } else if (isValid && result.success && result.data.status !== to) {
          failures.push(`${from} -> ${to}: expected status ${to} but got ${result.data.status}`);
        } else if (!isValid && result.success) {
          failures.push(`${from} -> ${to}: expected rejection but succeeded`);
        }
      }
    }
    expect(failures).toEqual([]);
  });

  test("FAILED -> COMPLETE (completeAllocation): idempotent, status stays FAILED", () => {
    const alloc = makeAllocation("FAILED");
    const result = completeAllocation(alloc.id);
    expect(result.success).toBe(true);
    if (result.success) expect(result.data.status).toBe("FAILED");
  });

  test("terminal transitions set completed_at; claim sets run_id; release clears run_id", () => {
    // ACTIVE -> COMPLETE sets completed_at
    const alloc1 = makeAllocation("ACTIVE");
    const r1 = completeAllocation(alloc1.id);
    expect(r1.success).toBe(true);
    if (r1.success) expect(r1.data.completed_at).not.toBeNull();

    // ACTIVE -> FAILED sets completed_at
    const alloc2 = makeAllocation("ACTIVE");
    const r2 = failAllocation(alloc2.id, "ACTIVE");
    expect(r2.success).toBe(true);
    if (r2.success) expect(r2.data.completed_at).not.toBeNull();

    // AVAILABLE -> CLAIMED sets run_id
    const alloc3 = makeAllocation("AVAILABLE");
    const run = makeRun();
    const r3 = claimAllocation(alloc3.id, run.id);
    expect(r3.success).toBe(true);
    if (r3.success) expect(r3.data.run_id).toBe(run.id);

    // CLAIMED -> AVAILABLE clears run_id
    const r4 = releaseAllocation(alloc3.id);
    expect(r4.success).toBe(true);
    if (r4.success) {
      expect(r4.data.run_id).toBeNull();
      expect(r4.data.status).toBe("AVAILABLE");
    }
  });
});

// =============================================================================
// 2. WORKFLOW STATE MACHINE
// =============================================================================

describe("Workflow State Machine", () => {
  const ALL_STATUSES: WfStatus[] = [
    "pending", "running", "paused", "cancelling", "completed", "failed", "cancelled", "rolling_back",
  ];

  const VALID_TRANSITIONS: Record<WfStatus, WfStatus[]> = {
    pending:      ["running", "failed"],
    running:      ["completed", "failed", "cancelling", "paused", "rolling_back"],
    paused:       ["running", "failed", "cancelling"],
    cancelling:   ["cancelled"],
    completed:    [],
    failed:       [],
    cancelled:    [],
    rolling_back: ["failed"],
  };

  function attemptWorkflowTransition(
    fromStatus: WfStatus,
    toStatus: WfStatus
  ): TransitionResult<Workflow> {
    const wf = makeWorkflow(fromStatus);
    switch (toStatus) {
      case "running":
        if (fromStatus === "paused") return resumeWorkflow(wf.id);
        return startWorkflow(wf.id);
      case "completed": return completeWorkflow(wf.id, { ok: true });
      case "failed": return failWorkflow(wf.id, "test error");
      case "cancelling": return cancelWorkflowTransition(wf.id);
      case "cancelled": return finalizeCancellation(wf.id);
      case "paused": return pauseWorkflow(wf.id);
      case "rolling_back": return startRollback(wf.id);
      default: throw new Error(`Unknown target status: ${toStatus}`);
    }
  }

  test("covers all workflow states and 'pending' is creation-only", () => {
    const expected: WfStatus[] = ["pending", "running", "paused", "cancelling", "completed", "failed", "cancelled", "rolling_back"];
    expect(ALL_STATUSES.sort()).toEqual(expected.sort());
    for (const from of ALL_STATUSES) {
      expect(VALID_TRANSITIONS[from].includes("pending")).toBe(false);
    }
  });

  test("N*N matrix: valid transitions succeed, invalid rejected", () => {
    const failures: string[] = [];
    for (const from of ALL_STATUSES) {
      for (const to of ALL_STATUSES) {
        if (from === to) continue;
        if (to === "pending") continue;

        const isValid = VALID_TRANSITIONS[from].includes(to);
        const result = attemptWorkflowTransition(from, to);
        if (isValid && !result.success) {
          failures.push(`${from} -> ${to}: expected success but got failure (${result.reason})`);
        } else if (isValid && result.success && result.data.status !== to) {
          failures.push(`${from} -> ${to}: expected status ${to} but got ${result.data.status}`);
        } else if (!isValid && result.success) {
          failures.push(`${from} -> ${to}: expected rejection but succeeded`);
        }
      }
    }
    expect(failures).toEqual([]);
  });

  test("timestamp invariants: terminal sets finished_at, start sets started_at", () => {
    for (const terminal of ["completed", "failed", "cancelled"] as WfStatus[]) {
      const validFrom = Object.entries(VALID_TRANSITIONS)
        .find(([, targets]) => targets.includes(terminal))!;
      const from = validFrom[0] as WfStatus;
      const result = attemptWorkflowTransition(from, terminal);
      expect(result.success).toBe(true);
      if (result.success) expect(result.data.finished_at).not.toBeNull();
    }
    const start = attemptWorkflowTransition("pending", "running");
    expect(start.success).toBe(true);
    if (start.success) expect(start.data.started_at).not.toBeNull();
  });
});

// =============================================================================
// 3. WORKFLOW NODE STATE MACHINE
// =============================================================================

describe("WorkflowNode State Machine", () => {
  const ALL_STATUSES: NodeStatus[] = ["pending", "running", "completed", "failed", "skipped"];

  const VALID_TRANSITIONS: Record<NodeStatus, NodeStatus[]> = {
    pending:   ["running", "skipped"],
    running:   ["completed", "failed"],
    completed: [],
    failed:    ["pending"],
    skipped:   [],
  };

  function attemptNodeTransition(
    fromStatus: NodeStatus,
    toStatus: NodeStatus
  ): TransitionResult<WorkflowNode> {
    const node = makeNode(fromStatus);
    switch (toStatus) {
      case "running": return startNode(node.id);
      case "completed": return completeNode(node.id, { ok: true });
      case "failed": return failNode(node.id, "test error");
      case "pending": return resetNodeForRetry(node.id, "test retry");
      case "skipped": return skipNode(node.id);
      default: throw new Error(`Unknown target status: ${toStatus}`);
    }
  }

  test("covers all node states", () => {
    const expected: NodeStatus[] = ["pending", "running", "completed", "failed", "skipped"];
    expect(ALL_STATUSES.sort()).toEqual(expected.sort());
  });

  test("N*N matrix: valid transitions succeed, invalid rejected", () => {
    const failures: string[] = [];
    for (const from of ALL_STATUSES) {
      for (const to of ALL_STATUSES) {
        if (from === to) continue;
        const isValid = VALID_TRANSITIONS[from].includes(to);
        const result = attemptNodeTransition(from, to);
        if (isValid && !result.success) {
          failures.push(`${from} -> ${to}: expected success but got failure (${result.reason})`);
        } else if (isValid && result.success && result.data.status !== to) {
          failures.push(`${from} -> ${to}: expected status ${to} but got ${result.data.status}`);
        } else if (!isValid && result.success) {
          failures.push(`${from} -> ${to}: expected rejection but succeeded`);
        }
      }
    }
    expect(failures).toEqual([]);
  });

  test("field invariants: attempt increment, retry clears error, skip sets finished_at", () => {
    // startNode increments attempt
    const n1 = makeNode("pending");
    expect(n1.attempt).toBe(0);
    const r1 = startNode(n1.id);
    expect(r1.success).toBe(true);
    if (r1.success) expect(r1.data.attempt).toBe(1);

    // resetNodeForRetry clears error_json and finished_at
    const n2 = makeNode("failed");
    expect(n2.error_json).not.toBeNull();
    const r2 = resetNodeForRetry(n2.id, "retry reason");
    expect(r2.success).toBe(true);
    if (r2.success) {
      expect(r2.data.error_json).toBeNull();
      expect(r2.data.finished_at).toBeNull();
      expect(r2.data.retry_reason).toBe("retry reason");
    }

    // skipNode sets finished_at
    const n3 = makeNode("pending");
    const r3 = skipNode(n3.id);
    expect(r3.success).toBe(true);
    if (r3.success) expect(r3.data.finished_at).not.toBeNull();
  });
});

// =============================================================================
// 4. MANIFEST STATE MACHINE
// =============================================================================

describe("Manifest State Machine", () => {
  type ManifestStatus = "DRAFT" | "SEALED";
  const ALL_STATUSES: ManifestStatus[] = ["DRAFT", "SEALED"];

  const VALID_TRANSITIONS: Record<ManifestStatus, ManifestStatus[]> = {
    DRAFT: ["SEALED"],
    SEALED: [],
  };

  test("N*N matrix: valid transitions succeed, invalid rejected", () => {
    for (const from of ALL_STATUSES) {
      for (const to of ALL_STATUSES) {
        if (from === to) continue;
        if (to === "DRAFT") continue;

        const manifest = makeManifest(from);
        const result = sealManifest(manifest.id, Date.now() + 86400000);
        const isValid = VALID_TRANSITIONS[from].includes(to);
        if (isValid) {
          if (!result.success) throw new Error(`${from} -> ${to}: expected success but got failure (${result.reason})`);
          expect(result.data.status).toBe(to);
        } else {
          if (result.success) throw new Error(`${from} -> ${to}: expected rejection but succeeded`);
        }
      }
    }
  });

  test("sealManifest sets released_at and expires_at", () => {
    const manifest = makeManifest("DRAFT");
    const expiresAt = Date.now() + 86400000;
    const result = sealManifest(manifest.id, expiresAt);
    expect(result.success).toBe(true);
    if (result.success) {
      expect(result.data.released_at).not.toBeNull();
      expect(result.data.expires_at).toBe(expiresAt);
    }
  });

  test("sealed manifest rejects resource additions", () => {
    const { addResourceToManifest } = require("../../control/src/material/db");
    const manifest = makeManifest("SEALED");
    expect(() => {
      addResourceToManifest(manifest.id, "instance", "1");
    }).toThrow("Cannot add resources to sealed manifest");
  });
});

// =============================================================================
// 5. atomicTransition - generic guard coverage
// =============================================================================

describe("atomicTransition guard coverage", () => {
  test("rejects SQL injection via invalid table name", () => {
    expect(() => {
      atomicTransition<Workflow>("users; DROP TABLE workflows", 1, "pending", "running");
    }).toThrow("Invalid table name");
  });

  test("returns NOT_FOUND for missing ID", () => {
    const result = atomicTransition<Workflow>("workflows", 999999, "pending", "running");
    expect(result.success).toBe(false);
    if (!result.success) expect(result.reason).toBe("NOT_FOUND");
  });

  test("returns WRONG_STATE when current status does not match", () => {
    const wf = makeWorkflow("pending");
    const result = atomicTransition<Workflow>("workflows", wf.id, "running", "completed");
    expect(result.success).toBe(false);
    if (!result.success) {
      expect(result.reason).toBe("WRONG_STATE");
      expect(result.current?.status).toBe("pending");
    }
  });
});
