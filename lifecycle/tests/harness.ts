// tests/harness.ts - Shared test lifecycle management
//
// Eliminates cross-test contamination by clearing all global singletons
// between tests. Every test file should use this instead of hand-rolled
// beforeEach/afterEach hooks.

import {
  initDatabase,
  closeDatabase,
  runMigrations,
  getWorkflow,
  getWorkflowNodes,
  getManifest,
  queryMany,
  type Allocation,
  type Workflow,
} from "../control/src/material/db";
import {
  resetEngineShutdown,
  requestEngineShutdown,
  awaitEngineQuiescence,
  clearNodeExecutors,
  clearBlueprints,
  _setSleepForTest,
  _resetSleep,
} from "../control/src/workflow/engine";
import {
  clearAllProviders,
} from "../control/src/provider/registry";
import { cacheClear, cacheResetStats } from "../control/src/resource/cache";
import { expect } from "bun:test";

export interface SetupTestOptions {
  /** Initialize engine shutdown coordination (reset + quiescence on cleanup). Default: false. */
  engine?: boolean;
}

/**
 * Set up a clean test environment. Call in beforeEach().
 * Returns an async cleanup function to call in afterEach().
 *
 * Usage:
 *   let cleanup: () => Promise<void>;
 *   beforeEach(() => { cleanup = setupTest({ engine: true }); });
 *   afterEach(() => cleanup());
 */
export function setupTest(opts: SetupTestOptions = {}): () => Promise<void> {
  // 1. Shim sleep to yield via macrotask (not microtask!) — lets in-flight node executors
  //    and setTimeout callbacks run between engine loop iterations.
  _setSleepForTest(() => new Promise((r) => setTimeout(r, 0)));

  // 2. Reset engine state (must come before DB init so loops from prior test don't write to new DB)
  if (opts.engine) {
    resetEngineShutdown();
  }

  // 3. Clear all registries and caches
  clearBlueprints();
  clearNodeExecutors();
  clearAllProviders();
  cacheClear();
  cacheResetStats();

  // 4. Fresh in-memory database
  initDatabase(":memory:");
  runMigrations();

  // Return cleanup function
  return async () => {
    if (opts.engine) {
      requestEngineShutdown();
      await awaitEngineQuiescence(5_000);
    }
    closeDatabase();
    _resetSleep();
  };
}

// =============================================================================
// Workflow Polling
// =============================================================================

const TERMINAL_STATUSES = new Set(["completed", "failed", "cancelled"]);

/**
 * Poll until a workflow reaches a terminal state.
 * Uses tight polling (50ms) with a hard deadline — NOT a blind sleep.
 * Throws if the workflow doesn't terminate within timeoutMs.
 */
export async function waitForWorkflow(
  workflowId: number,
  timeoutMs = 5_000
): Promise<Workflow> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const wf = getWorkflow(workflowId);
    if (wf && TERMINAL_STATUSES.has(wf.status)) {
      return wf;
    }
    await Bun.sleep(5);
  }
  const wf = getWorkflow(workflowId);
  throw new Error(
    `Workflow ${workflowId} did not terminate within ${timeoutMs}ms (status: ${wf?.status})`
  );
}

// =============================================================================
// Workflow End-State Invariant Verifier
// =============================================================================

const TERMINAL_NODE_STATUSES = new Set(["completed", "failed", "skipped"]);

export interface WorkflowInvariantOptions {
  /** If true, expect manifest to be SEALED (only for completed workflows). Default: true when completed. */
  expectManifestSealed?: boolean;
  /** If true, verify no active/claimed allocations remain. Default: true. */
  checkAllocations?: boolean;
}

/**
 * Verify post-completion invariants for a workflow.
 * Call after any test that drives a workflow to a terminal state.
 * Asserts:
 * - Workflow status is terminal (completed/failed/cancelled)
 * - All nodes are terminal (completed/failed/skipped)
 * - finished_at is set and started_at <= finished_at (if started_at is set)
 * - Manifest is SEALED if workflow completed
 * - No ACTIVE or CLAIMED allocations remain for this workflow's runs
 */
export function verifyWorkflowInvariant(
  workflowId: number,
  opts: WorkflowInvariantOptions = {}
): void {
  const workflow = getWorkflow(workflowId);
  expect(workflow).not.toBeNull();
  if (!workflow) return;

  // 1. Workflow must be terminal
  expect(TERMINAL_STATUSES.has(workflow.status)).toBe(true);

  // 2. finished_at must be set
  expect(workflow.finished_at).not.toBeNull();

  // 3. If started_at is set, started_at <= finished_at
  if (workflow.started_at != null && workflow.finished_at != null) {
    expect(workflow.started_at).toBeLessThanOrEqual(workflow.finished_at);
  }

  // 4. All nodes must be terminal
  const nodes = getWorkflowNodes(workflowId);
  for (const node of nodes) {
    expect(TERMINAL_NODE_STATUSES.has(node.status)).toBe(true);
  }

  // 5. Manifest check
  if (workflow.manifest_id != null) {
    const manifest = getManifest(workflow.manifest_id);
    expect(manifest).not.toBeNull();

    const expectSealed = opts.expectManifestSealed ?? (workflow.status === "completed");
    if (expectSealed && manifest) {
      expect(manifest.status).toBe("SEALED");
    }
  }

  // 6. No active/claimed allocations for this workflow's runs
  if (opts.checkAllocations !== false) {
    const danglingAllocations = queryMany<Allocation>(
      `SELECT a.* FROM allocations a
       JOIN runs r ON a.run_id = r.id
       JOIN workflows w ON w.id = ?
       WHERE a.status IN ('ACTIVE', 'CLAIMED')
         AND a.run_id IS NOT NULL`,
      [workflowId]
    );
    expect(danglingAllocations.length).toBe(0);
  }
}
