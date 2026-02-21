// workflow/engine/helpers.ts - Pure utility functions and low-level helpers

import type {
  NodeError,
  WorkflowError,
} from "../engine.types";
import {
  getWorkflowNodes,
  getManifest,
  queryOne,
  type Workflow,
  type WorkflowNode,
} from "../../material/db";

// =============================================================================
// Constants
// =============================================================================

export const MAX_SUBWORKFLOW_DEPTH = 3;
export const MAX_PARALLEL_BRANCHES = 16;

/** Polling interval for crash recovery: nodes running in DB but not tracked locally */
export const POLL_INTERVAL_MS = 100;

// =============================================================================
// Sleep
// =============================================================================

let sleepImpl: (ms: number) => Promise<void> = (ms) =>
  new Promise((resolve) => setTimeout(resolve, ms));

/** Replace the sleep implementation. Test-only. */
export function _setSleepForTest(fn: (ms: number) => Promise<void>): void {
  sleepImpl = fn;
}

/** Reset sleep to real implementation. Test-only. */
export function _resetSleep(): void {
  sleepImpl = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
}

export function sleep(ms: number): Promise<void> {
  return sleepImpl(ms);
}

/** Exported for use by compensation.ts and other workflow subsystems. */
export { sleep as engineSleep };

// =============================================================================
// Timeout
// =============================================================================

export function createTimeout(ms: number): { promise: Promise<never>; clear: () => void } {
  let timerId: Timer;
  const promise = new Promise<never>((_, reject) => {
    timerId = setTimeout(
      () =>
        reject(
          Object.assign(new Error("Operation timed out"), {
            code: "OPERATION_TIMEOUT",
            category: "timeout" as const,
            message: `Operation timed out after ${ms}ms`,
          })
        ),
      ms
    );
  });
  return { promise, clear: () => clearTimeout(timerId!) };
}

// =============================================================================
// Error Normalization
// =============================================================================

export function normalizeToNodeError(error: unknown): NodeError {
  if (
    error &&
    typeof error === "object" &&
    "code" in error &&
    typeof (error as Record<string, unknown>).code === "string"
  ) {
    const e = error as Record<string, unknown>;
    return {
      code: e.code as string,
      message:
        (e.message as string) ??
        (error instanceof Error ? error.message : String(error)),
      category: (e.category as string) ?? "internal",
      details: (e.details as Record<string, unknown>) ?? undefined,
    };
  }
  if (error instanceof Error) {
    return {
      code: "INTERNAL_ERROR",
      message: error.message,
      category: "internal",
    };
  }
  return {
    code: "INTERNAL_ERROR",
    message: String(error),
    category: "internal",
  };
}

// =============================================================================
// Error Mapping
// =============================================================================

export function mapErrorToHttpStatus(error: WorkflowError): number {
  switch (error.category) {
    case "validation":
      return 400;
    case "auth":
      return error.code === "UNAUTHORIZED" ? 401 : 403;
    case "not_found":
      return 404;
    case "conflict":
      return 409;
    case "rate_limit":
      return 429;
    case "timeout":
      return 504;
    case "provider":
      return 502;
    case "internal":
      return 500;
    default:
      return 500;
  }
}

// =============================================================================
// Observability
// =============================================================================

export function calculateTopologySignature(workflowId: number): string {
  const nodes = getWorkflowNodes(workflowId);

  const structure = {
    nodes: nodes
      .filter((n) => n.status !== "skipped")
      .map((n) => ({
        type: n.node_type,
        deps: (
          n.depends_on ? (JSON.parse(n.depends_on) as string[]) : []
        ).sort(),
      }))
      .sort((a, b) => a.type.localeCompare(b.type)),
  };

  // Use Bun's built-in hashing
  const hasher = new Bun.CryptoHasher("sha256");
  hasher.update(JSON.stringify(structure));
  return hasher.digest("hex").slice(0, 16);
}

// =============================================================================
// Find Ready Nodes (in-memory version for engine callers)
// =============================================================================

/**
 * In-memory version: find ready nodes from a given array.
 * Used when we already have nodes loaded.
 */
export function findReadyNodesFromArray(
  nodes: WorkflowNode[]
): WorkflowNode[] {
  const completedOrSkipped = new Set(
    nodes
      .filter((n) => n.status === "completed" || n.status === "skipped")
      .map((n) => n.node_id)
  );

  return nodes.filter((n) => {
    if (n.status !== "pending") return false;
    const deps: string[] = n.depends_on
      ? JSON.parse(n.depends_on)
      : [];
    return deps.every((depId) => completedOrSkipped.has(depId));
  });
}

// =============================================================================
// Seal Manifest Safe
// =============================================================================

/**
 * Seal a manifest using workflow-type-specific retention policy.
 * Looks up the workflow that owns this manifest to determine the correct
 * retention duration via sealManifestWithPolicy (SD-G1-04 fix).
 * Idempotent: returns success if manifest is already sealed (EX2 fix).
 */
export function sealManifestSafe(manifestId: number): void {
  const manifest = getManifest(manifestId);
  if (!manifest) return;

  // Already sealed -- idempotent success
  if (manifest.status === "SEALED") return;

  // Find the workflow that owns this manifest to determine retention policy
  const workflow = queryOne<Workflow>(
    "SELECT * FROM workflows WHERE manifest_id = ?",
    [manifestId]
  );

  const workflowType = workflow?.type ?? "unknown";

  // Dynamic import to avoid potential circular dependency
  const { sealManifestWithPolicy } = require("../../resource/manifest") as typeof import("../../resource/manifest");
  const result = sealManifestWithPolicy(manifestId, workflowType);
  if (!result.success) {
    console.warn(`[workflow] Failed to seal manifest ${manifestId}: ${result.reason}`);
  }
}
