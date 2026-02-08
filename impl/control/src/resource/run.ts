// resource/run.ts - Run Resource Operations
// Stub: All function bodies throw "not implemented"

import type { Run } from "../material/db";

// =============================================================================
// Run Lifecycle
// =============================================================================

export function createRunRecord(
  data: Omit<Run, "id" | "created_at">
): Run {
  throw new Error("not implemented");
}

export function getRunRecord(id: number): Run | null {
  throw new Error("not implemented");
}

export function updateRunRecord(
  id: number,
  updates: Partial<Run>
): Run {
  throw new Error("not implemented");
}

export function listRunRecords(filter?: {
  workflow_state?: string;
  created_after?: number;
  created_before?: number;
}): Run[] {
  throw new Error("not implemented");
}

// =============================================================================
// Run State Queries
// =============================================================================

export function getActiveRuns(): Run[] {
  throw new Error("not implemented");
}

export function getRunsByInstance(instanceId: number): Run[] {
  throw new Error("not implemented");
}

export function isRunInProgress(run: Run): boolean {
  throw new Error("not implemented");
}

export function getRunDuration(run: Run): number | null {
  throw new Error("not implemented");
}
