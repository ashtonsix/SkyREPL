// resource/run.ts - Run Resource Operations

import type { Run } from "../material/db";
import {
  getRun,
  createRun,
  updateRun,
  listRuns,
  queryMany,
} from "../material/db";

// =============================================================================
// Run Lifecycle
// =============================================================================

export function createRunRecord(
  data: Omit<Run, "id" | "created_at" | "tenant_id">,
  tenantId?: number
): Run {
  return createRun(data, tenantId);
}

export function getRunRecord(id: number): Run | null {
  return getRun(id);
}

export function updateRunRecord(
  id: number,
  updates: Partial<Run>
): Run {
  return updateRun(id, updates);
}

export function listRunRecords(filter?: {
  workflow_state?: string;
  created_after?: number;
  created_before?: number;
}): Run[] {
  // listRuns from DB supports workflow_state and current_manifest_id.
  // For time-range filters, we query directly with SQL.
  if (filter?.created_after || filter?.created_before) {
    let sql = "SELECT * FROM runs WHERE 1=1";
    const params: unknown[] = [];

    if (filter.workflow_state) {
      sql += " AND workflow_state = ?";
      params.push(filter.workflow_state);
    }
    if (filter.created_after) {
      sql += " AND created_at > ?";
      params.push(filter.created_after);
    }
    if (filter.created_before) {
      sql += " AND created_at < ?";
      params.push(filter.created_before);
    }

    return queryMany<Run>(sql, params);
  }

  return listRuns(
    filter?.workflow_state ? { workflow_state: filter.workflow_state } : undefined
  );
}

// =============================================================================
// Run State Queries
// =============================================================================

export function getActiveRuns(): Run[] {
  return queryMany<Run>(
    `SELECT * FROM runs
     WHERE workflow_state NOT LIKE '%:complete'
       AND workflow_state NOT LIKE '%:error'
       AND workflow_state NOT LIKE '%:cancelled'
       AND workflow_state NOT LIKE '%:timeout'`
  );
}

export function getRunsByInstance(instanceId: number): Run[] {
  return queryMany<Run>(
    `SELECT r.* FROM runs r
     JOIN allocations a ON a.run_id = r.id
     WHERE a.instance_id = ?`,
    [instanceId]
  );
}

export function isRunInProgress(run: Run): boolean {
  const state = run.workflow_state;
  if (
    state.endsWith(":complete") ||
    state.endsWith(":error") ||
    state.endsWith(":cancelled") ||
    state.endsWith(":timeout")
  ) {
    return false;
  }
  if (run.finished_at !== null) {
    return false;
  }
  return true;
}

export function getRunDuration(run: Run): number | null {
  if (run.started_at !== null && run.finished_at !== null) {
    return run.finished_at - run.started_at;
  }
  if (run.started_at !== null) {
    return Date.now() - run.started_at;
  }
  return null;
}
