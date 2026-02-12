// resource/instance.ts - Instance Resource Operations

import type { Instance } from "../material/db";
import {
  getInstance,
  createInstance,
  updateInstance,
  listInstances,
  queryMany,
} from "../material/db";
import { TIMING } from "@skyrepl/shared";

// =============================================================================
// Instance Lifecycle
// =============================================================================

export function createInstanceRecord(
  data: Omit<Instance, "id" | "created_at">
): Instance {
  return createInstance(data);
}

export function getInstanceRecord(id: number): Instance | null {
  return getInstance(id);
}

export function updateInstanceRecord(
  id: number,
  updates: Partial<Instance>
): Instance {
  return updateInstance(id, updates);
}

export function listInstanceRecords(filter?: {
  provider?: string;
  workflow_state?: string;
  spec?: string;
}): Instance[] {
  // listInstances from DB only supports provider and workflow_state filters.
  // For spec filtering, we query directly with SQL.
  if (filter?.spec) {
    let sql = "SELECT * FROM instances WHERE 1=1";
    const params: unknown[] = [];

    if (filter.provider) {
      sql += " AND provider = ?";
      params.push(filter.provider);
    }
    if (filter.workflow_state) {
      sql += " AND workflow_state = ?";
      params.push(filter.workflow_state);
    }
    sql += " AND spec = ?";
    params.push(filter.spec);

    return queryMany<Instance>(sql, params);
  }

  return listInstances(filter);
}

// =============================================================================
// Instance State Queries
// =============================================================================

export function isInstanceHealthy(instance: Instance): boolean {
  // Heartbeat must be fresh
  if (Date.now() - instance.last_heartbeat >= TIMING.STALE_DETECTION_MS) {
    return false;
  }

  // Workflow state must not be an error state
  if (
    instance.workflow_state.endsWith(":error") ||
    instance.workflow_state.endsWith(":compensated")
  ) {
    return false;
  }

  // Workflow state must not be terminal
  if (instance.workflow_state === "terminate:complete") {
    return false;
  }

  return true;
}

export function getInstancesByProvider(provider: string): Instance[] {
  return listInstances({ provider });
}

const TERMINAL_STATES = [
  "terminate:complete",
  "spawn:error",
  "spawn:compensated",
  "terminate:error",
  "terminate:compensated",
  "launch-run:error",
  "launch-run:compensated",
];

export function getActiveInstances(): Instance[] {
  const placeholders = TERMINAL_STATES.map(() => "?").join(", ");
  return queryMany<Instance>(
    `SELECT * FROM instances WHERE workflow_state NOT IN (${placeholders})`,
    TERMINAL_STATES
  );
}

export function getStaleInstances(cutoffMs: number): Instance[] {
  const cutoff = Date.now() - cutoffMs;
  const placeholders = TERMINAL_STATES.map(() => "?").join(", ");
  return queryMany<Instance>(
    `SELECT * FROM instances WHERE last_heartbeat < ? AND workflow_state NOT IN (${placeholders})`,
    [cutoff, ...TERMINAL_STATES]
  );
}

// =============================================================================
// Heartbeat
// =============================================================================

export function updateHeartbeat(instanceId: number, timestamp: number): void {
  updateInstance(instanceId, { last_heartbeat: timestamp });
}

export function detectStaleHeartbeats(thresholdMs: number): Instance[] {
  const cutoff = Date.now() - thresholdMs;
  const placeholders = TERMINAL_STATES.map(() => "?").join(", ");
  return queryMany<Instance>(
    `SELECT * FROM instances WHERE last_heartbeat < ? AND workflow_state NOT IN (${placeholders})`,
    [cutoff, ...TERMINAL_STATES]
  );
}
