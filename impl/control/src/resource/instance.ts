// resource/instance.ts - Instance Resource Operations
// Stub: All function bodies throw "not implemented"

import type { Instance } from "../material/db";

// =============================================================================
// Instance Lifecycle
// =============================================================================

export function createInstanceRecord(
  data: Omit<Instance, "id" | "created_at">
): Instance {
  throw new Error("not implemented");
}

export function getInstanceRecord(id: number): Instance | null {
  throw new Error("not implemented");
}

export function updateInstanceRecord(
  id: number,
  updates: Partial<Instance>
): Instance {
  throw new Error("not implemented");
}

export function listInstanceRecords(filter?: {
  provider?: string;
  workflow_state?: string;
  spec?: string;
}): Instance[] {
  throw new Error("not implemented");
}

// =============================================================================
// Instance State Queries
// =============================================================================

export function isInstanceHealthy(instance: Instance): boolean {
  throw new Error("not implemented");
}

export function getInstancesByProvider(provider: string): Instance[] {
  throw new Error("not implemented");
}

export function getActiveInstances(): Instance[] {
  throw new Error("not implemented");
}

export function getStaleInstances(cutoffMs: number): Instance[] {
  throw new Error("not implemented");
}

// =============================================================================
// Heartbeat
// =============================================================================

export function updateHeartbeat(instanceId: number, timestamp: number): void {
  throw new Error("not implemented");
}

export function detectStaleHeartbeats(thresholdMs: number): Instance[] {
  throw new Error("not implemented");
}
