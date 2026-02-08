// workflow/state-transitions.ts - State Machine Ownership and Transition Rules
// Stub: All function bodies throw "not implemented"

import type { Allocation, Workflow, WorkflowNode, Manifest } from "../material/db";

// =============================================================================
// Transition Result Type
// =============================================================================

export type TransitionResult<T> =
  | { success: true; data: T }
  | { success: false; reason: "NOT_FOUND" | "WRONG_STATE" | "RACE_LOST"; current?: T };

export function transitionSuccess<T>(data: T): TransitionResult<T> {
  throw new Error("not implemented");
}

export function transitionFailure<T>(
  reason: "NOT_FOUND" | "WRONG_STATE" | "RACE_LOST",
  current?: T
): TransitionResult<T> {
  throw new Error("not implemented");
}

// =============================================================================
// Generic Two-Phase CAS
// =============================================================================

export function atomicTransition<T extends { id: number; status: string; updated_at: number }>(
  table: string,
  id: number,
  fromStatus: string | string[],
  toStatus: string,
  additionalUpdates?: Record<string, unknown>
): TransitionResult<T> {
  throw new Error("not implemented");
}

// =============================================================================
// Allocation State Machine
// =============================================================================

export type AllocationStatus = "AVAILABLE" | "CLAIMED" | "ACTIVE" | "COMPLETE" | "FAILED";

export const ALLOCATION_TRANSITIONS: Record<AllocationStatus, AllocationStatus[]> = {
  AVAILABLE: ["CLAIMED", "FAILED"],
  CLAIMED: ["ACTIVE", "FAILED"],
  ACTIVE: ["COMPLETE", "FAILED"],
  COMPLETE: [],
  FAILED: [],
};

export function claimAllocation(
  allocationId: number,
  runId: number
): TransitionResult<Allocation> {
  throw new Error("not implemented");
}

export function activateAllocation(
  allocationId: number
): TransitionResult<Allocation> {
  throw new Error("not implemented");
}

export function completeAllocation(
  allocationId: number,
  options?: { debugHoldUntil?: number }
): TransitionResult<Allocation> {
  throw new Error("not implemented");
}

export function failAllocation(
  allocationId: number,
  expectedStatus: "AVAILABLE" | "CLAIMED" | "ACTIVE"
): TransitionResult<Allocation> {
  throw new Error("not implemented");
}

export function failAllocationAnyState(
  allocationId: number
): TransitionResult<Allocation> {
  throw new Error("not implemented");
}

// =============================================================================
// Manifest State Machine
// =============================================================================

export type ManifestStatus = "DRAFT" | "SEALED";

export function sealManifest(
  manifestId: number,
  expiresAt: number
): TransitionResult<Manifest> {
  throw new Error("not implemented");
}

// =============================================================================
// Workflow State Machine
// =============================================================================

export type WorkflowStatus =
  | "pending"
  | "running"
  | "completed"
  | "failed"
  | "cancelled"
  | "rolling_back";

export function startWorkflow(
  workflowId: number
): TransitionResult<Workflow> {
  throw new Error("not implemented");
}

export function completeWorkflow(
  workflowId: number,
  output: Record<string, unknown>
): TransitionResult<Workflow> {
  throw new Error("not implemented");
}

export function failWorkflow(
  workflowId: number,
  error: string
): TransitionResult<Workflow> {
  throw new Error("not implemented");
}

export function cancelWorkflow(
  workflowId: number
): TransitionResult<Workflow> {
  throw new Error("not implemented");
}

export function startRollback(
  workflowId: number
): TransitionResult<Workflow> {
  throw new Error("not implemented");
}

// =============================================================================
// Node State Machine
// =============================================================================

export type NodeStatus = "pending" | "running" | "completed" | "failed" | "skipped";

export function startNode(
  nodeId: number
): TransitionResult<WorkflowNode> {
  throw new Error("not implemented");
}

export function completeNode(
  nodeId: number,
  output: Record<string, unknown>
): TransitionResult<WorkflowNode> {
  throw new Error("not implemented");
}

export function failNode(
  nodeId: number,
  error: string
): TransitionResult<WorkflowNode> {
  throw new Error("not implemented");
}

export function resetNodeForRetry(
  nodeId: number,
  retryReason: string
): TransitionResult<WorkflowNode> {
  throw new Error("not implemented");
}

export function skipNode(
  nodeId: number
): TransitionResult<WorkflowNode> {
  throw new Error("not implemented");
}
