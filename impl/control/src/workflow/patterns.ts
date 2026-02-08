// workflow/patterns.ts - Workflow Pattern Implementations
// Stub: All function bodies throw "not implemented"

import type { WorkflowNode } from "../material/db";
import type { InlineNodeDef } from "./engine.types";

// =============================================================================
// Constants
// =============================================================================

export const MAX_PARALLEL_BRANCHES = 16;
export const MAX_RETRY_ATTEMPTS = 3;
export const MAX_IAR_DEPTH = 1;

// =============================================================================
// Pattern Types
// =============================================================================

export type PatternType =
  | "single-step"
  | "insert-and-reconverge"
  | "conditional-branch"
  | "parallel-fan-out"
  | "retry-with-alternative";

// =============================================================================
// Config Types
// =============================================================================

export interface InsertAndReconvergeConfig {
  insertedNode: InlineNodeDef;
  beforeNode: string;
}

export interface ConditionalBranchConfig {
  triggerOnError?: boolean;
  condition?: boolean;
  option1: InlineNodeDef;
  option2: InlineNodeDef;
  joinNode: string;
}

export interface ParallelFanOutConfig {
  branches: InlineNodeDef[];
  joinNode: string;
}

// =============================================================================
// Insert-and-Reconverge (IAR)
// =============================================================================

export function applyInsertAndReconverge(
  workflowId: number,
  config: InsertAndReconvergeConfig
): void {
  throw new Error("not implemented");
}

// =============================================================================
// Conditional Branch (CB)
// =============================================================================

export function applyConditionalBranch(
  workflowId: number,
  config: ConditionalBranchConfig
): void {
  throw new Error("not implemented");
}

// =============================================================================
// Parallel Fan-Out (PFO)
// =============================================================================

export function applyParallelFanOut(
  workflowId: number,
  config: ParallelFanOutConfig
): void {
  throw new Error("not implemented");
}

// =============================================================================
// Retry with Alternative (RWA)
// =============================================================================

export function applyRetryWithAlternative(
  workflowId: number,
  failedNodeId: string,
  alternativeNode: InlineNodeDef
): void {
  throw new Error("not implemented");
}

// =============================================================================
// Helpers
// =============================================================================

export function getPredecessors(
  workflowId: number,
  nodeId: string
): WorkflowNode[] {
  throw new Error("not implemented");
}

export function getDownstreamNodes(
  workflowId: number,
  nodeId: string
): WorkflowNode[] {
  throw new Error("not implemented");
}

export function updateJoinDependencies(
  workflowId: number,
  joinNodeId: string,
  newDeps: string[]
): void {
  throw new Error("not implemented");
}
