// resource/allocation.ts - Allocation State Machine, Warm Pool, Debug Holds
// Stub: All function bodies throw "not implemented"

import type { Allocation } from "../material/db";
import type { TransitionResult } from "../workflow/state-transitions";

// =============================================================================
// Types
// =============================================================================

export type AllocationStatus = "AVAILABLE" | "CLAIMED" | "ACTIVE" | "COMPLETE" | "FAILED";

export interface WarmPoolQuery {
  provider?: string;
  spec?: string;
  initChecksum?: string;
  region?: string;
  excludeInstanceIds?: number[];
}

export interface WarmPoolMatch extends Allocation {
  spec: string;
  init_checksum: string | null;
  region: string;
  checksum_score: number;
}

// =============================================================================
// Valid Transitions
// =============================================================================

export const VALID_TRANSITIONS: Record<AllocationStatus, AllocationStatus[]> = {
  AVAILABLE: ["CLAIMED"],
  CLAIMED: ["ACTIVE", "FAILED"],
  ACTIVE: ["COMPLETE", "FAILED"],
  COMPLETE: [],
  FAILED: [],
};

// =============================================================================
// State Validation
// =============================================================================

export function validateAllocationTransition(
  currentStatus: AllocationStatus,
  newStatus: AllocationStatus
): { valid: boolean; error?: string } {
  throw new Error("not implemented");
}

export function allowsSSHAccess(allocation: Allocation): boolean {
  throw new Error("not implemented");
}

export function isInWarmPool(status: AllocationStatus): boolean {
  throw new Error("not implemented");
}

// =============================================================================
// Warm Pool Operations
// =============================================================================

export function createAvailableAllocation(
  instanceId: number,
  options?: { user?: string; workdir?: string; manifestId?: number }
): Promise<Allocation> {
  throw new Error("not implemented");
}

export function queryWarmPool(
  query: WarmPoolQuery,
  limit?: number
): Promise<WarmPoolMatch[]> {
  throw new Error("not implemented");
}

export function claimWarmPoolAllocation(
  query: WarmPoolQuery,
  runId: number,
  maxRetries?: number
): Promise<{ success: boolean; allocation?: Allocation; error?: string }> {
  throw new Error("not implemented");
}

// =============================================================================
// Active-Complete Transition
// =============================================================================

export function transitionToComplete(
  allocationId: number,
  options: {
    exitCode: number;
    keepOnComplete?: boolean;
    debugHoldDurationMs?: number;
  }
): Promise<void> {
  throw new Error("not implemented");
}

export function checkReplenishmentEligibility(
  instanceId: number,
  completedAllocationId: number
): Promise<boolean> {
  throw new Error("not implemented");
}

// =============================================================================
// Debug Hold Extension
// =============================================================================

export function extendAllocationDebugHold(
  allocationId: number,
  options: { extensionMs: number; maxExtensionMs?: number }
): Promise<{ success: boolean; newDebugHoldUntil?: number }> {
  throw new Error("not implemented");
}

// =============================================================================
// Termination Blocking
// =============================================================================

export function canTerminateInstance(instanceId: number): Promise<boolean> {
  throw new Error("not implemented");
}

// =============================================================================
// Background Reconciliation
// =============================================================================

export function reconcileClaimedAllocations(): Promise<void> {
  throw new Error("not implemented");
}

export function holdExpiryTask(): Promise<void> {
  throw new Error("not implemented");
}
