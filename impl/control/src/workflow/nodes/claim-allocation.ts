// workflow/nodes/claim-allocation.ts - Claim Allocation Node
// Stub: All function bodies throw "not implemented"

import type { NodeExecutor, NodeContext } from "../engine.types";

// =============================================================================
// Types
// =============================================================================

export interface ClaimAllocationInput {
  runId: number;
  allocationId: number;
  instanceId?: number;
  manifestId: number;
}

export interface ClaimAllocationOutput {
  allocationId: number;
  instanceId: number;
  fromWarmPool: true;
}

// =============================================================================
// Node Executor
// =============================================================================

export const claimAllocationExecutor: NodeExecutor<ClaimAllocationInput, ClaimAllocationOutput> = {
  name: "claim-allocation",
  idempotent: true,

  async execute(ctx: NodeContext): Promise<ClaimAllocationOutput> {
    throw new Error("not implemented");
  },

  async compensate(ctx: NodeContext): Promise<void> {
    throw new Error("not implemented");
  },
};
