// workflow/nodes/create-allocation.ts - Create Allocation Node
// Stub: All function bodies throw "not implemented"

import type { NodeExecutor, NodeContext } from "../engine.types";

// =============================================================================
// Types
// =============================================================================

export interface CreateAllocationInput {
  runId: number;
  instanceId: number;
  manifestId: number;
  workdir?: string;
}

export interface CreateAllocationOutput {
  allocationId: number;
  instanceId: number;
  workdir: string;
}

// =============================================================================
// Node Executor
// =============================================================================

export const createAllocationExecutor: NodeExecutor<CreateAllocationInput, CreateAllocationOutput> = {
  name: "create-allocation",
  idempotent: true,

  async execute(ctx: NodeContext): Promise<CreateAllocationOutput> {
    throw new Error("not implemented");
  },

  async compensate(ctx: NodeContext): Promise<void> {
    throw new Error("not implemented");
  },
};
