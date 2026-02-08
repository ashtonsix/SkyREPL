// workflow/nodes/resolve-instance.ts - Resolve Instance Node
// Stub: All function bodies throw "not implemented"

import type { NodeExecutor, NodeContext } from "../engine.types";

// =============================================================================
// Types
// =============================================================================

export interface ResolveInstanceInput {
  runId: number;
  spec: string;
  provider: string;
  region?: string;
  initChecksum?: string;
  preferWarmPool: boolean;
}

export interface ResolveInstanceOutput {
  warmAvailable: boolean;
  allocationId?: number;
  instanceId?: number;
}

// =============================================================================
// Node Executor
// =============================================================================

export const resolveInstanceExecutor: NodeExecutor<ResolveInstanceInput, ResolveInstanceOutput> = {
  name: "resolve-instance",
  idempotent: true,

  async execute(ctx: NodeContext): Promise<ResolveInstanceOutput> {
    throw new Error("not implemented");
  },
};
