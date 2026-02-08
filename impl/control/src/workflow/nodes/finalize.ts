// workflow/nodes/finalize.ts - Finalize Node
// Stub: All function bodies throw "not implemented"

import type { NodeExecutor, NodeContext } from "../engine.types";

// =============================================================================
// Types
// =============================================================================

export interface FinalizeInput {
  runId: number;
  instanceId: number;
  allocationId: number;
  manifestId: number;
  exitCode: number;
  spotInterrupted: boolean;
  holdDurationMs?: number;
  createSnapshot?: boolean;
  initChecksum?: string;
}

export interface FinalizeOutput {
  allocationStatus: string;
  runStatus: string;
  manifestSealed: boolean;
  snapshotId?: number;
}

// =============================================================================
// Node Executor
// =============================================================================

export const finalizeExecutor: NodeExecutor<FinalizeInput, FinalizeOutput> = {
  name: "finalize",
  idempotent: true,

  async execute(ctx: NodeContext): Promise<FinalizeOutput> {
    throw new Error("not implemented");
  },

  async compensate(ctx: NodeContext): Promise<void> {
    throw new Error("not implemented");
  },
};
