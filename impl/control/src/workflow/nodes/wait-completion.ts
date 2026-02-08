// workflow/nodes/wait-completion.ts - Wait Completion Node
// Stub: All function bodies throw "not implemented"

import type { NodeExecutor, NodeContext } from "../engine.types";

// =============================================================================
// Types
// =============================================================================

export interface WaitCompletionInput {
  runId: number;
  instanceId: number;
  allocationId: number;
  maxDurationMs?: number;
}

export interface WaitCompletionOutput {
  exitCode: number;
  completedAt: number;
  durationMs: number;
  spotInterrupted: boolean;
  timedOut: boolean;
}

// =============================================================================
// Node Executor
// =============================================================================

export const waitCompletionExecutor: NodeExecutor<WaitCompletionInput, WaitCompletionOutput> = {
  name: "wait-completion",
  idempotent: false,

  async execute(ctx: NodeContext): Promise<WaitCompletionOutput> {
    throw new Error("not implemented");
  },

  async compensate(ctx: NodeContext): Promise<void> {
    throw new Error("not implemented");
  },
};
