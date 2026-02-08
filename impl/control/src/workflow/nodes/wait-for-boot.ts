// workflow/nodes/wait-for-boot.ts - Wait For Boot Node
// Stub: All function bodies throw "not implemented"

import type { NodeExecutor, NodeContext } from "../engine.types";

// =============================================================================
// Types
// =============================================================================

export interface WaitForBootInput {
  instance_id: number;
  provider: string;
  provider_id: string;
  poll_interval_ms?: number;
  timeout_ms?: number;
}

export interface WaitForBootOutput {
  instance_id: number;
  boot_duration_ms: number;
  ip: string;
}

// =============================================================================
// Node Executor
// =============================================================================

export const waitForBootExecutor: NodeExecutor<WaitForBootInput, WaitForBootOutput> = {
  name: "wait-for-boot",
  idempotent: true,

  async execute(ctx: NodeContext): Promise<WaitForBootOutput> {
    throw new Error("not implemented");
  },

  async compensate(ctx: NodeContext): Promise<void> {
    throw new Error("not implemented");
  },
};
