// workflow/nodes/finalize.ts - Finalize Node
// Completes allocation, updates run, seals manifest.

import type { NodeExecutor, NodeContext } from "../engine.types";
import { updateRun, sealManifest } from "../../material/db";
import { completeAllocation } from "../state-transitions";

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
    const input = ctx.workflowInput as FinalizeInput;

    // Complete allocation: ACTIVE -> COMPLETE
    const debugHoldUntil = input.holdDurationMs
      ? Date.now() + input.holdDurationMs
      : undefined;
    const completeResult = completeAllocation(input.allocationId, {
      debugHoldUntil,
    });

    // Update run final state
    updateRun(input.runId, {
      workflow_state: "launch-run:finalized",
    });

    // Seal manifest
    let manifestSealed = false;
    try {
      sealManifest(input.manifestId);
      manifestSealed = true;
    } catch (err) {
      ctx.log("warn", "Failed to seal manifest", {
        manifestId: input.manifestId,
        error: String(err),
      });
    }

    // Conditional snapshot (deferred for Slice 1 -- subworkflow not yet available)
    let snapshotId: number | undefined;
    // if (input.createSnapshot && input.exitCode === 0) { ... }

    const allocationStatus = completeResult.success ? "COMPLETE" : "FAILED";

    ctx.log("info", "Run finalized", {
      runId: input.runId,
      allocationId: input.allocationId,
      allocationStatus,
      manifestSealed,
      exitCode: input.exitCode,
    });

    return {
      allocationStatus,
      runStatus: "finalized",
      manifestSealed,
      snapshotId,
    };
  },

  async compensate(ctx: NodeContext): Promise<void> {
    // Finalize is a terminal node -- no compensation needed.
    // Allocation and run are already in terminal states.
  },
};
