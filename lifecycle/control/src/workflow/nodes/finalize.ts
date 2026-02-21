// workflow/nodes/finalize.ts - Finalize Node
// Completes allocation, updates run, replenishes warm pool.
// Manifest sealing is handled by handleWorkflowComplete in engine.ts.

import type { NodeExecutor, NodeContext } from "../engine.types";
import { updateRunRecord } from "../../resource/run";
import { getInstanceRecord } from "../../resource/instance";
import {
  getAllocation,
  createAllocation,
  countInstanceAllocations,
} from "../../material/db";
import { completeAllocation } from "../state-transitions";
import { getProvider } from "../../provider/registry";
import type { ProviderName } from "../../provider/types";
import { TIMING } from "@skyrepl/contracts";
import type {
  FinalizeOutput,
  LaunchRunWorkflowInput,
  CreateAllocationOutput,
  WaitCompletionOutput,
} from "../../intent/launch-run.schema";

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

// Output type re-exported from schema
export type { FinalizeOutput } from "../../intent/launch-run.schema";

// =============================================================================
// Node Executor
// =============================================================================

export const finalizeExecutor: NodeExecutor<FinalizeInput, FinalizeOutput> = {
  name: "finalize",
  idempotent: true,

  async execute(ctx: NodeContext): Promise<FinalizeOutput> {
    const wfInput = ctx.workflowInput as LaunchRunWorkflowInput;
    const allocOutput = ctx.getNodeOutput("create-allocation") as CreateAllocationOutput | null;
    const completionOutput = ctx.getNodeOutput("await-completion") as WaitCompletionOutput | null;
    if (!allocOutput) {
      throw new Error("create-allocation output not available");
    }
    const input: FinalizeInput = {
      runId: wfInput.runId,
      instanceId: allocOutput.instanceId,
      allocationId: allocOutput.allocationId,
      manifestId: ctx.manifestId!,
      exitCode: completionOutput?.exitCode ?? 1,
      spotInterrupted: completionOutput?.spotInterrupted ?? false,
      holdDurationMs: wfInput.holdDurationMs,
      createSnapshot: wfInput.createSnapshot,
      initChecksum: wfInput.initChecksum,
    };

    // Complete allocation: ACTIVE -> COMPLETE (idempotent: skip if already complete - EX8 fix)
    const allocation = getAllocation(input.allocationId);
    let completeResult: { success: boolean; reason?: string };
    if (allocation?.status === "COMPLETE") {
      // Already completed (idempotent retry)
      completeResult = { success: true };
    } else {
      const debugHoldUntil = input.holdDurationMs
        ? Date.now() + input.holdDurationMs
        : undefined;
      completeResult = completeAllocation(input.allocationId, {
        debugHoldUntil,
      });
    }

    // Update run final state
    updateRunRecord(input.runId, {
      workflow_state: "launch-run:finalized",
    });

    // Manifest sealing is handled by handleWorkflowComplete in engine.ts
    // using sealManifestSafe which applies the correct workflow-type retention
    // policy (Step 10: finalize retention policy fix).
    const manifestSealed = false;

    // Conditional snapshot (deferred for Slice 1 -- subworkflow not yet available)
    let snapshotId: number | undefined;
    // if (input.createSnapshot && input.exitCode === 0) { ... }

    // Replenishment: try to recycle the instance into the warm pool
    let instanceTerminated = false;
    const replenished = tryReplenishWarmPool(ctx, input);

    if (!replenished) {
      // No replenishment — terminate instance
      try {
        const instance = getInstanceRecord(input.instanceId);
        if (instance?.provider_id) {
          const provider = await getProvider(instance.provider as ProviderName);
          await provider.terminate(instance.provider_id);
          instanceTerminated = true;
        }
      } catch (err) {
        ctx.log("warn", "Failed to terminate instance during finalize", {
          instanceId: input.instanceId,
          error: String(err),
        });
      }
    }

    const allocationStatus = completeResult.success ? "COMPLETE" : "FAILED";

    ctx.log("info", "Run finalized", {
      runId: input.runId,
      allocationId: input.allocationId,
      allocationStatus,
      manifestSealed,
      instanceTerminated,
      replenished,
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

// =============================================================================
// Replenishment
// =============================================================================

/**
 * After run completion, check if this instance can be recycled into the warm pool.
 * Conditions for replenishment:
 *   1. Instance is healthy (heartbeat within stale threshold)
 *   2. Instance has fewer than MAX_ALLOCATIONS_PER_INSTANCE active allocations
 *   3. Instance was not spot-interrupted
 *
 * If eligible, creates a new AVAILABLE allocation for the instance.
 * Returns true if replenishment succeeded.
 */
function tryReplenishWarmPool(ctx: NodeContext, input: FinalizeInput): boolean {
  try {
    // Don't replenish spot-interrupted instances
    if (input.spotInterrupted) {
      ctx.log("debug", "Skipping replenishment: spot interrupted");
      return false;
    }

    const instance = getInstanceRecord(input.instanceId);
    if (!instance) {
      ctx.log("debug", "Skipping replenishment: instance not found");
      return false;
    }

    // Check heartbeat freshness (within stale detection window)
    const now = Date.now();
    if (now - instance.last_heartbeat > TIMING.STALE_DETECTION_MS) {
      ctx.log("debug", "Skipping replenishment: stale heartbeat", {
        instanceId: input.instanceId,
        lastHeartbeat: instance.last_heartbeat,
      });
      return false;
    }

    // Check allocation count
    const activeCount = countInstanceAllocations(input.instanceId);
    if (activeCount >= TIMING.MAX_ALLOCATIONS_PER_INSTANCE) {
      ctx.log("debug", "Skipping replenishment: max allocations reached", {
        instanceId: input.instanceId,
        activeCount,
        max: TIMING.MAX_ALLOCATIONS_PER_INSTANCE,
      });
      return false;
    }

    // Create new AVAILABLE allocation for this instance
    createAllocation({
      run_id: null,
      instance_id: input.instanceId,
      status: "AVAILABLE",
      current_manifest_id: null,
      user: "default",
      workdir: "/workspace",
      debug_hold_until: null,
      completed_at: null,
    });

    ctx.log("info", "Replenished warm pool", {
      instanceId: input.instanceId,
      spec: instance.spec,
      region: instance.region,
    });

    return true;
  } catch (err) {
    ctx.log("warn", "Failed to replenish warm pool", {
      instanceId: input.instanceId,
      error: String(err),
    });
    return false;
  }
}
