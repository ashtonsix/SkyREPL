// workflow/nodes/claim-allocation.ts - Claim Allocation Node
// Atomic claim of a warm-pool allocation for a run, with retry on race loss.

import type { NodeExecutor, NodeContext } from "../engine.types";
import { getAllocation } from "../../material/db";
import { claimAllocation, releaseAllocation } from "../state-transitions";
import { TIMING } from "@skyrepl/contracts";
import type {
  ClaimAllocationOutput,
  ResolveInstanceOutput,
  LaunchRunWorkflowInput,
} from "../../intent/launch-run.schema";

// =============================================================================
// Types
// =============================================================================

export interface ClaimAllocationInput {
  runId: number;
  allocationId: number;
  instanceId?: number;
  manifestId: number;
}

// Output type re-exported from schema
export type { ClaimAllocationOutput } from "../../intent/launch-run.schema";

// =============================================================================
// Node Executor
// =============================================================================

export const claimAllocationExecutor: NodeExecutor<ClaimAllocationInput, ClaimAllocationOutput> = {
  name: "claim-allocation",
  idempotent: true,

  async execute(ctx: NodeContext): Promise<ClaimAllocationOutput> {
    const wfInput = ctx.workflowInput as LaunchRunWorkflowInput;
    const resolveOutput = ctx.getNodeOutput("resolve-instance") as ResolveInstanceOutput | null;

    if (!resolveOutput?.allocationId) {
      throw Object.assign(
        new Error("No allocation ID from resolve-instance"),
        { code: "CAPACITY_ERROR", category: "capacity" }
      );
    }

    const allocationId = resolveOutput.allocationId;
    const runId = wfInput.runId;
    const maxRetries = TIMING.WARM_CLAIM_MAX_RETRIES;
    const retryDelay = TIMING.WARM_CLAIM_RETRY_DELAY_MS;

    // Retry atomic claim up to maxRetries times (race lost = another workflow claimed it)
    for (let attempt = 0; attempt < maxRetries; attempt++) {
      const result = claimAllocation(allocationId, runId);
      if (result.success) {
        ctx.emitResource("allocation", result.data.id, 90);
        // Step 13: Also emit the instance so it's tracked in the new manifest.
        // On the warm path, the instance was emitted to the previous workflow's
        // manifest by spawn-instance; re-emitting ensures it's tracked here too.
        ctx.emitResource("instance", result.data.instance_id, 50);

        return {
          allocationId: result.data.id,
          instanceId: result.data.instance_id,
          fromWarmPool: true,
        };
      }

      // If wrong state (already claimed by someone else), not retryable
      if (result.reason === "WRONG_STATE" || result.reason === "NOT_FOUND") {
        break;
      }

      // RACE_LOST: wait and retry
      if (attempt < maxRetries - 1) {
        await new Promise(resolve => setTimeout(resolve, retryDelay));
      }
    }

    // All retries exhausted — fall through to cold path
    throw Object.assign(
      new Error(`Failed to claim warm allocation ${allocationId} after ${maxRetries} attempts`),
      { code: "CAPACITY_ERROR", category: "capacity" }
    );
  },

  async compensate(ctx: NodeContext): Promise<void> {
    const output = ctx.output as ClaimAllocationOutput | undefined;
    if (!output?.allocationId) return;

    const allocation = getAllocation(output.allocationId);
    if (!allocation) return;

    // Only release back to AVAILABLE if still in CLAIMED state
    // (compensation reverses the claim via proper state transition)
    if (allocation.status === "CLAIMED") {
      const result = releaseAllocation(output.allocationId);
      if (result.success) {
        ctx.log("info", "Released claimed allocation back to warm pool", {
          allocationId: output.allocationId,
        });
      } else {
        ctx.log("warn", "Failed to release allocation during compensation (best-effort)", {
          allocationId: output.allocationId,
          reason: result.reason,
        });
      }
    }
  },
};
