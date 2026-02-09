// workflow/nodes/claim-allocation.ts - Claim Allocation Node
// CAS-based claim of a warm-pool allocation for a run.

import type { NodeExecutor, NodeContext } from "../engine.types";
import { getAllocation, updateAllocationStatus } from "../../material/db";
import { claimAllocation } from "../state-transitions";

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
    const input = ctx.input as ClaimAllocationInput;

    // Use CAS-based claim from state-transitions
    const result = claimAllocation(input.allocationId, input.runId);
    if (!result.success) {
      throw Object.assign(
        new Error(`Failed to claim allocation ${input.allocationId}: ${result.reason}`),
        { code: "CAPACITY_UNAVAILABLE", category: "capacity" }
      );
    }

    ctx.emitResource("allocation", result.data.id, 90);

    return {
      allocationId: result.data.id,
      instanceId: result.data.instance_id,
      fromWarmPool: true,
    };
  },

  async compensate(ctx: NodeContext): Promise<void> {
    const output = ctx.output as ClaimAllocationOutput | undefined;
    if (!output?.allocationId) return;

    const allocation = getAllocation(output.allocationId);
    if (!allocation) return;

    // Only release back to AVAILABLE if still in CLAIMED state
    // (compensation reverses the claim)
    if (allocation.status === "CLAIMED") {
      const { execute } = await import("../../material/db");
      const now = Date.now();
      execute(
        "UPDATE allocations SET status = ?, run_id = NULL, manifest_id = NULL, updated_at = ? WHERE id = ?",
        ["AVAILABLE", now, output.allocationId]
      );
      ctx.log("info", "Released claimed allocation back to warm pool", {
        allocationId: output.allocationId,
      });
    }
  },
};
