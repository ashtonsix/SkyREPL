// workflow/nodes/drain-allocations.ts - Drain Allocations Node
// Transitions all non-terminal allocations for the instance to terminal states.

import type { NodeExecutor, NodeContext } from "../engine.types";
import { queryMany, type Allocation } from "../../material/db";
import {
  failAllocationAnyState,
} from "../state-transitions";
import { sseManager } from "../../api/sse-protocol";
import type {
  TerminateInstanceInput,
  ValidateInstanceOutput,
  DrainAllocationsOutput,
} from "../../intent/terminate-instance.types";

// =============================================================================
// Node Executor
// =============================================================================

export const drainAllocationsExecutor: NodeExecutor<unknown, DrainAllocationsOutput> = {
  name: "drain-allocations",
  idempotent: true,

  async execute(ctx: NodeContext): Promise<DrainAllocationsOutput> {
    const input = ctx.workflowInput as TerminateInstanceInput;
    const validateOutput = ctx.getNodeOutput("validate-instance") as ValidateInstanceOutput;
    const instanceId = validateOutput?.instanceId ?? input.instanceId;

    // Find all non-terminal allocations for this instance
    const allocations = queryMany<Allocation>(
      "SELECT * FROM allocations WHERE instance_id = ? AND status NOT IN ('COMPLETE', 'FAILED')",
      [instanceId]
    );

    let drained = 0;
    let failed = 0;

    for (const alloc of allocations) {
      try {
        // For ACTIVE allocations, send cancel_run to the agent before failing
        if (alloc.status === "ACTIVE" && alloc.run_id) {
          try {
            await sseManager.sendCommand(String(instanceId), {
              type: "cancel_run",
              command_id: Math.floor(Math.random() * 1000000),
              run_id: alloc.run_id,
            });
          } catch (err) {
            ctx.log("warn", "Failed to send cancel_run SSE command", {
              allocationId: alloc.id,
              runId: alloc.run_id,
              error: err instanceof Error ? err.message : String(err),
            });
          }
        }

        // Fail all non-terminal allocations (AVAILABLE, CLAIMED, ACTIVE -> FAILED)
        const result = failAllocationAnyState(alloc.id);
        if (result.success) {
          drained++;
        } else {
          failed++;
        }
      } catch (err) {
        ctx.log("warn", "Failed to drain allocation", {
          allocationId: alloc.id,
          status: alloc.status,
          error: err instanceof Error ? err.message : String(err),
        });
        failed++;
      }
    }

    ctx.log("info", "Allocations drained", {
      instanceId,
      drained,
      failed,
      total: allocations.length,
    });

    return { drained, failed };
  },

  // No compensation needed — allocation state transitions are one-way
};
