// workflow/nodes/validate-instance.ts - Validate Instance Node
// Checks that the target instance exists and acquires a termination lock.

import type { NodeExecutor, NodeContext } from "../engine.types";
import { getInstance, updateInstance, queryMany } from "../../material/db";
import type {
  TerminateInstanceInput,
  ValidateInstanceOutput,
} from "../../intent/terminate-instance";
import type { Allocation } from "../../material/db";
import { NotFoundError, ConflictError } from "@skyrepl/contracts";

// =============================================================================
// Node Executor
// =============================================================================

export const validateInstanceExecutor: NodeExecutor<TerminateInstanceInput, ValidateInstanceOutput> = {
  name: "validate-instance",
  idempotent: true,

  async execute(ctx: NodeContext): Promise<ValidateInstanceOutput> {
    const input = ctx.workflowInput as TerminateInstanceInput;
    const instance = getInstance(input.instanceId);

    if (!instance) {
      throw new NotFoundError("Instance", input.instanceId);
    }

    // Already terminated — skip the entire workflow
    if (instance.workflow_state === "terminate:complete") {
      ctx.log("info", "Instance already terminated, skipping", {
        instanceId: instance.id,
      });
      return {
        instanceId: instance.id,
        provider: instance.provider,
        providerId: instance.provider_id,
        spec: instance.spec,
        region: instance.region,
      };
    }

    // Idempotent: if already in a terminate:* state, don't re-acquire lock
    if (instance.workflow_state.startsWith("terminate:")) {
      ctx.log("info", "Instance already in termination, continuing", {
        instanceId: instance.id,
        currentState: instance.workflow_state,
      });
    } else {
      // Check if termination is blocked by active debug holds
      // NOTE: We only check for debug holds here, not non-terminal allocations.
      // The drain-allocations node will handle draining non-terminal allocations.
      const holdingAllocations = queryMany<Allocation>(
        "SELECT * FROM allocations WHERE instance_id = ? AND status = 'COMPLETE' AND debug_hold_until IS NOT NULL AND debug_hold_until > ?",
        [instance.id, Date.now()]
      );

      if (holdingAllocations.length > 0) {
        throw new ConflictError(
          "Instance termination blocked by active debug holds",
          {
            code: "RESOURCE_BUSY",
            details: {
              instanceId: instance.id,
              holdingAllocations: holdingAllocations.length,
            },
          }
        );
      }

      // Acquire termination lock by transitioning to terminate:draining
      updateInstance(instance.id, {
        workflow_state: "terminate:draining",
      });
      ctx.log("info", "Acquired termination lock", {
        instanceId: instance.id,
        previousState: instance.workflow_state,
      });
    }

    ctx.emitResource("instance", instance.id, 100);

    return {
      instanceId: instance.id,
      provider: instance.provider,
      providerId: instance.provider_id,
      spec: instance.spec,
      region: instance.region,
    };
  },
};
