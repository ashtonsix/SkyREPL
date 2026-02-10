// workflow/nodes/create-allocation.ts - Create Allocation Node
// Creates an allocation record binding a run to an instance.

import type { NodeExecutor, NodeContext } from "../engine.types";
import {
  createAllocation,
  getAllocation,
  addResourceToManifest,
} from "../../material/db";
import { failAllocation } from "../state-transitions";
import type {
  CreateAllocationOutput,
  LaunchRunWorkflowInput,
  ClaimAllocationOutput,
  SpawnInstanceOutput,
  WaitForBootOutput,
} from "../../intent/launch-run.schema";

// =============================================================================
// Types
// =============================================================================

export interface CreateAllocationInput {
  runId: number;
  instanceId: number;
  manifestId: number;
  workdir?: string;
}

// Output type re-exported from schema
export type { CreateAllocationOutput } from "../../intent/launch-run.schema";

// =============================================================================
// Node Executor
// =============================================================================

export const createAllocationExecutor: NodeExecutor<CreateAllocationInput, CreateAllocationOutput> = {
  name: "create-allocation",
  idempotent: true,

  async execute(ctx: NodeContext): Promise<CreateAllocationOutput> {
    const input = ctx.workflowInput as CreateAllocationInput;

    // Check if warm path already provided an allocation
    const warmOutput = ctx.getNodeOutput("claim-allocation") as ClaimAllocationOutput | null;
    if (warmOutput?.allocationId) {
      const existing = getAllocation(warmOutput.allocationId);
      if (existing) {
        return {
          allocationId: existing.id,
          instanceId: existing.instance_id,
          workdir: existing.workdir || "/home/user",
        };
      }
    }

    // Cold path: get instanceId from spawn or boot output
    const spawnOutput = ctx.getNodeOutput("spawn-instance") as SpawnInstanceOutput | null;
    const bootOutput = ctx.getNodeOutput("wait-for-boot") as WaitForBootOutput | null;
    const instanceId =
      input.instanceId ||
      spawnOutput?.instanceId ||
      bootOutput?.instanceId;
    if (!instanceId) {
      throw new Error("No instanceId available from upstream nodes");
    }

    const workdir = input.workdir || `/home/user/run-${input.runId}`;

    const allocation = createAllocation({
      run_id: input.runId,
      instance_id: instanceId,
      status: "CLAIMED",
      current_manifest_id: ctx.manifestId,
      user: "default", // Slice 1: single-user
      workdir,
      debug_hold_until: null,
      completed_at: null,
    });

    // Emit resources to manifest for lifecycle tracking
    addResourceToManifest(ctx.manifestId, "allocation", String(allocation.id), {
      cleanupPriority: 90,
    });
    addResourceToManifest(ctx.manifestId, "run", String(input.runId), {
      cleanupPriority: 80,
    });

    ctx.log("info", "Allocation created", {
      allocationId: allocation.id,
      instanceId,
      runId: input.runId,
      workdir,
    });

    return {
      allocationId: allocation.id,
      instanceId,
      workdir,
    };
  },

  async compensate(ctx: NodeContext): Promise<void> {
    const output = ctx.output as CreateAllocationOutput | undefined;
    if (!output?.allocationId) return;

    // Best-effort: fail the allocation during rollback
    const allocation = getAllocation(output.allocationId);
    if (allocation && allocation.status === "CLAIMED") {
      failAllocation(output.allocationId, "CLAIMED");
      ctx.log("info", "Allocation failed during compensation", {
        allocationId: output.allocationId,
      });
    }
  },
};
