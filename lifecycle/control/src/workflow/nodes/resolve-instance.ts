// workflow/nodes/resolve-instance.ts - Resolve Instance Node

import type { NodeExecutor, NodeContext } from "../engine.types";
import { getWorkflowNode, findWarmAllocation, type WorkflowNode } from "../../material/db";
import { skipNode, failAllocation } from "../state-transitions";
import { materializeInstance, isTerminalState } from "../../resource/instance";
import type { ResolveInstanceOutput, LaunchRunWorkflowInput } from "../../intent/launch-run.schema";

// =============================================================================
// Types
// =============================================================================

export interface ResolveInstanceInput {
  runId: number;
  spec: string;
  provider: string;
  region?: string;
  initChecksum?: string;
  preferWarmPool: boolean;
}

// Output type re-exported from schema
export type { ResolveInstanceOutput } from "../../intent/launch-run.schema";

// =============================================================================
// Node Executor
// =============================================================================

export const resolveInstanceExecutor: NodeExecutor<ResolveInstanceInput, ResolveInstanceOutput> = {
  name: "resolve-instance",
  idempotent: true,

  async execute(ctx: NodeContext): Promise<ResolveInstanceOutput> {
    const wfInput = ctx.workflowInput as LaunchRunWorkflowInput;

    // Attempt warm pool lookup
    const warmAllocation = findWarmAllocation(
      { spec: wfInput.spec, region: wfInput.region, tenantId: ctx.tenantId },
      wfInput.initChecksum
    );

    if (warmAllocation) {
      // FRESH_STATE: Verify instance is still alive before committing to
      // warm path. Uses forceRefresh to bypass TTL cache — must have
      // definitive provider state before skipping spawn. This catches
      // out-of-band terminations (provider console, spot eviction, admin
      // cleanup) that stale cache wouldn't detect.
      const instance = await materializeInstance(warmAllocation.instance_id, {
        tier: 'decision',
        forceRefresh: true,
      });

      if (!instance || isTerminalState(instance.workflow_state)) {
        // Instance is dead — clean up stale allocation and fall through to cold path
        failAllocation(warmAllocation.id, "AVAILABLE");
        ctx.log("warn", "Warm pool instance terminated out-of-band, falling through to cold path", {
          allocationId: warmAllocation.id,
          instanceId: warmAllocation.instance_id,
        });
      } else {
        // Warm path: skip cold-path nodes (spawn + wait-for-boot)
        const spawnNode = getWorkflowNode(ctx.workflowId, "spawn-instance");
        if (spawnNode && spawnNode.status === "pending") {
          skipNode(spawnNode.id);
        }

        const bootNode = getWorkflowNode(ctx.workflowId, "wait-for-boot");
        if (bootNode && bootNode.status === "pending") {
          skipNode(bootNode.id);
        }

        ctx.log("info", "Resolved instance: warm path", {
          allocationId: warmAllocation.id,
          instanceId: warmAllocation.instance_id,
        });

        return {
          warmAvailable: true,
          allocationId: warmAllocation.id,
          instanceId: warmAllocation.instance_id,
        };
      }
    }

    // Cold path: skip the claim-warm-allocation node
    const claimNode = getWorkflowNode(ctx.workflowId, "claim-warm-allocation");
    if (claimNode && claimNode.status === "pending") {
      skipNode(claimNode.id);
    }

    ctx.log("info", "Resolved instance: cold path (no warm allocation available)");

    return { warmAvailable: false };
  },
};
