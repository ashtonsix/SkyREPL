// workflow/nodes/resolve-instance.ts - Resolve Instance Node

import type { NodeExecutor, NodeContext } from "../engine.types";
import { queryOne, findWarmAllocation, type WorkflowNode } from "../../material/db";
import { skipNode } from "../state-transitions";
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
      // Warm path: skip cold-path nodes (spawn + wait-for-boot)
      const spawnNode = queryOne<WorkflowNode>(
        "SELECT * FROM workflow_nodes WHERE workflow_id = ? AND node_id = ?",
        [ctx.workflowId, "spawn-instance"]
      );
      if (spawnNode && spawnNode.status === "pending") {
        skipNode(spawnNode.id);
      }

      const bootNode = queryOne<WorkflowNode>(
        "SELECT * FROM workflow_nodes WHERE workflow_id = ? AND node_id = ?",
        [ctx.workflowId, "wait-for-boot"]
      );
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

    // Cold path: skip the claim-warm-allocation node
    const claimNode = queryOne<WorkflowNode>(
      "SELECT * FROM workflow_nodes WHERE workflow_id = ? AND node_id = ?",
      [ctx.workflowId, "claim-warm-allocation"]
    );
    if (claimNode && claimNode.status === "pending") {
      skipNode(claimNode.id);
    }

    ctx.log("info", "Resolved instance: cold path (no warm allocation available)");

    return { warmAvailable: false };
  },
};
