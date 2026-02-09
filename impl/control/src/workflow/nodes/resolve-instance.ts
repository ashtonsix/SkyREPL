// workflow/nodes/resolve-instance.ts - Resolve Instance Node

import type { NodeExecutor, NodeContext } from "../engine.types";
import { queryOne, type WorkflowNode } from "../../material/db";
import { skipNode } from "../state-transitions";

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

export interface ResolveInstanceOutput {
  warmAvailable: boolean;
  allocationId?: number;
  instanceId?: number;
}

// =============================================================================
// Node Executor
// =============================================================================

export const resolveInstanceExecutor: NodeExecutor<ResolveInstanceInput, ResolveInstanceOutput> = {
  name: "resolve-instance",
  idempotent: true,

  async execute(ctx: NodeContext): Promise<ResolveInstanceOutput> {
    // Slice 1: always cold path (no warm pool)
    // Skip the claim-warm-allocation node so the DAG proceeds on cold path
    const claimNode = queryOne<WorkflowNode>(
      "SELECT * FROM workflow_nodes WHERE workflow_id = ? AND node_id = ?",
      [ctx.workflowId, "claim-warm-allocation"]
    );
    if (claimNode && claimNode.status === "pending") {
      skipNode(claimNode.id);
    }

    ctx.log("info", "Resolved instance: cold path (no warm pool in Slice 1)");

    return { warmAvailable: false };
  },
};
