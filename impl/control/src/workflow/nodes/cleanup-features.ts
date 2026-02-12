// workflow/nodes/cleanup-features.ts - Cleanup Features Node
// Stub: Features not yet implemented. Will detach feature providers in future.

import type { NodeExecutor, NodeContext } from "../engine.types";
import type { CleanupFeaturesOutput } from "../../intent/terminate-instance.types";

// =============================================================================
// Node Executor
// =============================================================================

export const cleanupFeaturesExecutor: NodeExecutor<unknown, CleanupFeaturesOutput> = {
  name: "cleanup-features",
  idempotent: true,

  async execute(ctx: NodeContext): Promise<CleanupFeaturesOutput> {
    ctx.log("info", "Feature cleanup not implemented, skipping");
    return { featuresCleanedUp: 0 };
  },
};
