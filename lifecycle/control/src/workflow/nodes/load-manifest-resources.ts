// workflow/nodes/load-manifest-resources.ts - Load Manifest Resources Node
// Queries manifest_resources for a given manifest, verifies manifest is SEALED.

import type { NodeExecutor, NodeContext } from "../engine.types";
import { getManifest, getManifestResources } from "../../material/db";
import type { CleanupManifestInput } from "../../intent/cleanup-manifest";

// =============================================================================
// Output Type
// =============================================================================

export interface LoadManifestResourcesOutput {
  manifestId: number;
  resourceCount: number;
  resources: Array<{ type: string; id: string; priority: number }>;
}

// =============================================================================
// Node Executor
// =============================================================================

export const loadManifestResourcesExecutor: NodeExecutor<unknown, LoadManifestResourcesOutput> = {
  name: "load-manifest-resources",
  idempotent: true,

  async execute(ctx: NodeContext): Promise<LoadManifestResourcesOutput> {
    const wfInput = ctx.workflowInput as CleanupManifestInput;
    const manifestId = wfInput.manifestId;

    // Verify manifest exists
    const manifest = getManifest(manifestId);
    if (!manifest) {
      throw Object.assign(new Error(`Manifest ${manifestId} not found`), {
        code: "NOT_FOUND",
        category: "not_found",
      });
    }

    // Verify manifest is SEALED
    if (manifest.status !== "SEALED") {
      throw Object.assign(
        new Error(`Cannot cleanup manifest ${manifestId}: status is ${manifest.status}, expected SEALED`),
        {
          code: "VALIDATION_ERROR",
          category: "validation",
        }
      );
    }

    // Load resources ordered by cleanup_priority DESC
    const resources = getManifestResources(manifestId);

    const mappedResources = resources.map((r) => ({
      type: r.resource_type,
      id: r.resource_id,
      priority: r.cleanup_priority ?? manifest.default_cleanup_priority,
    }));

    ctx.log("info", "Loaded manifest resources for cleanup", {
      manifestId,
      resourceCount: mappedResources.length,
    });

    return {
      manifestId,
      resourceCount: mappedResources.length,
      resources: mappedResources,
    };
  },
};
