// workflow/nodes/load-manifest-resources.ts - Load Manifest Resources Node
// Queries manifest_resources for a given manifest, verifies manifest is SEALED.
// Also resolves provider info for instance resources to enable per-provider fan-out.

import type { NodeExecutor, NodeContext } from "../engine.types";
import { getManifest, getManifestResources, queryOne } from "../../material/db";
import type { CleanupManifestInput } from "../../intent/cleanup-manifest";

// =============================================================================
// Output Type
// =============================================================================

export interface ManifestResourceEntry {
  type: string;
  id: string;
  priority: number;
  /** Provider name, resolved from instances table for instance/allocation resources. Null for non-instance types. */
  provider: string | null;
}

export interface LoadManifestResourcesOutput {
  manifestId: number;
  resourceCount: number;
  resources: ManifestResourceEntry[];
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

    const mappedResources: ManifestResourceEntry[] = resources.map((r) => {
      const entry: ManifestResourceEntry = {
        type: r.resource_type,
        id: r.resource_id,
        priority: r.cleanup_priority ?? manifest.default_cleanup_priority,
        provider: null,
      };

      // Resolve provider for instance resources — needed for per-provider error isolation.
      // Allocation and run resources inherit provider from their parent instance.
      if (r.resource_type === "instance") {
        const inst = queryOne<{ provider: string }>(
          "SELECT provider FROM instances WHERE id = ?",
          [parseInt(r.resource_id, 10)]
        );
        if (inst) entry.provider = inst.provider;
      } else if (r.resource_type === "allocation") {
        // Allocations belong to an instance — resolve via the allocation's instance_id
        const alloc = queryOne<{ instance_id: number }>(
          "SELECT instance_id FROM allocations WHERE id = ?",
          [parseInt(r.resource_id, 10)]
        );
        if (alloc) {
          const inst = queryOne<{ provider: string }>(
            "SELECT provider FROM instances WHERE id = ?",
            [alloc.instance_id]
          );
          if (inst) entry.provider = inst.provider;
        }
      }

      return entry;
    });

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
