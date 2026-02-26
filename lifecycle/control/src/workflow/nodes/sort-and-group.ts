// workflow/nodes/sort-and-group.ts - Sort and Group Resources Node
// Groups resources by provider, then by type within each provider group.
// Sorts by cleanup priority (highest first). Provider grouping enables
// per-provider error isolation in cleanup-resources.

import type { NodeExecutor, NodeContext } from "../engine.types";
import type { LoadManifestResourcesOutput, ManifestResourceEntry } from "./load-manifest-resources";

// =============================================================================
// Output Types
// =============================================================================

export interface ResourceGroup {
  type: string;
  priority: number;
  resourceIds: string[];
}

export interface ProviderResourceGroup {
  /** Provider name, or "_unknown" for resources without a resolved provider. */
  provider: string;
  /** Resource groups within this provider, sorted by priority descending. */
  groups: ResourceGroup[];
}

export interface SortAndGroupOutput {
  /** Per-provider resource groups. Each provider's failure is isolated. */
  providerGroups: ProviderResourceGroup[];
  /**
   * Flat groups (all providers merged) for backward compatibility.
   * cleanup-resources reads this when providerGroups is not available.
   * @deprecated Use providerGroups for per-provider error isolation.
   */
  groups: ResourceGroup[];
}

// =============================================================================
// Node Executor
// =============================================================================

export const sortAndGroupExecutor: NodeExecutor<unknown, SortAndGroupOutput> = {
  name: "sort-and-group",
  idempotent: true,

  async execute(ctx: NodeContext): Promise<SortAndGroupOutput> {
    const loadOutput = ctx.getNodeOutput("load-manifest-resources") as LoadManifestResourcesOutput | null;
    if (!loadOutput) {
      throw Object.assign(
        new Error("load-manifest-resources output not available"),
        { code: "INTERNAL_ERROR", category: "internal" }
      );
    }

    // -------------------------------------------------------------------------
    // 1. Build flat groups (backward compat)
    // -------------------------------------------------------------------------
    const flatGroupMap = new Map<string, { priority: number; resourceIds: string[] }>();

    for (const resource of loadOutput.resources) {
      const existing = flatGroupMap.get(resource.type);
      if (existing) {
        existing.resourceIds.push(resource.id);
        if (resource.priority > existing.priority) {
          existing.priority = resource.priority;
        }
      } else {
        flatGroupMap.set(resource.type, {
          priority: resource.priority,
          resourceIds: [resource.id],
        });
      }
    }

    const groups: ResourceGroup[] = Array.from(flatGroupMap.entries())
      .map(([type, data]) => ({
        type,
        priority: data.priority,
        resourceIds: data.resourceIds,
      }))
      .sort((a, b) => b.priority - a.priority);

    // -------------------------------------------------------------------------
    // 2. Build per-provider groups
    // -------------------------------------------------------------------------
    const providerMap = new Map<string, ManifestResourceEntry[]>();

    for (const resource of loadOutput.resources) {
      const providerKey = resource.provider ?? "_unknown";
      const existing = providerMap.get(providerKey);
      if (existing) {
        existing.push(resource);
      } else {
        providerMap.set(providerKey, [resource]);
      }
    }

    const providerGroups: ProviderResourceGroup[] = [];
    for (const [provider, resources] of providerMap) {
      // Group by type within this provider
      const typeMap = new Map<string, { priority: number; resourceIds: string[] }>();
      for (const r of resources) {
        const existing = typeMap.get(r.type);
        if (existing) {
          existing.resourceIds.push(r.id);
          if (r.priority > existing.priority) existing.priority = r.priority;
        } else {
          typeMap.set(r.type, { priority: r.priority, resourceIds: [r.id] });
        }
      }

      const providerResourceGroups: ResourceGroup[] = Array.from(typeMap.entries())
        .map(([type, data]) => ({
          type,
          priority: data.priority,
          resourceIds: data.resourceIds,
        }))
        .sort((a, b) => b.priority - a.priority);

      providerGroups.push({ provider, groups: providerResourceGroups });
    }

    ctx.log("info", "Resources grouped and sorted", {
      providerCount: providerGroups.length,
      providers: providerGroups.map(pg => ({
        provider: pg.provider,
        groupCount: pg.groups.length,
        totalResources: pg.groups.reduce((sum, g) => sum + g.resourceIds.length, 0),
      })),
      groupCount: groups.length,
      groups: groups.map((g) => ({ type: g.type, count: g.resourceIds.length, priority: g.priority })),
    });

    return { providerGroups, groups };
  },
};
