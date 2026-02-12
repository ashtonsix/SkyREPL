// workflow/nodes/sort-and-group.ts - Sort and Group Resources Node
// Groups resources by type and sorts groups by cleanup priority (highest first).

import type { NodeExecutor, NodeContext } from "../engine.types";
import type { LoadManifestResourcesOutput } from "./load-manifest-resources";

// =============================================================================
// Output Type
// =============================================================================

export interface ResourceGroup {
  type: string;
  priority: number;
  resourceIds: string[];
}

export interface SortAndGroupOutput {
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

    // Group resources by type
    const groupMap = new Map<string, { priority: number; resourceIds: string[] }>();

    for (const resource of loadOutput.resources) {
      const existing = groupMap.get(resource.type);
      if (existing) {
        existing.resourceIds.push(resource.id);
        // Use the highest priority within the group
        if (resource.priority > existing.priority) {
          existing.priority = resource.priority;
        }
      } else {
        groupMap.set(resource.type, {
          priority: resource.priority,
          resourceIds: [resource.id],
        });
      }
    }

    // Convert to array and sort by priority descending (highest first)
    const groups: ResourceGroup[] = Array.from(groupMap.entries())
      .map(([type, data]) => ({
        type,
        priority: data.priority,
        resourceIds: data.resourceIds,
      }))
      .sort((a, b) => b.priority - a.priority);

    ctx.log("info", "Resources grouped and sorted", {
      groupCount: groups.length,
      groups: groups.map((g) => ({ type: g.type, count: g.resourceIds.length, priority: g.priority })),
    });

    return { groups };
  },
};
