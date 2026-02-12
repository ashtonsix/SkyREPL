// intent/cleanup-manifest.ts - Cleanup Manifest Intent
// Handles garbage collection of expired manifests by deleting owned resources
// in priority order, then deleting the manifest itself.

import { getManifest, type Workflow, getWorkflow } from "../material/db";
import type { WorkflowBlueprint, NodeExecutor } from "../workflow/engine.types";
import { submit, registerBlueprint, registerNodeExecutor } from "../workflow/engine";
import type { CleanupManifestInput, CleanupManifestOutput } from "./cleanup-manifest.types";

// Node executor imports
import { loadManifestResourcesExecutor } from "../workflow/nodes/load-manifest-resources";
import { sortAndGroupExecutor } from "../workflow/nodes/sort-and-group";
import { cleanupResourcesExecutor } from "../workflow/nodes/cleanup-resources";
import { deleteManifestExecutor } from "../workflow/nodes/delete-manifest";

// =============================================================================
// Entry Point
// =============================================================================

export async function cleanupManifest(input: CleanupManifestInput): Promise<Workflow> {
  // Validate manifest exists before submitting workflow
  const manifest = getManifest(input.manifestId);
  if (!manifest) {
    throw Object.assign(new Error(`Manifest ${input.manifestId} not found`), {
      code: "NOT_FOUND",
      category: "not_found",
    });
  }

  // Submit the workflow
  const result = await submit({
    type: "cleanup-manifest",
    input: { manifestId: input.manifestId } as unknown as Record<string, unknown>,
  });

  // Return the workflow record
  return getWorkflow(result.workflowId)!;
}

// =============================================================================
// Workflow Blueprint
// =============================================================================

export const cleanupManifestBlueprint: WorkflowBlueprint = {
  type: "cleanup-manifest",
  entryNode: "load-manifest-resources",
  nodes: {
    "load-manifest-resources": {
      type: "load-manifest-resources",
    },
    "sort-and-group": {
      type: "sort-and-group",
      dependsOn: ["load-manifest-resources"],
    },
    "cleanup-resources": {
      type: "cleanup-resources",
      dependsOn: ["sort-and-group"],
    },
    "delete-manifest": {
      type: "delete-manifest",
      dependsOn: ["cleanup-resources"],
    },
  },
};

// =============================================================================
// Registration
// =============================================================================

export function registerCleanupManifest(): void {
  registerBlueprint(cleanupManifestBlueprint);
  registerNodeExecutor(loadManifestResourcesExecutor);
  registerNodeExecutor(sortAndGroupExecutor);
  registerNodeExecutor(cleanupResourcesExecutor);
  registerNodeExecutor(deleteManifestExecutor);
}
