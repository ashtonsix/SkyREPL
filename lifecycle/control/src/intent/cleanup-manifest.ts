// intent/cleanup-manifest.ts - Cleanup Manifest Intent
// Handles garbage collection of expired manifests by deleting owned resources
// in priority order, then deleting the manifest itself.

import { getManifest, type Workflow, getWorkflow, queryOne, listExpiredManifests, updateWorkflow, deleteManifest } from "../material/db"; // raw-db: workflow-by-manifest dedup check (Bucket B), see WL-057
import type { WorkflowBlueprint, NodeExecutor } from "../workflow/engine.types";
import { submit, registerBlueprint, registerNodeExecutor } from "../workflow/engine";

// =============================================================================
// Cleanup Manifest Input
// =============================================================================

export interface CleanupManifestInput {
  /** ID of the manifest to clean up */
  manifestId: number;
}

// =============================================================================
// Cleanup Manifest Output
// =============================================================================

export interface CleanupManifestOutput {
  /** ID of the manifest that was cleaned up */
  manifestId: number;
  /** Number of resources cleaned */
  resourcesCleaned: number;
  /** Whether the manifest was deleted */
  deleted: boolean;
}

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

  // Validate manifest is SEALED (SD-G1-09: reject DRAFT manifests early)
  if (manifest.status !== "SEALED") {
    throw Object.assign(
      new Error(`Manifest ${input.manifestId} status is ${manifest.status}, expected SEALED`),
      {
        code: "INVALID_STATE",
        category: "validation",
      }
    );
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

// Current: linear chain with per-provider error isolation inside cleanup-resources.
//   load-manifest-resources → sort-and-group → cleanup-resources → delete-manifest
//
// sort-and-group produces providerGroups[], and cleanup-resources iterates them
// sequentially with error isolation (one provider's failure doesn't block others).
//
// PFO upgrade path (§7.4): Replace cleanup-resources with a runtime-dynamic PFO:
//   load-manifest-resources → sort-and-group → PFO(provider-cleanup branches) → delete-manifest
// This requires a "setup" node (like resolve-instance in launch-run) that reads
// providerGroups from sort-and-group output and calls applyParallelFanOut to
// create one branch per provider. Each branch handles that provider's resources
// in priority order. delete-manifest becomes the PFO join node.
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

// =============================================================================
// Background: Manifest Cleanup Check
// =============================================================================

export async function manifestCleanupCheck(): Promise<void> {
  const now = Date.now();
  const expiredManifests = listExpiredManifests(now);

  const MAX_CLEANUPS_PER_CYCLE = 10;
  let spawned = 0;

  for (const manifest of expiredManifests) {
    if (spawned >= MAX_CLEANUPS_PER_CYCLE) break;

    // SQL dedup guard: check for active cleanup workflows for this manifest.
    //
    // Why SQL dedup instead of workflows.idempotency_key:
    // The workflow-level idempotency_key uses a UNIQUE index with no TTL —
    // once a workflow completes (even if it failed), a second submit() with
    // the same key returns the stale result forever. For background cleanup
    // we need re-submission after failure, so the SQL status guard is correct:
    // it only blocks while a cleanup is actively in-flight.
    //
    // manifest_id on cleanup-manifest workflows is set to the target manifest's
    // ID (not the workflow's own resource-tracking manifest) so this query is
    // exact, parameterized, and immune to the manifest_id=1 / 10 / 11 prefix
    // collision that LIKE-based substring matching would produce.
    const existing = queryOne<{ id: number }>(
      `SELECT id FROM workflows WHERE type = 'cleanup-manifest' AND manifest_id = ? AND status IN ('pending','running','cancelling')`,
      [manifest.id]
    );
    if (existing) {
      continue; // Skip — cleanup already in progress
    }

    try {
      const result = await submit({
        type: "cleanup-manifest",
        input: { manifestId: manifest.id },
      });

      // Point manifest_id at the *target* manifest so the query above works.
      // submit() auto-creates a fresh DRAFT manifest for resource tracking; we
      // don't need it here (cleanup workflows emit no resources of their own),
      // so retrieve its id, redirect the FK, then delete the orphaned DRAFT.
      const cleanupWorkflow = getWorkflow(result.workflowId);
      if (cleanupWorkflow) {
        const autoDraftManifestId = cleanupWorkflow.manifest_id;
        updateWorkflow(result.workflowId, { manifest_id: manifest.id });
        if (autoDraftManifestId !== null && autoDraftManifestId !== manifest.id) {
          try {
            deleteManifest(autoDraftManifestId);
          } catch {
            // Best effort — orphaned DRAFT manifests are harmless but logged
            console.warn(`[manifest-cleanup] Could not delete auto-draft manifest ${autoDraftManifestId} for cleanup workflow ${result.workflowId}`);
          }
        }
      }

      spawned++;
    } catch (err) {
      console.warn(`[manifest-cleanup] Failed to submit cleanup for manifest ${manifest.id}:`, err);
    }
  }

  if (spawned > 0) {
    console.log(`[manifest-cleanup] Spawned ${spawned} cleanup workflow(s) for expired manifests`);
  }
}
