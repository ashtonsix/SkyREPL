// workflow/nodes/delete-manifest.ts - Delete Manifest Node
// Final cleanup step: deletes the manifest and its manifest_resources records.

import type { NodeExecutor, NodeContext } from "../engine.types";
import { getManifest, deleteManifest, execute, updateWorkflow } from "../../material/db";
import type { CleanupManifestInput } from "../../intent/cleanup-manifest";

// =============================================================================
// Output Type
// =============================================================================

export interface DeleteManifestOutput {
  deleted: boolean;
  manifestId: number;
}

// =============================================================================
// Node Executor
// =============================================================================

export const deleteManifestExecutor: NodeExecutor<unknown, DeleteManifestOutput> = {
  name: "delete-manifest",
  idempotent: true,

  async execute(ctx: NodeContext): Promise<DeleteManifestOutput> {
    const wfInput = ctx.workflowInput as CleanupManifestInput;
    const manifestId = wfInput.manifestId;

    // Check if manifest still exists (may have been deleted by another process)
    const manifest = getManifest(manifestId);
    if (!manifest) {
      ctx.log("info", "Manifest already deleted, skipping", { manifestId });
      return { deleted: true, manifestId };
    }

    // Clean up manifest_cleanup_state if it exists
    try {
      execute(
        "DELETE FROM manifest_cleanup_state WHERE manifest_id = ?",
        [manifestId]
      );
    } catch {
      // Best effort - table might not have an entry
    }

    // Null ALL FKs referencing this manifest before deleting it,
    // otherwise SQLite's FK constraint fires (no ON DELETE SET NULL).
    execute(
      "UPDATE workflows SET manifest_id = NULL WHERE manifest_id = ?",
      [manifestId]
    );
    execute(
      "UPDATE runs SET current_manifest_id = NULL WHERE current_manifest_id = ?",
      [manifestId]
    );

    // Delete manifest and its resources
    deleteManifest(manifestId);

    ctx.log("info", "Manifest deleted", { manifestId });

    return { deleted: true, manifestId };
  },
};
