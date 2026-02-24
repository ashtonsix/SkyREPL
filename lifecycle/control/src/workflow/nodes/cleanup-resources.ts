// workflow/nodes/cleanup-resources.ts - Cleanup Resources Node
// Processes each resource group in priority order, calling appropriate cleanup handlers.

import type { NodeExecutor, NodeContext } from "../engine.types";
import {
  execute,
  queryOne,
  updateInstance,
  updateRun,
} from "../../material/db"; // raw-db: boutique queries (partial-column existence checks, cleanup-specific deletes), see WL-057
import { getProvider } from "../../provider/registry";
import type { ProviderName } from "../../provider/types";
import type { CleanupManifestInput } from "../../intent/cleanup-manifest";
import type { SortAndGroupOutput, ResourceGroup } from "./sort-and-group";

// =============================================================================
// Output Type
// =============================================================================

export interface CleanupResourcesOutput {
  cleaned: number;
  skipped: number;
  failed: number;
}

// =============================================================================
// Node Executor
// =============================================================================

export const cleanupResourcesExecutor: NodeExecutor<unknown, CleanupResourcesOutput> = {
  name: "cleanup-resources",
  idempotent: true,

  async execute(ctx: NodeContext): Promise<CleanupResourcesOutput> {
    const wfInput = ctx.workflowInput as CleanupManifestInput;
    const sortOutput = ctx.getNodeOutput("sort-and-group") as SortAndGroupOutput | null;
    if (!sortOutput) {
      throw Object.assign(
        new Error("sort-and-group output not available"),
        { code: "INTERNAL_ERROR", category: "internal" }
      );
    }

    let cleaned = 0;
    let skipped = 0;
    let failed = 0;

    for (const group of sortOutput.groups) {
      const result = await cleanupGroup(ctx, wfInput.manifestId, group);
      cleaned += result.cleaned;
      skipped += result.skipped;
      failed += result.failed;
    }

    ctx.log("info", "Resource cleanup complete", {
      manifestId: wfInput.manifestId,
      cleaned,
      skipped,
      failed,
    });

    return { cleaned, skipped, failed };
  },
};

// =============================================================================
// Cleanup Group Handler
// =============================================================================

async function cleanupGroup(
  ctx: NodeContext,
  manifestId: number,
  group: ResourceGroup
): Promise<{ cleaned: number; skipped: number; failed: number }> {
  let cleaned = 0;
  let skipped = 0;
  let failed = 0;

  for (const resourceId of group.resourceIds) {
    try {
      const result = await cleanupResource(ctx, group.type, resourceId);
      if (result === "cleaned") {
        cleaned++;
      } else {
        skipped++;
      }
      // Mark cleanup_processed_at for this resource
      markProcessed(manifestId, group.type, resourceId);
    } catch (err) {
      const error = err as { code?: string; message?: string };
      if (error.code === "NOT_FOUND") {
        // Resource already deleted by another process
        ctx.log("debug", "Resource already deleted, skipping", {
          type: group.type,
          resourceId,
        });
        skipped++;
        markProcessed(manifestId, group.type, resourceId);
      } else if (error.code === "DATABASE_ERROR") {
        // Critical DB error - fail the workflow
        throw err;
      } else {
        // Best effort: log and continue
        ctx.log("warn", "Failed to cleanup resource", {
          type: group.type,
          resourceId,
          error: error.message ?? String(err),
        });
        failed++;
      }
    }
  }

  return { cleaned, skipped, failed };
}

// =============================================================================
// Individual Resource Cleanup
// =============================================================================

async function cleanupResource(
  ctx: NodeContext,
  type: string,
  resourceId: string
): Promise<"cleaned" | "skipped"> {
  switch (type) {
    case "allocation":
      return cleanupAllocation(ctx, resourceId);
    case "run":
      return cleanupRun(ctx, resourceId);
    case "instance":
      return cleanupInstance(ctx, resourceId);
    case "object":
      return cleanupObject(ctx, resourceId);
    case "snapshot":
      ctx.log("debug", "Snapshot cleanup stubbed, skipping", { resourceId });
      return "skipped";
    case "artifact":
      ctx.log("debug", "Artifact cleanup stubbed, skipping", { resourceId });
      return "skipped";
    case "feature":
      ctx.log("debug", "Feature cleanup stubbed, skipping", { resourceId });
      return "skipped";
    default:
      ctx.log("warn", "Unknown resource type, skipping", { type, resourceId });
      return "skipped";
  }
}

// =============================================================================
// Resource Type Handlers
// =============================================================================

async function cleanupAllocation(
  _ctx: NodeContext,
  resourceId: string
): Promise<"cleaned" | "skipped"> {
  const id = parseInt(resourceId, 10);
  const existing = queryOne<{ id: number }>("SELECT id FROM allocations WHERE id = ?", [id]);
  if (!existing) {
    return "skipped"; // Already deleted
  }

  execute("DELETE FROM allocations WHERE id = ?", [id]);
  return "cleaned";
}

async function cleanupRun(
  _ctx: NodeContext,
  resourceId: string
): Promise<"cleaned" | "skipped"> {
  const id = parseInt(resourceId, 10);
  const existing = queryOne<{ id: number; workflow_state: string }>(
    "SELECT id, workflow_state FROM runs WHERE id = ?",
    [id]
  );
  if (!existing) {
    return "skipped"; // Already deleted
  }

  // Mark as cleaned rather than delete (preserve audit trail)
  execute(
    "UPDATE runs SET workflow_state = 'cleanup:complete' WHERE id = ?",
    [id]
  );
  return "cleaned";
}

async function cleanupInstance(
  ctx: NodeContext,
  resourceId: string
): Promise<"cleaned" | "skipped"> {
  const id = parseInt(resourceId, 10);
  const existing = queryOne<{ id: number; workflow_state: string; provider: string; provider_id: string | null }>(
    "SELECT id, workflow_state, provider, provider_id FROM instances WHERE id = ?",
    [id]
  );
  if (!existing) {
    return "skipped"; // Already deleted
  }

  if (existing.workflow_state === "terminate:complete") {
    return "skipped"; // Already terminated
  }

  // Step 12: Actually terminate the VM via provider API (best-effort)
  if (existing.provider_id) {
    try {
      const provider = await getProvider(existing.provider as ProviderName);
      await provider.terminate(existing.provider_id);
      ctx.log("info", "Terminated instance via provider", {
        instanceId: id,
        provider: existing.provider,
        providerId: existing.provider_id,
      });
    } catch (err) {
      ctx.log("warn", "Failed to terminate instance via provider (best-effort)", {
        instanceId: id,
        provider: existing.provider,
        providerId: existing.provider_id,
        error: String(err),
      });
    }
  }

  // Mark as terminated in DB
  updateInstance(id, { workflow_state: "terminate:complete" });
  ctx.log("info", "Marked instance as terminated for cleanup", {
    instanceId: id,
  });
  return "cleaned";
}

async function cleanupObject(
  _ctx: NodeContext,
  resourceId: string
): Promise<"cleaned" | "skipped"> {
  const id = parseInt(resourceId, 10);
  const existing = queryOne<{ id: number }>("SELECT id FROM objects WHERE id = ?", [id]);
  if (!existing) {
    return "skipped"; // Already deleted
  }

  execute("DELETE FROM objects WHERE id = ?", [id]);
  return "cleaned";
}

// =============================================================================
// Mark Processed Helper
// =============================================================================

function markProcessed(
  manifestId: number,
  resourceType: string,
  resourceId: string
): void {
  const now = Date.now();
  try {
    execute(
      `UPDATE manifest_resources SET cleanup_processed_at = ? WHERE manifest_id = ? AND resource_type = ? AND resource_id = ?`,
      [now, manifestId, resourceType, resourceId]
    );
  } catch {
    // Best effort - don't fail cleanup for bookkeeping errors
  }
}
