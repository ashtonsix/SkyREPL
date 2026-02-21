// workflow/nodes/cleanup-records.ts - Cleanup Records Node
// Sets instance to terminate:complete and closes open usage records.

import type { NodeExecutor, NodeContext } from "../engine.types";
import { updateInstance, queryMany, execute } from "../../material/db";
import type {
  TerminateInstanceInput,
  ValidateInstanceOutput,
  CleanupRecordsOutput,
} from "../../intent/terminate-instance";

// =============================================================================
// Node Executor
// =============================================================================

export const cleanupRecordsExecutor: NodeExecutor<unknown, CleanupRecordsOutput> = {
  name: "cleanup-records",
  idempotent: true,

  // NOT compensatable — termination is irreversible

  async execute(ctx: NodeContext): Promise<CleanupRecordsOutput> {
    const validateOutput = ctx.getNodeOutput("validate-instance") as ValidateInstanceOutput;
    const instanceId = validateOutput.instanceId;

    // Set instance workflow_state to terminate:complete
    updateInstance(instanceId, {
      workflow_state: "terminate:complete",
    });

    // Close any open usage records for this instance
    const now = Date.now();
    const openRecords = queryMany<{ id: number; started_at: number }>(
      "SELECT id, started_at FROM usage_records WHERE instance_id = ? AND finished_at IS NULL",
      [instanceId]
    );

    for (const record of openRecords) {
      const durationMs = now - record.started_at;
      execute(
        "UPDATE usage_records SET finished_at = ?, duration_ms = ? WHERE id = ?",
        [now, durationMs, record.id]
      );
    }

    ctx.log("info", "Instance records cleaned up", {
      instanceId,
      usageRecordsClosed: openRecords.length,
    });

    return { recordsCleaned: openRecords.length };
  },
};
