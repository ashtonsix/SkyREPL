// workflow/nodes/cleanup-records.ts - Cleanup Records Node
// Sets instance to terminate:complete and closes open usage records.

import type { NodeExecutor, NodeContext } from "../engine.types";
import { updateInstance, getOpenUsageRecordsForInstance, finishUsageRecord } from "../../material/db";
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
    const openRecords = getOpenUsageRecordsForInstance(instanceId);

    for (const record of openRecords) {
      finishUsageRecord(record.id, now);
    }

    ctx.log("info", "Instance records cleaned up", {
      instanceId,
      usageRecordsClosed: openRecords.length,
    });

    return { recordsCleaned: openRecords.length };
  },
};
