// workflow/nodes/cleanup-records.ts - Cleanup Records Node
// Sets instance to terminate:complete and emits metering_stop audit event.

import type { NodeExecutor, NodeContext } from "../engine.types";
import { updateInstance, emitAuditEvent, queryOne } from "../../material/db";
import type { Instance } from "../../material/db";
import type {
  TerminateInstanceInput,
  ValidateInstanceOutput,
  CleanupRecordsOutput,
} from "../../intent/terminate-instance";
import { computeMeteringStopData } from "../../billing/metering";

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

    // Fetch instance record for provider/spec/region context
    const instance = queryOne<Instance>("SELECT * FROM instances WHERE id = ?", [instanceId]);

    // Set instance workflow_state to terminate:complete
    updateInstance(instanceId, {
      workflow_state: "terminate:complete",
    });

    // Emit metering_stop audit event (replaces finishUsageRecord loop)
    const now = Date.now();
    if (instance) {
      try {
        // C-1: Look up paired metering_start to compute amount_cents
        const meterData = computeMeteringStopData(
          instanceId,
          instance.provider ?? "",
          instance.spec ?? "",
          instance.region ?? null,
          instance.is_spot === 1,
          now
        );

        emitAuditEvent({
          event_type: "metering_stop",
          tenant_id: instance.tenant_id,
          instance_id: instanceId,
          provider: instance.provider ?? undefined,
          spec: instance.spec ?? undefined,
          region: instance.region ?? undefined,
          source: "lifecycle",
          is_cost: true,
          is_usage: true,
          data: {
            provider_resource_id: instance.provider_id,
            metering_window_end_ms: now,
            ...meterData,
          },
          dedupe_key: `${instance.provider}:${instance.provider_id}:metering_stop`,
          occurred_at: now,
        });
      } catch (err) {
        // Non-fatal: metering event failure should not block cleanup
        ctx.log("warn", "Failed to emit metering_stop audit event", {
          instanceId,
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }

    ctx.log("info", "Instance records cleaned up", {
      instanceId,
      meteringStopEmitted: instance !== null,
    });

    return { recordsCleaned: instance ? 1 : 0 };
  },
};

// Metering stop enrichment: shared helper in billing/metering.ts
