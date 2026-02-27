// workflow/nodes/wait-for-boot.ts - Wait For Boot Node
// Polls the provider until instance reaches running state, then updates DB.

import type { NodeExecutor, NodeContext } from "../engine.types";
import { updateInstanceRecord, getInstanceRecordRaw } from "../../resource/instance";
import { getProvider } from "../../provider/registry";
import type { ProviderName } from "../../provider/types";
import { TIMING } from "@skyrepl/contracts";
import type { WaitForBootOutput } from "../../intent/launch-run.schema";
import type { LaunchRunWorkflowInput, SpawnInstanceOutput } from "../../intent/launch-run.schema";
import { emitAuditEvent, queryOne } from "../../material/db";
import { computeMeteringStopData } from "../../billing/metering";

// =============================================================================
// Types
// =============================================================================

export interface WaitForBootInput {
  instance_id: number;
  provider: string;
  provider_id: string;
  poll_interval_ms?: number;
  timeout_ms?: number;
}

// Output type re-exported from schema (camelCase: instanceId, bootDurationMs, ip)
export type { WaitForBootOutput } from "../../intent/launch-run.schema";

// =============================================================================
// Node Executor
// =============================================================================

export const waitForBootExecutor: NodeExecutor<WaitForBootInput, WaitForBootOutput> = {
  name: "wait-for-boot",
  idempotent: true,

  async execute(ctx: NodeContext): Promise<WaitForBootOutput> {
    const wfInput = ctx.workflowInput as LaunchRunWorkflowInput;
    const spawnOutput = ctx.getNodeOutput("spawn-instance") as SpawnInstanceOutput | null;
    if (!spawnOutput) {
      throw new Error("spawn-instance output not available");
    }
    if (!wfInput.provider) {
      throw new Error("wait-for-boot requires a resolved provider in workflow input");
    }
    const input: WaitForBootInput = {
      instance_id: spawnOutput.instanceId,
      provider_id: spawnOutput.providerId,
      provider: wfInput.provider,
    };
    const provider = await getProvider(input.provider as ProviderName);
    const pollInterval = input.poll_interval_ms ?? TIMING.BOOT_POLL_INTERVAL_MS;
    const timeout = input.timeout_ms ?? TIMING.INSTANCE_BOOT_TIMEOUT_MS;
    const startTime = Date.now();

    while (Date.now() - startTime < timeout) {
      ctx.checkCancellation();

      const instanceInfo = await provider.get(input.provider_id);
      if (instanceInfo && instanceInfo.status === "running") {
        const ip = instanceInfo.ip || null;
        // Update instance record with confirmed IP
        const bootNow = Date.now();
        updateInstanceRecord(input.instance_id, {
          ip,
          workflow_state: "boot:complete",
          last_heartbeat: bootNow,
        });

        // Emit metering_start: instance is now billable (COLD SPAWN PATH)
        //
        // NOTE: allocation_id may be null here — wait-for-boot runs before
        // create-allocation in the workflow DAG. v_cost_attributed joins on
        // instance_id (not allocation_id) to handle this timing gap.
        //
        // NOTE: If this emit fails silently, the reconciler emits a retroactive
        // metering_stop but has no recovery for a missing metering_start. The C-1
        // pre-computed amount_cents on stop events mitigates the cost gap.
        try {
          // H-8: Look up the most recent price_observation for this provider/spec/region
          // to include hourly_rate_cents in the metering_start event. The SQLite fallback
          // in budget.ts uses this field; without it, budget projection defaults to $1/hr.
          const spec = (wfInput as any).spec ?? undefined;
          const region = (wfInput as any).region ?? undefined;
          const priceObs = queryOne<{ data: string }>(
            `SELECT data FROM audit_log
             WHERE event_type = 'price_observation'
               AND provider = ?
               AND spec = ?
               AND region = ?
             ORDER BY occurred_at DESC LIMIT 1`,
            [input.provider, spec ?? null, region ?? null]
          );
          // G-3 fix: Always write hourly_rate_cents (even 0) so the SQLite budget
          // fallback doesn't apply the $1/hr legacy default for free providers.
          let hourlyRateCents: number = 0;
          if (priceObs) {
            try {
              const d = JSON.parse(priceObs.data);
              if (typeof d.rate_per_hour === "number") {
                hourlyRateCents = Math.round(d.rate_per_hour * 100);
              }
            } catch { /* malformed price_observation data */ }
          }

          emitAuditEvent({
            event_type: "metering_start",
            tenant_id: ctx.tenantId,
            instance_id: input.instance_id,
            allocation_id: (wfInput as any).allocationId ?? undefined,
            run_id: (wfInput as any).runId ?? undefined,
            manifest_id: ctx.manifestId ?? undefined,
            provider: input.provider,
            spec,
            region,
            source: "lifecycle",
            is_cost: true,
            is_usage: true,
            data: {
              provider_resource_id: input.provider_id,
              metering_window_start_ms: bootNow,
              hourly_rate_cents: hourlyRateCents,
            },
            dedupe_key: `${input.provider}:${input.provider_id}:metering_start`,
            occurred_at: bootNow,
          });
        } catch (err) {
          // Non-fatal: metering emission failure should not block boot completion
          ctx.log("warn", "Failed to emit metering_start audit event", {
            instanceId: input.instance_id,
            error: err instanceof Error ? err.message : String(err),
          });
        }

        return {
          instanceId: input.instance_id,
          bootDurationMs: bootNow - startTime,
          ip: ip || "127.0.0.1",
        };
      }

      await ctx.sleep(pollInterval);
    }

    throw Object.assign(new Error("Instance boot timed out"), {
      code: "OPERATION_TIMEOUT",
      category: "timeout",
    });
  },

  async compensate(ctx: NodeContext): Promise<void> {
    const wfInput = ctx.workflowInput as LaunchRunWorkflowInput;
    const spawnOutput = ctx.getNodeOutput("spawn-instance") as SpawnInstanceOutput | null;
    if (!spawnOutput || !spawnOutput.providerId) return;

    const input: WaitForBootInput = {
      instance_id: spawnOutput.instanceId,
      provider_id: spawnOutput.providerId,
      provider: wfInput.provider ?? "unknown",
    };

    // Capture instance state before termination to determine if metering was active.
    // metering_start is only emitted when the instance reaches "boot:complete" state,
    // so we emit metering_stop only if the instance had a confirmed running state.
    const instanceBeforeTerminate = getInstanceRecordRaw(input.instance_id);
    const meteringWasActive = instanceBeforeTerminate?.workflow_state === "boot:complete";

    try {
      const provider = await getProvider(input.provider as ProviderName);
      await provider.terminate(input.provider_id);
    } catch (err) {
      ctx.log("warn", "Failed to terminate instance during wait-for-boot compensation", {
        instanceId: input.instance_id,
        providerId: input.provider_id,
        error: err instanceof Error ? err.message : String(err),
      });
    }

    // Emit metering_stop if the instance had reached running state (metering_start was emitted)
    if (meteringWasActive && instanceBeforeTerminate) {
      const compensateNow = Date.now();
      try {
        // C-1: Compute amount_cents from paired metering_start and price observations
        const meterData = computeMeteringStopData(
          input.instance_id,
          input.provider,
          instanceBeforeTerminate.spec,
          instanceBeforeTerminate.region ?? null,
          instanceBeforeTerminate.is_spot === 1,
          compensateNow
        );

        emitAuditEvent({
          event_type: "metering_stop",
          tenant_id: instanceBeforeTerminate.tenant_id,
          instance_id: input.instance_id,
          provider: input.provider,
          spec: instanceBeforeTerminate.spec,
          region: instanceBeforeTerminate.region ?? undefined,
          source: "lifecycle",
          is_cost: true,
          is_usage: true,
          data: {
            provider_resource_id: input.provider_id,
            metering_window_end_ms: compensateNow,
            reason: "boot_compensation",
            ...meterData,
          },
          dedupe_key: `${input.provider}:${input.provider_id}:metering_stop`,
          occurred_at: compensateNow,
        });
      } catch (auditErr) {
        ctx.log("warn", "Failed to emit metering_stop audit event during wait-for-boot compensation", {
          instanceId: input.instance_id,
          error: auditErr instanceof Error ? auditErr.message : String(auditErr),
        });
      }
    }

    // Update instance state if record still exists
    const instance = getInstanceRecordRaw(input.instance_id);
    if (instance) {
      updateInstanceRecord(input.instance_id, {
        workflow_state: "boot:compensated",
      });
    }
  },
};

// Metering stop enrichment: shared helper in billing/metering.ts
