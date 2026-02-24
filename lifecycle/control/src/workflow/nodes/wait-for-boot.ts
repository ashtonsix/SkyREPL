// workflow/nodes/wait-for-boot.ts - Wait For Boot Node
// Polls the provider until instance reaches running state, then updates DB.

import type { NodeExecutor, NodeContext } from "../engine.types";
import { updateInstanceRecord, getInstanceRecordRaw } from "../../resource/instance";
import { getProvider } from "../../provider/registry";
import type { ProviderName } from "../../provider/types";
import { TIMING } from "@skyrepl/contracts";
import type { WaitForBootOutput } from "../../intent/launch-run.schema";
import type { LaunchRunWorkflowInput, SpawnInstanceOutput } from "../../intent/launch-run.schema";

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
        updateInstanceRecord(input.instance_id, {
          ip,
          workflow_state: "boot:complete",
          last_heartbeat: Date.now(),
        });
        return {
          instanceId: input.instance_id,
          bootDurationMs: Date.now() - startTime,
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

    // Update instance state if record still exists
    const instance = getInstanceRecordRaw(input.instance_id);
    if (instance) {
      updateInstanceRecord(input.instance_id, {
        workflow_state: "boot:compensated",
      });
    }
  },
};
