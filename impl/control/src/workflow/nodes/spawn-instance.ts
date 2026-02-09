// workflow/nodes/spawn-instance.ts - Spawn Instance Node
// Two-phase spawn: DB insert -> provider API call -> DB update.

import crypto from "crypto";
import type { NodeExecutor, NodeContext } from "../engine.types";
import { createInstance, updateInstance, getInstance } from "../../material/db";
import { getProvider } from "../../provider/registry";
import type { ProviderName } from "../../provider/types";

// =============================================================================
// Types
// =============================================================================

export interface SpawnInstanceInput {
  runId: number;
  manifestId: number;
  spec: string;
  provider: string;
  region?: string;
  spot?: boolean;
  initChecksum?: string;
}

export interface SpawnInstanceOutput {
  instanceId: number;
  providerId: string;
  ip: string | null;
  status: string;
}

// =============================================================================
// Node Executor
// =============================================================================

export const spawnInstanceExecutor: NodeExecutor<SpawnInstanceInput, SpawnInstanceOutput> = {
  name: "spawn-instance",
  idempotent: true,

  async execute(ctx: NodeContext): Promise<SpawnInstanceOutput> {
    const input = ctx.workflowInput as SpawnInstanceInput;

    // Phase 1: DB Insert — create instance record in pending state
    const instance = createInstance({
      provider: input.provider || "orbstack",
      provider_id: "", // filled after spawn
      spec: input.spec,
      region: input.region || "local",
      ip: null,
      workflow_state: "spawn:pending",
      workflow_error: null,
      current_manifest_id: input.manifestId ?? ctx.manifestId,
      spawn_idempotency_key: `wf-${ctx.workflowId}-spawn`,
      is_spot: input.spot ? 1 : 0,
      spot_request_id: null,
      init_checksum: input.initChecksum || null,
      last_heartbeat: Date.now(),
    });
    ctx.emitResource("instance", instance.id, 50);

    // Phase 2: Provider API Call — spawn the actual VM
    const providerName = (input.provider || "orbstack") as ProviderName;
    const provider = await getProvider(providerName);
    const controlPlaneUrl = process.env.SKYREPL_CONTROL_PLANE_URL || "http://localhost:3000";
    const registrationToken = generateAuthTokenHash();
    const bootstrapConfig = {
      agentUrl: `${controlPlaneUrl}/agent/download`,
      controlPlaneUrl,
      registrationToken,
    };
    const providerResult = await provider.spawn({
      spec: input.spec,
      bootstrap: bootstrapConfig,
    });

    // Phase 3: DB Update — record provider-assigned IDs
    updateInstance(instance.id, {
      provider_id: providerResult.id,
      ip: providerResult.ip || null,
      workflow_state: "spawn:complete",
      last_heartbeat: Date.now(),
    });

    return {
      instanceId: instance.id,
      providerId: providerResult.id,
      ip: providerResult.ip || null,
      status: providerResult.status,
    };
  },

  async compensate(ctx: NodeContext): Promise<void> {
    const output = ctx.output as SpawnInstanceOutput | undefined;
    if (!output?.instanceId) return;

    const instance = getInstance(output.instanceId);
    if (!instance) return;

    // If provider_id was assigned, terminate the actual VM
    if (instance.provider_id) {
      try {
        const provider = await getProvider(instance.provider as ProviderName);
        await provider.terminate(instance.provider_id);
      } catch (err) {
        ctx.log("warn", "Failed to terminate instance during compensation", {
          instanceId: output.instanceId,
          providerId: instance.provider_id,
          error: err instanceof Error ? err.message : String(err),
        });
      }
    }

    updateInstance(output.instanceId, {
      workflow_state: "spawn:compensated",
    });
  },
};

// =============================================================================
// Helpers
// =============================================================================

/**
 * Find a provider snapshot matching the given init checksum and spec.
 * Slice 1: returns undefined (no snapshot matching implemented yet).
 */
export async function findProviderSnapshotId(
  _provider: string,
  _initChecksum: string,
  _spec: string
): Promise<string | undefined> {
  return undefined;
}

/**
 * Generate a random auth token hash for instance registration.
 * Slice 1: simple random hex string.
 */
export function generateAuthTokenHash(): string {
  return crypto.randomBytes(16).toString("hex");
}
