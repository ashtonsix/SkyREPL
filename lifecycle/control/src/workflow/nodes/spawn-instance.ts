// workflow/nodes/spawn-instance.ts - Spawn Instance Node
// Two-phase spawn: DB insert -> provider API call -> DB update.
//
// retrySpawnNode: idempotency guard that checks for existing spawn:pending
// records before allowing Phase 1 to proceed, preventing duplicate DB records
// on engine retry. See spec §8.1.1 (Retry Protocol).

import crypto from "crypto";
import type { NodeExecutor, NodeContext } from "../engine.types";
import { createInstanceRecord, updateInstanceRecord, getInstanceRecordRaw } from "../../resource/instance";
import { getProvider } from "../../provider/registry";
import { getInstanceByIdempotencyKey } from "../../material/db";
import type { ProviderName } from "../../provider/types";
import type { Instance } from "../../material/db";
import type { SpawnInstanceOutput, LaunchRunWorkflowInput } from "../../intent/launch-run.schema";

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

// Output type re-exported from schema
export type { SpawnInstanceOutput } from "../../intent/launch-run.schema";

// =============================================================================
// Retry-Aware Spawn
// =============================================================================

/**
 * Terminal instance states — if an existing record is in one of these, do not
 * retry (the spawn definitively failed or a prior compensation ran).
 */
const TERMINAL_SPAWN_STATES = new Set([
  "spawn:error",
  "terminate:complete",
  "terminate:error",
  "launch-run:error",
  "launch-run:compensated",
]);

/**
 * Check for an existing instance record created by a prior attempt of this
 * workflow node (identified by spawn_idempotency_key).
 *
 * Returns the existing record, or null if none found.
 */
export function findExistingSpawnRecord(idempotencyKey: string): Instance | null {
  return getInstanceByIdempotencyKey(idempotencyKey);
}

/**
 * retrySpawnNode: Checks for an existing spawn:pending record before Phase 1.
 *
 * On engine retry the workflow node re-executes from the top. Without this
 * check, Phase 1 would insert a second DB record, creating an orphan pair.
 *
 * Decision tree:
 *   - No existing record           → proceed normally (first run)
 *   - Existing, terminal state     → do not retry (throw)
 *   - Existing, has provider_id    → skip Phase 1 + Phase 2, reconstruct output
 *   - Existing, spawn:pending      → skip Phase 1, re-run Phase 2 (provider call)
 *
 * Returns the instance to use, or null if Phase 1 should proceed normally.
 * Throws if the existing record is in a terminal state.
 */
export function retrySpawnNode(
  idempotencyKey: string
): { instance: Instance; skipProviderCall: boolean } | null {
  const existing = findExistingSpawnRecord(idempotencyKey);
  if (!existing) return null; // First attempt — proceed normally

  if (TERMINAL_SPAWN_STATES.has(existing.workflow_state)) {
    throw new Error(
      `Spawn instance in terminal state '${existing.workflow_state}' — cannot retry (instance ${existing.id})`
    );
  }

  if (existing.provider_id) {
    // Phase 2 already completed — provider already created the instance.
    // Skip both Phase 1 and Phase 2.
    return { instance: existing, skipProviderCall: true };
  }

  // Phase 1 completed but Phase 2 did not — skip Phase 1, re-run Phase 2.
  return { instance: existing, skipProviderCall: false };
}

// =============================================================================
// Node Executor
// =============================================================================

export const spawnInstanceExecutor: NodeExecutor<SpawnInstanceInput, SpawnInstanceOutput> = {
  name: "spawn-instance",
  idempotent: true,

  async execute(ctx: NodeContext): Promise<SpawnInstanceOutput> {
    const input = ctx.workflowInput as LaunchRunWorkflowInput & { manifestId?: number; spot?: boolean };
    if (!input.provider) {
      throw new Error("spawn-instance requires a resolved provider in workflow input");
    }
    const idempotencyKey = `${ctx.workflowId}:spawn-instance`;

    // Idempotency check: detect prior attempt records before Phase 1
    const retryResult = retrySpawnNode(idempotencyKey);

    let instance: Instance;

    if (retryResult === null) {
      // ─── Phase 1: DB Insert — create instance record in pending state ───────
      instance = createInstanceRecord({
        provider: input.provider,
        provider_id: "", // filled after spawn
        spec: input.spec,
        region: input.region || "unknown",
        ip: null,
        workflow_state: "spawn:pending",
        workflow_error: null,
        current_manifest_id: input.manifestId ?? ctx.manifestId,
        spawn_idempotency_key: idempotencyKey,
        is_spot: input.spot ? 1 : 0,
        spot_request_id: null,
        init_checksum: input.initChecksum || null,
        registration_token_hash: null,
        last_heartbeat: Date.now(),
      }, ctx.tenantId);
      ctx.emitResource("instance", instance.id, 50);
    } else {
      // Retry: reuse the existing record (Phase 1 already done)
      instance = retryResult.instance;
      ctx.log("info", "retrySpawnNode: reusing existing instance record", {
        instanceId: instance.id,
        workflowState: instance.workflow_state,
        skipProviderCall: retryResult.skipProviderCall,
      });

      if (retryResult.skipProviderCall) {
        // Phase 2 already done — reconstruct output from existing record
        return {
          instanceId: instance.id,
          providerId: instance.provider_id,
          ip: instance.ip,
          status: "running",
        };
      }
    }

    // ─── Phase 2: Provider API Call — spawn the actual VM ───────────────────
    const providerName = input.provider as ProviderName;
    const provider = await getProvider(providerName);
    let controlPlaneUrl = process.env.SKYREPL_CONTROL_PLANE_URL || "http://localhost:3000";

    // Generate auth token: raw token for agent, hash for DB
    // On retry, we generate a fresh token — the agent will use whichever token
    // the final successful registration attempt carries.
    const rawToken = crypto.randomBytes(16).toString("hex");
    const tokenHash = crypto.createHash("sha256").update(rawToken).digest("hex");

    // Store token hash in DB immediately after instance creation
    updateInstanceRecord(instance.id, {
      registration_token_hash: tokenHash,
    });

    const bootstrapConfig = {
      controlPlaneUrl,
      registrationToken: rawToken, // Pass raw token to agent, not the hash
      agentUrl: `${controlPlaneUrl}/v1/agent`,
      environment: {
        SKYREPL_INSTANCE_ID: String(instance.id),
      },
    };
    const providerResult = await provider.spawn({
      spec: input.spec,
      bootstrap: bootstrapConfig,
      instanceId: instance.id,
      controlId: ctx.controlId,
      manifestId: ctx.manifestId,
      region: input.region,
    });

    // ─── Phase 3: DB Update — record provider-assigned IDs + actual region ──
    updateInstanceRecord(instance.id, {
      provider_id: providerResult.id,
      ip: providerResult.ip || null,
      region: providerResult.region || input.region || "unknown",
      workflow_state: "launch-run:provisioning",
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

    const instance = getInstanceRecordRaw(output.instanceId);
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

    updateInstanceRecord(output.instanceId, {
      workflow_state: "spawn:error",
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

