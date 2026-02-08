// workflow/nodes/spawn-instance.ts - Spawn Instance Node
// Stub: All function bodies throw "not implemented"

import type { NodeExecutor, NodeContext } from "../engine.types";

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
  bootstrap: BootstrapConfig;
}

export interface SpawnInstanceOutput {
  instanceId: number;
  providerId: string;
  ip: string | null;
  status: string;
}

interface BootstrapConfig {
  controlPlaneUrl: string;
  authToken: string;
  agentVersion: string;
}

// =============================================================================
// Node Executor
// =============================================================================

export const spawnInstanceExecutor: NodeExecutor<SpawnInstanceInput, SpawnInstanceOutput> = {
  name: "spawn-instance",
  idempotent: true,

  async execute(ctx: NodeContext): Promise<SpawnInstanceOutput> {
    throw new Error("not implemented");
  },

  async compensate(ctx: NodeContext): Promise<void> {
    throw new Error("not implemented");
  },
};

// =============================================================================
// Helpers
// =============================================================================

export function findProviderSnapshotId(
  provider: string,
  initChecksum: string,
  spec: string
): Promise<string | undefined> {
  throw new Error("not implemented");
}

export function generateAuthTokenHash(): string {
  throw new Error("not implemented");
}
