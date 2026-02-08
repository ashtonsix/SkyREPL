// workflow/nodes/start-run.ts - Start Run Node
// Stub: All function bodies throw "not implemented"

import type { NodeExecutor, NodeContext } from "../engine.types";

// =============================================================================
// Types
// =============================================================================

export interface StartRunInput {
  runId: number;
  instanceId: number;
  allocationId: number;
  command: string;
  workdir: string;
  env?: Record<string, string>;
  files: FileManifestEntry[];
  artifactPatterns?: string[];
  maxDurationMs?: number;
}

export interface FileManifestEntry {
  path: string;
  checksum: string;
  sizeBytes: number;
}

export interface StartRunOutput {
  started: boolean;
  syncedAt: number;
  filesSynced: number;
}

// =============================================================================
// Node Executor
// =============================================================================

export const startRunExecutor: NodeExecutor<StartRunInput, StartRunOutput> = {
  name: "start-run",
  idempotent: false,

  async execute(ctx: NodeContext): Promise<StartRunOutput> {
    throw new Error("not implemented");
  },

  async compensate(ctx: NodeContext): Promise<void> {
    throw new Error("not implemented");
  },
};

// =============================================================================
// Agent Event Waiting
// =============================================================================

export function waitForAgentEvent(
  instanceId: number,
  eventType: string,
  options: { runId: number; timeout: number }
): Promise<{ success: boolean; timeout?: boolean; error?: string; data?: unknown }> {
  throw new Error("not implemented");
}

export const agentEventRegistry = new Map<string, (data: unknown) => void>();
