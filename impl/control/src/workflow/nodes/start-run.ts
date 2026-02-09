// workflow/nodes/start-run.ts - Start Run Node
// Activates allocation and initiates file sync + run start via agent bridge.

import type { NodeExecutor, NodeContext } from "../engine.types";
import { updateRun } from "../../material/db";

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
// Agent Bridge Interface
// =============================================================================

export interface AgentBridge {
  sendStartRun(msg: Record<string, unknown>): Promise<void>;
  waitForEvent(
    instanceId: number,
    eventType: string,
    opts: { runId: number; timeout: number }
  ): Promise<unknown>;
}

// Default bridge -- for Slice 1, immediately resolves (mock agent)
let _bridge: AgentBridge = {
  async sendStartRun() {
    /* noop until Step 7-8 */
  },
  async waitForEvent() {
    return { success: true };
  },
};

export function setAgentBridge(bridge: AgentBridge): void {
  _bridge = bridge;
}

export function getAgentBridge(): AgentBridge {
  return _bridge;
}

// =============================================================================
// Node Executor
// =============================================================================

export const startRunExecutor: NodeExecutor<StartRunInput, StartRunOutput> = {
  name: "start-run",
  idempotent: false,

  async execute(ctx: NodeContext): Promise<StartRunOutput> {
    const input = ctx.workflowInput as StartRunInput;

    // Update run started_at (allocation stays CLAIMED until agent reports sync_complete)
    updateRun(input.runId, {
      started_at: Date.now(),
      workflow_state: "launch-run:syncing",
    });

    // Send start_run command to agent via bridge
    const bridge = getAgentBridge();
    await bridge.sendStartRun({
      runId: input.runId,
      instanceId: input.instanceId,
      allocationId: input.allocationId,
      command: input.command,
      workdir: input.workdir,
      env: input.env || {},
      files: input.files || [],
      maxDurationMs: input.maxDurationMs,
    });

    // Wait for agent to complete file sync (CLAIMED -> ACTIVE transition
    // happens in handleSyncComplete when agent POSTs sync_complete to /v1/agent/logs)
    await bridge.waitForEvent(input.instanceId, "sync_complete", {
      runId: input.runId,
      timeout: 60_000, // 1 minute sync timeout
    });

    // Update workflow state now that sync is confirmed
    updateRun(input.runId, {
      workflow_state: "launch-run:running",
    });

    const syncedAt = Date.now();

    ctx.log("info", "Run started and files synced", {
      runId: input.runId,
      allocationId: input.allocationId,
      instanceId: input.instanceId,
      filesSynced: input.files?.length || 0,
    });

    return {
      started: true,
      syncedAt,
      filesSynced: input.files?.length || 0,
    };
  },

  async compensate(ctx: NodeContext): Promise<void> {
    const input = ctx.workflowInput as StartRunInput;

    // Best-effort: tell agent to stop the run
    const bridge = getAgentBridge();
    try {
      await bridge.sendStartRun({
        type: "cancel",
        runId: input.runId,
        instanceId: input.instanceId,
      });
    } catch {
      ctx.log("warn", "Failed to send cancel during compensation", {
        runId: input.runId,
      });
    }
  },
};
