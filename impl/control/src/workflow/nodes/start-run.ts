// workflow/nodes/start-run.ts - Start Run Node
// Activates allocation and initiates file sync + run start via agent bridge.

import type { NodeExecutor, NodeContext } from "../engine.types";
import { updateRunRecord } from "../../resource/run";
import type {
  StartRunOutput,
  LaunchRunWorkflowInput,
  CreateAllocationOutput,
} from "../../intent/launch-run.schema";

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

// Output type re-exported from schema
export type { StartRunOutput } from "../../intent/launch-run.schema";

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
    const wfInput = ctx.workflowInput as LaunchRunWorkflowInput;
    const allocOutput = ctx.getNodeOutput("create-allocation") as CreateAllocationOutput | null;
    if (!allocOutput) {
      throw new Error("create-allocation output not available");
    }
    const input: StartRunInput = {
      runId: wfInput.runId,
      instanceId: allocOutput.instanceId,
      allocationId: allocOutput.allocationId,
      command: wfInput.command,
      workdir: wfInput.workdir || allocOutput.workdir,
      env: wfInput.env,
      files: (wfInput.files || []).map(f => ({
        path: f.path,
        checksum: f.checksum,
        sizeBytes: f.sizeBytes ?? 0,
      })),
      maxDurationMs: wfInput.maxDurationMs,
      artifactPatterns: wfInput.artifactPatterns,
    };

    // Update run started_at (allocation stays CLAIMED until agent reports sync_complete)
    updateRunRecord(input.runId, {
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
    updateRunRecord(input.runId, {
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
    const wfInput = ctx.workflowInput as {
      runId: number;
    };
    const allocOutput = ctx.getNodeOutput("create-allocation") as {
      instanceId: number;
    } | null;
    if (!allocOutput) {
      ctx.log("warn", "create-allocation output not available during compensation", {
        runId: wfInput.runId,
      });
      return;
    }

    // Best-effort: tell agent to stop the run
    const bridge = getAgentBridge();
    try {
      await bridge.sendStartRun({
        type: "cancel",
        runId: wfInput.runId,
        instanceId: allocOutput.instanceId,
      });
    } catch {
      ctx.log("warn", "Failed to send cancel during compensation", {
        runId: wfInput.runId,
      });
    }
  },
};
