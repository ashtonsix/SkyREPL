// workflow/nodes/wait-completion.ts - Wait Completion Node
// Waits for agent to report run completion, then updates run record.

import type { NodeExecutor, NodeContext } from "../engine.types";
import { updateRun } from "../../material/db";
import { getAgentBridge } from "./start-run";
import { TIMING } from "@skyrepl/shared";

// =============================================================================
// Types
// =============================================================================

export interface WaitCompletionInput {
  runId: number;
  instanceId: number;
  allocationId: number;
  maxDurationMs?: number;
}

export interface WaitCompletionOutput {
  exitCode: number;
  completedAt: number;
  durationMs: number;
  spotInterrupted: boolean;
  timedOut: boolean;
}

// =============================================================================
// Node Executor
// =============================================================================

export const waitCompletionExecutor: NodeExecutor<WaitCompletionInput, WaitCompletionOutput> = {
  name: "wait-completion",
  idempotent: false,

  async execute(ctx: NodeContext): Promise<WaitCompletionOutput> {
    const input = ctx.workflowInput as WaitCompletionInput;
    const startTime = Date.now();

    const bridge = getAgentBridge();
    const result = await bridge.waitForEvent(
      input.instanceId,
      "run_complete",
      {
        runId: input.runId,
        timeout: input.maxDurationMs || TIMING.DEFAULT_WORKFLOW_TIMEOUT_MS,
      }
    );

    const now = Date.now();
    const exitCode = (result as { exitCode?: number })?.exitCode ?? 0;
    const spotInterrupted =
      (result as { spotInterrupted?: boolean })?.spotInterrupted ?? false;

    // Update run record with completion data
    updateRun(input.runId, {
      exit_code: exitCode,
      spot_interrupted: spotInterrupted ? 1 : 0,
      finished_at: now,
      workflow_state: "launch-run:complete",
    });

    ctx.log("info", "Run completed", {
      runId: input.runId,
      exitCode,
      spotInterrupted,
      durationMs: now - startTime,
    });

    return {
      exitCode,
      completedAt: now,
      durationMs: now - startTime,
      spotInterrupted,
      timedOut: false,
    };
  },

  async compensate(ctx: NodeContext): Promise<void> {
    // No compensation needed for wait-completion:
    // the run either completed or it didn't, no side effects to undo.
  },
};
