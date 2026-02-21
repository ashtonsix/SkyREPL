// workflow/nodes/wait-completion.ts - Wait Completion Node
// Waits for agent to report run completion, then updates run record.

import type { NodeExecutor, NodeContext } from "../engine.types";
import { updateRunRecord } from "../../resource/run";
import { getAgentBridge } from "./start-run";
import { TIMING } from "@skyrepl/contracts";
import type {
  WaitCompletionOutput,
  LaunchRunWorkflowInput,
  CreateAllocationOutput,
} from "../../intent/launch-run.schema";

// =============================================================================
// Types
// =============================================================================

export interface WaitCompletionInput {
  runId: number;
  instanceId: number;
  allocationId: number;
  maxDurationMs?: number;
}

// Output type re-exported from schema
export type { WaitCompletionOutput } from "../../intent/launch-run.schema";

// =============================================================================
// Node Executor
// =============================================================================

export const waitCompletionExecutor: NodeExecutor<WaitCompletionInput, WaitCompletionOutput> = {
  name: "wait-completion",
  idempotent: false,

  async execute(ctx: NodeContext): Promise<WaitCompletionOutput> {
    const wfInput = ctx.workflowInput as LaunchRunWorkflowInput;
    const allocOutput = ctx.getNodeOutput("create-allocation") as CreateAllocationOutput | null;
    if (!allocOutput) {
      throw new Error("create-allocation output not available");
    }
    const input: WaitCompletionInput = {
      runId: wfInput.runId,
      instanceId: allocOutput.instanceId,
      allocationId: allocOutput.allocationId,
      maxDurationMs: wfInput.maxDurationMs,
    };
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
    updateRunRecord(input.runId, {
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
