// intent/launch-run.ts - Launch Run Intent

import { TIMING } from "@skyrepl/shared";
import { createRun, getWorkflow, type Workflow } from "../material/db";
import type { WorkflowBlueprint, NodeExecutor, NodeContext } from "../workflow/engine.types";
import { submit, registerBlueprint, registerNodeExecutor } from "../workflow/engine";
import type { LaunchRunInput, LaunchRunOutput } from "./launch-run.types";
import type { CheckBudgetOutput } from "./launch-run.schema";

// Node executor imports
import { resolveInstanceExecutor } from "../workflow/nodes/resolve-instance";
import { claimAllocationExecutor } from "../workflow/nodes/claim-allocation";
import { spawnInstanceExecutor } from "../workflow/nodes/spawn-instance";
import { waitForBootExecutor } from "../workflow/nodes/wait-for-boot";
import { createAllocationExecutor } from "../workflow/nodes/create-allocation";
import { startRunExecutor } from "../workflow/nodes/start-run";
import { waitCompletionExecutor } from "../workflow/nodes/wait-completion";
import { finalizeExecutor } from "../workflow/nodes/finalize";

// =============================================================================
// Entry Point
// =============================================================================

export async function launchRun(input: LaunchRunInput): Promise<Workflow> {
  // Create a Run record in the database
  const run = createRun({
    command: input.command,
    workdir: input.workdir ?? "/workspace",
    max_duration_ms: input.maxDurationMs ?? TIMING.DEFAULT_WORKFLOW_TIMEOUT_MS,
    workflow_state: "pending",
    workflow_error: null,
    current_manifest_id: null,
    exit_code: null,
    init_checksum: input.initChecksum ?? null,
    create_snapshot: input.createSnapshot ? 1 : 0,
    spot_interrupted: 0,
    started_at: null,
    finished_at: null,
  });

  // Submit the workflow
  const result = await submit({
    type: "launch-run",
    input: { ...input, runId: run.id } as unknown as Record<string, unknown>,
    idempotencyKey: input.idempotencyKey,
  });

  // Return the workflow record
  return getWorkflow(result.workflowId)!;
}

// =============================================================================
// Workflow Blueprint
// =============================================================================

export const launchRunBlueprint: WorkflowBlueprint = {
  type: "launch-run",
  entryNode: "check-budget",
  nodes: {
    "check-budget": {
      type: "check-budget",
    },
    "resolve-instance": {
      type: "resolve-instance",
      dependsOn: ["check-budget"],
    },
    "claim-warm-allocation": {
      type: "claim-allocation",
      dependsOn: ["resolve-instance"],
    },
    "spawn-instance": {
      type: "spawn-instance",
      dependsOn: ["resolve-instance"],
    },
    "wait-for-boot": {
      type: "wait-for-boot",
      dependsOn: ["spawn-instance"],
      timeout: TIMING.INSTANCE_BOOT_TIMEOUT_MS,
    },
    "create-allocation": {
      type: "create-allocation",
      dependsOn: ["claim-warm-allocation", "wait-for-boot"],
    },
    "sync-files": {
      type: "start-run",
      dependsOn: ["create-allocation"],
      timeout: TIMING.SYNC_TIMEOUT_MS,
    },
    "await-completion": {
      type: "wait-completion",
      dependsOn: ["sync-files"],
      timeout: TIMING.DEFAULT_WORKFLOW_TIMEOUT_MS,
    },
    "finalize-run": {
      type: "finalize",
      dependsOn: ["await-completion"],
    },
  },
};

// =============================================================================
// Error Handling Map
// =============================================================================

export const LAUNCH_RUN_ERROR_HANDLING: Record<string, string> = {
  "check-budget": "fail_workflow",
  "resolve-instance": "fail_workflow",
  "claim-warm-allocation": "trigger_on_error_branch",
  "spawn-instance": "fail_workflow",
  "wait-for-boot": "fail_and_terminate",
  "sync-files": "fail_allocation_and_terminate",
  "await-completion": "timeout_then_cancel",
  "finalize-run": "skip_and_continue",
};

// =============================================================================
// Registration
// =============================================================================

export function registerLaunchRun(): void {
  registerBlueprint(launchRunBlueprint);
  registerNodeExecutor(checkBudgetExecutor);
  registerNodeExecutor(resolveInstanceExecutor);
  registerNodeExecutor(claimAllocationExecutor);
  registerNodeExecutor(spawnInstanceExecutor);
  registerNodeExecutor(waitForBootExecutor);
  registerNodeExecutor(createAllocationExecutor);
  registerNodeExecutor(startRunExecutor);
  registerNodeExecutor(waitCompletionExecutor);
  registerNodeExecutor(finalizeExecutor);
}

// =============================================================================
// Check Budget Executor
// =============================================================================

export const checkBudgetExecutor: NodeExecutor<unknown, CheckBudgetOutput> = {
  name: "check-budget",
  idempotent: true,
  async execute(_ctx: NodeContext): Promise<CheckBudgetOutput> {
    // Slice 1: no budget enforcement
    return { budgetOk: true };
  },
};

// =============================================================================
// Node Handlers
// =============================================================================

export function checkBudget(_input: LaunchRunInput): Promise<{ budgetOk: boolean }> {
  // Slice 1: no budget enforcement
  return Promise.resolve({ budgetOk: true });
}

export function resolveInstance(
  input: LaunchRunInput
): Promise<{ warmAvailable: boolean; warmAllocationId?: number; instanceId?: number }> {
  // Slice 1: always cold path (no warm pool)
  return Promise.resolve({ warmAvailable: false });
}

export function findSnapshotForSpec(_input: LaunchRunInput): Promise<string | undefined> {
  // Slice 1: no snapshot matching
  return Promise.resolve(undefined);
}
