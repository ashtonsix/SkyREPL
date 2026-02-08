// intent/launch-run.ts - Launch Run Intent
// Stub: All function bodies throw "not implemented"

import { TIMING } from "@skyrepl/shared";
import type { Workflow } from "../material/db";
import type { WorkflowBlueprint } from "../workflow/engine.types";
import type { LaunchRunInput, LaunchRunOutput } from "./launch-run.types";

// =============================================================================
// Entry Point
// =============================================================================

export async function launchRun(input: LaunchRunInput): Promise<Workflow> {
  throw new Error("not implemented");
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
      type: "wait-boot",
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
    },
    "await-completion": {
      type: "wait-completion",
      dependsOn: ["sync-files"],
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
// Node Handlers
// =============================================================================

export function checkBudget(input: LaunchRunInput): Promise<{ budgetOk: boolean }> {
  throw new Error("not implemented");
}

export function resolveInstance(
  input: LaunchRunInput
): Promise<{ warmAvailable: boolean; warmAllocationId?: number; instanceId?: number }> {
  throw new Error("not implemented");
}

export function findSnapshotForSpec(input: LaunchRunInput): Promise<string | undefined> {
  throw new Error("not implemented");
}
