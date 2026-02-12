// intent/terminate-instance.ts - Terminate Instance Intent

import { getInstance, getWorkflow, type Workflow } from "../material/db";
import type { WorkflowBlueprint, NodeExecutor, NodeContext } from "../workflow/engine.types";
import { submit, registerBlueprint, registerNodeExecutor } from "../workflow/engine";
import type { TerminateInstanceInput, TerminateInstanceOutput } from "./terminate-instance.types";
import { NotFoundError, ConflictError } from "@skyrepl/shared";

// Node executor imports
import { validateInstanceExecutor } from "../workflow/nodes/validate-instance";
import { drainAllocationsExecutor } from "../workflow/nodes/drain-allocations";
import { drainSshSessionsExecutor } from "../workflow/nodes/drain-ssh-sessions";
import { cleanupFeaturesExecutor } from "../workflow/nodes/cleanup-features";
import { terminateProviderExecutor } from "../workflow/nodes/terminate-provider";
import { cleanupRecordsExecutor } from "../workflow/nodes/cleanup-records";

// =============================================================================
// Entry Point
// =============================================================================

export async function terminateInstance(input: TerminateInstanceInput): Promise<Workflow> {
  // Validate instance exists
  const instance = getInstance(input.instanceId);
  if (!instance) {
    throw new NotFoundError("Instance", input.instanceId);
  }

  // Check if already terminated
  if (instance.workflow_state === "terminate:complete") {
    throw new ConflictError(
      `Instance ${input.instanceId} is already terminated`
    );
  }

  // Check if already being terminated (idempotent: return existing workflow)
  if (instance.workflow_state.startsWith("terminate:")) {
    throw new ConflictError(
      `Instance ${input.instanceId} is already being terminated (state: ${instance.workflow_state})`
    );
  }

  // Submit the workflow
  const result = await submit({
    type: "terminate-instance",
    input: { instanceId: input.instanceId } as unknown as Record<string, unknown>,
    idempotencyKey: `terminate-instance-${input.instanceId}`,
  });

  // Return the workflow record
  return getWorkflow(result.workflowId)!;
}

// =============================================================================
// Workflow Blueprint
// =============================================================================

export const terminateInstanceBlueprint: WorkflowBlueprint = {
  type: "terminate-instance",
  entryNode: "validate-instance",
  nodes: {
    "validate-instance": {
      type: "validate-instance",
    },
    "drain-allocations": {
      type: "drain-allocations",
      dependsOn: ["validate-instance"],
    },
    "drain-ssh-sessions": {
      type: "drain-ssh-sessions",
      dependsOn: ["drain-allocations"],
    },
    "cleanup-features": {
      type: "cleanup-features",
      dependsOn: ["drain-ssh-sessions"],
    },
    "terminate-provider": {
      type: "terminate-provider",
      dependsOn: ["cleanup-features"],
    },
    "cleanup-records": {
      type: "cleanup-records",
      dependsOn: ["terminate-provider"],
    },
  },
};

// =============================================================================
// Registration
// =============================================================================

export function registerTerminateInstance(): void {
  registerBlueprint(terminateInstanceBlueprint);
  registerNodeExecutor(validateInstanceExecutor);
  registerNodeExecutor(drainAllocationsExecutor);
  registerNodeExecutor(drainSshSessionsExecutor);
  registerNodeExecutor(cleanupFeaturesExecutor);
  registerNodeExecutor(terminateProviderExecutor);
  registerNodeExecutor(cleanupRecordsExecutor);
}
