// intent/terminate-instance.ts - Terminate Instance Intent

import { Type, type Static } from '@sinclair/typebox';
import { getInstance, getWorkflow, type Workflow } from "../material/db";
import type { WorkflowBlueprint, NodeExecutor, NodeContext } from "../workflow/engine.types";
import { submit, registerBlueprint, registerNodeExecutor } from "../workflow/engine";
import { NotFoundError, ConflictError } from "@skyrepl/contracts";

// =============================================================================
// Workflow Input Schema
// =============================================================================

export const TerminateInstanceInputSchema = Type.Object({
  instanceId: Type.Number(),
});

export type TerminateInstanceInput = Static<typeof TerminateInstanceInputSchema>;

// =============================================================================
// Workflow Output
// =============================================================================

export interface TerminateInstanceOutput {
  terminated: boolean;
  instanceId: number;
}

// =============================================================================
// Node Output Types
// =============================================================================

export interface ValidateInstanceOutput {
  instanceId: number;
  provider: string;
  providerId: string;
  spec: string;
  region: string;
}

export interface DrainAllocationsOutput {
  drained: number;
  failed: number;
}

export interface DrainSshSessionsOutput {
  sessionsTerminated: number;
}

export interface CleanupFeaturesOutput {
  featuresCleanedUp: number;
}

export interface TerminateProviderOutput {
  terminated: boolean;
  providerId: string;
}

export interface CleanupRecordsOutput {
  recordsCleaned: number;
}

// Node executor imports
import { validateInstanceExecutor } from "../workflow/nodes/validate-instance";
import { drainAllocationsExecutor, drainSshSessionsExecutor } from "../workflow/nodes/drain-allocations";
import { cleanupFeaturesExecutor } from "../workflow/nodes/cleanup-features";
import { terminateProviderExecutor } from "../workflow/nodes/terminate-provider";
import { cleanupRecordsExecutor } from "../workflow/nodes/cleanup-records";

// =============================================================================
// Entry Point
// =============================================================================

export async function terminateInstance(input: TerminateInstanceInput & { tenantId?: number }): Promise<Workflow> {
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

  // Submit the workflow.
  //
  // KNOWN GAP (WF2): The workflow-level idempotency_key is permanent dedup
  // (UNIQUE index, no TTL). If this workflow fails, a retry with the same
  // instanceId would return the failed workflow as "deduplicated" rather
  // than creating a new attempt. This is partially mitigated by the
  // workflow_state guard above (which rejects calls for instances already
  // in a terminate:* state), but a failed workflow that left the instance
  // in a non-terminal state would be unretriable until the idempotency_key
  // row is manually cleared. Fix: submit() should only dedup against
  // non-terminal workflows (#WF2-IDEM-RETRY).
  const result = await submit({
    type: "terminate-instance",
    input: { instanceId: input.instanceId } as unknown as Record<string, unknown>,
    idempotencyKey: `terminate-instance-${input.instanceId}`,
    tenantId: input.tenantId,
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
