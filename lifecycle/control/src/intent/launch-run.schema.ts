// intent/launch-run.schema.ts - TypeBox schemas for launch-run workflow I/O
// Intent-space artifact: defines the contract between workflow submission and
// node executors. Used at submission time (Value.Parse) and node completion
// (Value.Check) to catch type drift early.

import { Type, type Static, type TSchema } from '@sinclair/typebox';
import { Value } from '@sinclair/typebox/value';

// =============================================================================
// Workflow Input Schema
// =============================================================================

export const LaunchRunWorkflowInputSchema = Type.Object({
  runId: Type.Number(),
  command: Type.String(),
  spec: Type.String(),
  preferWarmPool: Type.Optional(Type.Boolean()),
  allowSpotFallback: Type.Optional(Type.Boolean()),
  provider: Type.Optional(Type.String()),
  region: Type.Optional(Type.String()),
  workdir: Type.Optional(Type.String()),
  env: Type.Optional(Type.Record(Type.String(), Type.String())),
  maxDurationMs: Type.Optional(Type.Number()),
  initChecksum: Type.Optional(Type.String()),
  files: Type.Optional(
    Type.Array(
      Type.Object({
        path: Type.String(),
        checksum: Type.String(),
        sizeBytes: Type.Optional(Type.Number()),
      })
    )
  ),
  artifactPatterns: Type.Optional(Type.Array(Type.String())),
  holdDurationMs: Type.Optional(Type.Number()),
  createSnapshot: Type.Optional(Type.Boolean()),
  idempotencyKey: Type.Optional(Type.String()),
  diskSizeGb: Type.Optional(Type.Number()),
});

export type LaunchRunWorkflowInput = Static<typeof LaunchRunWorkflowInputSchema>;

// =============================================================================
// Node Output Schemas
// =============================================================================

// check-budget
export const CheckBudgetOutputSchema = Type.Object({
  budgetOk: Type.Boolean(),
});
export type CheckBudgetOutput = Static<typeof CheckBudgetOutputSchema>;

// resolve-instance
export const ResolveInstanceOutputSchema = Type.Object({
  warmAvailable: Type.Boolean(),
  allocationId: Type.Optional(Type.Number()),
  instanceId: Type.Optional(Type.Number()),
});
export type ResolveInstanceOutput = Static<typeof ResolveInstanceOutputSchema>;

// claim-allocation
export const ClaimAllocationOutputSchema = Type.Object({
  allocationId: Type.Number(),
  instanceId: Type.Number(),
  fromWarmPool: Type.Literal(true),
});
export type ClaimAllocationOutput = Static<typeof ClaimAllocationOutputSchema>;

// spawn-instance
export const SpawnInstanceOutputSchema = Type.Object({
  instanceId: Type.Number(),
  providerId: Type.String(),
  ip: Type.Union([Type.String(), Type.Null()]),
  status: Type.String(),
});
export type SpawnInstanceOutput = Static<typeof SpawnInstanceOutputSchema>;

// wait-for-boot (FIXED: camelCase, not snake_case)
export const WaitForBootOutputSchema = Type.Object({
  instanceId: Type.Number(),
  bootDurationMs: Type.Number(),
  ip: Type.String(),
});
export type WaitForBootOutput = Static<typeof WaitForBootOutputSchema>;

// create-allocation
export const CreateAllocationOutputSchema = Type.Object({
  allocationId: Type.Number(),
  instanceId: Type.Number(),
  workdir: Type.String(),
});
export type CreateAllocationOutput = Static<typeof CreateAllocationOutputSchema>;

// start-run
export const StartRunOutputSchema = Type.Object({
  started: Type.Boolean(),
  syncedAt: Type.Number(),
  filesSynced: Type.Number(),
});
export type StartRunOutput = Static<typeof StartRunOutputSchema>;

// wait-completion
export const WaitCompletionOutputSchema = Type.Object({
  exitCode: Type.Number(),
  completedAt: Type.Number(),
  durationMs: Type.Number(),
  spotInterrupted: Type.Boolean(),
  timedOut: Type.Boolean(),
});
export type WaitCompletionOutput = Static<typeof WaitCompletionOutputSchema>;

// finalize
export const FinalizeOutputSchema = Type.Object({
  allocationStatus: Type.String(),
  runStatus: Type.String(),
  manifestSealed: Type.Boolean(),
  snapshotId: Type.Optional(Type.Number()),
});
export type FinalizeOutput = Static<typeof FinalizeOutputSchema>;

// =============================================================================
// Node Output Schema Registry
// =============================================================================

/** Maps node type names to their TypeBox output schemas for runtime validation. */
export const NODE_OUTPUT_SCHEMAS: Record<string, TSchema> = {
  'check-budget': CheckBudgetOutputSchema,
  'resolve-instance': ResolveInstanceOutputSchema,
  'claim-allocation': ClaimAllocationOutputSchema,
  'spawn-instance': SpawnInstanceOutputSchema,
  'wait-for-boot': WaitForBootOutputSchema,
  'create-allocation': CreateAllocationOutputSchema,
  'start-run': StartRunOutputSchema,
  'wait-completion': WaitCompletionOutputSchema,
  'finalize': FinalizeOutputSchema,
};

// =============================================================================
// Workflow Input Schema Registry
// =============================================================================

/** Maps workflow type names to their TypeBox input schemas for submission validation. */
export const WORKFLOW_INPUT_SCHEMAS: Record<string, TSchema> = {
  'launch-run': LaunchRunWorkflowInputSchema,
};

// =============================================================================
// Validation Helpers
// =============================================================================

export interface SchemaValidationError {
  path: string;
  message: string;
  value: unknown;
}

/**
 * Validate a node output value against its registered schema.
 * Returns null on success, or an array of errors on failure.
 * Returns null if no schema is registered for the given node type.
 */
export function checkNodeOutput(
  nodeType: string,
  output: unknown
): SchemaValidationError[] | null {
  const schema = NODE_OUTPUT_SCHEMAS[nodeType];
  if (!schema) return null;
  if (Value.Check(schema, output)) return null;
  return [...Value.Errors(schema, output)].map((e) => ({
    path: e.path,
    message: e.message,
    value: e.value,
  }));
}

/**
 * Validate a workflow input against its registered schema.
 * Returns null on success, or an array of errors on failure.
 * Returns null if no schema is registered for the given workflow type.
 */
export function checkWorkflowInput(
  workflowType: string,
  input: unknown
): SchemaValidationError[] | null {
  const schema = WORKFLOW_INPUT_SCHEMAS[workflowType];
  if (!schema) return null;
  if (Value.Check(schema, input)) return null;
  return [...Value.Errors(schema, input)].map((e) => ({
    path: e.path,
    message: e.message,
    value: e.value,
  }));
}
