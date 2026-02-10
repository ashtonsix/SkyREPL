// intent/launch-run.types.ts - Launch Run Intent Types
// Types are derived from TypeBox schemas in launch-run.schema.ts.
// This file re-exports for backward compatibility and adds the
// workflow-level LaunchRunOutput (not a node output, so not in schema).

// =============================================================================
// Re-exports from schema (all node output types + workflow input)
// =============================================================================

export type {
  LaunchRunWorkflowInput,
  CheckBudgetOutput,
  ResolveInstanceOutput,
  ClaimAllocationOutput,
  SpawnInstanceOutput,
  WaitForBootOutput,
  CreateAllocationOutput,
  StartRunOutput,
  WaitCompletionOutput,
  FinalizeOutput,
} from './launch-run.schema';

// =============================================================================
// Launch Run Input (alias for backward compatibility)
// =============================================================================

// LaunchRunInput is the same shape as LaunchRunWorkflowInput but without the
// required runId (which is added by launchRun() after creating the DB record).
// We keep this as a standalone interface because it's the public API surface,
// while LaunchRunWorkflowInput includes runId for the internal workflow.

export interface LaunchRunInput {
  /** DB ID for the run record */
  runId: number;

  /** Command to execute */
  command: string;

  /** Instance specification (e.g., "gpu-a100-80gb") */
  spec: string;

  /** Whether to check warm pool first (default: true) */
  preferWarmPool?: boolean;

  /** Try spot, fall back to on-demand (default: true) */
  allowSpotFallback?: boolean;

  /** Specific provider, or let system choose */
  provider?: string;

  /** Specific region, or let system choose */
  region?: string;

  /** Override workdir (default: auto-assigned) */
  workdir?: string;

  /** Environment variables */
  env?: Record<string, string>;

  /** Timeout in milliseconds (default: 24 hours) */
  maxDurationMs?: number;

  /** Checksum for warm pool matching */
  initChecksum?: string;

  /** Files to sync */
  files?: Array<{ path: string; checksum: string; sizeBytes?: number }>;

  /** Glob patterns for artifact collection */
  artifactPatterns?: string[];

  /** Hold period after run in ms (default: 5 min) */
  holdDurationMs?: number;

  /** Create snapshot after run (default: false) */
  createSnapshot?: boolean;

  /** For duplicate submission prevention */
  idempotencyKey?: string;
}

// =============================================================================
// Launch Run Output
// =============================================================================

export interface LaunchRunOutput {
  workflowId: number;
  runId: number;
  instanceId: number;
  allocationId: number;
  fromWarmPool: boolean;
  exitCode: number | null;
  spotInterrupted: boolean;
  snapshotId?: number;
  artifacts?: Array<{ id: number; path: string; sizeBytes: number }>;
}
