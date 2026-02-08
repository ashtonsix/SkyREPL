// intent/launch-run.types.ts - Launch Run Intent Types
// Fully defined types (no stubs needed for type-only files)

// =============================================================================
// Launch Run Input
// =============================================================================

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
