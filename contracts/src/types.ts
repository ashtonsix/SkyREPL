// types.ts - Cross-cutting Primitives and Utilities

// =============================================================================
// PRIMITIVES
// =============================================================================

/** INTEGER PRIMARY KEY, displayed as base-36 slug (e.g., ID 12345 -> "9ix") */
export type ID = number;

/** Unix timestamp in milliseconds */
export type TimestampMs = number;

/** Duration in milliseconds */
export type DurationMs = number;

/** SHA256 hash as hex string */
export type Checksum = string;

export type ProviderName = 'aws' | 'digitalocean' | 'lambda' | 'runpod' | 'orbstack';

/** Feature provider name */
export type FeatureProviderName = 'tailscale' | (string & {});

// =============================================================================
// SEMANTIC ID ALIASES
// =============================================================================

// All INTEGER PRIMARY KEY, but semantically distinct
export type InstanceId = ID;
export type RunId = ID;
export type AllocationId = ID;
export type WorkflowId = ID;
export type ManifestId = ID;
export type ObjectId = ID;
export type NodeRecordId = ID;

// Provider IDs (NOT INTEGER PKs - provider-specific format)
export type ProviderId = string; // i-xxxxx, pod-xxxxx, etc.
export type ProviderSnapshotId = string; // ami-xxxxx, snap-xxxxx, etc.

// =============================================================================
// ID UTILITIES
// =============================================================================

/** Convert integer ID to base-36 slug for display */
export function idToSlug(id: number): string {
  return id.toString(36);
}

/** Parse base-36 slug back to integer ID */
export function slugToId(slug: string): number {
  return parseInt(slug, 36);
}

/** Alias for idToSlug */
export const formatId = idToSlug;

/** Parse user-provided ID input (base-36 slug) to integer */
export function parseInputId(input: string): number {
  const id = parseInt(input, 36);
  if (!Number.isFinite(id) || id <= 0) {
    throw new Error(`Invalid ID: "${input}"`);
  }
  return id;
}

// =============================================================================
// INSTANCE SPEC
// =============================================================================

/** Normalized instance specification across providers */
export interface InstanceSpec {
  instanceType: string; // Provider-specific identifier (e.g., 'g5.xlarge', 'gpu_1x_a100')
  vcpus: number; // Virtual CPU cores
  memoryMb: number; // Memory in megabytes
  gpuCount?: number; // Number of GPUs
  gpuType?: string; // Canonical GPU model name (e.g., 'A100', 'H100', 'RTX4090')
  storageMb?: number; // Ephemeral storage in megabytes
  spot?: boolean; // Whether spot/preemptible instance
}

// =============================================================================
// STATUS ENUMS
// =============================================================================

/**
 * Workflow-aware state format: "<workflow>:<phase>"
 * Examples: 'spawn:pending', 'launch-run:provisioning', 'terminate:complete'
 *
 * Availability is INFERRED, not stored:
 *   isAvailable(r) = r.workflowState.endsWith(':complete') && r.currentManifestId === null
 */
export type WorkflowState = `${string}:${string}`;

/** @deprecated Use WorkflowState instead - kept for backward compatibility */
export type ResourceStatus = WorkflowState;

/** Allocation state machine states */
export type AllocationStatus = "AVAILABLE" | "CLAIMED" | "ACTIVE" | "COMPLETE" | "FAILED";

/** Workflow execution states */
export type WorkflowExecutionStatus =
  | 'pending'
  | 'running'
  | 'completed'
  | 'failed'
  | 'cancelled'
  | 'cancelling'
  | 'rolling_back';

/** Manifest ownership states */
export type ManifestStatus = 'DRAFT' | 'SEALED';
// Note: EXPIRED is derived (manifest with no owned resources), not a stored state

/** Workflow node states */
export type NodeStatus =
  | 'pending'
  | 'running'
  | 'completed'
  | 'failed'
  | 'skipped';

/** Tailscale installation status */
export type TailscaleStatus =
  | 'not_installed'
  | 'installing'
  | 'ready'
  | 'failed';

// =============================================================================
// WORKFLOW STATE PARSING
// =============================================================================

/** Status parsing result */
export interface ParsedStatus {
  workflow: string | null;
  state: string;
}

/** Parse workflow-aware status string */
export function parseStatus(status: string): ParsedStatus {
  const colonIndex = status.indexOf(':');
  if (colonIndex === -1) {
    return { workflow: null, state: status };
  }
  return {
    workflow: status.substring(0, colonIndex),
    state: status.substring(colonIndex + 1),
  };
}

/** Build workflow_state string */
export function buildWorkflowState(
  workflow: string,
  phase: string,
): WorkflowState {
  return `${workflow}:${phase}` as WorkflowState;
}

/** @deprecated Use buildWorkflowState instead */
export const buildStatus = buildWorkflowState;

/** Check if status indicates active workflow */
export function isActiveStatus(status: string): boolean {
  return status.includes(':');
}

/** Check if status is terminal (no active workflow) */
export function isTerminalStatus(status: string): boolean {
  return !status.includes(':');
}

/**
 * Check if resource is available for use.
 * Availability is INFERRED from workflow_state and ownership, not stored.
 */
export function isAvailable(resource: {
  workflowState: string;
  currentManifestId: number | null;
}): boolean {
  return (
    resource.workflowState?.endsWith(':complete') &&
    resource.currentManifestId === null
  );
}

// =============================================================================
// WORKFLOW STATE BUILDERS
// =============================================================================

// Pre-defined workflow_state builders for instances
export const InstanceWorkflowState = {
  // Spawn workflow
  spawnPending: () => buildWorkflowState('spawn', 'pending'),
  spawnError: () => buildWorkflowState('spawn', 'error'),

  // Launch-run workflow
  launchProvisioning: () =>
    buildWorkflowState('launch-run', 'provisioning'),
  launchBootstrapping: () =>
    buildWorkflowState('launch-run', 'bootstrapping'),
  launchInstallingFeatures: () =>
    buildWorkflowState('launch-run', 'installing_features'),
  launchComplete: () => buildWorkflowState('launch-run', 'complete'),
  launchError: () => buildWorkflowState('launch-run', 'error'),

  // Terminate workflow
  terminateDraining: () => buildWorkflowState('terminate', 'draining'),
  terminateComplete: () => buildWorkflowState('terminate', 'complete'),

  // Create-snapshot workflow
  snapshotPreparing: () =>
    buildWorkflowState('create-snapshot', 'preparing'),
  snapshotting: () =>
    buildWorkflowState('create-snapshot', 'snapshotting'),
  snapshotComplete: () =>
    buildWorkflowState('create-snapshot', 'complete'),
} as const;

// Pre-defined workflow_state builders for runs
export const RunWorkflowState = {
  pending: () => buildWorkflowState('launch-run', 'pending'),
  preparing: () => buildWorkflowState('launch-run', 'preparing'),
  running: () => buildWorkflowState('launch-run', 'running'),
  complete: () => buildWorkflowState('launch-run', 'complete'),
  error: () => buildWorkflowState('launch-run', 'error'),
  cancelled: () => buildWorkflowState('launch-run', 'cancelled'),
  timeout: () => buildWorkflowState('launch-run', 'timeout'),
} as const;

// =============================================================================
// TIMESTAMP UTILITIES
// =============================================================================

export const Timestamp = {
  now: (): TimestampMs => Date.now(),
  fromDate: (date: Date): TimestampMs => date.getTime(),
  toDate: (ts: TimestampMs): Date => new Date(ts),
  addMs: (ts: TimestampMs, ms: DurationMs): TimestampMs => ts + ms,
  isExpired: (expiresAt: TimestampMs | null): boolean =>
    expiresAt !== null && Date.now() > expiresAt,
} as const;

// =============================================================================
// DURATION UTILITIES
// =============================================================================

export const Duration = {
  SECOND: 1000 as const,
  MINUTE: 60_000 as const,
  HOUR: 3_600_000 as const,
  DAY: 86_400_000 as const,

  seconds: (n: number): DurationMs => n * 1000,
  minutes: (n: number): DurationMs => n * 60_000,
  hours: (n: number): DurationMs => n * 3_600_000,
  days: (n: number): DurationMs => n * 86_400_000,

  /** Format milliseconds to human-readable string */
  format: (ms: DurationMs): string => {
    if (ms < 1000) return `${ms}ms`;
    if (ms < 60_000)
      return `${(ms / 1000).toFixed(1).replace(/\.0$/, '')}s`;
    if (ms < 3_600_000)
      return `${(ms / 60_000).toFixed(1).replace(/\.0$/, '')}m`;
    if (ms < 86_400_000)
      return `${(ms / 3_600_000).toFixed(1).replace(/\.0$/, '')}h`;
    return `${(ms / 86_400_000).toFixed(1).replace(/\.0$/, '')}d`;
  },

  /** Parse duration string like "5m", "2h", "30s", "1d" */
  parse: (str: string): DurationMs => {
    const match = str.match(/^(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)$/i);
    if (!match) throw new Error(`Invalid duration: ${str}`);
    const [, num, unit] = match;
    const n = parseFloat(num!);
    switch (unit!.toLowerCase()) {
      case 'ms':
        return n;
      case 's':
        return n * 1000;
      case 'm':
        return n * 60_000;
      case 'h':
        return n * 3_600_000;
      case 'd':
        return n * 86_400_000;
      default:
        throw new Error(`Unknown duration unit: ${unit}`);
    }
  },
} as const;
