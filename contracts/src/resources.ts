// resources.ts - Materialized Resource Interfaces

import type {
  ID,
  TimestampMs,
  DurationMs,
  Checksum,
  ProviderName,
  WorkflowState,
  AllocationStatus,
  WorkflowExecutionStatus,
  ManifestStatus,
  NodeStatus,
} from './types';

// =============================================================================
// PRIMARY RESOURCES (Materialized from database records)
// =============================================================================

/** Materialized Instance resource */
export interface Instance {
  id: ID;
  tenantId: number;
  provider: ProviderName;
  providerId: string;
  spec: string;
  region: string;
  ip: string | null;
  workflowState: WorkflowState;
  workflowError: string | null;
  isSpot: boolean;
  spotRequestId: string | null;
  initChecksum: Checksum | null;
  spawnIdempotencyKey: string | null; // Format: ${workflow_id}:${node_id}
  createdAt: TimestampMs;
  lastHeartbeat: TimestampMs;
  currentManifestId: ID | null;

  // Relationships (populated based on materialization depth)
  features?: FeatureInstallation[];
  tailscaleMachine?: TailscaleMachine;

  // External state (populated if includeExternal=true)
  externalStatus?: string;
  externalIp?: string;
}

/** Materialized Run resource */
export interface Run {
  id: ID;
  tenantId: number;
  command: string;
  workdir: string;
  maxDurationMs: DurationMs;
  workflowState: WorkflowState;
  workflowError: string | null;
  exitCode: number | null;
  createSnapshot: boolean;
  initChecksum: Checksum | null;
  spotInterrupted: boolean;
  createdAt: TimestampMs;
  startedAt: TimestampMs | null;
  finishedAt: TimestampMs | null;
  currentManifestId: ID | null;

  // Relationships (populated based on materialization depth)
  files?: RunFile[];
  logs?: RunLogEntry[];
  artifacts?: Artifact[];
  allocation?: Allocation;
}

/** Materialized Allocation resource */
export interface Allocation {
  id: ID;
  tenantId: number;
  runId: ID | null;
  instanceId: ID;
  user: string;
  workdir: string;
  status: AllocationStatus;
  debugHoldUntil: TimestampMs | null; // Blocks termination if > NOW, enables SSH when COMPLETE
  claimedBy: number | null;
  completedAt: TimestampMs | null;
  createdAt: TimestampMs;
  updatedAt: TimestampMs;
  currentManifestId: ID | null;

  // Relationships (populated based on materialization depth)
  instance?: Instance;
  run?: Run;
}

/** Materialized Workflow resource */
export interface Workflow {
  id: ID;
  tenantId: number;
  type: string;
  parentWorkflowId: ID | null;
  depth: number;
  status: WorkflowExecutionStatus;
  currentNode: string | null;
  input: Record<string, unknown>;
  output: Record<string, unknown> | null;
  error: WorkflowError | null;
  manifestId: ID | null;
  traceId: string | null;
  idempotencyKey: string | null;
  createdBy: number | null;
  timeoutMs: number | null;
  timeoutAt: number | null;
  createdAt: TimestampMs;
  startedAt: TimestampMs | null;
  finishedAt: TimestampMs | null;

  // Relationships (populated based on materialization depth)
  nodes?: WorkflowNode[];
  manifest?: Manifest;
}

/** Materialized Manifest resource */
export interface Manifest {
  id: ID;
  tenantId: number;
  workflowId: ID;
  status: ManifestStatus;
  defaultCleanupPriority: number;
  retentionMs: DurationMs | null;
  createdBy: number | null;
  createdAt: TimestampMs;
  releasedAt: TimestampMs | null;
  expiresAt: TimestampMs | null;

  // Relationships (populated based on materialization depth)
  resources?: ManifestResource[];
}

// =============================================================================
// SIMPLE RESOURCES (Object-backed)
// =============================================================================

/** Snapshot resource (object-backed) */
export interface Snapshot {
  id: ID;
  provider: ProviderName;
  providerId: string;
  spec: string;
  initChecksum: Checksum;
  name: string;
  createdAt: TimestampMs;
  lastUsedAt: TimestampMs;
  useCount: number;

  // External state
  externalExists?: boolean;
}

/** Artifact resource (object-backed) */
export interface Artifact {
  id: ID;
  runId: ID;
  path: string;
  sizeBytes: number;
  keep: boolean;
  createdAt: TimestampMs;
}

/** Feature installation (object-backed) */
export interface FeatureInstallation {
  id: ID;
  instanceId: ID;
  featureName: string;
  version: string;
  status: 'installing' | 'ready' | 'failed';
  port: number | null;
  url: string | null;
  installedAt: TimestampMs;
}

/** Tailscale machine (object-backed) */
export interface TailscaleMachine {
  id: ID;
  instanceId: ID;
  tailscaleIp: string;
  hostname: string;
  authKeyUsed: boolean;
  createdAt: TimestampMs;
}

// =============================================================================
// SUPPORTING TYPES
// =============================================================================

/** Run file manifest entry */
export interface RunFile {
  path: string;
  checksum: Checksum;
  sizeBytes: number;
}

/** Run log entry */
export interface RunLogEntry {
  stream: 'stdout' | 'stderr';
  data: string;
  timestamp: TimestampMs;
}

/** Usage record (materialized billing data) */
export interface UsageRecord {
  id: ID;
  tenantId: number;
  instanceId: ID;
  allocationId: ID | null;
  runId: ID | null;
  provider: ProviderName;
  spec: string;
  region: string | null;
  isSpot: boolean;
  startedAt: TimestampMs;
  finishedAt: TimestampMs | null;
  durationMs: DurationMs | null;
  estimatedCostUsd: number | null;
}

/** Workflow node */
export interface WorkflowNode {
  id: ID;
  workflowId: ID;
  nodeId: string;
  nodeType: string;
  status: NodeStatus;
  input: Record<string, unknown>;
  output: Record<string, unknown> | null;
  error: NodeError | null;
  dependsOn: string;
  attempt: number;
  retryReason: string | null;
  createdAt: TimestampMs;
  startedAt: TimestampMs | null;
  finishedAt: TimestampMs | null;
}

/** Manifest resource link */
export interface ManifestResource {
  manifestId: ID;
  resourceType: 'instance' | 'run' | 'allocation' | 'object';
  resourceId: string;
  cleanupPriority: number | null;
  addedAt: TimestampMs;
  ownerType: 'manifest' | 'released' | 'workflow' | 'policy';
  ownerId: number | null; // ID of owner (workflow_id, policy_id, etc)
}

// =============================================================================
// ERROR DETAIL TYPES (used in resource fields)
// =============================================================================

/** Error categories */
export type ErrorCategory =
  | 'validation'
  | 'auth'
  | 'not_found'
  | 'conflict'
  | 'rate_limit'
  | 'provider'
  | 'timeout'
  | 'internal';

/**
 * Workflow error details.
 *
 * Used in workflow.error (typed field) and workflow_error (JSON string in
 * database records). When stored in the database, this object is JSON-serialized.
 */
export interface WorkflowError {
  code: string;
  message: string;
  nodeId?: string;
  category: ErrorCategory;
  details?: Record<string, unknown>;
}

/**
 * Node error details.
 *
 * Similar to WorkflowError but includes retryable flag for retry logic.
 * Also JSON-serialized when stored in node.error_json database field.
 */
export interface NodeError {
  code: string;
  message: string;
  category: ErrorCategory;
  details?: Record<string, unknown>;
  retryable: boolean;
}

/** Pattern audit entry (observability, not stored in core data) */
export interface PatternAuditEntry {
  pattern: PatternType;
  timestamp: TimestampMs;
  workflowId: ID;
  triggeredBy: string;
  details: Record<string, unknown>;
}

/** Pattern types from fixed library */
export type PatternType =
  | 'insert-and-reconverge'
  | 'conditional-branch'
  | 'parallel-fan-out'
  | 'retry-with-alternative';
