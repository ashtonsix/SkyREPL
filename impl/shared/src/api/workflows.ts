// api/workflows.ts - Workflow API Types + TypeBox Schemas

import { Type, type Static, type TSchema } from '@sinclair/typebox';
import type {
  ID,
  TimestampMs,
  DurationMs,
  Checksum,
  ProviderName,
  NodeStatus,
  WorkflowExecutionStatus,
} from '../types';
import type {
  WorkflowError,
  NodeError,
  PatternType,
} from '../resources';

// =============================================================================
// WORKFLOW DEFINITION (BLUEPRINTS)
// =============================================================================

/** Workflow type identifier */
export type WorkflowType =
  | 'launch-run'
  | 'terminate-instance'
  | 'create-snapshot'
  | 'cleanup'
  | (string & {}); // Extensible for provider-defined workflows

/** JSON Schema type (simplified) */
export interface JsonSchema {
  type: 'object' | 'array' | 'string' | 'number' | 'boolean';
  properties?: Record<string, JsonSchema>;
  required?: string[];
  items?: JsonSchema;
  enum?: unknown[];
  description?: string;
}

/** Retry policy for nodes */
export interface RetryPolicy {
  maxAttempts: number;
  backoffMs: number;
  backoffMultiplier: number;
  maxBackoffMs: number;
  retryableErrors: string[];
}

/** Node definition within blueprint */
export interface NodeDefinition {
  type: string;
  description?: string;
  inputSchema?: JsonSchema;
  outputSchema?: JsonSchema;
  next: string | null; // null = terminal
  dependencies?: string[]; // Nodes that must complete first
  timeout?: number;
  retryPolicy?: RetryPolicy;
  compensation?: string; // Compensation node type
}

/** Pattern-specific constraints */
export interface PatternConstraints {
  maxBranches?: number; // For parallel-fan-out
  maxRetries?: number; // For retry-with-alternative
}

/** Pattern allowlist for blueprint */
export interface AllowedPattern {
  pattern: PatternType;
  atNodes: string[];
  constraints?: PatternConstraints;
}

/** Workflow blueprint definition */
export interface WorkflowBlueprint {
  type: WorkflowType;
  version: string;
  description: string;
  inputSchema: JsonSchema;
  outputSchema: JsonSchema;
  nodes: Record<string, NodeDefinition>;
  entryNode: string;
  patterns?: AllowedPattern[];
}

// =============================================================================
// WORKFLOW INSTANCE (RUNTIME)
// =============================================================================

/** Workflow submission request */
export interface WorkflowSubmission {
  type: WorkflowType;
  input: Record<string, unknown>;
  idempotencyKey?: string;
  parentWorkflowId?: ID;
  priority?: 'low' | 'normal' | 'high';
  timeout?: number;
}

/** Workflow submission result */
export interface WorkflowSubmissionResult {
  workflowId: ID;
  status: 'created' | 'deduplicated';
  existingWorkflowId?: ID; // If deduplicated
}

/** Workflow event types */
export type WorkflowEventType =
  | 'workflow_started'
  | 'workflow_completed'
  | 'workflow_failed'
  | 'workflow_cancelled'
  | 'node_started'
  | 'node_completed'
  | 'node_failed'
  | 'node_retrying'
  | 'pattern_applied'
  | 'subworkflow_started'
  | 'subworkflow_completed'
  | 'resource_emitted'
  | 'resource_claimed';

/** Workflow event for streaming updates */
export interface WorkflowEvent {
  timestamp: TimestampMs;
  type: WorkflowEventType;
  nodeId?: string;
  message: string;
  details?: Record<string, unknown>;
}

/** Real-time workflow status (for progress display) */
export interface WorkflowProgress {
  workflowId: ID;
  type: WorkflowType;
  status: WorkflowExecutionStatus;
  currentNode: string | null;
  nodesTotal: number;
  nodesCompleted: number;
  nodesFailed: number;
  startedAt: TimestampMs | null;
  estimatedRemainingMs: number | null;
  recentEvents: WorkflowEvent[];
}

/** Node execution state */
export interface NodeExecutionState {
  nodeId: string;
  nodeType: string;
  status: NodeStatus;
  attempt: number;
  input: Record<string, unknown>;
  output: Record<string, unknown> | null;
  error: NodeError | null;
  startedAt: TimestampMs | null;
  finishedAt: TimestampMs | null;
  durationMs: number | null;
}

// =============================================================================
// NODE CONTEXT INTERFACE
// =============================================================================

export type ResourceType = 'instance' | 'run' | 'allocation' | 'object';
export type LogLevel = 'debug' | 'info' | 'warn' | 'error';

export interface EmittedResource {
  type: ResourceType;
  id: ID;
  priority: number;
  emittedAt: TimestampMs;
}

export interface AppliedPattern {
  pattern: PatternType;
  appliedAt: TimestampMs;
  nodeId: string;
  config: Record<string, unknown>;
}

export interface SubworkflowOptions {
  timeout?: number;
  priority?: 'low' | 'normal' | 'high';
}

export interface SubworkflowResult {
  status: 'completed' | 'failed' | 'cancelled';
  output: Record<string, unknown> | null;
  error: WorkflowError | null;
}

export interface SubworkflowHandle {
  workflowId: ID;
  manifestId: ID;
  wait(): Promise<SubworkflowResult>;
  cancel(reason: string): Promise<void>;
}

// =============================================================================
// PATTERN CONFIGURATION TYPES
// =============================================================================

/** Inline node definition for pattern application */
export interface InlineNodeDef {
  id: string;
  type: string;
  input: Record<string, unknown>;
  timeout?: number;
}

/** INSERT-AND-RECONVERGE: Insert parallel node before target */
export interface InsertAndReconvergeConfig {
  insertedNode: InlineNodeDef;
  beforeNode: string;
}

/** CONDITIONAL-BRANCH: Choose between 2 mutually exclusive paths */
export interface ConditionalBranchConfig {
  condition: boolean;
  triggerOnError?: boolean;
  option1: InlineNodeDef;
  option2: InlineNodeDef;
  joinNode: string;
}

/** PARALLEL-FAN-OUT: Spawn multiple parallel work items */
export interface ParallelFanOutConfig {
  branches: InlineNodeDef[];
  joinNode: string;
}

/** RETRY-WITH-ALTERNATIVE: On failure, retry with different params */
export interface RetryWithAlternativeConfig {
  failedNodeId: string;
  alternativeNode: InlineNodeDef;
  reason: string;
}

/** Pattern config discriminated union */
export type PatternConfig<P extends PatternType> =
  P extends 'insert-and-reconverge'
    ? InsertAndReconvergeConfig
    : P extends 'conditional-branch'
      ? ConditionalBranchConfig
      : P extends 'parallel-fan-out'
        ? ParallelFanOutConfig
        : P extends 'retry-with-alternative'
          ? RetryWithAlternativeConfig
          : never;

// =============================================================================
// NODE CONTEXT INTERFACE
// =============================================================================

/** Context provided to node executors */
export interface NodeContext<TInput = Record<string, unknown>> {
  // Identity
  readonly workflowId: ID;
  readonly nodeId: string;
  readonly nodeType: string;
  readonly attempt: number;

  // Input/Output
  readonly input: TInput;

  // Manifest (resource ownership)
  readonly manifestId: ID;

  // Hierarchy
  readonly depth: number;
  readonly parentWorkflowId: ID | null;

  // Resource Management
  emitResource(type: ResourceType, id: ID, priority?: number): Promise<void>;
  claimResource(fromNodeId: string, type: ResourceType): Promise<ID>;
  listEmittedResources(): Promise<EmittedResource[]>;

  // Pattern Application
  applyPattern<P extends PatternType>(
    pattern: P,
    config: PatternConfig<P>,
  ): void;
  readonly appliedPatterns: AppliedPattern[];

  // Subworkflow
  spawnSubworkflow(
    type: WorkflowType,
    input: Record<string, unknown>,
    options?: SubworkflowOptions,
  ): Promise<SubworkflowHandle>;

  // Node Communication
  getNodeOutput(nodeId: string): Promise<Record<string, unknown> | null>;
  waitForNode(
    nodeId: string,
    timeoutMs?: number,
  ): Promise<Record<string, unknown>>;

  // Logging
  log(level: LogLevel, message: string, data?: Record<string, unknown>): void;
  debug(message: string, data?: Record<string, unknown>): void;
  info(message: string, data?: Record<string, unknown>): void;
  warn(message: string, data?: Record<string, unknown>): void;
  error(message: string, data?: Record<string, unknown>): void;

  // Utilities
  sleep(ms: number): Promise<void>;
  checkCancellation(): void; // Throws if workflow cancelled
}

/**
 * SingleStepContext: Simplified context for single-step operations.
 *
 * Used by runSingleStep() helper for atomic operations that don't need
 * full workflow orchestration (DAG, patterns, compensation).
 */
export interface SingleStepContext {
  emitResource(type: string, id: number): void;
  claimResource(manifestId: number, type: string, id: number): void;
  log(level: string, message: string, data?: unknown): void;
}

// =============================================================================
// NODE EXECUTOR INTERFACE
// =============================================================================

/** Validation error */
export interface ValidationError {
  field: string;
  message: string;
  code: string;
}

/** Validation result */
export interface ValidationResult {
  valid: boolean;
  errors?: ValidationError[];
}

/** Context for compensation (single-node cleanup on failure) */
export interface CompensationContext<TOutput = Record<string, unknown>> {
  workflowId: ID;
  nodeId: string;
  output: TOutput;
  manifestId: ID;
  log(level: LogLevel, message: string, data?: Record<string, unknown>): void;
}

/** Node executor implementation */
export interface NodeExecutor<
  TInput = Record<string, unknown>,
  TOutput = Record<string, unknown>,
> {
  /** Execute the node */
  execute(ctx: NodeContext<TInput>): Promise<TOutput>;

  /** Compensate (single-node cleanup) if needed */
  compensate?(ctx: CompensationContext<TOutput>): Promise<void>;

  /** Validate input before execution */
  validate?(input: unknown): ValidationResult;

  /** Whether node is idempotent (safe to retry after crash) */
  readonly idempotent: boolean;
}

// =============================================================================
// WORKFLOW ENGINE INTERFACE
// =============================================================================

import type { Workflow } from '../resources';

/** Main workflow engine interface */
export interface WorkflowEngine {
  /** Submit a new workflow */
  submit(request: WorkflowSubmission): Promise<WorkflowSubmissionResult>;

  /** Get workflow by ID */
  get(workflowId: ID): Promise<Workflow | null>;

  /** Get workflow progress for UI */
  getProgress(workflowId: ID): Promise<WorkflowProgress | null>;

  /** Cancel a running workflow */
  cancel(workflowId: ID, reason: string): Promise<void>;

  /** Pause a running workflow */
  pause(workflowId: ID): Promise<void>;

  /** Resume a paused workflow */
  resume(workflowId: ID): Promise<void>;

  /** Retry a failed workflow from failure point */
  retry(workflowId: ID): Promise<void>;

  /** Register node executor */
  registerExecutor(nodeType: string, executor: NodeExecutor): void;

  /** Register workflow blueprint */
  registerBlueprint(blueprint: WorkflowBlueprint): void;

  /** Subscribe to workflow events */
  subscribe(
    workflowId: ID,
    callback: (event: WorkflowEvent) => void,
  ): () => void;
}

// =============================================================================
// INTENT TYPES (control/src/intent/ -- internal to control plane)
// CLI uses API contract types above; these are workflow engine inputs.
// =============================================================================

/** File manifest entry for launch-run */
export interface FileManifestEntry {
  path: string;
  checksum: Checksum;
  sizeBytes: number;
}

/** Input for launch-run intent */
export interface LaunchRunInput {
  runId: ID;
  command: string;
  workdir: string;
  spec: string;
  provider: ProviderName;
  region: string;
  initChecksum: Checksum | null;
  useTailscale: boolean;
  maxDurationMs: DurationMs;
  holdDurationMs: DurationMs;
  createSnapshot: boolean;
  files: FileManifestEntry[];
  artifactPatterns: string[];
  env?: Record<string, string>;
}

/** Output from launch-run intent */
export interface LaunchRunOutput {
  runId: ID;
  instanceId: ID;
  allocationId: ID;
  exitCode: number;
  snapshotId: ID | null;
  spotInterrupted: boolean;
}

/** Input for terminate intent */
export interface TerminateInstanceInput {
  instanceId: ID;
  force: boolean;
  sshDrainTimeoutMs: DurationMs;
}

/** Output from terminate intent */
export interface TerminateInstanceOutput {
  instanceId: ID;
  terminatedAt: TimestampMs;
  forcedSshDisconnects: number;
  tailscaleDeregistered: boolean;
}

/** Input for snapshot intent */
export interface CreateSnapshotInput {
  instanceId: ID;
  runId: ID | null;
  name: string;
  initChecksum: Checksum;
}

/** Output from snapshot intent */
export interface CreateSnapshotOutput {
  snapshotId: ID;
  providerSnapshotId: string;
  sizeBytes: number;
}

/** Input for cleanup intent */
export interface CleanupManifestInput {
  manifestId: ID;
}

/** Cleanup error detail */
export interface CleanupError {
  resourceType: string;
  resourceId: ID;
  error: string;
  action: 'skipped' | 'retried' | 'failed';
}

/** Output from cleanup intent */
export interface CleanupManifestOutput {
  resourcesDeleted: number;
  s3ObjectsDeleted: number;
  instancesTerminated: number;
  errors: CleanupError[];
}

// =============================================================================
// TYPEBOX SCHEMAS - API Request/Response
// =============================================================================

const Nullable = <T extends TSchema>(schema: T) => Type.Union([schema, Type.Null()]);

export const LaunchRunRequestSchema = Type.Object({
  command: Type.String({ minLength: 1 }),
  workdir: Type.Optional(Type.String({ default: '.' })),
  spec: Type.String({ minLength: 1 }),
  provider: Type.Union([
    Type.Literal('aws'),
    Type.Literal('lambda'),
    Type.Literal('runpod'),
    Type.Literal('orbstack'),
  ]),
  region: Type.String({ minLength: 1 }),
  initChecksum: Type.Optional(Type.Union([Type.String(), Type.Null()], { default: null })),
  useTailscale: Type.Optional(Type.Boolean({ default: false })),
  maxDurationMs: Type.Integer({ exclusiveMinimum: 0 }),
  holdDurationMs: Type.Optional(Type.Integer({ minimum: 0, default: 300_000 })),
  createSnapshot: Type.Optional(Type.Boolean({ default: false })),
  files: Type.Optional(
    Type.Array(
      Type.Object({
        path: Type.String(),
        checksum: Type.String(),
        sizeBytes: Type.Integer({ minimum: 0 }),
      }),
      { default: [] },
    ),
  ),
  artifactPatterns: Type.Optional(Type.Array(Type.String(), { default: [] })),
  env: Type.Optional(Type.Record(Type.String(), Type.String())),
  idempotencyKey: Type.Optional(Type.String()),
});
export type LaunchRunRequest = Static<typeof LaunchRunRequestSchema>;

export const LaunchRunResponseSchema = Type.Object({
  workflowId: Type.Integer(),
  runId: Type.Integer(),
  status: Type.Union([Type.Literal('created'), Type.Literal('deduplicated')]),
});
export type LaunchRunResponse = Static<typeof LaunchRunResponseSchema>;

export const WorkflowStatusResponseSchema = Type.Object({
  workflowId: Type.Integer(),
  type: Type.String(),
  status: Type.Union([
    Type.Literal('pending'),
    Type.Literal('running'),
    Type.Literal('completed'),
    Type.Literal('failed'),
    Type.Literal('cancelled'),
    Type.Literal('cancelling'),
    Type.Literal('rolling_back'),
  ]),
  currentNode: Nullable(Type.String()),
  nodesTotal: Type.Integer({ minimum: 0 }),
  nodesCompleted: Type.Integer({ minimum: 0 }),
  nodesFailed: Type.Integer({ minimum: 0 }),
  startedAt: Nullable(Type.Number()),
  finishedAt: Nullable(Type.Number()),
  output: Nullable(Type.Record(Type.String(), Type.Unknown())),
  error: Nullable(
    Type.Object({
      code: Type.String(),
      message: Type.String(),
      category: Type.String(),
      nodeId: Type.Optional(Type.String()),
      details: Type.Optional(Type.Record(Type.String(), Type.Unknown())),
    }),
  ),
});
export type WorkflowStatusResponse = Static<typeof WorkflowStatusResponseSchema>;
