// workflow/engine.types.ts - Workflow Engine Types
// Fully defined types (no stubs needed for type-only files)

import type { TSchema } from "@sinclair/typebox";

// =============================================================================
// Node Executor Interface
// =============================================================================

export interface NodeExecutor<TInput = unknown, TOutput = unknown> {
  /** Node type name */
  name: string;

  /** Whether this node is safe to retry after crash */
  idempotent: boolean;

  /** Optional pre-execution validation (§6.2). Rejects with VALIDATION_ERROR (non-retryable, no compensation). */
  validate?(ctx: NodeContext): Promise<void>;

  /** Execute the node logic */
  execute(ctx: NodeContext): Promise<TOutput>;

  /** Optional compensation for rollback */
  compensate?(ctx: NodeContext): Promise<void>;
}

// =============================================================================
// Node Context
// =============================================================================

export interface NodeContext {
  /** Parent workflow ID */
  workflowId: number;

  /** This node's ID within the workflow */
  nodeId: string;

  /** Node input data */
  input: unknown;

  /** Workflow-level input data (e.g., LaunchRunInput) */
  workflowInput: unknown;

  /** Node output (available during compensation) */
  output?: unknown;

  /** Manifest ID for resource tracking */
  manifestId: number | null;

  /** Control plane installation ID for naming convention */
  controlId: string;

  /** Tenant ID from the owning workflow (for resource creation) */
  tenantId: number;

  /** Emit a resource to the manifest */
  emitResource(type: string, id: number | string, cleanupPriority: number): void;

  /** Claim a resource from another manifest */
  claimResource(manifestId: number, resourceType: string, resourceId: string): Promise<boolean>;

  /** Apply a workflow pattern (CB, IAR, PFO, RWA) */
  applyPattern(pattern: string, config: unknown): void;

  /** Get output from a dependency node */
  getNodeOutput(nodeId: string): unknown;

  /** Log a message */
  log(level: "info" | "warn" | "error" | "debug", message: string, data?: Record<string, unknown>): void;

  /** Check if workflow has been cancelled */
  checkCancellation(): void;

  /** Sleep for a duration (cancellation-aware) */
  sleep(ms: number): Promise<void>;

  /** Spawn a subworkflow */
  spawnSubworkflow(type: string, input: unknown): Promise<SubworkflowHandle>;
}

// =============================================================================
// Workflow Submission
// =============================================================================

export interface WorkflowSubmission {
  /** Workflow type (e.g., 'launch-run') */
  type: string;

  /** Input parameters */
  input: Record<string, unknown>;

  /** Optional idempotency key */
  idempotencyKey?: string;

  /** Optional parent workflow ID (for subworkflows) */
  parentWorkflowId?: number;

  /** Optional priority */
  priority?: number;

  /** Tenant ID for multi-tenant isolation */
  tenantId?: number;

  /** Optional timeout in milliseconds */
  timeout?: number;
}

export interface WorkflowSubmissionResult {
  workflowId: number;
  status: "created" | "deduplicated";
  existingWorkflowId?: number;
}

// =============================================================================
// Workflow Blueprint
// =============================================================================

export interface WorkflowBlueprint {
  /** Workflow type name */
  type: string;

  /** Node definitions */
  nodes: Record<string, NodeDefinition>;

  /** Entry node ID */
  entryNode: string;

  /** Optional TypeBox schema for validating workflow input at submission time */
  inputSchema?: TSchema;

  /** Optional map of node type -> TypeBox schema for validating node outputs */
  nodeOutputSchemas?: Record<string, TSchema>;
}

export interface NodeDefinition {
  /** Node type (maps to a registered NodeExecutor) */
  type: string;

  /** Dependencies: node IDs that must complete before this node */
  dependsOn?: string[];

  /** Node-level timeout override (ms) */
  timeout?: number;

  /** Input schema for validation */
  inputSchema?: Record<string, unknown>;
}

// =============================================================================
// Inline Node Definition (for pattern applications)
// =============================================================================

export interface InlineNodeDef {
  id: string;
  type: string;
  input: Record<string, unknown>;
}

// =============================================================================
// Subworkflow Handle
// =============================================================================

export interface SubworkflowHandle {
  workflowId: number;
  wait(): Promise<SubworkflowResult>;
}

export interface SubworkflowResult {
  status: "completed" | "failed" | "cancelled";
  output?: Record<string, unknown>;
  error?: string;
  manifestId?: number;
}

// =============================================================================
// Retry Decision
// =============================================================================

export interface RetryDecision {
  shouldRetry: boolean;
  strategy: "same_params" | "alternative" | "fallback" | null;
  delayMs: number;
  maxAttempts: number;
}

// =============================================================================
// Node Error
// =============================================================================

export interface NodeError {
  code: string;
  message: string;
  category?: string;
  details?: Record<string, unknown>;
}

// =============================================================================
// Workflow Error
// =============================================================================

export interface WorkflowError {
  code: string;
  message: string;
  category: string;
  details?: Record<string, unknown>;
}
