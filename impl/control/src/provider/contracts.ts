// provider/contracts.ts - Provider Workflow Constraints & Contracts
// Stub: All function bodies throw "not implemented"

import type { ProviderName, BootstrapConfig } from "./types";

// =============================================================================
// Types
// =============================================================================

export interface ProviderWorkflowConstraints {
  maxDurationMs: number;
  requiredOutputFields: string[];
  inputSchema?: Record<string, unknown>;
  emittableResources: string[];
  canSpawnSubworkflows: boolean;
  maxSubworkflowDepth: number;
}

export interface ProviderWorkflowContract {
  type: string;
  provider: ProviderName;
  description: string;
  constraints: ProviderWorkflowConstraints;
  validateInput(input: unknown): ValidationResult;
  compensate(context: CompensationContext): Promise<void>;
}

export interface ValidationResult {
  valid: boolean;
  errors?: string[];
}

export interface CompensationContext {
  workflowId: number;
  nodeId: string;
  input: unknown;
  output?: unknown;
}

// =============================================================================
// Spawn Instance Contract
// =============================================================================

export interface SpawnInstanceContract extends ProviderWorkflowContract {
  type: "spawn-instance";
}

// =============================================================================
// Terminate Instance Contract
// =============================================================================

export interface TerminateInstanceContract extends ProviderWorkflowContract {
  type: "terminate-instance";
}

// =============================================================================
// Create Snapshot Contract
// =============================================================================

export interface CreateSnapshotContract extends ProviderWorkflowContract {
  type: "create-snapshot";
}

// =============================================================================
// Registry
// =============================================================================

export function registerProviderWorkflow(contract: ProviderWorkflowContract): void {
  throw new Error("not implemented");
}

export function getProviderWorkflow(
  provider: ProviderName,
  type: string
): ProviderWorkflowContract | undefined {
  throw new Error("not implemented");
}
