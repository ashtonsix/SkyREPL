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

  input: {
    spec: string;
    region?: string;
    spot?: boolean;
    bootstrap: BootstrapConfig;
  };

  output: {
    instanceId: string;      // Our internal ID
    providerId: string;      // Provider's instance ID
    ip: string | null;       // Public IP (may be null initially)
    status: string;
  };
}

// =============================================================================
// Terminate Instance Contract
// =============================================================================

export interface TerminateInstanceContract extends ProviderWorkflowContract {
  type: "terminate-instance";

  input: {
    providerId: string;
    gracePeriodMs?: number;
  };

  output: {
    terminated: boolean;
    finalStatus: string;
  };
}

// =============================================================================
// Create Snapshot Contract
// =============================================================================

export interface CreateSnapshotContract extends ProviderWorkflowContract {
  type: "create-snapshot";

  input: {
    providerId: string;
    name: string;
    spec: string;
    initChecksum?: string;
  };

  output: {
    snapshotId: string;
    providerSnapshotId: string;
    sizeBytes?: number;
  };
}

// =============================================================================
// Registry
// =============================================================================

const PROVIDER_WORKFLOW_REGISTRY = new Map<string, ProviderWorkflowContract>();

export function registerProviderWorkflow(contract: ProviderWorkflowContract): void {
  const key = `${contract.provider}:${contract.type}`;
  if (PROVIDER_WORKFLOW_REGISTRY.has(key)) {
    throw new Error(`Provider workflow already registered: ${key}`);
  }
  PROVIDER_WORKFLOW_REGISTRY.set(key, contract);
}

export function getProviderWorkflow(
  provider: ProviderName,
  type: string
): ProviderWorkflowContract | undefined {
  const key = `${provider}:${type}`;
  return PROVIDER_WORKFLOW_REGISTRY.get(key);
}
