// intent/terminate-instance.types.ts - Terminate Instance Intent Types

import { Type, type Static } from '@sinclair/typebox';

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
