// api/workflows.ts - Workflow API Types + TypeBox Schemas

import { Type, type Static, type TSchema } from '@sinclair/typebox';
import type {
  ID,
  TimestampMs,
  DurationMs,
  Checksum,
  ProviderName,
} from '../types';

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
  init_checksum: Checksum | null;
  use_tailscale: boolean;
  max_duration_ms: DurationMs;
  hold_duration_ms: DurationMs;
  create_snapshot: boolean;
  files: FileManifestEntry[];
  artifact_patterns: string[];
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
  provider: Type.String({ minLength: 1 }),
  region: Type.Optional(Type.String({ minLength: 1 })),
  init_checksum: Type.Optional(Type.Union([Type.String(), Type.Null()], { default: null })),
  use_tailscale: Type.Optional(Type.Boolean({ default: false })),
  max_duration_ms: Type.Integer({ exclusiveMinimum: 0 }),
  hold_duration_ms: Type.Optional(Type.Integer({ minimum: 0, default: 300_000 })),
  create_snapshot: Type.Optional(Type.Boolean({ default: false })),
  files: Type.Optional(
    Type.Array(
      Type.Object({
        path: Type.String(),
        checksum: Type.String(),
        size_bytes: Type.Integer({ minimum: 0 }),
      }),
      { default: [] },
    ),
  ),
  artifact_patterns: Type.Optional(Type.Array(Type.String(), { default: [] })),
  env: Type.Optional(Type.Record(Type.String(), Type.String())),
  idempotency_key: Type.Optional(Type.String()),
  disk_size_gb: Type.Optional(Type.Integer({ minimum: 1, maximum: 16384 })),
});
export type LaunchRunRequest = Static<typeof LaunchRunRequestSchema>;

export const LaunchRunResponseSchema = Type.Object({
  workflow_id: Type.Integer(),
  run_id: Type.Integer(),
  status: Type.Union([Type.Literal('created'), Type.Literal('deduplicated')]),
  status_url: Type.String(),
  stream_url: Type.String(),
});
export type LaunchRunResponse = Static<typeof LaunchRunResponseSchema>;

export const WorkflowStatusResponseSchema = Type.Object({
  workflow_id: Type.Integer(),
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
  current_node: Nullable(Type.String()),
  nodes_total: Type.Integer({ minimum: 0 }),
  nodes_completed: Type.Integer({ minimum: 0 }),
  nodes_failed: Type.Integer({ minimum: 0 }),
  started_at: Nullable(Type.Number()),
  finished_at: Nullable(Type.Number()),
  output: Nullable(Type.Record(Type.String(), Type.Unknown())),
  error: Nullable(
    Type.Object({
      code: Type.String(),
      message: Type.String(),
      category: Type.String(),
      node_id: Type.Optional(Type.String()),
      details: Type.Optional(Type.Record(Type.String(), Type.Unknown())),
    }),
  ),
});
export type WorkflowStatusResponse = Static<typeof WorkflowStatusResponseSchema>;
