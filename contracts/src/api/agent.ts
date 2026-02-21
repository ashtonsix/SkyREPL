// api/agent.ts - Agent Protocol Message Types (TypeBox schemas, single source of truth)

import { Type, type Static, type TSchema } from '@sinclair/typebox';
import type { ID, TimestampMs, TailscaleStatus } from '../types';

/** Helper: nullable union (T | null) */
const Nullable = <T extends TSchema>(t: T) => Type.Union([t, Type.Null()]);

// =============================================================================
// CONTROL -> AGENT (sent via SSE stream)
// =============================================================================

/** File entry for agent download */
export const AgentFileEntrySchema = Type.Object({
  /** Relative path in workdir */
  path: Type.String(),
  /** SHA256 checksum for verification */
  checksum: Type.String(),
  /** Presigned S3 URL or control plane URL */
  url: Type.String({ format: 'uri' }),
  /** Secondary presigned URL for resilience */
  failover_url: Type.Optional(Type.String({ format: 'uri' })),
});
export type AgentFileEntry = Static<typeof AgentFileEntrySchema>;

/** Start a run on the instance */
export const StartRunMessageSchema = Type.Object({
  type: Type.Literal('start_run'),
  /** Required: for acknowledgment protocol */
  command_id: Type.Integer(),
  run_id: Type.Integer(),
  allocation_id: Type.Integer(),
  command: Type.String(),
  workdir: Type.String(),
  files: Type.Array(AgentFileEntrySchema),
  /** Glob patterns to collect (e.g., ["**\/*.pt"]) */
  artifacts: Type.Array(Type.String()),
  /** Run init script before command */
  run_init: Type.Optional(Type.Boolean()),
  /** Init script content (if run_init=true) */
  init_script: Type.Optional(Type.String()),
  /** Create snapshot after run completes */
  create_snapshot: Type.Optional(Type.Boolean()),
});
export type StartRunMessage = Static<typeof StartRunMessageSchema>;

/** Cancel a running run */
export const CancelRunMessageSchema = Type.Object({
  type: Type.Literal('cancel_run'),
  /** Required: for acknowledgment protocol */
  command_id: Type.Integer(),
  run_id: Type.Integer(),
});
export type CancelRunMessage = Static<typeof CancelRunMessageSchema>;

/** Prepare instance for snapshot */
export const PrepareSnapshotMessageSchema = Type.Object({
  type: Type.Literal('prepare_snapshot'),
  /** Required: for acknowledgment protocol */
  command_id: Type.Integer(),
  instance_id: Type.Integer(),
});
export type PrepareSnapshotMessage = Static<typeof PrepareSnapshotMessageSchema>;

/** Capture artifacts from a run */
export const CaptureArtifactsMessageSchema = Type.Object({
  type: Type.Literal('capture_artifacts'),
  /** Optional (fire-and-forget acceptable) */
  command_id: Type.Optional(Type.Integer()),
  run_id: Type.Integer(),
});
export type CaptureArtifactsMessage = Static<typeof CaptureArtifactsMessageSchema>;

/** Trigger Tailscale installation */
export const TriggerTailscaleMessageSchema = Type.Object({
  type: Type.Literal('trigger_tailscale'),
  /** Optional (fire-and-forget acceptable) */
  command_id: Type.Optional(Type.Integer()),
});
export type TriggerTailscaleMessage = Static<typeof TriggerTailscaleMessageSchema>;

/** Acknowledge agent heartbeat */
export const HeartbeatAckMessageSchema = Type.Object({
  type: Type.Literal('heartbeat_ack'),
  /** Unique control plane identifier */
  control_plane_id: Type.String(),
});
export type HeartbeatAckMessage = Static<typeof HeartbeatAckMessageSchema>;

/** Union of all control -> agent message types */
export const ControlToAgentMessageSchema = Type.Union([
  StartRunMessageSchema,
  CancelRunMessageSchema,
  PrepareSnapshotMessageSchema,
  CaptureArtifactsMessageSchema,
  TriggerTailscaleMessageSchema,
  HeartbeatAckMessageSchema,
]);
export type ControlToAgentMessage = Static<typeof ControlToAgentMessageSchema>;

// =============================================================================
// AGENT -> CONTROL (sent via HTTP POST)
// =============================================================================

/** Log output from a run */
export const LogMessageSchema = Type.Object({
  type: Type.Literal('log'),
  run_id: Type.Integer(),
  allocation_id: Type.Integer(),
  stream: Type.Union([Type.Literal('stdout'), Type.Literal('stderr'), Type.Literal('sync_complete')]),
  data: Type.String(),
  phase: Type.Optional(
    Type.Union([
      Type.Literal('sync'),
      Type.Literal('execution'),
    ])
  ),
  timestamp: Type.Number(),
  /** Monotonic counter per agent session for ordering */
  batch_sequence_id: Type.Integer(),
  sync_success: Type.Optional(Type.Boolean()),
});
export type LogMessage = Static<typeof LogMessageSchema>;

/** Run completed */
export const RunCompleteMessageSchema = Type.Object({
  type: Type.Literal('run_complete'),
  run_id: Type.Integer(),
  allocation_id: Type.Integer(),
  exit_code: Type.Integer(),
  workflow_state: Type.String(),
  interrupted: Type.Optional(Type.Boolean()),
});
export type RunCompleteMessage = Static<typeof RunCompleteMessageSchema>;

/** Agent heartbeat */
export const HeartbeatMessageSchema = Type.Object({
  type: Type.Literal('heartbeat'),
  tailscale_ip: Nullable(Type.String()),
  active_allocations: Type.Array(Type.Object({
    allocation_id: Type.Integer(),
    has_ssh_sessions: Type.Boolean(),
  })),
  tailscale_status: Type.Union([
    Type.Literal('not_installed'),
    Type.Literal('installing'),
    Type.Literal('ready'),
    Type.Literal('failed'),
  ]),
  workflow_state: Type.String(),
  /** Command IDs to acknowledge */
  pending_command_acks: Type.Array(Type.Integer()),
  /** Logs dropped due to buffer overflow */
  dropped_logs_count: Type.Optional(Type.Integer({ minimum: 0 })),
});
export type HeartbeatMessage = Static<typeof HeartbeatMessageSchema>;

/** Spot interrupt detected - Phase 1 (immediate notification) */
export const SpotInterruptStartMessageSchema = Type.Object({
  type: Type.Literal('spot_interrupt_start'),
  run_id: Type.Integer(),
  /** Time until forced termination (from IMDS) */
  time_remaining_ms: Type.Integer({ minimum: 0 }),
});
export type SpotInterruptStartMessage = Static<typeof SpotInterruptStartMessageSchema>;

/** Spot interrupt - Phase 2 (checkpoint complete) */
export const SpotInterruptCompleteMessageSchema = Type.Object({
  type: Type.Literal('spot_interrupt_complete'),
  run_id: Type.Integer(),
  checkpoint_success: Type.Boolean(),
  artifacts_uploaded: Type.Integer({ minimum: 0 }),
});
export type SpotInterruptCompleteMessage = Static<typeof SpotInterruptCompleteMessageSchema>;

/** Heartbeat panic detected - Phase 1 (immediate notification) */
export const HeartbeatPanicStartMessageSchema = Type.Object({
  type: Type.Literal('heartbeat_panic_start'),
  run_id: Type.Integer(),
  /** Time since last successful heartbeat ack (~15m) */
  time_since_ack_ms: Type.Integer({ minimum: 0 }),
});
export type HeartbeatPanicStartMessage = Static<typeof HeartbeatPanicStartMessageSchema>;

/** Heartbeat panic - Phase 2 (checkpoint complete) */
export const HeartbeatPanicCompleteMessageSchema = Type.Object({
  type: Type.Literal('heartbeat_panic_complete'),
  run_id: Type.Integer(),
  checkpoint_exit_code: Type.Optional(Type.Integer()),
  artifacts_uploaded: Type.Integer({ minimum: 0 }),
});
export type HeartbeatPanicCompleteMessage = Static<typeof HeartbeatPanicCompleteMessageSchema>;

/** Agent panic (critical failure) */
export const PanicRequestSchema = Type.Object({
  type: Type.Literal('panic'),
  instance_id: Type.Integer(),
  reason: Type.String(),
  last_state_json: Type.String(),
  diagnostics_json: Type.String(),
  error_logs_json: Type.String(),
  timestamp: Type.Number(),
});
export type PanicRequest = Static<typeof PanicRequestSchema>;

/** Acknowledge snapshot preparation */
export const PrepareSnapshotAckMessageSchema = Type.Object({
  type: Type.Literal('prepare_snapshot_ack'),
  instance_id: Type.Integer(),
  success: Type.Boolean(),
  error: Type.Optional(Type.String()),
});
export type PrepareSnapshotAckMessage = Static<typeof PrepareSnapshotAckMessageSchema>;

/** Artifact metadata from agent */
export const ArtifactMessageSchema = Type.Object({
  type: Type.Literal('artifact'),
  run_id: Type.Integer(),
  allocation_id: Type.Integer(),
  path: Type.String(),
  checksum: Type.String(),
  size_bytes: Type.Integer(),
  content_base64: Type.Optional(Type.String()),
});
export type ArtifactMessage = Static<typeof ArtifactMessageSchema>;

/** Agent initialization complete */
export const InitCompleteMessageSchema = Type.Object({
  type: Type.Literal('init_complete'),
  instance_id: Type.Integer(),
});
export type InitCompleteMessage = Static<typeof InitCompleteMessageSchema>;

/** Union schema for all agent -> control messages */
export const AgentToControlMessageSchema = Type.Union([
  LogMessageSchema,
  RunCompleteMessageSchema,
  HeartbeatMessageSchema,
  SpotInterruptStartMessageSchema,
  SpotInterruptCompleteMessageSchema,
  HeartbeatPanicStartMessageSchema,
  HeartbeatPanicCompleteMessageSchema,
  PanicRequestSchema,
  PrepareSnapshotAckMessageSchema,
  ArtifactMessageSchema,
  InitCompleteMessageSchema,
]);
export type AgentToControlMessage = Static<typeof AgentToControlMessageSchema>;
