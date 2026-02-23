// index.ts - Re-exports from all modules

// Types & primitives
export type {
  ID,
  TimestampMs,
  DurationMs,
  Checksum,
  ProviderName,
  FeatureProviderName,
  InstanceId,
  RunId,
  AllocationId,
  WorkflowId,
  ManifestId,
  ObjectId,
  NodeRecordId,
  ProviderId,
  ProviderSnapshotId,
  InstanceSpec,
  WorkflowState,
  ResourceStatus,
  AllocationStatus,
  WorkflowExecutionStatus,
  ManifestStatus,
  NodeStatus,
  TailscaleStatus,
  ParsedStatus,
} from './types';

export {
  idToSlug,
  slugToId,
  formatId,
  parseInputId,
  parseStatus,
  buildWorkflowState,
  buildStatus,
  isActiveStatus,
  isTerminalStatus,
  isAvailable,
  InstanceWorkflowState,
  RunWorkflowState,
  Timestamp,
  Duration,
} from './types';

// Resources
export type {
  Instance,
  Run,
  Allocation,
  Workflow,
  Manifest,
  Snapshot,
  Artifact,
  FeatureInstallation,
  TailscaleMachine,
  RunFile,
  RunLogEntry,
  UsageRecord,
  WorkflowNode,
  ManifestResource,
  ErrorCategory,
  WorkflowError,
  NodeError,
  PatternAuditEntry,
  PatternType,
} from './resources';

// Errors
export {
  SkyREPLError,
  ProviderError,
  WorkflowExecutionError,
  ValidationError,
  NotFoundError,
  ConflictError,
  TimeoutError,
  ERROR_CATEGORY_HTTP_STATUS,
  ERROR_CODES_BY_CATEGORY,
  httpStatusForCategory,
  httpStatusForErrorCode,
  createApiError,
  createWorkflowError,
  createNodeError,
  categoryForCode,
  errorToApiError,
  httpStatusForError,
} from './errors';

export type { ApiError } from './errors';

// API - Agent protocol
export type {
  AgentFileEntry,
  StartRunMessage,
  CancelRunMessage,
  PrepareSnapshotMessage,
  CaptureArtifactsMessage,
  TriggerTailscaleMessage,
  HeartbeatAckMessage,
  PrepareSnapshotAckMessage,
  ControlToAgentMessage,
  LogMessage,
  RunCompleteMessage,
  HeartbeatMessage,
  SpotInterruptStartMessage,
  SpotInterruptCompleteMessage,
  HeartbeatPanicStartMessage,
  HeartbeatPanicCompleteMessage,
  PanicRequest,
  ArtifactMessage,
  InitCompleteMessage,
  AgentToControlMessage,
} from './api/agent';

export {
  AgentFileEntrySchema,
  StartRunMessageSchema,
  CancelRunMessageSchema,
  PrepareSnapshotMessageSchema,
  CaptureArtifactsMessageSchema,
  TriggerTailscaleMessageSchema,
  HeartbeatAckMessageSchema,
  PrepareSnapshotAckMessageSchema,
  ControlToAgentMessageSchema,
  LogMessageSchema,
  RunCompleteMessageSchema,
  HeartbeatMessageSchema,
  SpotInterruptStartMessageSchema,
  SpotInterruptCompleteMessageSchema,
  HeartbeatPanicStartMessageSchema,
  HeartbeatPanicCompleteMessageSchema,
  PanicRequestSchema,
  ArtifactMessageSchema,
  InitCompleteMessageSchema,
  AgentToControlMessageSchema,
} from './api/agent';

// API - Workflows
export type {
  FileManifestEntry,
  LaunchRunInput,
  LaunchRunOutput,
  TerminateInstanceInput,
  TerminateInstanceOutput,
  CreateSnapshotInput,
  CreateSnapshotOutput,
  CleanupManifestInput,
  CleanupError,
  CleanupManifestOutput,
  LaunchRunRequest,
  LaunchRunResponse,
  WorkflowStatusResponse,
} from './api/workflows';

export {
  LaunchRunRequestSchema,
  LaunchRunResponseSchema,
  WorkflowStatusResponseSchema,
} from './api/workflows';

// Materialization
export type {
  FreshnessTier,
  MaterializeOptions,
  Materialized,
} from './materialization';

// Config - Timing
export {
  TIMING,
  getProviderTiming,
  getProviderTimings,
  validateTimingConstraints,
  calculateBackoff,
  formatDuration,
  isWithinWindow,
  isHeartbeatStale,
  isHeartbeatDegraded,
  calculateHoldExpiry,
} from './config/timing';

export type { TimingConfig, TimingKey } from './config/timing';
