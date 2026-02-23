// config/timing.ts - Centralized Timing Constants

// =============================================================================
// DURATION PARSING
// =============================================================================

function parseDuration(value: string): number {
  const match = value.match(/^(\d+(?:\.\d+)?)\s*(ms|s|m|h|d)$/i);
  if (!match) throw new Error(`Invalid duration: ${value}`);
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
}

function getEnvDuration(key: string, defaultMs: number): number {
  const value = process.env[key];
  return value ? parseDuration(value) : defaultMs;
}

// =============================================================================
// BASE TIMING CONSTANTS
// =============================================================================

export const TIMING = {
  // HEARTBEAT CHAIN -- Constraint: interval < degraded < ack_timeout < stale_detection
  HEARTBEAT_INTERVAL_MS: getEnvDuration('REPL_HEARTBEAT_INTERVAL_MS', 10_000), // 10s
  HEARTBEAT_DEGRADED_MS: getEnvDuration('REPL_HEARTBEAT_DEGRADED_MS', 120_000), // 2m
  HEARTBEAT_ACK_TIMEOUT_MS: getEnvDuration(
    'REPL_HEARTBEAT_ACK_TIMEOUT',
    900_000,
  ), // 15m
  STALE_DETECTION_MS: getEnvDuration('REPL_STALE_DETECTION', 1_500_000), // 25m

  // HOLD CHAIN -- Constraint: default <= max_ssh <= max_explicit <= absolute_max
  DEFAULT_HOLD_MS: getEnvDuration('REPL_DEFAULT_HOLD', 300_000), // 5m
  SSH_EXTENSION_MS: getEnvDuration('REPL_SSH_EXTENSION', 300_000), // 5m per activity
  MAX_SSH_EXTENSION_MS: getEnvDuration('REPL_MAX_SSH_EXTENSION', 7_200_000), // 2h cumulative
  DEFAULT_EXPLICIT_EXTENSION_MS: getEnvDuration(
    'REPL_EXPLICIT_EXTENSION',
    1_800_000,
  ), // 30m
  MAX_EXPLICIT_EXTENSION_MS: getEnvDuration(
    'REPL_MAX_EXPLICIT_EXTENSION',
    28_800_000,
  ), // 8h
  ABSOLUTE_MAX_HOLD_MS: getEnvDuration('REPL_ABSOLUTE_MAX_HOLD', 86_400_000), // 24h

  // BACKGROUND TASKS -- Constraint: reconciliation < manifest_cleanup
  HEARTBEAT_CHECK_INTERVAL_MS: getEnvDuration(
    'REPL_HEARTBEAT_CHECK_INTERVAL',
    60_000,
  ), // 60s
  HOLD_EXPIRY_CHECK_INTERVAL_MS: getEnvDuration(
    'REPL_HOLD_EXPIRY_CHECK_INTERVAL',
    60_000,
  ), // 60s
  RECONCILIATION_INTERVAL_MS: getEnvDuration(
    'REPL_RECONCILIATION_INTERVAL',
    60_000,
  ), // 60s
  MANIFEST_CLEANUP_INTERVAL_MS: getEnvDuration(
    'REPL_MANIFEST_CLEANUP_INTERVAL',
    300_000,
  ), // 5m
  ORPHAN_SCAN_INTERVAL_MS: getEnvDuration(
    'REPL_ORPHAN_SCAN_INTERVAL',
    3_600_000,
  ), // 1h
  BLOB_GC_INTERVAL_MS: getEnvDuration('REPL_BLOB_GC_INTERVAL', 86_400_000), // 24h

  // WORKFLOW ENGINE -- Constraint: node_timeout < workflow_timeout
  DEFAULT_NODE_TIMEOUT_MS: getEnvDuration('REPL_NODE_TIMEOUT', 300_000), // 5m
  DEFAULT_WORKFLOW_TIMEOUT_MS: getEnvDuration(
    'REPL_WORKFLOW_TIMEOUT',
    3_600_000,
  ), // 60m
  PENDING_WORKFLOW_TIMEOUT_MS: getEnvDuration(
    'REPL_PENDING_WORKFLOW_TIMEOUT',
    1_800_000,
  ), // 30m
  COMPENSATION_TIMEOUT_MS: getEnvDuration(
    'REPL_COMPENSATION_TIMEOUT',
    300_000,
  ), // 5m
  CANCEL_VERIFICATION_MS: getEnvDuration('REPL_CANCEL_VERIFICATION', 90_000), // 90s

  // PROVIDER OPERATIONS
  SPAWN_TIMEOUT_MS: getEnvDuration('REPL_SPAWN_TIMEOUT', 600_000), // 10m
  SPAWN_POLL_INTERVAL_MS: getEnvDuration('REPL_SPAWN_POLL_INTERVAL', 5_000), // 5s
  INSTANCE_BOOT_TIMEOUT_MS: getEnvDuration(
    'REPL_INSTANCE_BOOT_TIMEOUT',
    600_000,
  ), // 10m
  BOOT_POLL_INTERVAL_MS: getEnvDuration('REPL_BOOT_POLL_INTERVAL', 5_000), // 5s
  TERMINATE_TIMEOUT_MS: getEnvDuration('REPL_TERMINATE_TIMEOUT', 300_000), // 5m
  SNAPSHOT_TIMEOUT_MS: getEnvDuration('REPL_SNAPSHOT_TIMEOUT', 1_800_000), // 30m
  SNAPSHOT_PREPARE_TIMEOUT_MS: getEnvDuration(
    'REPL_SNAPSHOT_PREPARE_TIMEOUT',
    300_000,
  ), // 5m
  SNAPSHOT_POLL_INTERVAL_MS: getEnvDuration(
    'REPL_SNAPSHOT_POLL_INTERVAL',
    30_000,
  ), // 30s
  DRAIN_TIMEOUT_MS: getEnvDuration('REPL_DRAIN_TIMEOUT', 60_000), // 60s
  PENDING_SPAWN_STALE_THRESHOLD_MS: getEnvDuration(
    'REPL_PENDING_SPAWN_STALE_THRESHOLD',
    600_000,
  ), // 10m

  // ALLOCATION STATE MACHINE
  CLAIMED_TIMEOUT_MS: getEnvDuration('REPL_CLAIMED_TIMEOUT', 300_000), // 5m
  SSH_ACTIVITY_SLACK_MS: getEnvDuration('REPL_SSH_ACTIVITY_SLACK', 60_000), // 60s

  // WARM POOL
  WARM_POOL_EXPIRY_MS: getEnvDuration('REPL_WARM_POOL_EXPIRY', 3_600_000), // 1h
  WARM_POOL_RECONCILE_INTERVAL_MS: getEnvDuration('REPL_WARM_POOL_RECONCILE_INTERVAL', 60_000), // 60s
  WARM_CLAIM_RETRY_DELAY_MS: getEnvDuration('REPL_WARM_CLAIM_RETRY_DELAY', 50), // 50ms
  WARM_CLAIM_MAX_RETRIES: parseInt(process.env.REPL_WARM_CLAIM_MAX_RETRIES || '3', 10),
  MAX_ALLOCATIONS_PER_INSTANCE: parseInt(process.env.REPL_MAX_ALLOCATIONS_PER_INSTANCE || '10', 10),

  // CLEANUP & GC
  ORPHAN_MIN_AGE_MS: getEnvDuration('REPL_ORPHAN_MIN_AGE', 600_000), // 10m
  BLOB_GRACE_PERIOD_MS: getEnvDuration('REPL_BLOB_GRACE_PERIOD', 86_400_000), // 24h
  RECENT_SESSION_THRESHOLD_MS: getEnvDuration(
    'REPL_RECENT_SESSION_THRESHOLD',
    3_600_000,
  ), // 1h
  DEFAULT_MANIFEST_RETENTION_MS: getEnvDuration(
    'REPL_DEFAULT_MANIFEST_RETENTION',
    86_400_000,
  ), // 24h

  // RETRY & BACKOFF -- Constraint: base < max
  RETRY_BASE_DELAY_MS: getEnvDuration('REPL_RETRY_BASE_DELAY', 2_000), // 2s
  RETRY_MAX_DELAY_MS: getEnvDuration('REPL_RETRY_MAX_DELAY', 60_000), // 60s
  RETRY_MAX_ATTEMPTS: parseInt(process.env.REPL_RETRY_MAX_ATTEMPTS || '5', 10),
  SSE_RECONNECT_BASE_MS: getEnvDuration('REPL_SSE_RECONNECT_BASE', 2_000), // 2s
  SSE_RECONNECT_MAX_MS: getEnvDuration('REPL_SSE_RECONNECT_MAX', 64_000), // 64s
  SSE_MAX_RETRIES: parseInt(process.env.REPL_SSE_MAX_RETRIES || '10', 10),

  // AGENT
  LOG_LOCAL_BUFFER_MS: getEnvDuration('REPL_LOG_LOCAL_BUFFER', 100), // 100ms
  LOG_NETWORK_BATCH_MS: getEnvDuration('REPL_LOG_NETWORK_BATCH', 5_000), // 5s
  SPOT_CHECK_INTERVAL_MS: getEnvDuration('REPL_SPOT_CHECK_INTERVAL', 5_000), // 5s
  SPOT_CHECKPOINT_BUDGET_MS: getEnvDuration(
    'REPL_SPOT_CHECKPOINT_BUDGET',
    90_000,
  ), // 90s
  PANIC_CHECKPOINT_BUDGET_MS: getEnvDuration(
    'REPL_PANIC_CHECKPOINT_BUDGET',
    300_000,
  ), // 5m
  COMMAND_ACK_TIMEOUT_MS: getEnvDuration('REPL_COMMAND_ACK_TIMEOUT', 30_000), // 30s
  AGENT_SHUTDOWN_GRACE_MS: getEnvDuration('REPL_AGENT_SHUTDOWN_GRACE', 5_000), // 5s
  PANIC_DIAGNOSTICS_TIMEOUT_MS: getEnvDuration(
    'REPL_PANIC_DIAGNOSTICS_TIMEOUT',
    2_000,
  ), // 2s

  // FILE TRANSFER
  PRESIGNED_URL_TTL_MS: getEnvDuration('REPL_PRESIGNED_URL_TTL', 14_400_000), // 4h
  SYNC_TIMEOUT_MS: getEnvDuration('REPL_SYNC_TIMEOUT', 600_000), // 10m
  ARTIFACT_COLLECTION_TIMEOUT_MS: getEnvDuration(
    'REPL_ARTIFACT_COLLECTION_TIMEOUT',
    1_800_000,
  ), // 30m

  // CLI
  CLI_DEFAULT_TIMEOUT_MS: getEnvDuration('REPL_CLI_DEFAULT_TIMEOUT', 3_600_000), // 1h
  CLI_API_TIMEOUT_MS: getEnvDuration('REPL_CLI_API_TIMEOUT', 30_000), // 30s
  COMPLETION_CACHE_TTL_MS: getEnvDuration('REPL_COMPLETION_CACHE_TTL', 30_000), // 30s
  COMPLETION_API_TIMEOUT_MS: getEnvDuration(
    'REPL_COMPLETION_API_TIMEOUT',
    200,
  ), // 200ms

  // CACHE TTLs
  CACHE_SPOT_PRICES_TTL_MS: getEnvDuration(
    'REPL_CACHE_SPOT_PRICES_TTL',
    300_000,
  ), // 5m
  CACHE_CAPACITY_TTL_MS: getEnvDuration('REPL_CACHE_CAPACITY_TTL', 60_000), // 1m
  CACHE_REFRESH_MIN_INTERVAL: getEnvDuration(
    'REPL_CACHE_REFRESH_MIN_INTERVAL',
    60_000,
  ), // 1m
  CACHE_INSTANCE_STATE_TTL_MS: getEnvDuration(
    'REPL_CACHE_INSTANCE_STATE_TTL',
    30_000,
  ), // 30s
  CACHE_INSTANCE_TYPES_TTL_MS: getEnvDuration(
    'REPL_CACHE_INSTANCE_TYPES_TTL',
    3_600_000,
  ), // 1h
} as const;

export type TimingConfig = typeof TIMING;
export type TimingKey = keyof TimingConfig;

// =============================================================================
// PROVIDER OVERRIDES
// =============================================================================

interface ProviderTimingOverrides {
  SPAWN_TIMEOUT_MS?: number;
  SPAWN_POLL_INTERVAL_MS?: number;
  TERMINATE_TIMEOUT_MS?: number;
  CACHE_INSTANCE_STATE_TTL_MS?: number;
  RECONCILIATION_INTERVAL_MS?: number;
}

const PROVIDER_OVERRIDES: Record<string, ProviderTimingOverrides> = {
  aws: {}, // uses base defaults
  lambda: { SPAWN_TIMEOUT_MS: 300_000 }, // 5m
  runpod: { SPAWN_TIMEOUT_MS: 300_000 }, // 5m
  orbstack: {
    SPAWN_TIMEOUT_MS: 30_000, // 30s
    SPAWN_POLL_INTERVAL_MS: 1_000, // 1s
    TERMINATE_TIMEOUT_MS: 10_000, // 10s
    CACHE_INSTANCE_STATE_TTL_MS: 10_000, // 10s
    RECONCILIATION_INTERVAL_MS: 10_000, // 10s
  },
};

export function getProviderTiming<K extends TimingKey>(
  key: K,
  provider: string,
): TimingConfig[K] {
  const overrides = PROVIDER_OVERRIDES[provider];
  if (overrides && key in overrides) {
    return overrides[key as keyof ProviderTimingOverrides] as TimingConfig[K];
  }
  return TIMING[key];
}

export function getProviderTimings(provider: string): TimingConfig {
  return { ...TIMING, ...(PROVIDER_OVERRIDES[provider] || {}) };
}

// =============================================================================
// CONSTRAINT VALIDATION
// =============================================================================

export function validateTimingConstraints(): void {
  const errors: string[] = [];

  // Heartbeat chain: interval < degraded < ack_timeout < stale_detection
  if (TIMING.HEARTBEAT_INTERVAL_MS >= TIMING.HEARTBEAT_DEGRADED_MS) {
    errors.push('Heartbeat: interval must be < degraded');
  }
  if (TIMING.HEARTBEAT_DEGRADED_MS >= TIMING.HEARTBEAT_ACK_TIMEOUT_MS) {
    errors.push('Heartbeat: degraded must be < ack_timeout');
  }
  if (TIMING.HEARTBEAT_ACK_TIMEOUT_MS >= TIMING.STALE_DETECTION_MS) {
    errors.push('Heartbeat: ack_timeout must be < stale_detection');
  }

  // Hold chain
  if (TIMING.DEFAULT_HOLD_MS > TIMING.MAX_SSH_EXTENSION_MS) {
    errors.push('Hold: default must be <= max_ssh');
  }
  if (TIMING.MAX_SSH_EXTENSION_MS > TIMING.MAX_EXPLICIT_EXTENSION_MS) {
    errors.push('Hold: max_ssh must be <= max_explicit');
  }
  if (TIMING.MAX_EXPLICIT_EXTENSION_MS > TIMING.ABSOLUTE_MAX_HOLD_MS) {
    errors.push('Hold: max_explicit must be <= absolute_max');
  }

  // Cleanup chain
  if (TIMING.RECONCILIATION_INTERVAL_MS >= TIMING.MANIFEST_CLEANUP_INTERVAL_MS) {
    errors.push('Cleanup: reconciliation must be < manifest_cleanup');
  }

  // Workflow chain
  if (TIMING.DEFAULT_NODE_TIMEOUT_MS >= TIMING.DEFAULT_WORKFLOW_TIMEOUT_MS) {
    errors.push('Workflow: node_timeout must be < workflow_timeout');
  }

  // Retry chain
  if (TIMING.RETRY_BASE_DELAY_MS >= TIMING.RETRY_MAX_DELAY_MS) {
    errors.push('Retry: base_delay must be < max_delay');
  }

  if (errors.length > 0) {
    throw new Error(`Timing constraint violations:\n${errors.join('\n')}`);
  }
}

// Validate at module load -- fail fast
validateTimingConstraints();

// =============================================================================
// HELPERS
// =============================================================================

/** Calculate exponential backoff with jitter */
export function calculateBackoff(attempt: number): number {
  const delay = Math.min(
    TIMING.RETRY_BASE_DELAY_MS * Math.pow(2, attempt),
    TIMING.RETRY_MAX_DELAY_MS,
  );
  const jitter = delay * 0.1 * (Math.random() * 2 - 1);
  return Math.round(delay + jitter);
}

/** Format milliseconds to human-readable string */
export function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${ms / 1000}s`;
  if (ms < 3_600_000) return `${ms / 60_000}m`;
  if (ms < 86_400_000) return `${ms / 3_600_000}h`;
  return `${ms / 86_400_000}d`;
}

/** Check if timestamp is within a time window from now */
export function isWithinWindow(timestamp: number, windowMs: number): boolean {
  return Date.now() - timestamp < windowMs;
}

/** Check if heartbeat is stale (control plane force-term threshold) */
export function isHeartbeatStale(lastHeartbeat: number): boolean {
  return !isWithinWindow(lastHeartbeat, TIMING.STALE_DETECTION_MS);
}

/** Check if heartbeat is degraded (early warning, not yet stale) */
export function isHeartbeatDegraded(lastHeartbeat: number): boolean {
  const elapsed = Date.now() - lastHeartbeat;
  return (
    elapsed >= TIMING.HEARTBEAT_DEGRADED_MS &&
    elapsed < TIMING.STALE_DETECTION_MS
  );
}

/** Calculate hold expiry timestamp, clamped to absolute max */
export function calculateHoldExpiry(
  completedAt: number,
  requestedHoldMs: number,
): number {
  return completedAt + Math.min(requestedHoldMs, TIMING.ABSOLUTE_MAX_HOLD_MS);
}
