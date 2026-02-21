// provider/extensions.ts - Extension Framework: Lifecycle Hooks & Caching

import type { ProviderName } from "./types";

// =============================================================================
// InMemoryCache
// =============================================================================

interface CacheEntry<T> {
  value: T;
  expiresAt: number;
}

const cache = new Map<string, CacheEntry<unknown>>();

export async function getWithCache<T>(
  key: string,
  ttlMs: number,
  fetcher: () => Promise<T>
): Promise<T> {
  const cached = cache.get(key);
  if (cached && Date.now() < cached.expiresAt) {
    return cached.value as T;
  }
  const value = await fetcher();
  cache.set(key, { value, expiresAt: Date.now() + ttlMs });
  return value;
}

// =============================================================================
// Lifecycle Hooks
// =============================================================================

export interface HeartbeatExpectations {
  tasks: HeartbeatTask[];
  deadline: number;
}

export interface HeartbeatTask {
  type: 'reconcile' | 'health_check' | 'cache_refresh' | 'pool_maintenance';
  priority: 'high' | 'normal' | 'low';
  lastRun?: number;
  params?: Record<string, unknown>;
}

export interface HeartbeatReceipts {
  receipts: TaskReceipt[];
  providerTasks?: ProviderTaskReport[];
  workflowsLaunched?: WorkflowLaunchReport[];
  nextHeartbeat?: {
    preferredInterval: number;
  };
}

// ─── Design Note ─────────────────────────────────────────────────────────────
// WorkflowLaunchReport is returned in HeartbeatReceipts.workflowsLaunched when
// a heartbeat hook submits a workflow (e.g., spot interruption handling).
// Normative per spec §8.5.2.
//
// Currently not consumed because no hook implementation produces workflow
// launches yet. invokeHook() now threads return values (D7, 031B Wave 2).
//
// Will be wired when #AGENT-06 (spot interruption handling) lands.
// See 031B_WORKLOG §4 "Event-Driven Hooks" for the blueprint.
// ─────────────────────────────────────────────────────────────────────────────
export interface WorkflowLaunchReport {
  workflowId: string;
  workflowType: string;
  trigger: 'spot_interruption' | 'budget_threshold' | 'drift_detected' | 'provider_event';
  context?: Record<string, unknown>;
}

export interface TaskReceipt {
  type: string;
  status: 'completed' | 'skipped' | 'deferred' | 'failed';
  result?: unknown;
  reason?: string;
}

export interface ProviderTaskReport {
  type: string;
  status: 'completed' | 'failed';
  result?: unknown;
}

export interface ProviderLifecycleHooks {
  onStartup?(): Promise<void>;
  onShutdown?(): Promise<void>;
  onHeartbeat?(expectations: HeartbeatExpectations): Promise<HeartbeatReceipts>;
}

// =============================================================================
// TTL Configuration
// =============================================================================

export interface TTLConfig {
  default: number;
  byPrefix: Record<string, number | null>;
  maxTtl: number;
}

const AWS_TTL_CONFIG: TTLConfig = {
  default: 300_000,  // 5 minutes
  maxTtl: 3600_000,  // 1 hour

  byPrefix: {
    'spot_prices:': 300_000,       // 5 minutes
    'capacity:': 60_000,           // 1 minute
    'instance:': 30_000,           // 30 seconds
    'warm_volume_pool:': null,     // No TTL (explicitly managed)
    'cost:': 3600_000,             // 1 hour
    'api_calls:': 60_000,          // 1 minute (rolling window)
  },
};

const ORBSTACK_TTL_CONFIG: TTLConfig = {
  default: 30_000,   // 30 seconds (local, fast)
  maxTtl: 300_000,   // 5 minutes

  byPrefix: {
    'instance:': 10_000,           // 10 seconds
    'capacity:': 5_000,            // 5 seconds
  },
};

const PROVIDER_TTL_CONFIGS: Record<string, TTLConfig> = {
  aws: AWS_TTL_CONFIG,
  orbstack: ORBSTACK_TTL_CONFIG,
};

export function getTTL(provider: ProviderName, key: string): number | null {
  const config = PROVIDER_TTL_CONFIGS[provider] ?? {
    default: 300_000,
    byPrefix: {},
    maxTtl: 3600_000
  };

  for (const [prefix, ttl] of Object.entries(config.byPrefix)) {
    if (key.startsWith(prefix)) {
      if (ttl === null) return null; // explicitly unmanaged
      return Math.min(ttl, config.maxTtl);
    }
  }

  return Math.min(config.default, config.maxTtl);
}

export async function setWithAutoTTL<T>(
  provider: ProviderName,
  key: string,
  value: T
): Promise<void> {
  const ttl = getTTL(provider, key);
  if (ttl !== null) {
    cache.set(key, { value, expiresAt: Date.now() + ttl });
  }
}

// =============================================================================
// Hook Registry
// =============================================================================

const providerHooks = new Map<ProviderName, ProviderLifecycleHooks>();

export function registerProviderHooks(
  provider: ProviderName,
  hooks: ProviderLifecycleHooks
): void {
  providerHooks.set(provider, hooks);
}

export async function invokeHook(
  provider: ProviderName,
  hook: keyof ProviderLifecycleHooks,
  ...args: unknown[]
): Promise<{ success: boolean; result?: unknown; error?: unknown }> {
  const hooks = providerHooks.get(provider);
  if (!hooks?.[hook]) return { success: true }; // No-op if not registered

  try {
    const result = await (hooks[hook] as Function)(...args);
    return { success: true, result };
  } catch (error) {
    // Hooks should not crash control plane, but callers must know about failures
    console.error(`HOOK/${provider}/${hook}/error`, { error });
    return { success: false, error };
  }
}

export async function invokeAllHooks(
  hook: keyof ProviderLifecycleHooks,
  ...args: unknown[]
): Promise<void> {
  // Parallel execution, wait for all to complete
  await Promise.allSettled(
    Array.from(providerHooks.entries()).map(
      ([provider, _]) => invokeHook(provider, hook, ...args)
    )
  );
}
