// provider/storage/registry.ts - BlobProvider registry
// Dynamic registration with per-tenant resolution and caching.
// Mirrors the compute provider registry pattern (registerProvider/unregisterProvider).

import type { BlobProvider, BlobProviderLifecycleHooks, StorageHeartbeatExpectations, StorageHeartbeatReceipts } from "./types";
import { SqlBlobProvider } from "./sql-blob";
import { S3BlobProvider, type S3BlobProviderConfig } from "./s3";
import { queryOne } from "../../material/db";

// =============================================================================
// Registration Types
// =============================================================================

export interface BlobProviderRegistration {
  provider: BlobProvider;
  hooks?: BlobProviderLifecycleHooks;
}

// =============================================================================
// Dynamic Provider Registry
// =============================================================================

/** Registered blob provider factories: name → constructor from config */
const blobProviderFactories = new Map<string, (name: string, config: any) => BlobProvider>();

/** Active registrations with lifecycle hooks */
const blobProviderRegistrations = new Map<string, BlobProviderRegistration>();

// Built-in factories
blobProviderFactories.set("sql", () => new SqlBlobProvider());
blobProviderFactories.set("aws-s3", (name, config) => new S3BlobProvider(name, config));
blobProviderFactories.set("minio", (name, config) => new S3BlobProvider(name, config));
blobProviderFactories.set("backblaze-b2", (name, config) => new S3BlobProvider(name, config));
blobProviderFactories.set("do-spaces", (name, config) => new S3BlobProvider(name, config));

/**
 * Register a blob provider factory. Adding a new blob backend should not
 * require modifying this module — just call registerBlobProviderFactory().
 */
export function registerBlobProviderFactory(
  name: string,
  factory: (name: string, config: any) => BlobProvider,
): void {
  blobProviderFactories.set(name, factory);
}

/**
 * Register a blob provider instance with lifecycle hooks.
 * Calls onStartup() during registration — fails registration if startup fails.
 */
export async function registerBlobProvider(registration: BlobProviderRegistration): Promise<void> {
  const { provider, hooks } = registration;

  // Call lifecycle hook before registration — fail if startup fails
  if (hooks?.onStartup) {
    await hooks.onStartup();
  }

  blobProviderRegistrations.set(provider.name, registration);
}

/**
 * Unregister a blob provider, calling its shutdown hook.
 */
export async function unregisterBlobProvider(name: string): Promise<void> {
  const registration = blobProviderRegistrations.get(name);
  if (registration?.hooks?.onShutdown) {
    await registration.hooks.onShutdown();
  }
  blobProviderRegistrations.delete(name);
  // Clear tenant cache entries that may reference this provider
  for (const [key, provider] of providerCache) {
    if (provider.name === name) {
      providerCache.delete(key);
    }
  }
}

/**
 * Invoke heartbeat hooks on all registered blob providers.
 * Returns a map of provider name to StorageHeartbeatReceipts.
 */
export async function invokeAllStorageHeartbeats(
  expectations: StorageHeartbeatExpectations
): Promise<Map<string, StorageHeartbeatReceipts>> {
  const results = new Map<string, StorageHeartbeatReceipts>();

  for (const [name, reg] of blobProviderRegistrations) {
    if (reg.hooks?.onHeartbeat) {
      try {
        const receipts = await reg.hooks.onHeartbeat(expectations);
        results.set(name, receipts);
      } catch {
        results.set(name, { receipts: [{ type: 'health_check', status: 'failed', reason: 'onHeartbeat threw' }] });
      }
    }
  }

  // Also check the default provider if no explicit registration
  if (defaultProvider && !blobProviderRegistrations.has(defaultProvider.name)) {
    // Default provider (SqlBlobProvider) has no lifecycle hooks — skip heartbeat
  }

  return results;
}

/**
 * Invoke health check hooks on all registered blob providers.
 * Used by the heartbeat system to surface blob provider health alongside compute.
 */
export async function invokeAllBlobHealthChecks(): Promise<
  Array<{ provider: string; healthy: boolean; latencyMs: number }>
> {
  const results: Array<{ provider: string; healthy: boolean; latencyMs: number }> = [];

  // Check dynamically registered providers (with hooks)
  for (const [name, reg] of blobProviderRegistrations) {
    if (reg.hooks?.onHealthCheck) {
      try {
        const result = await reg.hooks.onHealthCheck();
        results.push({ provider: name, ...result });
      } catch {
        results.push({ provider: name, healthy: false, latencyMs: 0 });
      }
    }
  }

  // Also check the default provider if no explicit registration
  if (defaultProvider && !blobProviderRegistrations.has(defaultProvider.name)) {
    try {
      const result = await defaultProvider.healthCheck();
      results.push({ provider: defaultProvider.name, ...result });
    } catch {
      results.push({ provider: defaultProvider.name, healthy: false, latencyMs: 0 });
    }
  }

  return results;
}

// =============================================================================
// Provider Cache
// =============================================================================

let defaultProvider: BlobProvider | null = null;
const providerCache = new Map<string, BlobProvider>();

// =============================================================================
// Public API
// =============================================================================

export function getDefaultBlobProvider(): BlobProvider {
  if (!defaultProvider) {
    defaultProvider = new SqlBlobProvider();
  }
  return defaultProvider;
}

export function getBlobProvider(tenantId: number = 1): BlobProvider {
  const cacheKey = `tenant:${tenantId}`;
  const cached = providerCache.get(cacheKey);
  if (cached) return cached;

  // Look up per-tenant config
  const config = queryOne<{ provider_name: string; config_json: string }>(
    "SELECT provider_name, config_json FROM blob_provider_configs WHERE tenant_id = ?",
    [tenantId]
  );

  if (!config || config.provider_name === "sql") {
    const provider = getDefaultBlobProvider();
    providerCache.set(cacheKey, provider);
    return provider;
  }

  // Use registered factory (open/closed: new backends don't require editing this file)
  const factory = blobProviderFactories.get(config.provider_name);
  if (!factory) {
    console.warn(`[blob-registry] Unknown blob provider '${config.provider_name}', falling back to SQL`);
    const provider = getDefaultBlobProvider();
    providerCache.set(cacheKey, provider);
    return provider;
  }

  const parsed = JSON.parse(config.config_json);
  const provider = factory(config.provider_name, parsed);
  providerCache.set(cacheKey, provider);
  return provider;
}

/** Clear cache -- call when provider config changes */
export function clearBlobProviderCache(): void {
  providerCache.clear();
  defaultProvider = null;
}

/** Set the default provider (used in testing) */
export function setDefaultBlobProvider(provider: BlobProvider): void {
  defaultProvider = provider;
}

/** Clear all state. Test-only. */
export function clearAllBlobProviders(): void {
  providerCache.clear();
  defaultProvider = null;
  blobProviderRegistrations.clear();
}
