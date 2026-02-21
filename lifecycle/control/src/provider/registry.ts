// provider/registry.ts - Provider Registry & Dispatch

import type { Provider, ProviderName } from "./types";
import type { ProviderWorkflowContract } from "./types";
import type { ProviderLifecycleHooks } from "./extensions";

// =============================================================================
// Compute Provider Registry
// =============================================================================

export const providerRegistry = {
  aws: () => import("./compute/aws"),
  // lambda: () => import("./compute/lambda"), // Slice 2+
  // runpod: () => import("./compute/runpod"), // Slice 2+
  orbstack: () => import("./compute/orbstack"),
} as const;

// Provider cache to avoid repeated imports
const providerCache = new Map<ProviderName, Provider>();

// =============================================================================
// Runtime Access
// =============================================================================

export async function getProvider(name: ProviderName): Promise<Provider> {
  if (providerCache.has(name)) {
    return providerCache.get(name)!;
  }

  if (!(name in providerRegistry)) {
    throw new Error(`Provider '${name}' not found in registry`);
  }

  const loader = providerRegistry[name as keyof typeof providerRegistry];
  const module = await loader();
  const provider = module.default;

  providerCache.set(name, provider);
  return provider;
}

export function getAllProviders(): ProviderName[] {
  return Object.keys(providerRegistry) as ProviderName[];
}

export function isProviderRegistered(name: string): name is ProviderName {
  return name in providerRegistry || dynamicProviders.has(name);
}

export function clearProviderCache(): void {
  providerCache.clear();
}

/** Clear all provider registries including dynamic providers. Test-only. */
export function clearAllProviders(): void {
  providerCache.clear();
  dynamicProviders.clear();
}

// =============================================================================
// Dynamic Provider Registration
// =============================================================================

export interface ProviderRegistration {
  provider: Provider;
  hooks?: ProviderLifecycleHooks;
  contracts?: ProviderWorkflowContract[];
}

// Dynamic registry for plugins (future extensibility)
const dynamicProviders = new Map<string, ProviderRegistration>();

export async function registerProvider(registration: ProviderRegistration): Promise<void> {
  const { provider, hooks } = registration;

  // Call lifecycle hook before registration — fail registration if startup fails
  if (hooks?.onStartup) {
    await hooks.onStartup();
  }

  // Store in dynamic registry
  dynamicProviders.set(provider.name, registration);

  // Cache the provider instance
  providerCache.set(provider.name as ProviderName, provider);
}

export async function unregisterProvider(name: string): Promise<void> {
  // Call shutdown hook if present
  const registration = dynamicProviders.get(name);
  if (registration?.hooks?.onShutdown) {
    await registration.hooks.onShutdown();
  }

  dynamicProviders.delete(name);
  providerCache.delete(name as ProviderName);
}

// =============================================================================
// Test Utilities
// =============================================================================

export function getTestableProviders(): ProviderName[] {
  const all = getAllProviders();

  // Allow explicit provider selection via env var
  const envProviders = process.env.SKYREPL_TEST_PROVIDERS;
  if (envProviders) {
    const explicit = envProviders.split(",") as ProviderName[];
    return explicit.filter((name) => all.includes(name));
  }

  // Default: OrbStack only (local, no credentials required)
  return ["orbstack"];
}
