// provider/registry.ts - Provider Registry & Dispatch
// Stub: All function bodies throw "not implemented"

import type { Provider, ProviderName, FeatureProvider } from "./types";
import type { ProviderWorkflowContract } from "./contracts";

// =============================================================================
// Compute Provider Registry
// =============================================================================

export const providerRegistry = {
  // aws: () => import("./compute/aws"),       // Slice 2+
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
  return name in providerRegistry;
}

export function clearProviderCache(): void {
  providerCache.clear();
}

// =============================================================================
// Dynamic Provider Registration
// =============================================================================

export interface ProviderRegistration {
  provider: Provider;
  hooks?: unknown;
  contracts?: ProviderWorkflowContract[];
}

// Dynamic registry for plugins (future extensibility)
const dynamicProviders = new Map<string, ProviderRegistration>();

export function registerProvider(registration: ProviderRegistration): void {
  const { provider, hooks } = registration;

  // Store in dynamic registry
  dynamicProviders.set(provider.name, registration);

  // Cache the provider instance
  providerCache.set(provider.name as ProviderName, provider);

  // Call lifecycle hook if present
  if (hooks && typeof (hooks as any).onStartup === "function") {
    (hooks as any).onStartup();
  }
}

export function unregisterProvider(name: string): void {
  // Call shutdown hook if present
  const registration = dynamicProviders.get(name);
  if (registration?.hooks && typeof (registration.hooks as any).onShutdown === "function") {
    (registration.hooks as any).onShutdown();
  }

  dynamicProviders.delete(name);
  providerCache.delete(name as ProviderName);
}

// =============================================================================
// Feature Provider Registry
// =============================================================================

export const featureProviderRegistry = {} as const;

export type FeatureProviderName = keyof typeof featureProviderRegistry;

const featureProviderCache = new Map<FeatureProviderName, FeatureProvider>();

export async function getFeatureProvider(
  name: FeatureProviderName
): Promise<FeatureProvider> {
  if (featureProviderCache.has(name)) {
    return featureProviderCache.get(name)!;
  }

  const loader = featureProviderRegistry[name];
  if (!loader) {
    throw new Error(`Feature provider '${name}' not found in registry`);
  }

  // @ts-expect-error - loader exists but type is never when registry is empty
  const module = await loader();
  const featureProvider = module.default;

  featureProviderCache.set(name, featureProvider);
  return featureProvider;
}

export function getAllFeatureProviders(): FeatureProviderName[] {
  return Object.keys(featureProviderRegistry) as FeatureProviderName[];
}

export function isFeatureProviderRegistered(
  name: string
): name is FeatureProviderName {
  return name in featureProviderRegistry;
}

export function clearFeatureProviderCache(): void {
  featureProviderCache.clear();
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

export function getTestableFeatureProviders(): FeatureProviderName[] {
  const all = getAllFeatureProviders();

  // Allow explicit selection via env var
  const envFeatures = process.env.SKYREPL_TEST_FEATURE_PROVIDERS;
  if (envFeatures) {
    const explicit = envFeatures.split(",") as FeatureProviderName[];
    return explicit.filter((name) => all.includes(name));
  }

  // Default: none (no feature providers yet)
  return [];
}
