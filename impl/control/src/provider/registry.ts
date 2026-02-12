// provider/registry.ts - Provider Registry & Dispatch
// Stub: All function bodies throw "not implemented"

import type { Provider, ProviderName, FeatureProvider } from "./types";
import type { ProviderWorkflowContract } from "./contracts";
import type { ProviderCategory, StorageProvider, TunnelProvider } from "./categories";
import type { ProviderLifecycleHooks } from "./extensions";

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
  categoryCache.clear();
}

// =============================================================================
// Category Registry
// =============================================================================

// Maps category → name → loader
type CategoryRegistry = {
  compute: Map<string, () => Promise<Provider>>;
  storage: Map<string, () => Promise<StorageProvider>>;
  tunnel: Map<string, () => Promise<TunnelProvider>>;
};

const categoryRegistry: CategoryRegistry = {
  compute: new Map(),
  storage: new Map(),
  tunnel: new Map(),
};

const categoryCache = new Map<string, unknown>(); // key: "category:name"

// Initialize compute registry from existing static registry
function initComputeRegistry(): void {
  for (const [name, loader] of Object.entries(providerRegistry)) {
    categoryRegistry.compute.set(name, async () => {
      const module = await loader();
      return module.default;
    });
  }
}

// Call on module load
initComputeRegistry();

// =============================================================================
// Cross-Category Access
// =============================================================================

// Type helper
type CategoryProviderType<C extends ProviderCategory> =
  C extends 'compute' ? Provider :
  C extends 'storage' ? StorageProvider :
  C extends 'tunnel' ? TunnelProvider :
  never;

export function registerCategoryProvider<C extends ProviderCategory>(
  category: C,
  name: string,
  loader: () => Promise<CategoryProviderType<C>>
): void {
  const reg = categoryRegistry[category] as Map<string, () => Promise<unknown>>;
  reg.set(name, loader);
}

export async function getCategoryProvider<C extends ProviderCategory>(
  category: C,
  name: string
): Promise<CategoryProviderType<C>> {
  const cacheKey = `${category}:${name}`;
  if (categoryCache.has(cacheKey)) {
    return categoryCache.get(cacheKey) as CategoryProviderType<C>;
  }
  const reg = categoryRegistry[category] as Map<string, () => Promise<unknown>>;
  const loader = reg.get(name);
  if (!loader) {
    throw new Error(`Provider '${name}' not found in category '${category}'`);
  }
  const provider = await loader();
  categoryCache.set(cacheKey, provider);
  return provider as CategoryProviderType<C>;
}

export function listCategoryProviders(category: ProviderCategory): string[] {
  return Array.from(categoryRegistry[category].keys());
}

export function getAnyProvider(category: ProviderCategory, name: string): Promise<unknown> {
  return getCategoryProvider(category as any, name);
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
