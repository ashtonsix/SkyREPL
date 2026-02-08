// provider/registry.ts - Provider Registry & Dispatch
// Stub: All function bodies throw "not implemented"

import type { Provider, ProviderName, FeatureProvider } from "./types";
import type { ProviderWorkflowContract } from "./contracts";

// =============================================================================
// Compute Provider Registry
// =============================================================================

export const providerRegistry = {
  orbstack: () => import("./compute/orbstack"),
} as const;

// =============================================================================
// Runtime Access
// =============================================================================

export async function getProvider(name: ProviderName): Promise<Provider> {
  throw new Error("not implemented");
}

export function getAllProviders(): ProviderName[] {
  throw new Error("not implemented");
}

export function isProviderRegistered(name: string): name is ProviderName {
  throw new Error("not implemented");
}

export function clearProviderCache(): void {
  throw new Error("not implemented");
}

// =============================================================================
// Dynamic Provider Registration
// =============================================================================

export interface ProviderRegistration {
  provider: Provider;
  hooks?: unknown;
  contracts?: ProviderWorkflowContract[];
}

export function registerProvider(registration: ProviderRegistration): void {
  throw new Error("not implemented");
}

export function unregisterProvider(name: string): void {
  throw new Error("not implemented");
}

// =============================================================================
// Feature Provider Registry
// =============================================================================

export const featureProviderRegistry = {} as const;

export type FeatureProviderName = keyof typeof featureProviderRegistry;

export async function getFeatureProvider(
  name: FeatureProviderName
): Promise<FeatureProvider> {
  throw new Error("not implemented");
}

export function getAllFeatureProviders(): FeatureProviderName[] {
  throw new Error("not implemented");
}

export function isFeatureProviderRegistered(
  name: string
): name is FeatureProviderName {
  throw new Error("not implemented");
}

export function clearFeatureProviderCache(): void {
  throw new Error("not implemented");
}

// =============================================================================
// Test Utilities
// =============================================================================

export function getTestableProviders(): ProviderName[] {
  throw new Error("not implemented");
}

export function getTestableFeatureProviders(): FeatureProviderName[] {
  throw new Error("not implemented");
}
