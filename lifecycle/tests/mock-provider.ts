// tests/mock-provider.ts - Reusable Mock Provider Factory (#WF2-01)
//
// Extracts and extends the mock provider pattern from spawn-instance.test.ts
// into a shared factory for all workflow/provider tests.

import type {
  Provider,
  ProviderInstance,
  ProviderCapabilities,
  SpawnOptions,
  ListFilter,
  BootstrapConfig,
} from "../control/src/provider/types";

// =============================================================================
// Mock Provider Config
// =============================================================================

export interface MockProviderConfig {
  /** Control list() return value. Static array, Error to throw, or dynamic function. */
  list?: ProviderInstance[] | Error | ((filter?: ListFilter) => ProviderInstance[] | Error);
  /** Control spawn() return value. Static instance, Error to throw, or dynamic function. */
  spawn?: ProviderInstance | Error | ((input: SpawnOptions) => ProviderInstance | Error);
  /** Control terminate() behavior. Void (success), Error to throw, or dynamic function. */
  terminate?: void | Error | ((id: string) => void | Error);
  /** Control get() return value. Static instance/null, or dynamic function. */
  get?: ProviderInstance | null | ((id: string) => ProviderInstance | null);
  /** Override default capabilities. */
  capabilities?: Partial<ProviderCapabilities>;
}

// =============================================================================
// Default Capabilities
// =============================================================================

const DEFAULT_CAPABILITIES: ProviderCapabilities = {
  snapshots: false,
  spot: false,
  gpu: false,
  multiRegion: false,
  persistentVolumes: false,
  warmVolumes: false,
  hibernation: false,
  costExplorer: false,
  tailscaleNative: false,
  idempotentSpawn: true,
  customNetworking: false,
};

// =============================================================================
// Mock Provider Factory
// =============================================================================

/**
 * Build a configurable mock provider for tests.
 *
 * Each method field can be:
 * - A static value (returned on every call)
 * - An Error instance (thrown on every call)
 * - A function for dynamic per-call behavior
 *
 * Defaults: list returns [], spawn returns a basic mock instance,
 * terminate succeeds, get returns null.
 */
export function makeMockProvider(config?: MockProviderConfig): Provider {
  const cfg = config ?? {};

  return {
    name: "orbstack" as const,
    capabilities: {
      ...DEFAULT_CAPABILITIES,
      ...cfg.capabilities,
    },

    async spawn(options: SpawnOptions): Promise<ProviderInstance> {
      if (cfg.spawn === undefined) {
        // Default: return a basic mock instance
        return makeMockInstance(`mock-${options.instanceId}`, null);
      }
      if (cfg.spawn instanceof Error) throw cfg.spawn;
      if (typeof cfg.spawn === "function") {
        const result = cfg.spawn(options);
        if (result instanceof Error) throw result;
        return result;
      }
      return cfg.spawn;
    },

    async terminate(providerId: string): Promise<void> {
      if (cfg.terminate === undefined) return; // Default: success
      if (cfg.terminate instanceof Error) throw cfg.terminate;
      if (typeof cfg.terminate === "function") {
        const result = cfg.terminate(providerId);
        if (result instanceof Error) throw result;
        return;
      }
      // void — success
    },

    async list(filter?: ListFilter): Promise<ProviderInstance[]> {
      if (cfg.list === undefined) return []; // Default: empty
      if (cfg.list instanceof Error) throw cfg.list;
      if (typeof cfg.list === "function") {
        const result = cfg.list(filter);
        if (result instanceof Error) throw result;
        return result;
      }
      return cfg.list;
    },

    async get(providerId: string): Promise<ProviderInstance | null> {
      if (cfg.get === undefined) return null; // Default: not found
      if (typeof cfg.get === "function") return cfg.get(providerId);
      return cfg.get;
    },

    generateBootstrap(_config: BootstrapConfig) {
      return { content: "#!/bin/sh", format: "shell" as const, checksum: "abc" };
    },
  };
}

// =============================================================================
// Mock Instance Factory
// =============================================================================

/**
 * Create a minimal ProviderInstance for testing.
 */
export function makeMockInstance(id: string, ip?: string | null): ProviderInstance {
  return {
    id,
    status: "running" as const,
    spec: "ubuntu:noble:arm64",
    ip: ip ?? null,
    isSpot: false,
    createdAt: Date.now(),
  };
}

// =============================================================================
// Mock Call Queue
// =============================================================================

/**
 * Returns a function that yields sequential results from a queue.
 *
 * On each call, returns (or throws) the next value in the queue.
 * After exhaustion, repeats the last value indefinitely.
 *
 * Usage:
 *   const term = mockQueue<void>(undefined, new Error("fail"), undefined);
 *   await term(); // succeeds
 *   await term(); // throws "fail"
 *   await term(); // succeeds (repeats last)
 */
export function mockQueue<T>(...results: (T | Error)[]): () => T {
  if (results.length === 0) {
    throw new Error("mockQueue requires at least one result");
  }

  let index = 0;

  return () => {
    const current = index < results.length ? results[index]! : results[results.length - 1]!;
    if (index < results.length) index++;
    if (current instanceof Error) throw current;
    return current as T;
  };
}
