// tests/unit/provider-categories.test.ts - Provider Category Registry Tests

import { describe, test, expect, beforeEach } from "bun:test";
import {
  getCategoryProvider,
  listCategoryProviders,
  registerCategoryProvider,
  clearProviderCache,
  getProvider,
} from "../../control/src/provider/registry";
import type {
  StorageProvider,
  TunnelProvider,
  ProviderCategory,
} from "../../control/src/provider/types";

describe("Provider Category Registry", () => {
  beforeEach(() => {
    clearProviderCache();
  });

  test("listCategoryProviders returns compute providers", () => {
    const providers = listCategoryProviders("compute");
    expect(providers).toContain("orbstack");
  });

  test("listCategoryProviders returns empty for storage and tunnel", () => {
    const storage = listCategoryProviders("storage");
    const tunnel = listCategoryProviders("tunnel");
    expect(storage).toEqual([]);
    expect(tunnel).toEqual([]);
  });

  test("getCategoryProvider retrieves orbstack from compute category", async () => {
    const provider = await getCategoryProvider("compute", "orbstack");
    expect(provider).toBeDefined();
    expect(provider.name).toBe("orbstack");
  });

  test("getCategoryProvider throws for non-existent provider", async () => {
    expect(async () => {
      await getCategoryProvider("storage", "nonexistent");
    }).toThrow("Provider 'nonexistent' not found in category 'storage'");
  });

  test("registerCategoryProvider adds storage provider", async () => {
    const mockStorage: StorageProvider = {
      name: "test-storage",
      category: "storage",
      capabilities: {
        maxObjectSizeBytes: 1000000,
        supportsRangeRequests: false,
        supportsMultipartUpload: false,
        supportsCAS: false,
        supportsExpiration: false,
      },
      async putObject() {},
      async getObject() {
        return null;
      },
      async deleteObject() {},
      async listObjects() {
        return { objects: [] };
      },
      async healthCheck() {
        return { healthy: true, latencyMs: 10 };
      },
    };

    registerCategoryProvider("storage", "test-storage", async () => mockStorage);

    const providers = listCategoryProviders("storage");
    expect(providers).toContain("test-storage");

    const retrieved = await getCategoryProvider("storage", "test-storage");
    expect(retrieved.name).toBe("test-storage");
  });

  test("registerCategoryProvider adds tunnel provider", async () => {
    const mockTunnel: TunnelProvider = {
      name: "test-tunnel",
      category: "tunnel",
      capabilities: {
        supportsCustomDomains: false,
        supportsTLS: true,
        maxConcurrentTunnels: 10,
        supportsWebSocket: true,
      },
      async expose() {
        return {
          tunnelId: "test-id",
          publicUrl: "http://test.example.com",
          localPort: 8080,
          protocol: "http",
          createdAt: Date.now(),
          status: "active",
        };
      },
      async revoke() {},
      async status() {
        return "active";
      },
      async list() {
        return [];
      },
      async healthCheck() {
        return { healthy: true, latencyMs: 5 };
      },
    };

    registerCategoryProvider("tunnel", "test-tunnel", async () => mockTunnel);

    const providers = listCategoryProviders("tunnel");
    expect(providers).toContain("test-tunnel");

    const retrieved = await getCategoryProvider("tunnel", "test-tunnel");
    expect(retrieved.name).toBe("test-tunnel");
  });

  test("backward compatibility: getProvider still works for compute", async () => {
    const provider = await getProvider("orbstack");
    expect(provider).toBeDefined();
    expect(provider.name).toBe("orbstack");
  });

  test("clearProviderCache clears both provider and category caches", async () => {
    // Load a provider to populate cache
    await getCategoryProvider("compute", "orbstack");

    // Clear cache
    clearProviderCache();

    // Should reload on next access (no way to directly verify cache state,
    // but function should still work)
    const provider = await getCategoryProvider("compute", "orbstack");
    expect(provider).toBeDefined();
  });
});
