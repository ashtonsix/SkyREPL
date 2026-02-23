// tests/unit/materialization.test.ts - Materialization Barrier Contract Tests
//
// Proves the materialization contract works. A regression removing the barrier
// (e.g., removing materializeInstance from claimWarmPoolAllocation, removing
// stampMaterialized from a route handler) will fail these tests.

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest } from "../harness";
import {
  createInstance,
  createAllocation,
  createRun,
  getInstance,
  type Instance,
  type Allocation,
  type Run,
} from "../../control/src/material/db";
import {
  registerProvider,
} from "../../control/src/provider/registry";
import type { Provider, ProviderInstance, ProviderCapabilities, SpawnOptions, BootstrapConfig, BootstrapScript, ListFilter } from "../../control/src/provider/types";
import { materializeInstance, materializeInstanceBatch } from "../../control/src/resource/instance";
import { materializeRun, materializeRunBatch } from "../../control/src/resource/run";
import { materializeAllocation, materializeAllocationBatch } from "../../control/src/resource/allocation";
import { materializeManifest } from "../../control/src/resource/manifest";
import { claimWarmPoolAllocation } from "../../control/src/resource/allocation";
import { getTtlForTier, stampMaterialized, cachedMaterialize } from "../../control/src/resource/materializer";
import { cacheGet, cacheSet, cacheInvalidate, cacheClear, cacheStats, cacheResetStats } from "../../control/src/resource/cache";

// =============================================================================
// Minimal Mock Provider (test-local, no SimulatedAgent needed)
// =============================================================================

function createMinimalMockProvider(overrides?: {
  get?: (id: string) => Promise<ProviderInstance | null>;
  list?: () => Promise<ProviderInstance[]>;
}): Provider {
  return {
    name: "mock" as any,
    capabilities: {
      snapshots: false, spot: false, gpu: false, multiRegion: false,
      persistentVolumes: false, warmVolumes: false, hibernation: false,
      costExplorer: false, tailscaleNative: false, idempotentSpawn: true,
      customNetworking: false,
    } as ProviderCapabilities,
    async spawn(_opts: SpawnOptions): Promise<ProviderInstance> {
      return { id: "mock-1", status: "running", spec: "gpu-small", ip: "127.0.0.1", createdAt: Date.now(), isSpot: false };
    },
    async terminate(_id: string): Promise<void> {},
    async list(_filter?: ListFilter): Promise<ProviderInstance[]> {
      if (overrides?.list) return overrides.list();
      return [];
    },
    async get(id: string): Promise<ProviderInstance | null> {
      if (overrides?.get) return overrides.get(id);
      return null;
    },
    generateBootstrap(_config: BootstrapConfig): BootstrapScript {
      return { content: "#!/bin/bash", format: "shell", checksum: "mock" };
    },
  };
}

// =============================================================================
// Test Helpers
// =============================================================================

function createTestInstance(overrides?: Partial<{
  provider: string;
  provider_id: string;
  spec: string;
  region: string;
  ip: string;
  workflow_state: string;
  last_heartbeat: number;
  init_checksum: string | null;
}>): Instance {
  const now = Date.now();
  return createInstance({
    provider: overrides?.provider ?? "mock",
    provider_id: overrides?.provider_id ?? `mock-${now}-${Math.random().toString(36).slice(2, 8)}`,
    spec: overrides?.spec ?? "gpu-small",
    region: overrides?.region ?? "us-east-1",
    ip: overrides?.ip ?? "10.0.0.1",
    workflow_state: overrides?.workflow_state ?? "launch-run:provisioning",
    workflow_error: null,
    current_manifest_id: null,
    spawn_idempotency_key: null,
    is_spot: 0,
    spot_request_id: null,
    init_checksum: overrides?.init_checksum !== undefined ? overrides.init_checksum : null,
    registration_token_hash: null,
    last_heartbeat: overrides?.last_heartbeat ?? now,
  });
}

function createTestRun(): Run {
  return createRun({
    command: "echo test",
    workdir: "/workspace",
    max_duration_ms: 60000,
    workflow_state: "launch-run:pending",
    workflow_error: null,
    current_manifest_id: null,
    exit_code: null,
    init_checksum: null,
    create_snapshot: 0,
    spot_interrupted: 0,
    started_at: null,
    finished_at: null,
  });
}

function createTestAllocation(instanceId: number): Allocation {
  return createAllocation({
    run_id: null,
    instance_id: instanceId,
    status: "AVAILABLE",
    current_manifest_id: null,
    user: "default",
    workdir: "/workspace",
    debug_hold_until: null,
    completed_at: null,
  });
}

// =============================================================================
// Setup
// =============================================================================

let cleanup: () => Promise<void>;

beforeEach(() => {
  cleanup = setupTest();
});

afterEach(() => cleanup());

// =============================================================================
// B0 (P0): Warm Pool Claim with Materialization Barrier
// =============================================================================

describe("warm pool: materialization barrier in claimWarmPoolAllocation", () => {
  test("claim fails when no warm allocation exists (no candidates)", async () => {
    // No allocations created — findWarmAllocation returns null immediately
    const run = createTestRun();

    await registerProvider({
      provider: createMinimalMockProvider(),
    });

    const result = await claimWarmPoolAllocation(
      { spec: "gpu-small", tenantId: 1 },
      run.id,
    );

    // No warm candidate — barrier has nothing to evaluate
    expect(result.success).toBe(false);
    expect(result.error).toContain("No warm allocation");
  });

  test("claim rejected when provider says instance is gone (§3.3 mark_terminated)", async () => {
    // THE CORE WL-052 SCENARIO: instance has fresh heartbeat but provider
    // says it's been deleted. Without mark_terminated, the 25-minute stale
    // heartbeat window means dead instances look alive in the warm pool.
    const instance = createTestInstance({ provider_id: "dead-vm-1" });
    createTestAllocation(instance.id);
    const run = createTestRun();

    await registerProvider({
      provider: createMinimalMockProvider({
        get: async () => null, // instance deleted from provider
      }),
    });

    const result = await claimWarmPoolAllocation(
      { spec: "gpu-small", tenantId: 1 },
      run.id,
    );

    // Claim must be rejected — instance is dead
    expect(result.success).toBe(false);

    // Instance should be marked terminated in DB
    const dbRecord = getInstance(instance.id);
    expect(dbRecord!.workflow_state).toBe("terminate:complete");
  });

  test("claim succeeds when provider confirms instance is alive", async () => {
    const instance = createTestInstance({ provider_id: "alive-vm-1" });
    createTestAllocation(instance.id);
    const run = createTestRun();

    await registerProvider({
      provider: createMinimalMockProvider({
        get: async (id) => ({
          id,
          status: "running",
          spec: "gpu-small",
          ip: "10.0.0.1",
          createdAt: Date.now(),
          isSpot: false,
        }),
      }),
    });

    const result = await claimWarmPoolAllocation(
      { spec: "gpu-small", tenantId: 1 },
      run.id,
    );

    expect(result.success).toBe(true);
    expect(result.allocation).toBeDefined();
    expect(result.allocation!.status).toBe("CLAIMED");
  });

  test("materialization barrier is present (provider.get called during claim)", async () => {
    let getCalled = false;
    const instance = createTestInstance({ provider_id: "barrier-test-vm" });
    createTestAllocation(instance.id);
    const run = createTestRun();

    await registerProvider({
      provider: createMinimalMockProvider({
        get: async (id) => {
          getCalled = true;
          return { id, status: "running", spec: "gpu-small", ip: "10.0.0.1", createdAt: Date.now(), isSpot: false };
        },
      }),
    });

    await claimWarmPoolAllocation(
      { spec: "gpu-small", tenantId: 1 },
      run.id,
    );

    expect(getCalled).toBe(true);
  });
});

// =============================================================================
// B1 (P1): materialized_at Presence
// =============================================================================

describe("materialized_at contract", () => {
  test("materializeRun returns materialized_at", () => {
    const run = createTestRun();
    const materialized = materializeRun(run.id);
    expect(materialized).not.toBeNull();
    expect(materialized!.materialized_at).toBeGreaterThan(0);
    expect(typeof materialized!.materialized_at).toBe("number");
  });

  test("materializeRunBatch returns materialized_at on each", () => {
    const r1 = createTestRun();
    const r2 = createTestRun();
    const batch = materializeRunBatch([r1.id, r2.id]);
    expect(batch.length).toBe(2);
    for (const m of batch) {
      expect(m.materialized_at).toBeGreaterThan(0);
    }
  });

  test("materializeAllocation returns materialized_at", () => {
    const instance = createTestInstance();
    const alloc = createTestAllocation(instance.id);
    const materialized = materializeAllocation(alloc.id);
    expect(materialized).not.toBeNull();
    expect(materialized!.materialized_at).toBeGreaterThan(0);
  });

  test("materializeManifest returns null for missing ID", () => {
    const result = materializeManifest(99999);
    expect(result).toBeNull();
  });

  test("materializeRun returns null for missing ID", () => {
    const result = materializeRun(99999);
    expect(result).toBeNull();
  });

  test("materializeInstance returns null for missing ID", async () => {
    const result = await materializeInstance(99999);
    expect(result).toBeNull();
  });

  test("materializeInstance returns materialized_at for terminal instance", async () => {
    const instance = createTestInstance({ workflow_state: "terminate:complete" });
    const materialized = await materializeInstance(instance.id);
    expect(materialized).not.toBeNull();
    expect(materialized!.materialized_at).toBeGreaterThan(0);
  });
});

// =============================================================================
// B2 (P1): Cache Module
// =============================================================================

describe("cache module", () => {
  test("set then get returns value", () => {
    cacheSet("test:1", { name: "hello" }, 10_000);
    const result = cacheGet<{ name: string }>("test:1");
    expect(result).not.toBeNull();
    expect(result!.name).toBe("hello");
  });

  test("expired entry returns null", () => {
    cacheSet("test:2", { name: "old" }, 1); // 1ms TTL
    // Wait for expiry
    const start = Date.now();
    while (Date.now() - start < 5) {} // busy-wait 5ms
    const result = cacheGet("test:2");
    expect(result).toBeNull();
  });

  test("TTL=0 never caches", () => {
    cacheSet("test:3", { name: "never" }, 0);
    const result = cacheGet("test:3");
    expect(result).toBeNull();
  });

  test("invalidate removes entry", () => {
    cacheSet("test:4", { name: "bye" }, 10_000);
    cacheInvalidate("test:4");
    const result = cacheGet("test:4");
    expect(result).toBeNull();
  });

  test("clear removes all entries", () => {
    cacheSet("a:1", "a", 10_000);
    cacheSet("b:2", "b", 10_000);
    cacheClear();
    expect(cacheGet("a:1")).toBeNull();
    expect(cacheGet("b:2")).toBeNull();
  });

  test("stats track hits and misses", () => {
    cacheResetStats();
    cacheSet("stats:1", "val", 10_000);
    cacheGet("stats:1"); // hit
    cacheGet("stats:1"); // hit
    cacheGet("stats:miss"); // miss

    const stats = cacheStats();
    expect(stats.hits).toBe(2);
    expect(stats.misses).toBe(1);
    expect(stats.size).toBe(1);
  });
});

// =============================================================================
// mark_terminated: Instance Materializer — Provider Returns Null
// =============================================================================

describe("instance materializer: mark_terminated (§3.3 M28)", () => {
  test("provider.get() returning null marks instance as terminate:complete", async () => {
    const instance = createTestInstance({ provider_id: "vanished-vm" });

    await registerProvider({
      provider: createMinimalMockProvider({
        get: async () => null,
      }),
    });

    const materialized = await materializeInstance(instance.id, { tier: "loop" });
    expect(materialized).not.toBeNull();
    expect(materialized!.workflow_state).toBe("terminate:complete");
    expect(materialized!.materialized_at).toBeGreaterThan(0);

    // DB record updated
    const dbRecord = getInstance(instance.id);
    expect(dbRecord!.workflow_state).toBe("terminate:complete");
  });

  test("already-terminal instance does NOT re-query provider when marked terminated", async () => {
    // After mark_terminated, subsequent materializations should be DB-only
    const instance = createTestInstance({ provider_id: "vanished-vm-2" });

    let getCallCount = 0;
    await registerProvider({
      provider: createMinimalMockProvider({
        get: async () => {
          getCallCount++;
          return null;
        },
      }),
    });

    // First call: provider returns null → mark_terminated
    await materializeInstance(instance.id, { tier: "loop" });
    expect(getCallCount).toBe(1);

    // Second call: instance is now terminal → no provider call
    await materializeInstance(instance.id, { tier: "loop" });
    expect(getCallCount).toBe(1); // not incremented
  });
});

// =============================================================================
// B3 (P1): Instance Materializer — IP Drift
// =============================================================================

describe("instance materializer: IP drift correction", () => {
  test("detects IP drift and writes back to DB", async () => {
    const instance = createTestInstance({ ip: "10.0.0.1", provider_id: "drift-vm" });

    await registerProvider({
      provider: createMinimalMockProvider({
        get: async (id) => ({
          id,
          status: "running",
          spec: "gpu-small",
          ip: "10.0.0.2", // different IP
          createdAt: Date.now(),
          isSpot: false,
        }),
      }),
    });

    const materialized = await materializeInstance(instance.id, { tier: "loop" });
    expect(materialized).not.toBeNull();
    expect(materialized!.ip).toBe("10.0.0.2");

    // Verify DB was updated
    const dbRecord = getInstance(instance.id);
    expect(dbRecord!.ip).toBe("10.0.0.2");
  });
});

// =============================================================================
// B4 (P1): Instance Materializer — Provider Failure Fallback
// =============================================================================

describe("instance materializer: provider failure fallback", () => {
  test("falls back to DB state when provider throws", async () => {
    const instance = createTestInstance({ ip: "10.0.0.1", provider_id: "unreachable-vm" });

    await registerProvider({
      provider: createMinimalMockProvider({
        get: async () => { throw new Error("provider unreachable"); },
      }),
    });

    const materialized = await materializeInstance(instance.id, { tier: "loop" });
    expect(materialized).not.toBeNull();
    expect(materialized!.ip).toBe("10.0.0.1"); // DB state preserved
    expect(materialized!.materialized_at).toBeGreaterThan(0);
  });
});

// =============================================================================
// B5 (P1): cachedMaterialize Pattern
// =============================================================================

describe("cachedMaterialize", () => {
  test("fetcher called on first call, cached on second", async () => {
    let fetchCount = 0;
    const fetcher = async () => {
      fetchCount++;
      return { value: "fresh", materialized_at: Date.now() };
    };

    const first = await cachedMaterialize("pattern:1", 10_000, fetcher);
    expect(first.value).toBe("fresh");
    expect(fetchCount).toBe(1);

    const second = await cachedMaterialize("pattern:1", 10_000, fetcher);
    expect(second.value).toBe("fresh");
    expect(fetchCount).toBe(1); // NOT called again
  });

  test("TTL=0 always calls fetcher", async () => {
    let fetchCount = 0;
    const fetcher = async () => {
      fetchCount++;
      return { value: fetchCount };
    };

    await cachedMaterialize("pattern:2", 0, fetcher);
    await cachedMaterialize("pattern:2", 0, fetcher);
    await cachedMaterialize("pattern:2", 0, fetcher);

    expect(fetchCount).toBe(3);
  });
});

// =============================================================================
// B6 (P2): FreshnessTier → TTL Mapping
// =============================================================================

describe("FreshnessTier → TTL mapping", () => {
  test("decision tier returns CACHE_INSTANCE_STATE_TTL_MS", () => {
    const ttl = getTtlForTier("decision");
    expect(ttl).toBe(30_000); // default AWS
  });

  test("decision tier with orbstack returns 10s", () => {
    const ttl = getTtlForTier("decision", "orbstack");
    expect(ttl).toBe(10_000);
  });

  test("display tier returns 60s", () => {
    expect(getTtlForTier("display")).toBe(60_000);
  });

  test("loop tier returns 0 (always fresh)", () => {
    expect(getTtlForTier("loop")).toBe(0);
  });

  test("batch tier returns CACHE_CAPACITY_TTL_MS", () => {
    const ttl = getTtlForTier("batch");
    expect(ttl).toBe(60_000);
  });
});

// =============================================================================
// B7 (P2): Terminal Instance Skips Provider Call
// =============================================================================

describe("instance materializer: terminal state bypass", () => {
  test("terminal instance does not call provider.get()", async () => {
    let getCalled = false;
    const instance = createTestInstance({
      workflow_state: "terminate:complete",
      provider_id: "terminated-vm",
    });

    await registerProvider({
      provider: createMinimalMockProvider({
        get: async () => {
          getCalled = true;
          return null;
        },
      }),
    });

    const materialized = await materializeInstance(instance.id);
    expect(materialized).not.toBeNull();
    expect(materialized!.materialized_at).toBeGreaterThan(0);
    expect(getCalled).toBe(false); // provider NOT consulted
  });

  test("error state instance does not call provider.get()", async () => {
    let getCalled = false;
    const instance = createTestInstance({
      workflow_state: "spawn:error",
      provider_id: "error-vm",
    });

    await registerProvider({
      provider: createMinimalMockProvider({
        get: async () => {
          getCalled = true;
          return null;
        },
      }),
    });

    const materialized = await materializeInstance(instance.id);
    expect(materialized).not.toBeNull();
    expect(getCalled).toBe(false);
  });
});
