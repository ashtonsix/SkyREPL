// workflow/nodes/spawn-instance.test.ts - Two-phase spawn recovery tests (#WF2-01)
//
// Tests for retrySpawnNode (idempotency guard) and reconcilePendingSpawn /
// reconcileStalePendingSpawns (background sweep). Each test verifies both
// what SHOULD happen and what SHOULD NOT happen (negative cases).

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest } from "../../../../tests/harness";
import { createInstance, getInstance, updateInstance, listInstances } from "../../material/db";
import {
  retrySpawnNode,
  findExistingSpawnRecord,
} from "./spawn-instance";
import {
  reconcilePendingSpawn,
  reconcileStalePendingSpawns,
} from "../../background/reconciliation";
import type { Instance } from "../../material/db";
import type { Provider, ProviderInstance, ListFilter } from "../../provider/types";
import { registerProvider as _registerProvider, clearAllProviders } from "../../provider/registry";
import { TIMING } from "@skyrepl/contracts";

// =============================================================================
// Helpers
// =============================================================================

function makeInstance(overrides: Partial<Omit<Instance, "id" | "created_at">> = {}): Instance {
  return createInstance({
    provider: "orbstack",
    provider_id: "",
    spec: "ubuntu:noble:arm64",
    region: "local",
    ip: null,
    workflow_state: "spawn:pending",
    workflow_error: null,
    current_manifest_id: null,
    spawn_idempotency_key: null,
    is_spot: 0,
    spot_request_id: null,
    init_checksum: null,
    registration_token_hash: null,
    last_heartbeat: Date.now(),
    ...overrides,
  });
}

/**
 * Build a minimal mock provider. Pass `listResult` to control what list()
 * returns. If `listResult` is an Error, list() will throw it.
 */
function makeMockProvider(listResult: ProviderInstance[] | Error = []): Provider {
  return {
    name: "orbstack" as const,
    capabilities: {
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
    },
    async spawn() { throw new Error("not implemented"); },
    async terminate() {},
    async list(_filter?: ListFilter) {
      if (listResult instanceof Error) throw listResult;
      return listResult;
    },
    async get() { return null; },
    generateBootstrap() {
      return { content: "#!/bin/sh", format: "shell" as const, checksum: "abc" };
    },
  };
}

function makeMockInstance(id: string, ip?: string): ProviderInstance {
  return {
    id,
    status: "running" as const,
    spec: "ubuntu:noble:arm64",
    ip: ip ?? null,
    isSpot: false,
    createdAt: Date.now(),
  };
}

/** Seed a provider into the provider cache without invoking lifecycle hooks. */
async function registerProvider(_name: "orbstack", provider: Provider): Promise<void> {
  await _registerProvider({ provider });
}

let cleanup: () => Promise<void>;

beforeEach(() => {
  cleanup = setupTest();
});

afterEach(async () => {
  clearAllProviders();
  await cleanup();
});

// =============================================================================
// retrySpawnNode
// =============================================================================

describe("retrySpawnNode", () => {
  test("returns null when no existing record — first attempt proceeds normally", () => {
    const result = retrySpawnNode("wf-99:spawn-instance");
    expect(result).toBeNull();
  });

  test("existing spawn:pending record (no provider_id) — skip Phase 1, re-run Phase 2", () => {
    const instance = makeInstance({
      spawn_idempotency_key: "wf-1:spawn-instance",
      workflow_state: "spawn:pending",
      provider_id: "",
    });

    const result = retrySpawnNode("wf-1:spawn-instance");

    expect(result).not.toBeNull();
    expect(result!.instance.id).toBe(instance.id);
    expect(result!.skipProviderCall).toBe(false); // Phase 2 must still run

    // NEGATIVE: ensure we are NOT creating a second record
    const allInstances = listInstances();
    expect(allInstances.length).toBe(1); // still only one record
  });

  test("existing record with provider_id — skip Phase 1 AND Phase 2", () => {
    const instance = makeInstance({
      spawn_idempotency_key: "wf-2:spawn-instance",
      workflow_state: "launch-run:provisioning",
      provider_id: "vm-already-created",
    });

    const result = retrySpawnNode("wf-2:spawn-instance");

    expect(result).not.toBeNull();
    expect(result!.instance.id).toBe(instance.id);
    expect(result!.skipProviderCall).toBe(true); // Both phases done

    // NEGATIVE: no duplicate instance should exist
    const allInstances = listInstances();
    expect(allInstances.length).toBe(1);
  });

  test("existing record in terminal spawn:error state — throws, not retried", () => {
    makeInstance({
      spawn_idempotency_key: "wf-3:spawn-instance",
      workflow_state: "spawn:error",
      provider_id: "",
    });

    expect(() => retrySpawnNode("wf-3:spawn-instance")).toThrow(/terminal state/);
  });

  test("existing record in other terminal states — throws", () => {
    for (const state of ["terminate:complete", "launch-run:compensated"]) {
      const key = `wf-term-${state}:spawn-instance`;
      makeInstance({
        spawn_idempotency_key: key,
        workflow_state: state,
        provider_id: "",
      });

      expect(() => retrySpawnNode(key)).toThrow(/terminal state/);
    }
  });

  test("findExistingSpawnRecord returns null when no matching key exists", () => {
    const result = findExistingSpawnRecord("nonexistent-key");
    expect(result).toBeNull();
  });

  test("findExistingSpawnRecord returns the correct instance by idempotency key", () => {
    const instance = makeInstance({
      spawn_idempotency_key: "wf-find:spawn-instance",
    });

    const found = findExistingSpawnRecord("wf-find:spawn-instance");
    expect(found).not.toBeNull();
    expect(found!.id).toBe(instance.id);

    // NEGATIVE: wrong key returns null
    const notFound = findExistingSpawnRecord("wrong-key");
    expect(notFound).toBeNull();
  });
});

// =============================================================================
// reconcilePendingSpawn
// =============================================================================

describe("reconcilePendingSpawn", () => {
  test("provider has matching instance → DB updated with provider_id and state", async () => {
    const instance = makeInstance({
      spawn_idempotency_key: "spawn:ctrl-1-1",
    });

    const provider = makeMockProvider([
      makeMockInstance("provider-vm-99", "10.0.0.5"),
    ]);
    registerProvider("orbstack", provider);

    const result = await reconcilePendingSpawn(instance);

    expect(result.outcome).toBe("found");
    if (result.outcome === "found") {
      expect(result.providerId).toBe("provider-vm-99");
    }

    // DB should be updated
    const updated = getInstance(instance.id)!;
    expect(updated.provider_id).toBe("provider-vm-99");
    expect(updated.ip).toBe("10.0.0.5");
    expect(updated.workflow_state).toBe("launch-run:provisioning");

    // NEGATIVE: state must NOT remain spawn:pending
    expect(updated.workflow_state).not.toBe("spawn:pending");
  });

  test("provider has NO matching instance → DB marked spawn:error", async () => {
    const instance = makeInstance({
      spawn_idempotency_key: "spawn:ctrl-1-2",
    });

    const provider = makeMockProvider([]); // empty list → not found
    registerProvider("orbstack", provider);

    const result = await reconcilePendingSpawn(instance);

    expect(result.outcome).toBe("not_found");

    const updated = getInstance(instance.id)!;
    expect(updated.workflow_state).toBe("spawn:error");

    // NEGATIVE: provider_id must NOT be set
    expect(updated.provider_id).toBe("");
  });

  test("provider has multiple matches → picks first, returns multiple_found", async () => {
    const instance = makeInstance({
      spawn_idempotency_key: "spawn:ctrl-1-3",
    });

    const provider = makeMockProvider([
      makeMockInstance("provider-vm-A"),
      makeMockInstance("provider-vm-B"),
    ]);
    registerProvider("orbstack", provider);

    const result = await reconcilePendingSpawn(instance);

    expect(result.outcome).toBe("multiple_found");
    if (result.outcome === "multiple_found") {
      expect(result.providerId).toBe("provider-vm-A"); // picks first
    }

    const updated = getInstance(instance.id)!;
    expect(updated.provider_id).toBe("provider-vm-A");
    expect(updated.workflow_state).toBe("launch-run:provisioning");
  });

  test("instance not in spawn:pending state → skipped without DB change", async () => {
    const instance = makeInstance({
      workflow_state: "launch-run:provisioning",
      provider_id: "already-set",
    });

    const provider = makeMockProvider([]);
    registerProvider("orbstack", provider);

    const result = await reconcilePendingSpawn(instance);

    expect(result.outcome).toBe("skipped_non_pending");

    // NEGATIVE: DB should be unchanged
    const unchanged = getInstance(instance.id)!;
    expect(unchanged.workflow_state).toBe("launch-run:provisioning");
    expect(unchanged.provider_id).toBe("already-set");
  });

  test("provider without idempotentSpawn capability → no_idempotent_spawn outcome", async () => {
    const instance = makeInstance({
      spawn_idempotency_key: "spawn:ctrl-1-4",
    });

    const nonIdempotentProvider: Provider = {
      ...makeMockProvider([]),
      capabilities: {
        ...makeMockProvider([]).capabilities,
        idempotentSpawn: false,
      },
    };
    registerProvider("orbstack", nonIdempotentProvider);

    const result = await reconcilePendingSpawn(instance);

    expect(result.outcome).toBe("no_idempotent_spawn");

    // NEGATIVE: DB state should be unchanged (caller decides what to do)
    const unchanged = getInstance(instance.id)!;
    expect(unchanged.workflow_state).toBe("spawn:pending");
  });

  test("instance with no spawn_idempotency_key → marked spawn:error", async () => {
    const instance = makeInstance({
      spawn_idempotency_key: null,
    });

    const provider = makeMockProvider([]);
    registerProvider("orbstack", provider);

    const result = await reconcilePendingSpawn(instance);

    expect(result.outcome).toBe("not_found");

    const updated = getInstance(instance.id)!;
    expect(updated.workflow_state).toBe("spawn:error");
  });
});

// =============================================================================
// reconcileStalePendingSpawns
// =============================================================================

describe("reconcileStalePendingSpawns", () => {
  test("only processes records older than the stale threshold", async () => {
    const freshInstance = makeInstance({
      spawn_idempotency_key: "spawn:fresh-1",
      // created_at defaults to Date.now() — within threshold
    });

    const staleInstance = makeInstance({
      spawn_idempotency_key: "spawn:stale-1",
    });

    // Manually backdate the stale instance's created_at
    const staleCutoff = Date.now() - TIMING.PENDING_SPAWN_STALE_THRESHOLD_MS - 1000;
    updateInstance(staleInstance.id, { last_heartbeat: staleCutoff });
    // We need to patch created_at in the DB directly since createInstance sets it to now
    const { getDatabase } = await import("../../material/db");
    getDatabase().prepare("UPDATE instances SET created_at = ? WHERE id = ?")
      .run(staleCutoff, staleInstance.id);

    const provider = makeMockProvider([]); // no match → spawn:error
    registerProvider("orbstack", provider);

    const processed = await reconcileStalePendingSpawns();

    // Only the stale instance should be processed
    expect(processed).toBe(1);

    // Stale instance → marked spawn:error (not found in provider)
    const updatedStale = getInstance(staleInstance.id)!;
    expect(updatedStale.workflow_state).toBe("spawn:error");

    // NEGATIVE: Fresh instance must NOT be touched
    const updatedFresh = getInstance(freshInstance.id)!;
    expect(updatedFresh.workflow_state).toBe("spawn:pending");
  });

  test("processes zero records when none are stale", async () => {
    makeInstance({ spawn_idempotency_key: "spawn:new-1" }); // fresh
    makeInstance({ spawn_idempotency_key: "spawn:new-2" }); // fresh

    const provider = makeMockProvider([]);
    registerProvider("orbstack", provider);

    const processed = await reconcileStalePendingSpawns();
    expect(processed).toBe(0);
  });

  test("stale record found in provider → recovered (launch-run:provisioning)", async () => {
    const staleInstance = makeInstance({
      spawn_idempotency_key: "spawn:stale-found",
    });

    const staleCutoff = Date.now() - TIMING.PENDING_SPAWN_STALE_THRESHOLD_MS - 1000;
    const { getDatabase } = await import("../../material/db");
    getDatabase().prepare("UPDATE instances SET created_at = ? WHERE id = ?")
      .run(staleCutoff, staleInstance.id);

    const provider = makeMockProvider([makeMockInstance("provider-vm-recovered", "10.1.2.3")]);
    registerProvider("orbstack", provider);

    const processed = await reconcileStalePendingSpawns();
    expect(processed).toBe(1);

    const updated = getInstance(staleInstance.id)!;
    expect(updated.provider_id).toBe("provider-vm-recovered");
    expect(updated.workflow_state).toBe("launch-run:provisioning");
    expect(updated.ip).toBe("10.1.2.3");

    // NEGATIVE: must NOT be marked spawn:error
    expect(updated.workflow_state).not.toBe("spawn:error");
  });

  test("stale record with no_idempotent_spawn provider → marked spawn:error", async () => {
    const staleInstance = makeInstance({
      spawn_idempotency_key: "spawn:stale-no-idem",
    });

    const staleCutoff = Date.now() - TIMING.PENDING_SPAWN_STALE_THRESHOLD_MS - 1000;
    const { getDatabase } = await import("../../material/db");
    getDatabase().prepare("UPDATE instances SET created_at = ? WHERE id = ?")
      .run(staleCutoff, staleInstance.id);

    const nonIdempotentProvider: Provider = {
      ...makeMockProvider([]),
      capabilities: {
        ...makeMockProvider([]).capabilities,
        idempotentSpawn: false,
      },
    };
    registerProvider("orbstack", nonIdempotentProvider);

    const processed = await reconcileStalePendingSpawns();
    expect(processed).toBe(1);

    // Sweep marks it spawn:error since we can't query by tag
    const updated = getInstance(staleInstance.id)!;
    expect(updated.workflow_state).toBe("spawn:error");
  });
});
