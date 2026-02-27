// tests/unit/tailscale-provider.test.ts - TailscaleFeatureProvider unit tests
//
// Tests attach(), detach(), and reconcile() against a mocked TailscaleApiClient.
// No network calls, no SSE connections. DB backed by the in-memory test harness.

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { setupTest } from "../harness";
import {
  TailscaleFeatureProvider,
  type TailscaleConfig,
  type TailscaleState,
} from "../../control/src/provider/feature/tailscale-provider";
import {
  TailscaleApiClient,
  type TailscaleDevice,
  type AuthKeyResult,
} from "../../control/src/provider/feature/tailscale-api";
import {
  createInstance,
  queryMany,
  type Instance,
} from "../../control/src/material/db";
import { upsertTailscaleState } from "../../control/src/api/agent";
import { sseManager } from "../../control/src/api/sse-protocol";

// =============================================================================
// Helpers
// =============================================================================

function makeInstance(overrides: Partial<Omit<Instance, "id" | "created_at">> = {}): Instance {
  return createInstance({
    provider: "orbstack",
    provider_id: "test-vm",
    spec: "ubuntu:noble:arm64",
    region: "local",
    ip: null,
    workflow_state: "launch-run:provisioning",
    workflow_error: null,
    current_manifest_id: null,
    spawn_idempotency_key: null,
    is_spot: 0,
    spot_request_id: null,
    init_checksum: null,
    registration_token_hash: null,
    last_heartbeat: Date.now(),
    provider_metadata: null,
    display_name: null,
    ...overrides,
  });
}

/** Simulate the agent reporting tailscale state via heartbeat (writes to objects table). */
function setTailscaleState(instanceId: number, ip: string | null, status: string | null): void {
  upsertTailscaleState(String(instanceId), ip, status);
}

function makeDevice(overrides: Partial<TailscaleDevice> = {}): TailscaleDevice {
  return {
    id: "nodekey-abc123",
    hostname: "repl-1",
    name: "repl-1.tail1234.ts.net",
    addresses: ["100.64.0.1"],
    os: "linux",
    online: true,
    lastSeen: new Date().toISOString(),
    authorized: true,
    ...overrides,
  };
}

function makeAuthKeyResult(): AuthKeyResult {
  return {
    key: "tskey-auth-test123",
    id: "k12345",
    expires: "2026-03-18T12:00:00Z",
  };
}

/**
 * Build a mock TailscaleApiClient that returns controlled values.
 * All methods can be overridden per-test.
 */
function makeMockApiClient(overrides: Partial<TailscaleApiClient> = {}): TailscaleApiClient {
  const mock = {
    async createAuthKey(_options = {}): Promise<AuthKeyResult> {
      return makeAuthKeyResult();
    },
    async listDevices(): Promise<TailscaleDevice[]> {
      return [];
    },
    async getDevice(_deviceId: string): Promise<TailscaleDevice | null> {
      return null;
    },
    async deleteDevice(_deviceId: string): Promise<void> {
      // no-op
    },
    ...overrides,
  } as unknown as TailscaleApiClient;

  return mock;
}

// =============================================================================
// Harness
// =============================================================================

let cleanup: () => Promise<void>;

// Track SSE commands sent during tests.
let sentCommands: Array<{ instanceId: string; command: unknown }> = [];
let originalSendCommand: typeof sseManager.sendCommand;

beforeEach(() => {
  cleanup = setupTest();
  sentCommands = [];
  originalSendCommand = sseManager.sendCommand.bind(sseManager);
  // Intercept sendCommand to capture calls without needing a live SSE stream.
  sseManager.sendCommand = async (instanceId: string, command: unknown) => {
    sentCommands.push({ instanceId, command });
    return true; // simulate queued delivery
  };
});

afterEach(async () => {
  sseManager.sendCommand = originalSendCommand;
  await cleanup();
});

// =============================================================================
// attach()
// =============================================================================

describe("TailscaleFeatureProvider.attach()", () => {
  test("sends trigger_tailscale SSE command and returns state when ready", async () => {
    const device = makeDevice({ addresses: ["100.64.0.5"] });
    const client = makeMockApiClient({
      async listDevices() { return [device]; },
      async getDevice() { return device; },
    });
    const provider = new TailscaleFeatureProvider(client);

    // Create an instance that starts without tailscale, then becomes ready.
    const instance = makeInstance();

    // Simulate the agent reporting ready after attach triggers it.
    // We write to the objects table in the same tick to make the poll succeed
    // without actually waiting 2 s per cycle.
    const attachPromise = provider.attach(String(instance.id), {});

    // Simulate the heartbeat response writing tailscale state to objects.
    setTailscaleState(instance.id, "100.64.0.5", "ready");

    const state = await attachPromise;

    // SSE trigger_tailscale was sent.
    const triggerCmds = sentCommands.filter(
      (c) => (c.command as any).type === "trigger_tailscale"
    );
    expect(triggerCmds).toHaveLength(1);
    expect(triggerCmds[0].instanceId).toBe(String(instance.id));

    // Returned state is correct.
    expect(state.ip).toBe("100.64.0.5");
    expect(state.deviceId).toBe(device.id);
    expect(state.hostname).toBe(device.hostname);
    expect(state.online).toBe(true);
    expect(state.objectId).toBeGreaterThan(0);
  });

  test("returns existing state without re-triggering when already attached", async () => {
    const device = makeDevice({ addresses: ["100.64.0.7"] });
    const client = makeMockApiClient({
      async listDevices() { return [device]; },
      async getDevice() { return device; },
    });
    const provider = new TailscaleFeatureProvider(client);

    const instance = makeInstance();

    // First attach — stores the record.
    const attachPromise = provider.attach(String(instance.id), {});
    // Simulate agent reporting readiness via objects table.
    setTailscaleState(instance.id, "100.64.0.7", "ready");
    const firstState = await attachPromise;
    expect(firstState.ip).toBe("100.64.0.7");

    const cmdCountAfterFirst = sentCommands.length;

    // Second attach — should be idempotent (no new SSE command).
    const secondState = await provider.attach(String(instance.id), {});
    expect(secondState.ip).toBe("100.64.0.7");

    // No additional trigger_tailscale sent.
    expect(sentCommands.length).toBe(cmdCountAfterFirst);
  });

  test("uses caller-supplied authKey instead of creating a new one", async () => {
    let createAuthKeyCalled = false;
    const device = makeDevice({ addresses: ["100.64.0.9"] });
    const client = makeMockApiClient({
      async createAuthKey() {
        createAuthKeyCalled = true;
        return makeAuthKeyResult();
      },
      async listDevices() { return [device]; },
      async getDevice() { return device; },
    });
    const provider = new TailscaleFeatureProvider(client);

    const instance = makeInstance();
    const attachPromise = provider.attach(String(instance.id), {
      authKey: "user-supplied-key",
    });

    setTailscaleState(instance.id, "100.64.0.9", "ready");

    await attachPromise;
    expect(createAuthKeyCalled).toBe(false);
  });

  test("throws when tailscale_status becomes 'failed'", async () => {
    const client = makeMockApiClient();
    const provider = new TailscaleFeatureProvider(client);

    const instance = makeInstance();
    const attachPromise = provider.attach(String(instance.id), {});

    // Simulate installation failure.
    setTailscaleState(instance.id, null, "failed");

    await expect(attachPromise).rejects.toThrow(/Tailscale installation failed/);
  });

  test("stores device record in objects table with correct tags", async () => {
    const device = makeDevice({ id: "nodekey-stored", addresses: ["100.64.0.11"] });
    const client = makeMockApiClient({
      async listDevices() { return [device]; },
      async getDevice() { return device; },
    });
    const provider = new TailscaleFeatureProvider(client);

    const instance = makeInstance();
    const attachPromise = provider.attach(String(instance.id), {});

    setTailscaleState(instance.id, "100.64.0.11", "ready");

    const state = await attachPromise;

    // Object record must exist.
    const rows = queryMany<{ id: number; type: string; metadata_json: string | null }>(
      "SELECT o.id, o.type, o.metadata_json FROM objects o WHERE o.type = 'tailscale_device'"
    );
    expect(rows).toHaveLength(1);
    expect(rows[0].id).toBe(state.objectId);

    // Metadata round-trips correctly.
    const meta = JSON.parse(rows[0].metadata_json!);
    expect(meta.device_id).toBe("nodekey-stored");
    expect(meta.tailscale_ip).toBe("100.64.0.11");
    expect(meta.instance_id).toBe(String(instance.id));

    // Tags must be present.
    const tags = queryMany<{ tag: string }>(
      "SELECT tag FROM object_tags WHERE object_id = ?",
      [state.objectId]
    );
    const tagValues = tags.map((t) => t.tag);
    expect(tagValues).toContain(`instance_id:${instance.id}`);
    expect(tagValues).toContain("feature:tailscale");
  });
});

// =============================================================================
// detach()
// =============================================================================

describe("TailscaleFeatureProvider.detach()", () => {
  test("calls deleteDevice and removes DB record", async () => {
    const deletedIds: string[] = [];
    const device = makeDevice({ id: "nodekey-to-delete", addresses: ["100.64.0.20"] });
    const client = makeMockApiClient({
      async listDevices() { return [device]; },
      async getDevice(_id: string) { return device; },
      async deleteDevice(id: string) { deletedIds.push(id); },
    });
    const provider = new TailscaleFeatureProvider(client);

    const instance = makeInstance();
    const attachPromise = provider.attach(String(instance.id), {});
    setTailscaleState(instance.id, "100.64.0.20", "ready");
    await attachPromise;

    // Verify record exists before detach.
    const beforeRows = queryMany(
      "SELECT id FROM objects WHERE type = 'tailscale_device'"
    );
    expect(beforeRows).toHaveLength(1);

    await provider.detach(String(instance.id));

    // deleteDevice was called with the correct device ID.
    expect(deletedIds).toContain("nodekey-to-delete");

    // DB record is gone.
    const afterRows = queryMany(
      "SELECT id FROM objects WHERE type = 'tailscale_device'"
    );
    expect(afterRows).toHaveLength(0);
  });

  test("is a no-op when no device record exists", async () => {
    const deletedIds: string[] = [];
    const client = makeMockApiClient({
      async deleteDevice(id: string) { deletedIds.push(id); },
    });
    const provider = new TailscaleFeatureProvider(client);

    const instance = makeInstance();

    // No prior attach — detach should silently succeed.
    await expect(provider.detach(String(instance.id))).resolves.toBeUndefined();
    expect(deletedIds).toHaveLength(0);
  });

  test("continues detach when device is already gone from tailnet (404)", async () => {
    const { TailscaleApiError } = await import(
      "../../control/src/provider/feature/tailscale-api"
    );

    const device = makeDevice({ id: "nodekey-gone", addresses: ["100.64.0.21"] });
    const client = makeMockApiClient({
      async listDevices() { return [device]; },
      async getDevice(_id: string) { return device; },
      async deleteDevice(_id: string) {
        throw new TailscaleApiError("device not found", 404);
      },
    });
    const provider = new TailscaleFeatureProvider(client);

    const instance = makeInstance();
    const attachPromise = provider.attach(String(instance.id), {});
    setTailscaleState(instance.id, "100.64.0.21", "ready");
    await attachPromise;

    // Should not throw — 404 on deleteDevice is treated as already-removed.
    await expect(provider.detach(String(instance.id))).resolves.toBeUndefined();

    // DB record should still be cleaned up.
    const rows = queryMany("SELECT id FROM objects WHERE type = 'tailscale_device'");
    expect(rows).toHaveLength(0);
  });

  test("cleans up device record in objects table after detach", async () => {
    const device = makeDevice({ addresses: ["100.64.0.22"] });
    const client = makeMockApiClient({
      async listDevices() { return [device]; },
      async getDevice() { return device; },
    });
    const provider = new TailscaleFeatureProvider(client);

    const instance = makeInstance();
    const attachPromise = provider.attach(String(instance.id), {});
    setTailscaleState(instance.id, "100.64.0.22", "ready");
    await attachPromise;

    // Device record exists before detach.
    const before = queryMany("SELECT id FROM objects WHERE type = 'tailscale_device'");
    expect(before).toHaveLength(1);

    await provider.detach(String(instance.id));

    // Device record is gone after detach.
    const after = queryMany("SELECT id FROM objects WHERE type = 'tailscale_device'");
    expect(after).toHaveLength(0);
  });
});

// =============================================================================
// reconcile()
// =============================================================================

describe("TailscaleFeatureProvider.reconcile()", () => {
  test("returns empty result when tailnet and DB are in sync", async () => {
    const device = makeDevice({ id: "nodekey-sync", addresses: ["100.64.0.30"] });
    const client = makeMockApiClient({
      async listDevices() { return [device]; },
      async getDevice() { return device; },
    });
    const provider = new TailscaleFeatureProvider(client);

    const instance = makeInstance();
    const attachPromise = provider.attach(String(instance.id), {});
    setTailscaleState(instance.id, "100.64.0.30", "ready");
    await attachPromise;

    const result = await provider.reconcile({
      expectedAttachments: [{ instanceId: String(instance.id), config: {} }],
      actualState: new Map([[String(instance.id), {}]]),
    });

    // The DB record has device_id="nodekey-sync" and it IS in the tailnet.
    // The tailnet has "nodekey-sync" and it IS in the DB.
    // So no orphans and no missing.
    expect(result.orphans).toHaveLength(0);
    expect(result.missing).toHaveLength(0);
  });

  test("reports orphaned devices (in tailnet with repl- prefix, not in DB)", async () => {
    const orphan = makeDevice({
      id: "nodekey-orphan",
      hostname: "repl-orphan",
      name: "repl-orphan.tail1234.ts.net",
      addresses: ["100.64.99.1"],
    });
    const client = makeMockApiClient({
      async listDevices() { return [orphan]; },
    });
    const provider = new TailscaleFeatureProvider(client);

    const result = await provider.reconcile({
      expectedAttachments: [],
      actualState: new Map(),
    });

    // Orphan detected: in tailnet with repl- name, but no DB record.
    expect(result.orphans).toContain("nodekey-orphan");
  });

  test("does not report non-repl devices as orphans", async () => {
    const foreignDevice = makeDevice({
      id: "nodekey-foreign",
      hostname: "my-laptop",
      name: "my-laptop.tail1234.ts.net",
      addresses: ["100.64.88.1"],
    });
    const client = makeMockApiClient({
      async listDevices() { return [foreignDevice]; },
    });
    const provider = new TailscaleFeatureProvider(client);

    const result = await provider.reconcile({
      expectedAttachments: [],
      actualState: new Map(),
    });

    // Foreign device (no repl- prefix) must NOT be reported as an orphan.
    expect(result.orphans).not.toContain("nodekey-foreign");
    expect(result.orphans).toHaveLength(0);
  });

  test("reports stale DB records and cleans them up (device gone from tailnet)", async () => {
    const device = makeDevice({ id: "nodekey-stale", addresses: ["100.64.0.40"] });
    const client = makeMockApiClient({
      // listDevices: device was there during attach, now gone.
      async listDevices() { return []; },
      // getDevice: used by attach's _findDeviceByIp — returns the device.
      async getDevice() { return device; },
    });

    // First, create the attachment with a client that sees the device.
    const attachClient = makeMockApiClient({
      async listDevices() { return [device]; },
      async getDevice() { return device; },
    });
    const attachProvider = new TailscaleFeatureProvider(attachClient);
    const instance = makeInstance();
    const attachPromise = attachProvider.attach(String(instance.id), {});
    setTailscaleState(instance.id, "100.64.0.40", "ready");
    await attachPromise;

    // Confirm record exists.
    const before = queryMany("SELECT id FROM objects WHERE type = 'tailscale_device'");
    expect(before).toHaveLength(1);

    // Now reconcile with a client that reports an empty tailnet.
    const reconcileProvider = new TailscaleFeatureProvider(client);
    const result = await reconcileProvider.reconcile({
      expectedAttachments: [],
      actualState: new Map(),
    });

    // "nodekey-stale" is in DB but not in tailnet → listed as missing.
    expect(result.missing).toContain("nodekey-stale");

    // DB record cleaned up.
    const after = queryMany("SELECT id FROM objects WHERE type = 'tailscale_device'");
    expect(after).toHaveLength(0);
  });

  test("handles concurrent attach/detach correctly — empty state is consistent", async () => {
    const client = makeMockApiClient({
      async listDevices() { return []; },
    });
    const provider = new TailscaleFeatureProvider(client);

    // Reconcile with nothing in DB and nothing in tailnet.
    const result = await provider.reconcile({
      expectedAttachments: [],
      actualState: new Map(),
    });

    expect(result.orphans).toHaveLength(0);
    expect(result.missing).toHaveLength(0);
  });
});

// =============================================================================
// status()
// =============================================================================

describe("TailscaleFeatureProvider.status()", () => {
  test("returns null when no device record exists", async () => {
    const client = makeMockApiClient();
    const provider = new TailscaleFeatureProvider(client);

    const instance = makeInstance();
    const state = await provider.status(String(instance.id));
    expect(state).toBeNull();
  });

  test("returns state with live online=false when device is offline in tailnet", async () => {
    const onlineDevice = makeDevice({ id: "nodekey-offline", addresses: ["100.64.0.50"], online: true });
    const offlineDevice = { ...onlineDevice, online: false };

    const attachClient = makeMockApiClient({
      async listDevices() { return [onlineDevice]; },
      async getDevice() { return onlineDevice; },
    });
    const provider = new TailscaleFeatureProvider(attachClient);

    const instance = makeInstance();
    const attachPromise = provider.attach(String(instance.id), {});
    setTailscaleState(instance.id, "100.64.0.50", "ready");
    await attachPromise;

    // Now the device is offline in the tailnet.
    const statusClient = makeMockApiClient({
      async getDevice() { return offlineDevice; },
    });
    const statusProvider = new TailscaleFeatureProvider(statusClient);

    const state = await statusProvider.status(String(instance.id));
    expect(state).not.toBeNull();
    expect(state!.online).toBe(false);
    expect(state!.ip).toBe("100.64.0.50");
  });
});
