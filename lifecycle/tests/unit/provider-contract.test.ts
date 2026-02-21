// tests/unit/provider-contract.test.ts - Parameterized Provider Contract Tests
//
// Defines a behavioral conformance suite that runs against any Provider
// implementation. If these tests pass for one provider, success generalizes
// to ~80% for all other providers.
//
// Two invocations:
//   1. MockProvider — fully in-memory, no I/O
//   2. OrbStackProvider with mock exec — hermetic, no orbctl dependency

import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import type {
  Provider,
  SpawnOptions,
  BootstrapConfig,
} from "../../control/src/provider/types";
import { MockProvider } from "../integration/helpers/mock-provider";
import {
  OrbStackProvider,
  type ExecFunction,
} from "../../control/src/provider/compute/orbstack";

// =============================================================================
// Test Helpers
// =============================================================================

/** Create a minimal SpawnOptions for contract testing. */
function testSpawnOptions(overrides?: Partial<SpawnOptions>): SpawnOptions {
  return {
    spec: "ubuntu:noble:arm64",
    controlId: "test01",
    manifestId: 1,
    instanceId: 1,
    bootstrap: {
      controlPlaneUrl: "http://localhost:3000",
      registrationToken: "test-token",
      agentUrl: "http://localhost:3000/v1/agent",
      environment: { SKYREPL_INSTANCE_ID: "1" },
    },
    ...overrides,
  };
}

// =============================================================================
// Parameterized Contract Suite
// =============================================================================

/**
 * Run the provider behavioral contract suite against any Provider implementation.
 *
 * Pass a factory (called per-test via beforeEach) and an optional cleanup
 * callback. The suite exercises spawn, get, terminate, list, generateBootstrap,
 * and capabilities — the minimal surface all providers must implement.
 */
function runProviderContract(
  name: string,
  factory: () => Provider,
  cleanup?: (provider: Provider) => void
) {
  describe(`Provider Contract: ${name}`, () => {
    let provider: Provider;

    beforeEach(() => {
      provider = factory();
    });

    afterEach(() => {
      if (cleanup) cleanup(provider);
    });

    // -------------------------------------------------------------------------
    // spawn
    // -------------------------------------------------------------------------

    test("spawn returns a ProviderInstance with required fields", async () => {
      const result = await provider.spawn(testSpawnOptions());
      expect(result.id).toBeDefined();
      expect(typeof result.id).toBe("string");
      expect(result.id.length).toBeGreaterThan(0);
      expect(result.status).toBeDefined();
      expect(typeof result.spec).toBe("string");
      expect(typeof result.createdAt).toBe("number");
      expect(typeof result.isSpot).toBe("boolean");
    });

    // -------------------------------------------------------------------------
    // get
    // -------------------------------------------------------------------------

    test("get on spawned instance returns it", async () => {
      const spawned = await provider.spawn(testSpawnOptions());
      const fetched = await provider.get(spawned.id);
      expect(fetched).not.toBeNull();
      expect(fetched!.id).toBe(spawned.id);
    });

    test("get on nonexistent id returns null (not throws)", async () => {
      const result = await provider.get("nonexistent-id-12345");
      expect(result).toBeNull();
    });

    // -------------------------------------------------------------------------
    // terminate
    // -------------------------------------------------------------------------

    test("terminate on nonexistent id doesn't throw (idempotency)", async () => {
      // Should not throw — providers must be idempotent on termination
      await provider.terminate("nonexistent-id-12345");
    });

    test("terminate then get returns null or a terminal status", async () => {
      const spawned = await provider.spawn(testSpawnOptions());
      await provider.terminate(spawned.id);
      const fetched = await provider.get(spawned.id);
      // Either the instance is gone (null) or it has a terminal status
      if (fetched !== null) {
        expect(["terminated", "stopped", "stopping", "shutting-down"]).toContain(
          fetched.status
        );
      }
    });

    // -------------------------------------------------------------------------
    // list
    // -------------------------------------------------------------------------

    test("list after spawn includes the spawned instance", async () => {
      const spawned = await provider.spawn(testSpawnOptions());
      const all = await provider.list();
      const found = all.find((i) => i.id === spawned.id);
      expect(found).toBeDefined();
    });

    // -------------------------------------------------------------------------
    // generateBootstrap
    // -------------------------------------------------------------------------

    test("generateBootstrap returns non-empty content, format, and checksum", () => {
      const config: BootstrapConfig = {
        controlPlaneUrl: "http://localhost:3000",
        registrationToken: "test-token",
        agentUrl: "http://localhost:3000/v1/agent",
        environment: { SKYREPL_INSTANCE_ID: "1" },
      };
      const script = provider.generateBootstrap(config);
      expect(script.content).toBeDefined();
      expect(script.content.length).toBeGreaterThan(0);
      expect(script.format).toBeDefined();
      expect(["cloud-init", "shell", "ignition"]).toContain(script.format);
      expect(script.checksum).toBeDefined();
      expect(script.checksum.length).toBeGreaterThan(0);
    });

    // -------------------------------------------------------------------------
    // capabilities
    // -------------------------------------------------------------------------

    test("capabilities object has all required flags as booleans", () => {
      const caps = provider.capabilities;
      const requiredFlags = [
        "snapshots",
        "spot",
        "gpu",
        "multiRegion",
        "persistentVolumes",
        "warmVolumes",
        "hibernation",
        "costExplorer",
        "tailscaleNative",
        "idempotentSpawn",
        "customNetworking",
      ];
      for (const flag of requiredFlags) {
        expect(typeof (caps as unknown as Record<string, unknown>)[flag]).toBe("boolean");
      }
    });
  });
}

// =============================================================================
// Mock Exec for OrbStackProvider
// =============================================================================

/**
 * Creates a stateful mock exec function that simulates orbctl responses.
 * Tracks created VMs in a Map so that list/info/delete work correctly.
 */
function createMockExec(): { exec: ExecFunction; cleanup: () => void } {
  const vms = new Map<string, { state: string; spec: string }>();

  const exec: ExecFunction = async (command, _options) => {
    const cmd = command[0];
    const args = command.slice(1);

    if (cmd === "orbctl") {
      const subcmd = args[0];

      if (subcmd === "create") {
        // orbctl create ubuntu:noble name -a arm64
        // args: ["create", "ubuntu:noble", "<name>", "-a", "arm64"]
        const spec = args[1] ?? "ubuntu:noble";
        const vmName = args[2] ?? "unknown";
        vms.set(vmName, { state: "running", spec });
        return { stdout: "", stderr: "", exitCode: 0 };
      }

      if (subcmd === "info") {
        // orbctl info <name> --format json
        const vmName = args[1] ?? "";
        const vm = vms.get(vmName);
        if (!vm) {
          return { stdout: "", stderr: "not found", exitCode: 1 };
        }
        const specParts = vm.spec.split(":");
        const distro = specParts[0] ?? "ubuntu";
        const version = specParts[1] ?? "noble";
        const output: {
          record: {
            id: string;
            name: string;
            image: { distro: string; version: string; arch: string };
            state: string;
          };
          ip4: string;
        } = {
          record: {
            id: vmName,
            name: vmName,
            image: { distro, version, arch: "arm64" },
            state: vm.state,
          },
          ip4: "192.168.1.100",
        };
        return { stdout: JSON.stringify(output), stderr: "", exitCode: 0 };
      }

      if (subcmd === "list") {
        // orbctl list --format json
        const items = Array.from(vms.entries()).map(([vmName, vm]) => {
          const specParts = vm.spec.split(":");
          return {
            id: vmName,
            name: vmName,
            image: {
              distro: specParts[0] ?? "ubuntu",
              version: specParts[1] ?? "noble",
              arch: "arm64",
            },
            state: vm.state,
          };
        });
        return { stdout: JSON.stringify(items), stderr: "", exitCode: 0 };
      }

      if (subcmd === "delete") {
        // orbctl delete -f <name>
        // The name is the argument that doesn't start with "-"
        const vmName = args.find((a) => !a.startsWith("-")) ?? args[args.length - 1] ?? "";
        vms.delete(vmName);
        return { stdout: "", stderr: "", exitCode: 0 };
      }

      if (subcmd === "stop") {
        // orbctl stop <name>
        const vmName = args[1] ?? "";
        const vm = vms.get(vmName);
        if (vm) vm.state = "stopped";
        return { stdout: "", stderr: "", exitCode: 0 };
      }

      if (subcmd === "run") {
        // orbctl run ... (bootstrap write/start, IP discovery via hostname -I)
        return { stdout: "192.168.1.100", stderr: "", exitCode: 0 };
      }

      if (subcmd === "version") {
        return { stdout: "OrbStack 1.0.0-mock", stderr: "", exitCode: 0 };
      }
    }

    if (cmd === "getent") {
      // getent hosts <name>.orb.local
      return { stdout: "192.168.1.100 test.orb.local", stderr: "", exitCode: 0 };
    }

    return { stdout: "", stderr: "unknown command", exitCode: 1 };
  };

  return { exec, cleanup: () => vms.clear() };
}

// =============================================================================
// Invocations
// =============================================================================

// 1. MockProvider — fully in-process, no network
runProviderContract(
  "MockProvider",
  () => new MockProvider(),
  (provider) => (provider as unknown as MockProvider).cleanup()
);

// 2. OrbStackProvider with mock exec — hermetic, no orbctl dependency
{
  // Create one shared mock exec instance; cleanup resets VM state between tests
  const orbMock = createMockExec();

  runProviderContract(
    "OrbStackProvider (mock exec)",
    () => new OrbStackProvider({ _execFactory: orbMock.exec }),
    () => orbMock.cleanup()
  );
}
