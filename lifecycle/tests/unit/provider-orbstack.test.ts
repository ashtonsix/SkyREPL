// tests/unit/provider-orbstack.test.ts - OrbStack Provider Unit Tests

import { describe, test, expect, beforeAll, mock, setSystemTime } from "bun:test";
import { createHash } from "crypto";
import {
  OrbStackProvider,
  mapOrbStackStatus,
  generateVmName,
  parseSpec,
  VM_PREFIX,
  isReplVM,
  extractTimestamp,
  createOrbStackHooks,
} from "../../control/src/provider/compute/orbstack";
import { formatResourceName, isReplResource } from "../../control/src/material/control-id";
import { ConcreteProviderError, ProviderOperationError } from "../../control/src/provider/errors";

// =============================================================================
// Unit Tests (no orbctl dependency)
// =============================================================================

const testBootstrap = {
  agentUrl: "http://localhost:3000/v1/agent/download",
  controlPlaneUrl: "http://localhost:3000",
  registrationToken: "test-token-abc",
};

describe("OrbStack Provider - Unit Tests", () => {
  const provider = new OrbStackProvider();

  describe("mapOrbStackStatus", () => {
    test("maps 'running' to 'running'", () => {
      expect(mapOrbStackStatus("running")).toBe("running");
    });

    test("maps 'stopped' to 'stopped'", () => {
      expect(mapOrbStackStatus("stopped")).toBe("stopped");
    });

    test("maps 'starting' to 'starting'", () => {
      expect(mapOrbStackStatus("starting")).toBe("starting");
    });

    test("maps 'error' to 'error'", () => {
      expect(mapOrbStackStatus("error")).toBe("error");
    });

    test("maps unknown state to 'pending'", () => {
      expect(mapOrbStackStatus("unknown")).toBe("pending");
      expect(mapOrbStackStatus("")).toBe("pending");
      expect(mapOrbStackStatus("creating")).toBe("pending");
    });
  });

  describe("generateVmName", () => {
    test("generates spec-compliant name from naming triple", () => {
      const name = generateVmName({
        spec: "ubuntu:noble:arm64",
        bootstrap: testBootstrap,
        controlId: "abc123",
        instanceId: 42,
        manifestId: 7,
      });
      expect(name).toBe("repl-abc123-7-16"); // 7 in base36 = "7", 42 in base36 = "16"
      expect(isReplResource(name)).toBe(true);
    });

    test("uses manifestId=0 when provided", () => {
      const name = generateVmName({
        spec: "ubuntu:noble:arm64",
        bootstrap: testBootstrap,
        controlId: "abc123",
        instanceId: 1,
        manifestId: 0,
      });
      expect(name).toBe("repl-abc123-0-1");
    });
  });

  describe("parseSpec", () => {
    test("parses full spec string", () => {
      const result = parseSpec("ubuntu:noble:arm64");
      expect(result).toEqual({
        distro: "ubuntu",
        version: "noble",
        arch: "arm64",
      });
    });

    test("parses spec with amd64 arch", () => {
      const result = parseSpec("debian:bookworm:amd64");
      expect(result).toEqual({
        distro: "debian",
        version: "bookworm",
        arch: "amd64",
      });
    });

    test("applies defaults for missing parts", () => {
      expect(parseSpec("ubuntu")).toEqual({
        distro: "ubuntu",
        version: "noble",
        arch: "arm64",
      });
    });

    test("applies defaults for partial spec (distro only)", () => {
      const result = parseSpec("debian");
      expect(result).toEqual({
        distro: "debian",
        version: "noble",
        arch: "arm64",
      });
    });

    test("applies defaults for partial spec (distro:version)", () => {
      const result = parseSpec("ubuntu:jammy");
      expect(result).toEqual({
        distro: "ubuntu",
        version: "jammy",
        arch: "arm64",
      });
    });
  });

  describe("generateBootstrap", () => {
    const config = {
      agentUrl: "https://control.example.com/v1/agent/download",
      controlPlaneUrl: "https://control.example.com",
      registrationToken: "test-token-123",
    };

    test("produces shell format script", () => {
      const result = provider.generateBootstrap(config);
      expect(result.format).toBe("shell");
    });

    test("script starts with shebang", () => {
      const result = provider.generateBootstrap(config);
      expect(result.content.startsWith("#!/bin/bash")).toBe(true);
    });

    test("script contains set -eu for strict mode", () => {
      const result = provider.generateBootstrap(config);
      expect(result.content).toContain("set -eu");
    });

    test("script redirects output to bootstrap.log", () => {
      const result = provider.generateBootstrap(config);
      expect(result.content).toContain("exec > /tmp/bootstrap.log 2>&1");
    });

    test("script disables apt-daily timers", () => {
      const result = provider.generateBootstrap(config);
      expect(result.content).toContain("apt-daily.timer");
      expect(result.content).toContain("apt-daily-upgrade.timer");
    });

    test("script writes agent config with control plane URL", () => {
      const result = provider.generateBootstrap(config);
      expect(result.content).toContain(config.controlPlaneUrl);
      expect(result.content).toContain("/etc/skyrepl/config.json");
    });

    test("script writes agent config with registration token", () => {
      const result = provider.generateBootstrap(config);
      expect(result.content).toContain(config.registrationToken);
    });

    test("script downloads agent files from control plane", () => {
      const result = provider.generateBootstrap(config);
      expect(result.content).toContain(
        `${config.controlPlaneUrl}/v1/agent/download`
      );
      expect(result.content).toContain("curl -fsSL");
    });

    test("script sets environment variables", () => {
      const result = provider.generateBootstrap(config);
      expect(result.content).toContain(
        `export SKYREPL_CONTROL_PLANE_URL="${config.controlPlaneUrl}"`
      );
      expect(result.content).toContain(
        `export SKYREPL_REGISTRATION_TOKEN="${config.registrationToken}"`
      );
    });

    test("script starts agent with exec", () => {
      const result = provider.generateBootstrap(config);
      expect(result.content).toContain("exec python3 -u agent.py");
    });

    test("checksum is valid SHA-256 hex string", () => {
      const result = provider.generateBootstrap(config);
      expect(result.checksum).toMatch(/^[0-9a-f]{64}$/);
    });

    test("checksum matches script content", () => {
      const result = provider.generateBootstrap(config);
      const expected = createHash("sha256")
        .update(result.content)
        .digest("hex");
      expect(result.checksum).toBe(expected);
    });

    test("deterministic output for same config", () => {
      const result1 = provider.generateBootstrap(config);
      const result2 = provider.generateBootstrap(config);
      expect(result1.content).toBe(result2.content);
      expect(result1.checksum).toBe(result2.checksum);
    });

    test("includes initScript when provided", () => {
      const configWithInit = {
        ...config,
        initScript: "apt-get install -y vim",
      };
      const result = provider.generateBootstrap(configWithInit);
      expect(result.content).toContain("apt-get install -y vim");
      expect(result.content).toContain("# User init script");
    });

    test("includes custom environment variables", () => {
      const configWithEnv = {
        ...config,
        environment: {
          MY_CUSTOM_VAR: "custom-value",
          ANOTHER_VAR: "another-value",
        },
      };
      const result = provider.generateBootstrap(configWithEnv);
      expect(result.content).toContain(
        'export MY_CUSTOM_VAR="custom-value"'
      );
      expect(result.content).toContain(
        'export ANOTHER_VAR="another-value"'
      );
    });
  });

  describe("isReplVM", () => {
    test("matches new naming format repl-{cid}-{mid}-{rid}", () => {
      expect(isReplVM("repl-abc123-7-16")).toBe(true);
    });

    test("matches legacy timestamp format", () => {
      expect(isReplVM("repl-1708000000000")).toBe(true);
    });

    test("matches legacy id-timestamp format", () => {
      expect(isReplVM("repl-i42-1708000000000")).toBe(true);
    });

    test("rejects non-repl VMs", () => {
      expect(isReplVM("ubuntu-noble")).toBe(false);
      expect(isReplVM("my-vm")).toBe(false);
      expect(isReplVM("")).toBe(false);
    });
  });

  describe("extractTimestamp", () => {
    test("returns a number for new format names (no embedded timestamp)", () => {
      const before = Date.now();
      const result = extractTimestamp("repl-abc123-7-16");
      const after = Date.now();
      expect(typeof result).toBe("number");
      expect(result).toBeGreaterThanOrEqual(before);
      expect(result).toBeLessThanOrEqual(after);
    });

    test("extracts embedded timestamp from legacy format repl-{timestamp}", () => {
      expect(extractTimestamp("repl-1708000000000")).toBe(1708000000000);
    });

    test("extracts embedded timestamp from legacy id-timestamp format repl-i{id}-{timestamp}", () => {
      expect(extractTimestamp("repl-i42-1708000000000")).toBe(1708000000000);
    });

    test("falls back to Date.now() for unrecognized names", () => {
      const before = Date.now();
      const result = extractTimestamp("ubuntu-noble");
      const after = Date.now();
      expect(result).toBeGreaterThanOrEqual(before);
      expect(result).toBeLessThanOrEqual(after);
    });

    test("falls back to Date.now() for empty string", () => {
      const before = Date.now();
      const result = extractTimestamp("");
      const after = Date.now();
      expect(result).toBeGreaterThanOrEqual(before);
      expect(result).toBeLessThanOrEqual(after);
    });
  });

  describe("provider properties", () => {
    test("provider name is orbstack", () => {
      expect(provider.name).toBe("orbstack");
    });

    test("capabilities reflect OrbStack features", () => {
      expect(provider.capabilities.snapshots).toBe(true);
      expect(provider.capabilities.spot).toBe(false);
      expect(provider.capabilities.gpu).toBe(false);
      expect(provider.capabilities.multiRegion).toBe(false);
      expect(provider.capabilities.tailscaleNative).toBe(true);
      expect(provider.capabilities.idempotentSpawn).toBe(true);
    });
  });
});

// =============================================================================
// Exec Injection Tests (hermetic, no orbctl dependency)
// =============================================================================

describe("OrbStack Provider - exec injection", () => {
  const spawnOptions = {
    spec: "ubuntu:noble:arm64",
    bootstrap: {
      agentUrl: "http://localhost:3000/v1/agent/download",
      controlPlaneUrl: "http://localhost:3000",
      registrationToken: "test-token",
    },
    controlId: "ctrl01",
    instanceId: 5,
    manifestId: 3,
  };

  // Expected VM name: repl-ctrl01-3-5
  const expectedVmName = "repl-ctrl01-3-5";

  test("spawn calls orbctl create with correct args", async () => {
    const calls: string[][] = [];

    // Mock exec: intercept all calls
    // create → success
    // info (polling) → running state
    // info (discoverIp method 1) → no ip4
    // getent (discoverIp method 2) → fail (non-zero exit)
    // orbctl run hostname -I (discoverIp method 3) → fail
    // orbctl run -u root (bootstrap write) → success
    // orbctl run -u root (bootstrap start) → success
    const mockExec = async (command: string[], _opts?: { timeout?: number }) => {
      calls.push(command);
      const joined = command.join(" ");

      if (joined.includes("orbctl create")) {
        return { stdout: "", stderr: "", exitCode: 0 };
      }
      if (joined.includes("orbctl info") && joined.includes("--format json")) {
        return {
          stdout: JSON.stringify({
            record: { id: expectedVmName, name: expectedVmName, state: "running" },
          }),
          stderr: "",
          exitCode: 0,
        };
      }
      if (joined.includes("getent hosts")) {
        return { stdout: "", stderr: "not found", exitCode: 1 };
      }
      if (joined.includes("hostname -I")) {
        return { stdout: "", stderr: "", exitCode: 1 };
      }
      if (joined.includes("orbctl run")) {
        return { stdout: "", stderr: "", exitCode: 0 };
      }
      return { stdout: "", stderr: "", exitCode: 0 };
    };

    const provider = new OrbStackProvider({ _execFactory: mockExec });
    const instance = await provider.spawn(spawnOptions);

    // Verify the create call used the correct orbctl args
    const createCall = calls.find(
      (c) => c.includes("orbctl") && c.includes("create")
    );
    expect(createCall).toBeDefined();
    expect(createCall).toContain("create");
    expect(createCall).toContain("ubuntu:noble");
    expect(createCall).toContain(expectedVmName);
    expect(createCall).toContain("-a");
    expect(createCall).toContain("arm64");

    // Verify the returned instance has the right id
    expect(instance.id).toBe(expectedVmName);
    expect(instance.vmName).toBe(expectedVmName);
  });

  test("terminate calls orbctl stop then delete with correct args", async () => {
    const calls: string[][] = [];

    const mockExec = async (command: string[], _opts?: { timeout?: number }) => {
      calls.push(command);
      return { stdout: "", stderr: "", exitCode: 0 };
    };

    const provider = new OrbStackProvider({ _execFactory: mockExec });
    await provider.terminate(expectedVmName);

    const stopCall = calls.find(
      (c) => c.includes("orbctl") && c.includes("stop")
    );
    expect(stopCall).toBeDefined();
    expect(stopCall).toContain(expectedVmName);

    const deleteCall = calls.find(
      (c) => c.includes("orbctl") && c.includes("delete")
    );
    expect(deleteCall).toBeDefined();
    expect(deleteCall).toContain("-f");
    expect(deleteCall).toContain(expectedVmName);

    // stop must be called before delete
    const stopIdx = calls.findIndex((c) => c.includes("orbctl") && c.includes("stop"));
    const deleteIdx = calls.findIndex((c) => c.includes("orbctl") && c.includes("delete"));
    expect(stopIdx).toBeLessThan(deleteIdx);
  });

  test("terminate treats 'not found' delete response as success", async () => {
    const mockExec = async (command: string[], _opts?: { timeout?: number }) => {
      if (command.includes("delete")) {
        return { stdout: "", stderr: "vm not found", exitCode: 1 };
      }
      return { stdout: "", stderr: "", exitCode: 0 };
    };

    const provider = new OrbStackProvider({ _execFactory: mockExec });
    // Should not throw even though delete returned non-zero with "not found"
    await expect(provider.terminate(expectedVmName)).resolves.toBeUndefined();
  });
});

// =============================================================================
// Lifecycle Hooks Tests (hermetic, no orbctl dependency)
// =============================================================================

describe("lifecycle hooks", () => {
  test("onStartup succeeds when orbctl available", async () => {
    const mockExec = async (_command: string[], _opts?: { timeout?: number }) => ({
      stdout: "OrbStack 1.0",
      stderr: "",
      exitCode: 0,
    });

    const hooks = createOrbStackHooks(mockExec);
    await expect(hooks.onStartup!()).resolves.toBeUndefined();
  });

  test("onStartup fails when orbctl not found", async () => {
    const mockExec = async (_command: string[], _opts?: { timeout?: number }) => {
      throw new Error("orbctl: not found");
    };

    const hooks = createOrbStackHooks(mockExec);
    await expect(hooks.onStartup!()).rejects.toThrow("orbctl not found");
  });

  test("onHeartbeat health_check reports repl- VM count", async () => {
    const vmList = [
      { name: "repl-ctrl01-1-1", state: "running" },
      { name: "repl-ctrl01-1-2", state: "running" },
      { name: "other-vm", state: "running" },
    ];

    const mockExec = async (_command: string[], _opts?: { timeout?: number }) => ({
      stdout: JSON.stringify(vmList),
      stderr: "",
      exitCode: 0,
    });

    const hooks = createOrbStackHooks(mockExec);
    const result = await hooks.onHeartbeat!({
      tasks: [{ type: "health_check", priority: "normal" }],
      deadline: Date.now() + 5000,
    });

    expect(result.receipts).toHaveLength(1);
    expect(result.receipts[0]!.type).toBe("health_check");
    expect(result.receipts[0]!.status).toBe("completed");
    expect((result.receipts[0]!.result as { replVmCount: number }).replVmCount).toBe(2);
  });

  test("onHeartbeat skips unknown task types", async () => {
    const mockExec = async (_command: string[], _opts?: { timeout?: number }) => ({
      stdout: "",
      stderr: "",
      exitCode: 0,
    });

    const hooks = createOrbStackHooks(mockExec);
    const result = await hooks.onHeartbeat!({
      tasks: [{ type: "cache_refresh", priority: "low" }],
      deadline: Date.now() + 5000,
    });

    expect(result.receipts).toHaveLength(1);
    expect(result.receipts[0]!.type).toBe("cache_refresh");
    expect(result.receipts[0]!.status).toBe("skipped");
  });
});

// Integration tests in tests/integration/orbstack-provider.test.ts
// Run: ORBSTACK_TESTS=1 bun test tests/integration/orbstack-provider.test.ts

// =============================================================================
// Spawn Failure Tests (C2)
// =============================================================================

describe("OrbStack Provider - spawn failure injection", () => {
  const spawnOpts = {
    spec: "ubuntu:noble:arm64",
    bootstrap: {
      agentUrl: "http://localhost:3000/v1/agent/download",
      controlPlaneUrl: "http://localhost:3000",
      registrationToken: "test-token",
    },
    controlId: "ctrl01",
    instanceId: 5,
    manifestId: 3,
  };

  test("spawn throws ConcreteProviderError with PROVIDER_INTERNAL on orbctl create failure", async () => {
    const mockExec = async (command: string[], _opts?: { timeout?: number }) => {
      const joined = command.join(" ");
      if (joined.includes("orbctl create")) {
        // Non-zero exit and non-"already exists" stderr
        return { stdout: "", stderr: "error: failed to provision VM", exitCode: 1 };
      }
      return { stdout: "", stderr: "", exitCode: 0 };
    };

    const provider = new OrbStackProvider({ _execFactory: mockExec });

    const err = await provider.spawn(spawnOpts).catch((e) => e);

    expect(err).toBeInstanceOf(ConcreteProviderError);
    expect(err.code).toBe("PROVIDER_INTERNAL");
  });

  test("spawn throws TIMEOUT_ERROR when VM never reaches running state", async () => {
    // Use setSystemTime to advance Date.now() past the 60_000ms poll timeout
    // so the timeout branch inside the poll loop is triggered immediately.
    const realNow = Date.now();

    // Mock exec: create succeeds, info always returns "starting"
    const mockExec = async (command: string[], _opts?: { timeout?: number }) => {
      const joined = command.join(" ");
      if (joined.includes("orbctl create")) {
        return { stdout: "", stderr: "", exitCode: 0 };
      }
      if (joined.includes("orbctl info") && joined.includes("--format json")) {
        // Advance time by 61 seconds on each info poll so the timeout triggers
        setSystemTime(new Date(Date.now() + 61_000));
        return {
          stdout: JSON.stringify({
            record: { id: "repl-ctrl01-3-5", name: "repl-ctrl01-3-5", state: "starting" },
          }),
          stderr: "",
          exitCode: 0,
        };
      }
      return { stdout: "", stderr: "", exitCode: 0 };
    };

    const provider = new OrbStackProvider({ _execFactory: mockExec });

    let err: any;
    try {
      await provider.spawn(spawnOpts);
    } catch (e) {
      err = e;
    } finally {
      // Restore real time regardless of outcome
      setSystemTime(new Date(realNow));
    }

    expect(err).toBeInstanceOf(ConcreteProviderError);
    expect(err.code).toBe("TIMEOUT_ERROR");
  });
});

// =============================================================================
// IP Discovery Fallback Chain Tests (C4)
// =============================================================================

describe("OrbStack Provider - IP discovery fallback chain", () => {
  const vmName = "repl-ctrl01-3-5";
  const spawnOpts = {
    spec: "ubuntu:noble:arm64",
    bootstrap: {
      agentUrl: "http://localhost:3000/v1/agent/download",
      controlPlaneUrl: "http://localhost:3000",
      registrationToken: "test-token",
    },
    controlId: "ctrl01",
    instanceId: 5,
    manifestId: 3,
  };

  test("IP discovery returns null when all three methods fail", async () => {
    // create: succeeds, info (polling): running but no ip4,
    // getent: fails, hostname -I: fails, bootstrap: succeeds
    const mockExec = async (command: string[], _opts?: { timeout?: number }) => {
      const joined = command.join(" ");

      if (joined.includes("orbctl create")) {
        return { stdout: "", stderr: "", exitCode: 0 };
      }
      // Poll info: running, no ip4
      if (joined.includes("orbctl info") && joined.includes("--format json")) {
        return {
          stdout: JSON.stringify({
            record: { id: vmName, name: vmName, state: "running" },
            // ip4 deliberately absent
          }),
          stderr: "",
          exitCode: 0,
        };
      }
      // Method 2: getent fails
      if (joined.includes("getent hosts")) {
        return { stdout: "", stderr: "not found", exitCode: 1 };
      }
      // Method 3: hostname -I fails
      if (joined.includes("hostname -I")) {
        return { stdout: "", stderr: "", exitCode: 1 };
      }
      // Bootstrap write and start: succeed
      if (joined.includes("orbctl run")) {
        return { stdout: "", stderr: "", exitCode: 0 };
      }
      return { stdout: "", stderr: "", exitCode: 0 };
    };

    const provider = new OrbStackProvider({ _execFactory: mockExec });
    const instance = await provider.spawn(spawnOpts);

    expect(instance.ip).toBeNull();
  });

  test("IP discovery uses getent fallback when orbctl info returns no ip4", async () => {
    // create: succeeds, poll info: running (no ip4),
    // getent (discoverIp method 2): returns an IP
    const expectedIp = "192.168.64.42";

    const mockExec = async (command: string[], _opts?: { timeout?: number }) => {
      const joined = command.join(" ");

      if (joined.includes("orbctl create")) {
        return { stdout: "", stderr: "", exitCode: 0 };
      }
      // All orbctl info calls: the poll call returns running (no ip4),
      // the discoverIp method-1 call also returns no ip4
      if (joined.includes("orbctl info") && joined.includes("--format json")) {
        return {
          stdout: JSON.stringify({
            record: { id: vmName, name: vmName, state: "running" },
            // ip4 deliberately absent — forces fallback to getent
          }),
          stderr: "",
          exitCode: 0,
        };
      }
      // Method 2: getent returns an IP
      if (joined.includes("getent hosts")) {
        return { stdout: `${expectedIp}  ${vmName}.orb.local`, stderr: "", exitCode: 0 };
      }
      // Bootstrap write and start: succeed
      if (joined.includes("orbctl run")) {
        return { stdout: "", stderr: "", exitCode: 0 };
      }
      return { stdout: "", stderr: "", exitCode: 0 };
    };

    const provider = new OrbStackProvider({ _execFactory: mockExec });
    const instance = await provider.spawn(spawnOpts);

    expect(instance.ip).toBe(expectedIp);
  });
});
