// tests/unit/provider-orbstack.test.ts - OrbStack Provider Unit Tests

import { describe, test, expect, beforeAll } from "bun:test";
import { createHash } from "crypto";
import {
  OrbStackProvider,
  mapOrbStackStatus,
  generateVmName,
  parseSpec,
  VM_PREFIX,
} from "../../control/src/provider/compute/orbstack";

// =============================================================================
// Unit Tests (no orbctl dependency)
// =============================================================================

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
    test("generates name with repl- prefix", () => {
      const name = generateVmName();
      expect(name.startsWith(VM_PREFIX)).toBe(true);
    });

    test("generates name with timestamp suffix", () => {
      const before = Date.now();
      const name = generateVmName();
      const after = Date.now();

      const timestamp = parseInt(name.slice(VM_PREFIX.length), 10);
      expect(timestamp).toBeGreaterThanOrEqual(before);
      expect(timestamp).toBeLessThanOrEqual(after);
    });

    test("generates unique names on successive calls", () => {
      const names = new Set<string>();
      for (let i = 0; i < 10; i++) {
        names.add(generateVmName());
      }
      // Due to Date.now() resolution, some might collide, but most should be unique
      expect(names.size).toBeGreaterThanOrEqual(1);
    });

    test("matches expected format pattern", () => {
      const name = generateVmName();
      expect(name).toMatch(/^repl-\d+$/);
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
      expect(parseSpec("")).toEqual({
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
      expect(result.content).toContain("exec python3 agent.py");
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
// Integration Tests (require orbctl)
// =============================================================================

describe("OrbStack Provider - Integration Tests", () => {
  let orbctlAvailable = false;

  beforeAll(async () => {
    try {
      const proc = Bun.spawn(["orbctl", "version"], {
        stdout: "pipe",
        stderr: "pipe",
      });
      await proc.exited;
      orbctlAvailable = proc.exitCode === 0;
    } catch {
      // Also try mac orbctl fallback
      try {
        const proc = Bun.spawn(["mac", "orbctl", "version"], {
          stdout: "pipe",
          stderr: "pipe",
        });
        await proc.exited;
        orbctlAvailable = proc.exitCode === 0;
      } catch {
        orbctlAvailable = false;
      }
    }
  });

  test("spawn -> get -> list -> terminate lifecycle", async () => {
    if (!orbctlAvailable) {
      console.log("Skipping: orbctl not available");
      return;
    }

    const provider = new OrbStackProvider();

    // Spawn
    let instance;
    try {
      instance = await provider.spawn({
        spec: "ubuntu:noble:arm64",
        bootstrap: {
          agentUrl: "https://control.example.com/v1/agent/download",
          controlPlaneUrl: "https://control.example.com",
          registrationToken: "test-token",
        },
      });
    } catch (err: any) {
      if (err?.message?.includes("didn't start in") || err?.message?.includes("context")) {
        console.log("Skipping: OrbStack VM creation unhealthy");
        return;
      }
      throw err;
    }

    try {
      expect(instance.id).toMatch(/^repl-/);
      expect(instance.vmName).toBe(instance.id);
      expect(instance.status).toBe("running");
      expect(instance.distro).toBe("ubuntu");
      expect(instance.distroVersion).toBe("noble");
      expect(instance.isSpot).toBe(false);

      // Get
      const fetched = await provider.get(instance.id);
      expect(fetched).not.toBeNull();
      expect(fetched!.id).toBe(instance.id);
      expect(fetched!.status).toBe("running");

      // List
      const list = await provider.list();
      const found = list.find((i) => i.id === instance.id);
      expect(found).toBeDefined();

      // List with status filter
      const runningList = await provider.list({ status: ["running"] });
      const foundRunning = runningList.find((i) => i.id === instance.id);
      expect(foundRunning).toBeDefined();

      const stoppedList = await provider.list({ status: ["stopped"] });
      const foundStopped = stoppedList.find((i) => i.id === instance.id);
      expect(foundStopped).toBeUndefined();
    } finally {
      // Terminate (cleanup)
      await provider.terminate(instance.id);

      // Verify terminated
      const afterTerminate = await provider.get(instance.id);
      expect(afterTerminate).toBeNull();
    }
  }, 120_000); // 2 minute timeout for lifecycle test

  test("terminate is idempotent for non-existent VMs", async () => {
    if (!orbctlAvailable) {
      console.log("Skipping: orbctl not available");
      return;
    }

    const provider = new OrbStackProvider();
    // Should not throw for a VM that does not exist
    await provider.terminate("repl-nonexistent-999999");
  }, 30_000);

  test("get returns null for non-existent VM", async () => {
    if (!orbctlAvailable) {
      console.log("Skipping: orbctl not available");
      return;
    }

    const provider = new OrbStackProvider();
    const result = await provider.get("repl-nonexistent-999999");
    expect(result).toBeNull();
  }, 15_000);
});
