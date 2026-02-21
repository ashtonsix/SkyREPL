// tests/integration/orbstack-provider.test.ts - OrbStack Provider Integration Tests
// Requires live OrbStack VMs. Skipped by default.
// Run: ORBSTACK_TESTS=1 bun test tests/integration/orbstack-provider.test.ts

import { describe, test, expect } from "bun:test";
import { OrbStackProvider } from "../../control/src/provider/compute/orbstack";

describe.skipIf(!process.env.ORBSTACK_TESTS)("OrbStack Provider - Integration Tests", () => {
  test("spawn -> get -> list -> terminate lifecycle", async () => {
    const provider = new OrbStackProvider();

    const instance = await provider.spawn({
      spec: "ubuntu:noble:arm64",
      controlId: "test",
      instanceId: 1,
      manifestId: null,
      bootstrap: {
        agentUrl: "https://control.example.com/v1/agent/download",
        controlPlaneUrl: "https://control.example.com",
        registrationToken: "test-token",
      },
    });

    try {
      expect(instance.id).toMatch(/^repl-/);
      expect(instance.vmName).toBe(instance.id);
      expect(instance.status).toBe("running");
      expect(instance.distro).toBe("ubuntu");
      expect(instance.distroVersion).toBe("noble");
      expect(instance.isSpot).toBe(false);

      const fetched = await provider.get(instance.id);
      expect(fetched).not.toBeNull();
      expect(fetched!.id).toBe(instance.id);
      expect(fetched!.status).toBe("running");

      const list = await provider.list();
      expect(list.find((i) => i.id === instance.id)).toBeDefined();

      const runningList = await provider.list({ status: ["running"] });
      expect(runningList.find((i) => i.id === instance.id)).toBeDefined();

      const stoppedList = await provider.list({ status: ["stopped"] });
      expect(stoppedList.find((i) => i.id === instance.id)).toBeUndefined();
    } finally {
      await provider.terminate(instance.id);
      const afterTerminate = await provider.get(instance.id);
      expect(afterTerminate).toBeNull();
    }
  }, 120_000);

  test("terminate is idempotent for non-existent VMs", async () => {
    const provider = new OrbStackProvider();
    await provider.terminate("repl-nonexistent-999999");
  }, 30_000);

  test("get returns null for non-existent VM", async () => {
    const provider = new OrbStackProvider();
    const result = await provider.get("repl-nonexistent-999999");
    expect(result).toBeNull();
  }, 15_000);
});
