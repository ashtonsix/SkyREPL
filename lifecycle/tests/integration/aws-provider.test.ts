// tests/integration/aws-provider.test.ts - AWS Provider Integration Tests
// Requires real AWS credentials. Skipped by default.
// Run: TEST_REAL_AWS=1 bun test tests/integration/aws-provider.test.ts
//
// Reads credentials from ~/.repl/aws-dev-credentials.env
// Creates real resources in us-east-1 (t4g.nano, ~$0.004/hr)

import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { readFileSync, existsSync } from "fs";
import { join } from "path";
import { homedir } from "os";
import { AWSProvider, type AWSInstance } from "../../control/src/provider/compute/aws";

// ─── Credential loading ──────────────────────────────────────────────────────

function loadCredentials(): { accessKeyId: string; secretAccessKey: string } | null {
  const credPath = join(homedir(), ".repl", "aws-dev-credentials.env");
  if (!existsSync(credPath)) return null;
  const lines = readFileSync(credPath, "utf8").split("\n");
  const creds: Record<string, string> = {};
  for (const line of lines) {
    const match = line.match(/^(\w+)=(.+)$/);
    if (match) creds[match[1]] = match[2];
  }
  if (!creds.AWS_ACCESS_KEY_ID || !creds.AWS_SECRET_ACCESS_KEY) return null;
  return { accessKeyId: creds.AWS_ACCESS_KEY_ID, secretAccessKey: creds.AWS_SECRET_ACCESS_KEY };
}

const TEST_REAL_AWS = process.env.TEST_REAL_AWS === "1";

describe.skipIf(!TEST_REAL_AWS)("AWS Provider - Integration Tests", () => {
  let provider: AWSProvider;
  let spawnedInstanceId: string | null = null;

  beforeAll(() => {
    const credentials = loadCredentials();
    if (!credentials) throw new Error("No credentials at ~/.repl/aws-dev-credentials.env");
    provider = new AWSProvider({
      region: "us-east-1",
      credentials,
    });
  });

  afterAll(async () => {
    // Safety net: terminate any lingering instance
    if (spawnedInstanceId) {
      try { await provider.terminate(spawnedInstanceId); } catch {}
    }
  });

  test("startup verifies credentials and ensures SG + key pair", async () => {
    const result = await provider.startup();
    expect(result.accountId).toBeTruthy();
    expect(result.region).toBe("us-east-1");
    expect(result.securityGroupId).toMatch(/^sg-/);
    expect(result.keyName).toBe("skyrepl-us-east-1");
  });

  test("idempotent startup reuses existing infrastructure", async () => {
    const result1 = await provider.startup();
    const result2 = await provider.startup();
    expect(result1.securityGroupId).toBe(result2.securityGroupId);
    expect(result1.keyName).toBe(result2.keyName);
  });

  test("spawn → running → terminate lifecycle", async () => {
    const instance = await provider.spawn({
      spec: "ubuntu:noble:arm64",
      region: "us-east-1",
      bootstrap: {
        agentUrl: "http://localhost:3000/v1/agent/download",
        controlPlaneUrl: "http://localhost:3000",
        registrationToken: "integration-test-token",
      },
      tags: {
        instance_type: "t4g.nano",
        "skyrepl:control_id": "inttest",
      },
      instanceId: 1,
      manifestId: 0,
      controlId: "inttest",
    });
    spawnedInstanceId = instance.id;

    expect(instance.id).toMatch(/^i-/);
    expect(instance.instanceType).toBe("t4g.nano");
    expect(instance.status).toBeOneOf(["pending", "running"]);

    // Wait for running (timeout 120s)
    let current: AWSInstance | null = instance;
    const deadline = Date.now() + 120_000;
    while (current && current.status !== "running" && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 10_000));
      current = await provider.get(instance.id);
    }
    expect(current?.status).toBe("running");
    expect(current?.ip).toBeTruthy();

    // list() should find our instance
    const listed = await provider.list({ region: "us-east-1" });
    expect(listed.find((i) => i.id === instance.id)).toBeDefined();

    // Terminate
    await provider.terminate(instance.id);
    spawnedInstanceId = null;

    // Verify termination started
    await new Promise((r) => setTimeout(r, 3000));
    const after = await provider.get(instance.id);
    expect(after?.status).toBeOneOf(["terminating", "terminated", undefined]);
  }, 180_000); // 3 minute timeout

  test("getSpotPrices returns pricing data", async () => {
    const prices = await provider.getSpotPrices("ubuntu:noble:arm64", ["us-east-1"]);
    expect(prices.length).toBeGreaterThan(0);
    for (const price of prices) {
      expect(price.price).toBeGreaterThan(0);
      expect(price.region).toBe("us-east-1");
      expect(price.availabilityZone).toMatch(/^us-east-1[a-f]$/);
    }
  });
});
