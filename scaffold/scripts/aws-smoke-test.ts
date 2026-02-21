#!/usr/bin/env bun
// aws-smoke-test.ts — First real AWS spawn/terminate cycle
// Usage: bun scaffold/scripts/aws-smoke-test.ts
//
// Reads credentials from ~/.repl/aws-dev-credentials.env
// Creates real infra in us-east-1: SG, key pair, t4g.nano instance
// Cleans up the instance on exit (SG + key pair persist as infrastructure)

import { readFileSync } from "fs";
import { join } from "path";
import { homedir } from "os";
import { AWSProvider } from "../../lifecycle/control/src/provider/compute/aws";

// ─── Load credentials ──────────────────────────────────────────────────────────

const credPath = join(homedir(), ".repl", "aws-dev-credentials.env");
const credLines = readFileSync(credPath, "utf8").split("\n");
const creds: Record<string, string> = {};
for (const line of credLines) {
  const match = line.match(/^(\w+)=(.+)$/);
  if (match) creds[match[1]] = match[2];
}

if (!creds.AWS_ACCESS_KEY_ID || !creds.AWS_SECRET_ACCESS_KEY) {
  console.error("Missing credentials in", credPath);
  process.exit(1);
}

console.log("Credentials loaded from", credPath);
console.log("  Access Key ID:", creds.AWS_ACCESS_KEY_ID.slice(0, 8) + "...");

// ─── Create provider ─────────────────────────────────────────────────────────

const provider = new AWSProvider({
  region: "us-east-1",
  credentials: {
    accessKeyId: creds.AWS_ACCESS_KEY_ID,
    secretAccessKey: creds.AWS_SECRET_ACCESS_KEY,
  },
});

let instanceId: string | null = null;

// ─── Cleanup on exit ─────────────────────────────────────────────────────────

async function cleanup() {
  if (instanceId) {
    console.log(`\nCleaning up: terminating ${instanceId}...`);
    try {
      await provider.terminate(instanceId);
      console.log("Terminated.");
    } catch (err) {
      console.error("Cleanup failed:", err);
    }
  }
}

process.on("SIGINT", async () => { await cleanup(); process.exit(1); });
process.on("SIGTERM", async () => { await cleanup(); process.exit(1); });

// ─── Main ──────────────────────────────────────────────────────────────────────

async function main() {
  const startTime = Date.now();

  // Step 1: startup — verifies creds, creates SG + key pair
  console.log("\n=== Step 1: startup() ===");
  const startupResult = await provider.startup();
  console.log("  Account:", startupResult.accountId);
  console.log("  Region:", startupResult.region);
  console.log("  Security Group:", startupResult.securityGroupId);
  console.log("  Key Pair:", startupResult.keyName);
  console.log("  Time:", Date.now() - startTime, "ms");

  // Step 2: spawn t4g.nano (cheapest ARM instance)
  console.log("\n=== Step 2: spawn() ===");
  const spawnStart = Date.now();
  const instance = await provider.spawn({
    spec: "ubuntu:noble:arm64",
    region: "us-east-1",
    bootstrap: {
      agentUrl: "http://localhost:3000/v1/agent/download",
      controlPlaneUrl: "http://localhost:3000",
      registrationToken: "smoke-test-token",
    },
    tags: {
      instance_type: "t4g.nano",
      "skyrepl:control_id": "smoketest",
    },
    instanceId: 1,
    manifestId: 0,
    clientRequestId: `smoke-${Date.now()}`,
  });
  instanceId = instance.id;
  console.log("  Instance ID:", instance.id);
  console.log("  Status:", instance.status);
  console.log("  Instance Type:", instance.instanceType);
  console.log("  AZ:", instance.availabilityZone);
  console.log("  Spawn time:", Date.now() - spawnStart, "ms");

  // Step 3: poll until running (timeout 120s)
  console.log("\n=== Step 3: wait for running ===");
  const pollStart = Date.now();
  const timeout = 120_000;
  let current = instance;
  while (current.status !== "running" && Date.now() - pollStart < timeout) {
    await new Promise((r) => setTimeout(r, 10_000));
    const fetched = await provider.get(instance.id);
    if (!fetched) {
      console.error("  Instance disappeared!");
      break;
    }
    current = fetched;
    console.log(`  Status: ${current.status} (${Math.round((Date.now() - pollStart) / 1000)}s)`);
  }

  if (current.status === "running") {
    console.log("  RUNNING!");
    console.log("  Public IP:", current.ip);
    console.log("  Private IP:", current.privateIp);
    console.log("  Time to running:", Date.now() - pollStart, "ms");
  } else {
    console.log("  WARNING: Instance did not reach running state within timeout");
    console.log("  Final status:", current.status);
  }

  // Step 4: list() — verify our instance appears
  console.log("\n=== Step 4: list() ===");
  const listed = await provider.list({ region: "us-east-1" });
  console.log("  Managed instances:", listed.length);
  const ours = listed.find((i) => i.id === instance.id);
  console.log("  Our instance found:", !!ours);

  // Step 5: terminate
  console.log("\n=== Step 5: terminate() ===");
  await provider.terminate(instance.id);
  instanceId = null; // Prevent double-terminate in cleanup
  console.log("  Terminated.");

  // Step 6: verify terminated
  await new Promise((r) => setTimeout(r, 5000));
  const after = await provider.get(instance.id);
  console.log("  Post-terminate status:", after?.status ?? "(not found)");

  // Summary
  console.log("\n=== SMOKE TEST PASSED ===");
  console.log("  Total time:", Math.round((Date.now() - startTime) / 1000), "s");
  console.log("  Instance:", instance.id, "→", after?.status ?? "terminated");
}

main().catch(async (err) => {
  console.error("\n=== SMOKE TEST FAILED ===");
  console.error(err);
  await cleanup();
  process.exit(1);
});
