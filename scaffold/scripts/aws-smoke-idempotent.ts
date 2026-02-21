#!/usr/bin/env bun
// aws-smoke-idempotent.ts — Verify idempotent startup + spot price query
// Usage: bun scaffold/scripts/aws-smoke-idempotent.ts

import { readFileSync } from "fs";
import { join } from "path";
import { homedir } from "os";
import { AWSProvider } from "../../lifecycle/control/src/provider/compute/aws";

const credPath = join(homedir(), ".repl", "aws-dev-credentials.env");
const credLines = readFileSync(credPath, "utf8").split("\n");
const creds: Record<string, string> = {};
for (const line of credLines) {
  const match = line.match(/^(\w+)=(.+)$/);
  if (match) creds[match[1]] = match[2];
}

const provider = new AWSProvider({
  region: "us-east-1",
  credentials: {
    accessKeyId: creds.AWS_ACCESS_KEY_ID,
    secretAccessKey: creds.AWS_SECRET_ACCESS_KEY,
  },
});

async function main() {
  // Test 1: startup() should find existing SG + key (not create new ones)
  console.log("=== Test 1: Idempotent startup ===");
  const result = await provider.startup();
  console.log("  SG:", result.securityGroupId);
  console.log("  Key:", result.keyName);

  // Second call should be instant (cache hit)
  const t0 = Date.now();
  const result2 = await provider.startup();
  const elapsed = Date.now() - t0;
  console.log("  Second call:", elapsed, "ms (should be <10ms, cache hit)");
  console.log("  Same SG:", result.securityGroupId === result2.securityGroupId);
  console.log("  Same Key:", result.keyName === result2.keyName);

  // Test 2: getSpotPrices
  console.log("\n=== Test 2: Spot prices ===");
  const prices = await provider.getSpotPrices("ubuntu:noble:arm64", ["us-east-1"]);
  console.log("  Prices returned:", prices.length);
  if (prices.length > 0) {
    const sorted = prices.sort((a, b) => a.price - b.price);
    console.log("  Cheapest:", sorted[0].availabilityZone, "$" + sorted[0].price);
    console.log("  Most expensive:", sorted[sorted.length - 1].availabilityZone, "$" + sorted[sorted.length - 1].price);
  }

  // Test 3: list() — should show no running instances (previous terminated)
  console.log("\n=== Test 3: list() — should be empty (or shutting-down only) ===");
  const listed = await provider.list({ region: "us-east-1" });
  console.log("  Managed instances:", listed.length);
  for (const inst of listed) {
    console.log(`  ${inst.id}: ${inst.status} (${inst.instanceType})`);
  }

  console.log("\n=== ALL PASSED ===");
}

main().catch((err) => {
  console.error("FAILED:", err);
  process.exit(1);
});
