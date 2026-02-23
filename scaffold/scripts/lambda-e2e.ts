#!/usr/bin/env bun
// Lambda Labs E2E validation script — WL-054D
// Usage: LAMBDA_API_KEY=... LAMBDA_SSH_KEY_NAME=workbench bun run scaffold/scripts/lambda-e2e.ts
//
// Tests: spawn, get, list, materialization, terminate, mark_terminated

import { LambdaLabsProvider } from "../../lifecycle/control/src/provider/compute/lambda";

const token = process.env.LAMBDA_API_KEY;
const sshKeyName = process.env.LAMBDA_SSH_KEY_NAME ?? "workbench";
const defaultRegion = process.env.LAMBDA_REGION ?? "us-west-3";

if (!token) {
  console.error("Set LAMBDA_API_KEY env var");
  process.exit(1);
}

const provider = new LambdaLabsProvider({
  token,
  defaultRegion,
  sshKeyName,
});

let spawnedId: string | null = null;

async function cleanup() {
  if (spawnedId) {
    console.log(`\n[cleanup] Terminating ${spawnedId}...`);
    try {
      await provider.terminate(spawnedId);
      console.log("[cleanup] Terminated.");
    } catch (e) {
      console.error("[cleanup] Failed:", e);
    }
  }
}

process.on("SIGINT", async () => { await cleanup(); process.exit(1); });
process.on("SIGTERM", async () => { await cleanup(); process.exit(1); });

async function main() {
  const results: { test: string; pass: boolean; detail?: string }[] = [];

  function check(name: string, pass: boolean, detail?: string) {
    results.push({ test: name, pass, detail });
    console.log(`  ${pass ? "PASS" : "FAIL"} ${name}${detail ? ` — ${detail}` : ""}`);
  }

  // ─── 1. listAvailableSpecs ───────────────────────────────────────
  console.log("\n=== 1. listAvailableSpecs ===");
  const specs = await provider.listAvailableSpecs();
  check("listAvailableSpecs returns data", specs.length > 0, `${specs.length} types`);

  // Pick a type that actually has capacity right now
  const capacityResp = await fetch("https://cloud.lambdalabs.com/api/v1/instance-types", {
    headers: { Authorization: `Bearer ${token}` },
  });
  const capacityData = (await capacityResp.json()) as { data: Record<string, any> };
  let targetType = "";
  let targetRegion = "";
  for (const [name, info] of Object.entries(capacityData.data)) {
    const regions = info.regions_with_capacity_available ?? [];
    if (regions.length > 0) {
      // Prefer cheapest option
      if (!targetType || info.instance_type.price_cents_per_hour < (capacityData.data[targetType]?.instance_type?.price_cents_per_hour ?? Infinity)) {
        targetType = name;
        targetRegion = regions[0].name;
      }
    }
  }
  if (!targetType) {
    console.error("No Lambda instance types have capacity right now. Retry later.");
    // Still report the checks that passed
    console.log("\n=== SUMMARY (partial — no capacity) ===");
    const passed = results.filter(r => r.pass).length;
    console.log(`${passed} PASS out of ${results.length} checks (spawn skipped: no capacity)`);
    process.exit(2); // Distinct exit code for "no capacity"
  }
  console.log(`  Using ${targetType} in ${targetRegion} (has capacity)`);

  const specEntry = specs.find(s => s.name === targetType);
  check("target type in spec list", !!specEntry, specEntry ? `${targetType} $${specEntry.hourlyRate}/hr` : "missing");

  // ─── 2. getHourlyRate ────────────────────────────────────────────
  console.log("\n=== 2. getHourlyRate ===");
  const rate = await provider.getHourlyRate(targetType);
  check("getHourlyRate returns non-zero", rate > 0, `$${rate}/hr`);

  // ─── 3. spawn ────────────────────────────────────────────────────
  console.log("\n=== 3. spawn ===");
  const instance = await provider.spawn({
    spec: `gpu:${targetType}:1`,
    bootstrap: {
      agentUrl: "http://localhost:3000/v1/agent/download/agent.py",
      controlPlaneUrl: "http://localhost:3000",
      registrationToken: "e2e-test-token",
    },
    controlId: "e2etst",
    manifestId: null,
    instanceId: 999,
    instanceType: targetType,
  });
  spawnedId = instance.id;
  check("spawn returned instance ID", !!instance.id, instance.id);
  check("spawn status is starting or running", ["starting", "running", "pending"].includes(instance.status), instance.status);
  check("spawn instanceTypeName set", instance.instanceTypeName === targetType, instance.instanceTypeName);
  check("spawn isSpot is false", instance.isSpot === false);
  check("spawn region set", !!instance.region, instance.region);

  // ─── 4. get ──────────────────────────────────────────────────────
  console.log("\n=== 4. get ===");
  const fetched = await provider.get(spawnedId);
  check("get returns instance", !!fetched, fetched?.status);
  check("get has GPU fields", !!fetched?.gpuDescription, fetched?.gpuDescription);
  check("get has gpuCount", typeof fetched?.gpuCount === "number", `${fetched?.gpuCount}`);
  check("get has vcpus", typeof fetched?.vcpus === "number", `${fetched?.vcpus}`);
  check("get has priceCentsPerHour", typeof fetched?.priceCentsPerHour === "number", `${fetched?.priceCentsPerHour}`);

  // ─── 5. list ─────────────────────────────────────────────────────
  console.log("\n=== 5. list ===");
  const all = await provider.list();
  check("list includes spawned instance", all.some(i => i.id === spawnedId), `${all.length} total`);

  const filtered = await provider.list({ spec: targetType });
  check("list with spec filter works", filtered.some(i => i.id === spawnedId), `${filtered.length} matched`);

  // ─── 6. Wait for IP (poll) ───────────────────────────────────────
  console.log("\n=== 6. Wait for IP (up to 3 min) ===");
  let gotIp = false;
  for (let i = 0; i < 36; i++) {
    const polled = await provider.get(spawnedId);
    if (polled?.ip) {
      check("instance got IP", true, polled.ip);
      check("instance status is running or starting", ["running", "starting"].includes(polled.status), polled.status);
      if (polled.privateIp) {
        check("instance has private IP", true, polled.privateIp);
      }
      gotIp = true;
      break;
    }
    process.stdout.write(".");
    await new Promise(r => setTimeout(r, 5000));
  }
  if (!gotIp) {
    check("instance got IP within 3 min", false, "timed out");
  }

  // ─── 7. terminate ────────────────────────────────────────────────
  console.log("\n=== 7. terminate ===");
  await provider.terminate(spawnedId);
  check("terminate completed", true);

  // ─── 8. post-terminate status ────────────────────────────────────
  console.log("\n=== 8. post-terminate status ===");
  // Lambda keeps instances in terminating/terminated state for a while before 404.
  // We verify the instance reaches a terminal status; full purge may take minutes.
  let reachedTerminal = false;
  let reachedNull = false;
  for (let i = 0; i < 12; i++) {
    const after = await provider.get(spawnedId);
    if (!after) {
      reachedNull = true;
      break;
    }
    if (after.status === "terminated" || after.status === "terminating") {
      reachedTerminal = true;
    }
    await new Promise(r => setTimeout(r, 5000));
  }
  check("instance reached terminal status", reachedTerminal || reachedNull,
    reachedNull ? "purged (null)" : "terminating/terminated");
  if (reachedNull) {
    check("get returns null (instance purged)", true);
  }
  spawnedId = null; // Already terminated

  // ─── 9. idempotent terminate ─────────────────────────────────────
  console.log("\n=== 9. idempotent terminate ===");
  try {
    await provider.terminate("nonexistent-id-12345");
    check("terminate on nonexistent ID is idempotent", true);
  } catch (e: any) {
    check("terminate on nonexistent ID is idempotent", false, e.message);
  }

  // ─── Summary ─────────────────────────────────────────────────────
  console.log("\n=== SUMMARY ===");
  const passed = results.filter(r => r.pass).length;
  const failed = results.filter(r => !r.pass).length;
  console.log(`${passed} PASS, ${failed} FAIL out of ${results.length} checks`);

  if (failed > 0) {
    console.log("\nFailed checks:");
    for (const r of results.filter(r => !r.pass)) {
      console.log(`  FAIL: ${r.test}${r.detail ? ` — ${r.detail}` : ""}`);
    }
  }

  process.exit(failed > 0 ? 1 : 0);
}

main().catch(async (err) => {
  console.error("Fatal:", err);
  await cleanup();
  process.exit(1);
});
