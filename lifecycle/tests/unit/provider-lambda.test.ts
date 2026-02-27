// tests/unit/provider-lambda.test.ts - Lambda Labs Provider Tests (#PROV-054D)
//
// Tests the LambdaLabsProvider that uses the Lambda Labs REST API via fetch().
// Injects a mock fetch via the _fetchImpl option — no real Lambda API calls.
//
// Mock strategy: _fetchImpl dispatches on URL + method to return canned responses.

import { describe, test, expect, beforeEach } from "bun:test";
import {
  LambdaLabsProvider,
  mapLambdaStatus,
  mapLambdaError,
  lambdaRequest,
  createLambdaHooks,
  type LambdaLabsProviderConfig,
} from "../../control/src/provider/compute/lambda";
import type {
  Provider,
  LambdaInstance,
  BootstrapConfig,
  ListFilter,
} from "../../control/src/provider/types";
import { PROVIDER_CAPABILITIES } from "../../control/src/provider/types";
import {
  ProviderOperationError,
} from "../../control/src/provider/errors";

// =============================================================================
// Mock Lambda API (fetch dispatch)
// =============================================================================

interface MockInstanceType {
  instance_type: {
    name: string;
    description: string;
    gpu_description: string;
    price_cents_per_hour: number;
    specs: {
      vcpus: number;
      memory_gib: number;
      storage_gib: number;
      gpus: number;
    };
  };
  regions_with_capacity_available: Array<{ name: string; description: string }>;
}

interface MockState {
  instances: Map<string, any>;
  instanceTypes: Record<string, MockInstanceType>;
  lastLaunchBody?: any;
  launchError?: { status: number; body: any };
  terminateError?: { status: number; body: any };
}

function createState(): MockState {
  return {
    instances: new Map(),
    instanceTypes: {
      gpu_1x_a100: {
        instance_type: {
          name: "gpu_1x_a100",
          description: "1x A100 (80 GB SXM4)",
          gpu_description: "A100 80GB SXM4",
          price_cents_per_hour: 110,
          specs: { vcpus: 30, memory_gib: 200, storage_gib: 512, gpus: 1 },
        },
        regions_with_capacity_available: [
          { name: "us-west-1", description: "California, USA" },
        ],
      },
      gpu_8x_a100: {
        instance_type: {
          name: "gpu_8x_a100",
          description: "8x A100 (80 GB SXM4)",
          gpu_description: "A100 80GB SXM4",
          price_cents_per_hour: 880,
          specs: { vcpus: 240, memory_gib: 1800, storage_gib: 4096, gpus: 8 },
        },
        regions_with_capacity_available: [
          { name: "us-east-1", description: "Virginia, USA" },
        ],
      },
      gpu_1x_h100_pcie: {
        instance_type: {
          name: "gpu_1x_h100_pcie",
          description: "1x H100 (80 GB PCIe)",
          gpu_description: "H100 80GB PCIe",
          price_cents_per_hour: 200,
          specs: { vcpus: 26, memory_gib: 200, storage_gib: 512, gpus: 1 },
        },
        regions_with_capacity_available: [
          { name: "us-east-1", description: "Virginia, USA" },
        ],
      },
      // Type with zero capacity — used for capacity pre-check tests
      gpu_1x_a10: {
        instance_type: {
          name: "gpu_1x_a10",
          description: "1x A10 (24 GB)",
          gpu_description: "A10 24GB",
          price_cents_per_hour: 60,
          specs: { vcpus: 16, memory_gib: 128, storage_gib: 256, gpus: 1 },
        },
        regions_with_capacity_available: [], // No capacity anywhere
      },
    },
  };
}

function createMockFetch(state: MockState): (input: RequestInfo | URL, init?: RequestInit) => Promise<Response> {
  return async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const url = typeof input === "string" ? input : input.toString();
    const method = init?.method ?? "GET";
    const body = init?.body ? JSON.parse(init.body as string) : undefined;

    // Route dispatch
    if (method === "GET" && url.endsWith("/api/v1/instances")) {
      return handleListInstances(state);
    }
    if (method === "GET" && url.match(/\/api\/v1\/instances\/[^/]+$/)) {
      return handleGetInstance(state, url);
    }
    if (method === "GET" && url.endsWith("/api/v1/instance-types")) {
      return handleListInstanceTypes(state);
    }
    if (method === "POST" && url.endsWith("/api/v1/instance-operations/launch")) {
      return handleLaunch(state, body);
    }
    if (method === "POST" && url.endsWith("/api/v1/instance-operations/terminate")) {
      return handleTerminate(state, body);
    }
    if (method === "GET" && url.endsWith("/api/v1/ssh-keys")) {
      return jsonResponse({ data: [{ id: "key-abc", name: "my-key", public_key: "ssh-rsa ..." }] });
    }

    return new Response(
      JSON.stringify({ error: { code: "global/route-not-found", message: "Mock route not found" } }),
      { status: 404, headers: { "Content-Type": "application/json" } },
    );
  };
}

function handleListInstances(state: MockState): Response {
  return jsonResponse({ data: Array.from(state.instances.values()) });
}

function handleGetInstance(state: MockState, url: string): Response {
  const id = url.split("/").pop()!;
  const instance = state.instances.get(id);
  if (!instance) {
    return jsonResponse(
      { error: { code: "global/object-does-not-exist", message: `Instance ${id} not found` } },
      404,
    );
  }
  return jsonResponse({ data: instance });
}

function handleListInstanceTypes(state: MockState): Response {
  return jsonResponse({ data: state.instanceTypes });
}

function handleLaunch(state: MockState, body: any): Response {
  if (state.launchError) {
    const { status, body: errBody } = state.launchError;
    state.launchError = undefined;
    return new Response(JSON.stringify(errBody), {
      status,
      headers: { "Content-Type": "application/json" },
    });
  }

  state.lastLaunchBody = body;

  const id = `inst-${Math.random().toString(36).slice(2, 10)}`;
  const instanceTypeName: string = body.instance_type_name ?? "gpu_1x_a100";
  const typeData = state.instanceTypes[instanceTypeName];

  const instance = {
    id,
    name: body.name ?? "",
    ip: null,
    private_ip: null,
    status: "booting",
    ssh_key_names: body.ssh_key_names ?? [],
    region: { name: body.region_name, description: "" },
    instance_type: typeData?.instance_type ?? {
      name: instanceTypeName,
      description: "",
      gpu_description: "",
      price_cents_per_hour: 0,
      specs: { vcpus: 0, memory_gib: 0, storage_gib: 0, gpus: 0 },
    },
    hostname: body.hostname ?? null,
  };

  state.instances.set(id, instance);
  return jsonResponse({ data: { instance_ids: [id] } });
}

function handleTerminate(state: MockState, body: any): Response {
  if (state.terminateError) {
    const { status, body: errBody } = state.terminateError;
    state.terminateError = undefined;
    return new Response(JSON.stringify(errBody), {
      status,
      headers: { "Content-Type": "application/json" },
    });
  }

  const terminated = [];
  for (const id of body.instance_ids ?? []) {
    const inst = state.instances.get(id);
    if (inst) {
      terminated.push(inst);
      state.instances.delete(id);
    }
  }
  return jsonResponse({ data: { terminated_instances: terminated } });
}

function jsonResponse(data: any, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

// =============================================================================
// Test Helpers
// =============================================================================

function createProvider(
  state: MockState,
  overrides: Partial<LambdaLabsProviderConfig> = {},
): LambdaLabsProvider {
  return new LambdaLabsProvider({
    token: "test-lambda-token",
    defaultRegion: "us-west-1",
    sshKeyName: "my-key",
    _fetchImpl: createMockFetch(state) as unknown as typeof fetch,
    ...overrides,
  });
}

const testBootstrap: BootstrapConfig = {
  agentUrl: "http://localhost:3000/v1/agent/download/agent.py",
  controlPlaneUrl: "http://localhost:3000",
  registrationToken: "test-reg-token",
};

const testIdentity = { controlId: "test01", manifestId: 1, instanceId: 1 };

// =============================================================================
// Tests: provider interface satisfaction
// =============================================================================

describe("PROV-054D: Lambda provider type satisfaction", () => {
  test("satisfies Provider interface with correct capabilities", () => {
    const state = createState();
    const provider = createProvider(state);

    // Type-level: Provider assignment proves interface conformance
    const p: Provider = provider;
    expect(p.name).toBe("lambda");

    // Capabilities — toEqual checks every field at once
    expect(provider.capabilities).toEqual(PROVIDER_CAPABILITIES.lambda);
  });
});

// =============================================================================
// Tests: mapLambdaStatus
// =============================================================================

describe("PROV-054D: mapLambdaStatus", () => {
  test("maps all Lambda statuses to correct ProviderInstanceStatus", () => {
    const cases: [string, string][] = [
      ["active", "running"],
      ["booting", "starting"],
      ["unhealthy", "error"],
      ["terminated", "terminated"],
      ["terminating", "terminating"],
      ["preempted", "terminated"],
      ["unknown_status", "pending"],
      ["", "pending"],
    ];
    for (const [input, expected] of cases) {
      expect(mapLambdaStatus(input)).toBe(expected as any);
    }
  });
});

// =============================================================================
// Tests: mapLambdaError
// =============================================================================

describe("PROV-054D: mapLambdaError", () => {
  test("maps all HTTP error statuses to correct error code and retryable flag", () => {
    const cases: [number, string, boolean][] = [
      [401, "AUTH_ERROR", false],
      [403, "AUTH_ERROR", false],
      [404, "NOT_FOUND", false],
      [429, "RATE_LIMIT_ERROR", true],
      [500, "PROVIDER_INTERNAL", true],
      [503, "PROVIDER_INTERNAL", true],
      [418, "PROVIDER_INTERNAL", false], // unknown status → default (not retryable)
    ];
    for (const [status, code, retryable] of cases) {
      const err = mapLambdaError(status, { error: { code: "test", message: "test" } });
      expect(err).toBeInstanceOf(ProviderOperationError);
      expect(err.code).toBe(code as any);
      expect(err.retryable).toBe(retryable);
    }
  });

  test("capacity error: 400 with insufficient-capacity code maps to CAPACITY_ERROR", () => {
    const err = mapLambdaError(400, {
      error: {
        code: "instance-operations/launch/insufficient-capacity",
        message: "No capacity available",
      },
    });
    expect(err).toBeInstanceOf(ProviderOperationError);
    expect(err.code).toBe("CAPACITY_ERROR");
    expect(err.retryable).toBe(true);
  });

  test("quota exceeded: 400 with global/quota-exceeded code", () => {
    const err = mapLambdaError(400, {
      error: { code: "global/quota-exceeded", message: "Quota exceeded" },
    });
    expect(err.code).toBe("QUOTA_EXCEEDED");
    expect(err.retryable).toBe(false);
  });
});

// =============================================================================
// Tests: lambdaRequest
// =============================================================================

describe("PROV-054D: lambdaRequest", () => {
  test("parses 2xx JSON correctly", async () => {
    const mockFetch = async () =>
      jsonResponse({ data: [{ id: "inst-abc", status: "active" }] });
    const result = await lambdaRequest<{ data: any[] }>(
      "token",
      "GET",
      "/api/v1/instances",
      undefined,
      mockFetch as unknown as typeof fetch,
    );
    expect(result.data[0].id).toBe("inst-abc");
  });

  test("throws mapped error for non-2xx (proves wiring to mapLambdaError)", async () => {
    const mockFetch = async () =>
      jsonResponse({ error: { code: "unauthorized", message: "Bad token" } }, 401);
    await expect(
      lambdaRequest("token", "GET", "/api/v1/instances", undefined, mockFetch as unknown as typeof fetch),
    ).rejects.toMatchObject({ code: "AUTH_ERROR" });
  });
});

// =============================================================================
// Tests: spawn
// =============================================================================

describe("PROV-054D: spawn", () => {
  test("spawn creates instance with correct POST body including user_data", async () => {
    const state = createState();
    const provider = createProvider(state);

    const result = await provider.spawn({
      spec: "gpu_1x_a100",
      bootstrap: testBootstrap,
      ...testIdentity,
    });

    expect(result.id).toBeDefined();
    expect(result.status).toBe("starting"); // "booting" maps to "starting"
    expect(result.isSpot).toBe(false);

    // Verify request body fields
    expect(state.lastLaunchBody!.region_name).toBe("us-west-1");
    expect(state.lastLaunchBody!.instance_type_name).toBe("gpu_1x_a100");
    expect(state.lastLaunchBody!.ssh_key_names).toContain("my-key");

    // Bootstrap content passed as user_data
    expect(state.lastLaunchBody!.user_data).toContain("#!/bin/bash");
    expect(state.lastLaunchBody!.user_data).toContain("test-reg-token");
  });

  test("spawn uses hostname from formatResourceName convention", async () => {
    const state = createState();
    const provider = createProvider(state);

    await provider.spawn({
      spec: "gpu_1x_a100",
      bootstrap: testBootstrap,
      controlId: "abc123",
      manifestId: 7,
      instanceId: 42,
    });

    // Both hostname and name should be set from formatResourceName
    expect(state.lastLaunchBody!.hostname).toMatch(/^repl-abc123-7-/);
    expect(state.lastLaunchBody!.name).toBe(state.lastLaunchBody!.hostname);
  });

  test("spawn with spot=true throws UNSUPPORTED_OPERATION", async () => {
    const state = createState();
    const provider = createProvider(state);

    await expect(
      provider.spawn({
        spec: "gpu_1x_a100",
        bootstrap: testBootstrap,
        ...testIdentity,
        spot: true,
      }),
    ).rejects.toMatchObject({ code: "UNSUPPORTED_OPERATION" });
  });

  test("spawn without spec throws INVALID_SPEC", async () => {
    const state = createState();
    const provider = createProvider(state);

    await expect(
      provider.spawn({
        spec: "",
        bootstrap: testBootstrap,
        ...testIdentity,
      }),
    ).rejects.toMatchObject({ code: "INVALID_SPEC" });
  });

  test("spawn pre-checks capacity: zero-capacity type throws CAPACITY_ERROR before launch", async () => {
    const state = createState();
    const provider = createProvider(state);

    // gpu_1x_a10 has zero regions_with_capacity_available in mock state.
    // The provider should throw CAPACITY_ERROR from findAvailableRegion()
    // BEFORE ever calling /instance-operations/launch.
    await expect(
      provider.spawn({
        spec: "gpu_1x_a10",
        bootstrap: testBootstrap,
        ...testIdentity,
      }),
    ).rejects.toMatchObject({ code: "CAPACITY_ERROR" });

    // Prove launch was never called
    expect(state.lastLaunchBody).toBeUndefined();
  });

  test("spawn falls back to first available region when default and preferred lack capacity", async () => {
    const state = createState();
    // gpu_8x_a100 has capacity only in us-east-1
    // Provider default is us-west-1 (no capacity for 8x)
    // No explicit preferred region → should fall back to us-east-1
    const provider = createProvider(state, { defaultRegion: "us-west-1" });

    const result = await provider.spawn({
      spec: "gpu_8x_a100",
      bootstrap: testBootstrap,
      ...testIdentity,
      // No region specified — neither default nor preferred match
    });

    expect(result.id).toBeDefined();
    expect(state.lastLaunchBody!.region_name).toBe("us-east-1");
  });

  test("spawn prefers explicit region over default when both have capacity", async () => {
    const state = createState();
    // Give gpu_1x_a100 capacity in both regions
    state.instanceTypes.gpu_1x_a100.regions_with_capacity_available = [
      { name: "us-west-1", description: "California" },
      { name: "us-east-1", description: "Virginia" },
    ];
    const provider = createProvider(state, { defaultRegion: "us-west-1" });

    await provider.spawn({
      spec: "gpu_1x_a100",
      bootstrap: testBootstrap,
      ...testIdentity,
      region: "us-east-1",
    });

    expect(state.lastLaunchBody!.region_name).toBe("us-east-1");
  });
});

// =============================================================================
// Tests: terminate
// =============================================================================

describe("PROV-054D: terminate", () => {
  test("terminate removes instance from provider state", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "gpu_1x_a100",
      bootstrap: testBootstrap,
      ...testIdentity,
    });

    await provider.terminate(inst.id);
    expect(state.instances.has(inst.id)).toBe(false);
  });

  test("terminate is idempotent (double-terminate, 404 swallowed)", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "gpu_1x_a100",
      bootstrap: testBootstrap,
      ...testIdentity,
    });

    await provider.terminate(inst.id);
    // Second terminate: instance is gone, should be swallowed silently
    await expect(provider.terminate(inst.id)).resolves.toBeUndefined();
  });

  test("terminate propagates non-NOT_FOUND errors", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "gpu_1x_a100",
      bootstrap: testBootstrap,
      ...testIdentity,
    });

    // Inject a 500 error on next terminate call
    state.terminateError = {
      status: 500,
      body: { error: { code: "internal", message: "Server error" } },
    };

    await expect(provider.terminate(inst.id)).rejects.toMatchObject({
      code: "PROVIDER_INTERNAL",
    });
  });
});

// =============================================================================
// Tests: list and get
// =============================================================================

describe("PROV-054D: list and get", () => {
  test("list with spec filter returns only matching instances", async () => {
    const state = createState();
    const provider = createProvider(state);

    await provider.spawn({
      spec: "gpu_1x_a100",
      bootstrap: testBootstrap,
      ...testIdentity,
      instanceId: 1,
    });
    await provider.spawn({
      spec: "gpu_1x_h100_pcie",
      bootstrap: testBootstrap,
      ...testIdentity,
      instanceId: 2,
    });

    const all = await provider.list();
    expect(all.length).toBe(2);

    // Lambda's projectInstance sets spec = instanceTypeName, not the SkyREPL spec string
    const a100Only = await provider.list({ spec: "gpu_1x_a100" });
    expect(a100Only.length).toBe(1);
    expect(a100Only[0]!.spec).toBe("gpu_1x_a100");
  });

  test("list with status filter", async () => {
    const state = createState();
    const provider = createProvider(state);

    await provider.spawn({
      spec: "gpu_1x_a100",
      bootstrap: testBootstrap,
      ...testIdentity,
    });

    // Instances start as "booting" → "starting"
    const starting = await provider.list({ status: ["starting"] });
    expect(starting.length).toBe(1);

    const running = await provider.list({ status: ["running"] });
    expect(running.length).toBe(0);
  });

  test("list with region filter", async () => {
    const state = createState();
    const provider = createProvider(state);

    await provider.spawn({
      spec: "gpu_1x_a100",
      bootstrap: testBootstrap,
      ...testIdentity,
    });

    // Instance was spawned in us-west-1 (default region, has capacity for a100)
    expect((await provider.list({ region: "us-west-1" })).length).toBe(1);
    expect((await provider.list({ region: "eu-west-1" })).length).toBe(0);
  });

  test("list with limit", async () => {
    const state = createState();
    const provider = createProvider(state);

    await provider.spawn({ spec: "gpu_1x_a100", bootstrap: testBootstrap, ...testIdentity, instanceId: 1 });
    await provider.spawn({ spec: "gpu_1x_a100", bootstrap: testBootstrap, ...testIdentity, instanceId: 2 });
    await provider.spawn({ spec: "gpu_1x_a100", bootstrap: testBootstrap, ...testIdentity, instanceId: 3 });

    const limited = await provider.list({ limit: 2 });
    expect(limited.length).toBe(2);
  });

  test("list excludes terminated by default, includes with flag", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "gpu_1x_a100",
      bootstrap: testBootstrap,
      ...testIdentity,
    });

    // Mark as terminated in mock state
    state.instances.get(inst.id)!.status = "terminated";

    expect((await provider.list()).length).toBe(0);
    expect((await provider.list({ includeTerminated: true })).length).toBe(1);
  });

  test("get returns null for non-existent instance", async () => {
    const state = createState();
    const provider = createProvider(state);

    const result = await provider.get("inst-does-not-exist");
    expect(result).toBeNull();
  });

  test("get populates GPU fields from instance_type data", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "gpu_1x_a100",
      bootstrap: testBootstrap,
      ...testIdentity,
    });

    const found = await provider.get(inst.id) as LambdaInstance;
    expect(found).not.toBeNull();
    expect(found!.gpuCount).toBe(1);
    expect(found!.gpuDescription).toBe("A100 80GB SXM4");
    expect(found!.vcpus).toBe(30);
    expect(found!.memoryGib).toBe(200);
    expect(found!.storageGib).toBe(512);
    expect(found!.priceCentsPerHour).toBe(110);
  });
});

// =============================================================================
// Tests: generateBootstrap
// =============================================================================

describe("PROV-054D: generateBootstrap", () => {
  test("returns shell format (not cloud-init)", () => {
    const state = createState();
    const provider = createProvider(state);

    const result = provider.generateBootstrap(testBootstrap);

    expect(result.format).toBe("shell");
    expect(result.content).toContain("#!/bin/bash");
    expect(result.checksum).toHaveLength(64);
  });

  test("shell script includes config.json with controlPlaneUrl and registrationToken", () => {
    const state = createState();
    const provider = createProvider(state);

    const result = provider.generateBootstrap(testBootstrap);

    expect(result.content).toContain("http://localhost:3000");
    expect(result.content).toContain("test-reg-token");
  });

  test("checksum is deterministic and changes with different configs", () => {
    const state = createState();
    const provider = createProvider(state);

    const r1a = provider.generateBootstrap(testBootstrap);
    const r1b = provider.generateBootstrap(testBootstrap);
    expect(r1a.checksum).toBe(r1b.checksum); // same input → same output

    const r2 = provider.generateBootstrap({
      ...testBootstrap,
      initScript: "echo hello",
    });
    expect(r1a.checksum).not.toBe(r2.checksum); // different input → different output
  });
});

// =============================================================================
// Tests: pricing
// =============================================================================

describe("PROV-054D: pricing", () => {
  test("listAvailableSpecs returns spec list with rates and regions", async () => {
    const state = createState();
    const provider = createProvider(state);

    const specs = await provider.listAvailableSpecs();

    expect(specs.length).toBe(4); // all 4 types in mock, including zero-capacity

    const a100Spec = specs.find((s) => s.name === "gpu_1x_a100");
    expect(a100Spec).toBeDefined();
    expect(a100Spec!.hourlyRate).toBe(1.1); // 110 cents / 100 = $1.10
    expect(a100Spec!.availableRegions).toContain("us-west-1");

    // Zero-capacity type still has pricing info
    const a10Spec = specs.find((s) => s.name === "gpu_1x_a10");
    expect(a10Spec).toBeDefined();
    expect(a10Spec!.hourlyRate).toBe(0.6);
    expect(a10Spec!.availableRegions).toEqual([]);
  });

  test("getHourlyRate returns rate for valid spec and 0 for unknown", async () => {
    const state = createState();
    const provider = createProvider(state);

    const rate = await provider.getHourlyRate("gpu_1x_a100");
    expect(rate).toBe(1.1);

    const unknown = await provider.getHourlyRate("gpu_nonexistent");
    expect(unknown).toBe(0);
  });
});

// =============================================================================
// Tests: lifecycle hooks
// =============================================================================

describe("PROV-054D: lifecycle hooks", () => {
  test("onStartup verifies API access without throwing", async () => {
    const state = createState();
    const provider = createProvider(state);
    const hooks = createLambdaHooks(provider);

    await expect(hooks.onStartup!()).resolves.toBeUndefined();
  });

  test("onHeartbeat: health_check completed, unknown type skipped", async () => {
    const state = createState();
    const provider = createProvider(state);
    const hooks = createLambdaHooks(provider);

    const result = await hooks.onHeartbeat!({
      tasks: [
        { type: "health_check", priority: "normal" },
        { type: "unknown_task_type", priority: "low" },
      ],
      deadline: Date.now() + 10_000,
    });

    expect(result.receipts).toHaveLength(2);
    expect(result.receipts[0]!.type).toBe("health_check");
    expect(result.receipts[0]!.status).toBe("completed");
    expect(result.receipts[1]!.type).toBe("unknown_task_type");
    expect(result.receipts[1]!.status).toBe("skipped");
    expect(result.receipts[1]!.reason).toContain("Unknown task type");
  });
});

// =============================================================================
// Tests: instance mapping
// =============================================================================

describe("PROV-054D: instance mapping", () => {
  test("maps Lambda instance fields to LambdaInstance correctly", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "gpu_1x_a100",
      bootstrap: testBootstrap,
      ...testIdentity,
    });

    // Simulate active instance with IPs
    const raw = state.instances.get(inst.id)!;
    raw.status = "active";
    raw.ip = "203.0.113.42";
    raw.private_ip = "10.0.1.5";

    const fetched = await provider.get(inst.id) as LambdaInstance;
    expect(fetched).not.toBeNull();
    expect(fetched!.status).toBe("running");
    expect(fetched!.ip).toBe("203.0.113.42");
    expect(fetched!.privateIp).toBe("10.0.1.5");
    expect(fetched!.region).toBe("us-west-1");
    expect(fetched!.isSpot).toBe(false);
    expect(fetched!.instanceTypeName).toBe("gpu_1x_a100");
    expect(fetched!.gpuDescription).toBe("A100 80GB SXM4");
    expect(fetched!.gpuCount).toBe(1);
  });
});

// =============================================================================
// Tests: registry
// =============================================================================

describe("PROV-054D: registry", () => {
  test("getProvider('lambda') resolves and returns provider with name 'lambda'", async () => {
    const { getProvider, clearProviderCache } = await import(
      "../../control/src/provider/registry"
    );
    clearProviderCache();

    const provider = await getProvider("lambda");

    expect(provider).toBeDefined();
    expect(provider.name).toBe("lambda");
    expect(typeof provider.spawn).toBe("function");
  });
});
