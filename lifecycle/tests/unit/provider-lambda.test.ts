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
    },
  };
}

function createMockFetch(state: MockState): typeof fetch {
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
    _fetchImpl: createMockFetch(state),
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
      expect(mapLambdaStatus(input)).toBe(expected);
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
      expect(err.code).toBe(code);
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
      mockFetch as typeof fetch,
    );
    expect(result.data[0].id).toBe("inst-abc");
  });

  test("throws mapped error for non-2xx (proves wiring to mapLambdaError)", async () => {
    const mockFetch = async () =>
      jsonResponse({ error: { code: "unauthorized", message: "Bad token" } }, 401);
    await expect(
      lambdaRequest("token", "GET", "/api/v1/instances", undefined, mockFetch as typeof fetch),
    ).rejects.toMatchObject({ code: "AUTH_ERROR" });
  });
});

// =============================================================================
// Tests: spawn
// =============================================================================

describe("PROV-054D: spawn", () => {
  test("spawn creates instance with correct POST body", async () => {
    const state = createState();
    const provider = createProvider(state);

    const result = await provider.spawn({
      spec: "gpu:a100:1",
      bootstrap: testBootstrap,
      ...testIdentity,
      instanceType: "gpu_1x_a100",
    });

    expect(result.id).toBeDefined();
    expect(result.status).toBe("starting"); // "booting" maps to "starting"
    expect(result.isSpot).toBe(false);

    // Verify request body fields
    expect(state.lastLaunchBody!.region_name).toBe("us-west-1");
    expect(state.lastLaunchBody!.instance_type_name).toBe("gpu_1x_a100");
    expect(state.lastLaunchBody!.ssh_key_names).toContain("my-key");
  });

  test("spawn uses hostname from formatResourceName convention", async () => {
    const state = createState();
    const provider = createProvider(state);

    await provider.spawn({
      spec: "gpu:a100:1",
      bootstrap: testBootstrap,
      controlId: "abc123",
      manifestId: 7,
      instanceId: 42,
      instanceType: "gpu_1x_a100",
    });

    // formatResourceName("abc123", 7, 42) = "repl-abc123-7-16" (42 in base36 = "16")
    const hostname: string = state.lastLaunchBody!.hostname ?? state.lastLaunchBody!.name;
    expect(hostname).toMatch(/repl-abc123/);
  });

  test("spawn with spot=true throws UNSUPPORTED_OPERATION", async () => {
    const state = createState();
    const provider = createProvider(state);

    await expect(
      provider.spawn({
        spec: "gpu:a100:1",
        bootstrap: testBootstrap,
        ...testIdentity,
        instanceType: "gpu_1x_a100",
        spot: true,
      }),
    ).rejects.toMatchObject({ code: "UNSUPPORTED_OPERATION" });
  });

  test("spawn with no capacity throws CAPACITY_ERROR", async () => {
    const state = createState();
    const provider = createProvider(state);

    state.launchError = {
      status: 400,
      body: {
        error: {
          code: "instance-operations/launch/insufficient-capacity",
          message: "No capacity available for gpu_1x_a100 in us-west-1",
        },
      },
    };

    await expect(
      provider.spawn({
        spec: "gpu:a100:1",
        bootstrap: testBootstrap,
        ...testIdentity,
        instanceType: "gpu_1x_a100",
      }),
    ).rejects.toMatchObject({ code: "CAPACITY_ERROR" });
  });

  test("spawn falls back to alternative region when default has no capacity", async () => {
    const state = createState();
    // gpu_8x_a100 has capacity in us-east-1, not us-west-1
    const provider = createProvider(state, { defaultRegion: "us-west-1" });

    const result = await provider.spawn({
      spec: "gpu:a100:8",
      bootstrap: testBootstrap,
      ...testIdentity,
      instanceType: "gpu_8x_a100",
      region: "us-east-1",
    });

    expect(result.id).toBeDefined();
    expect(state.lastLaunchBody!.region_name).toBe("us-east-1");
  });
});

// =============================================================================
// Tests: terminate
// =============================================================================

describe("PROV-054D: terminate", () => {
  test("terminate sends correct POST body", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "gpu:a100:1",
      bootstrap: testBootstrap,
      ...testIdentity,
      instanceType: "gpu_1x_a100",
    });

    await provider.terminate(inst.id);
    expect(state.instances.has(inst.id)).toBe(false);
  });

  test("terminate is idempotent (double-terminate, 404 swallowed)", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "gpu:a100:1",
      bootstrap: testBootstrap,
      ...testIdentity,
      instanceType: "gpu_1x_a100",
    });

    await provider.terminate(inst.id);
    // Second terminate: instance is gone, should be swallowed silently
    await expect(provider.terminate(inst.id)).resolves.toBeUndefined();
  });
});

// =============================================================================
// Tests: list and get
// =============================================================================

describe("PROV-054D: list and get", () => {
  test("list returns all instances", async () => {
    const state = createState();
    const provider = createProvider(state);

    await provider.spawn({
      spec: "gpu:a100:1",
      bootstrap: testBootstrap,
      ...testIdentity,
      instanceId: 1,
      instanceType: "gpu_1x_a100",
    });
    await provider.spawn({
      spec: "gpu:a100:1",
      bootstrap: testBootstrap,
      ...testIdentity,
      instanceId: 2,
      instanceType: "gpu_1x_a100",
    });

    const all = await provider.list();
    expect(all.length).toBe(2);
  });

  test("list with spec filter", async () => {
    const state = createState();
    const provider = createProvider(state);

    await provider.spawn({
      spec: "gpu:a100:1",
      bootstrap: testBootstrap,
      ...testIdentity,
      instanceId: 1,
      instanceType: "gpu_1x_a100",
    });
    await provider.spawn({
      spec: "gpu:h100:1",
      bootstrap: testBootstrap,
      ...testIdentity,
      instanceId: 2,
      instanceType: "gpu_1x_h100_pcie",
    });

    // Lambda's projectInstance sets spec = instanceTypeName, not the SkyREPL spec string
    const a100Only = await provider.list({ spec: "gpu_1x_a100" });
    expect(a100Only.length).toBe(1);
    expect(a100Only[0]!.spec).toBe("gpu_1x_a100");
  });

  test("list with status filter", async () => {
    const state = createState();
    const provider = createProvider(state);

    await provider.spawn({
      spec: "gpu:a100:1",
      bootstrap: testBootstrap,
      ...testIdentity,
      instanceType: "gpu_1x_a100",
    });

    // Instances start as "booting" → "starting"
    const starting = await provider.list({ status: ["starting"] });
    expect(starting.length).toBe(1);

    const running = await provider.list({ status: ["running"] });
    expect(running.length).toBe(0);
  });

  test("list excludes terminated by default", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "gpu:a100:1",
      bootstrap: testBootstrap,
      ...testIdentity,
      instanceType: "gpu_1x_a100",
    });

    // Mark as terminated in mock state
    state.instances.get(inst.id)!.status = "terminated";

    const results = await provider.list();
    expect(results.length).toBe(0);
  });

  test("get returns instance by ID", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "gpu:a100:1",
      bootstrap: testBootstrap,
      ...testIdentity,
      instanceType: "gpu_1x_a100",
    });

    const found = await provider.get(inst.id);
    expect(found).not.toBeNull();
    expect(found!.id).toBe(inst.id);
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
      spec: "gpu:a100:1",
      bootstrap: testBootstrap,
      ...testIdentity,
      instanceType: "gpu_1x_a100",
    });

    const found = await provider.get(inst.id) as LambdaInstance;
    expect(found).not.toBeNull();
    expect(found!.gpuCount).toBe(1);
    expect(found!.gpuDescription).toBe("A100 80GB SXM4");
    expect(found!.vcpus).toBe(30);
    expect(found!.memoryGib).toBe(200);
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

  test("checksum changes with different configs", () => {
    const state = createState();
    const provider = createProvider(state);

    const r1 = provider.generateBootstrap(testBootstrap);
    const r2 = provider.generateBootstrap({
      ...testBootstrap,
      initScript: "echo hello",
    });

    expect(r1.checksum).not.toBe(r2.checksum);
  });
});

// =============================================================================
// Tests: pricing
// =============================================================================

describe("PROV-054D: pricing", () => {
  test("listAvailableSpecs returns spec list with rates and regions", async () => {
    const state = createState();
    const provider = createProvider(state);

    const specs = await provider.listAvailableSpecs!();

    expect(Array.isArray(specs)).toBe(true);
    // gpu_1x_a100 and gpu_8x_a100 have capacity, gpu_1x_h100_pcie does not
    expect(specs.length).toBeGreaterThanOrEqual(2);

    const a100Spec = specs.find((s: any) => s.name === "gpu_1x_a100");
    expect(a100Spec).toBeDefined();
    expect(a100Spec!.hourlyRate).toBe(1.1); // 110 cents / 100 = $1.10
    expect(a100Spec!.availableRegions).toContain("us-west-1");
  });

  test("getHourlyRate returns rate from cache", async () => {
    const state = createState();
    const provider = createProvider(state);

    // Prime cache by listing first
    await provider.listAvailableSpecs!();

    const rate = await provider.getHourlyRate!("gpu_1x_a100");
    expect(rate).toBe(1.1); // 110 cents / 100 = $1.10
  });
});

// =============================================================================
// Tests: lifecycle hooks
// =============================================================================

describe("PROV-054D: lifecycle hooks", () => {
  test("onStartup verifies API access", async () => {
    const state = createState();
    const provider = createProvider(state);

    const hooks = createLambdaHooks(provider);
    // Should not throw — mock handles /ssh-keys
    await expect(hooks.onStartup!()).resolves.toBeUndefined();
  });

  test("onHeartbeat static responses: table-driven", async () => {
    const state = createState();
    const provider = createProvider(state);
    const hooks = createLambdaHooks(provider);

    const cases: [string, string, string?][] = [
      ["health_check", "completed"],
      ["reconcile", "skipped", "DB access"],
      ["cache_refresh", "completed"],
      ["unknown_task_type", "skipped", "Unknown task type"],
    ];

    for (const [type, expectedStatus, expectedReason] of cases) {
      const result = await hooks.onHeartbeat!({
        tasks: [{ type: type as any, priority: "normal" }],
        deadline: Date.now() + 10_000,
      });
      expect(result.receipts[0]!.type).toBe(type);
      expect(result.receipts[0]!.status).toBe(expectedStatus);
      if (expectedReason) {
        expect(result.receipts[0]!.reason).toContain(expectedReason);
      }
    }
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
      spec: "gpu:a100:1",
      bootstrap: testBootstrap,
      ...testIdentity,
      instanceType: "gpu_1x_a100",
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
