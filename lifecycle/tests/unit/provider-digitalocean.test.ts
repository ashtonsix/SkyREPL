// tests/unit/provider-digitalocean.test.ts - DigitalOcean Provider Tests (#PROV-054C)
//
// Tests the DigitalOceanProvider that uses the DO v2 REST API via fetch().
// Injects a mock fetch via the _fetchImpl option — no real DO API calls.
//
// Mock strategy: _fetchImpl dispatches on URL + method to return canned responses.

import { describe, test, expect, beforeEach } from "bun:test";
import {
  DigitalOceanProvider,
  mapDOStatus,
  mapDOError,
  doRequest,
  createDigitalOceanHooks,
  type DigitalOceanProviderConfig,
} from "../../control/src/provider/compute/digitalocean";
import type {
  Provider,
  SpawnOptions,
  BootstrapConfig,
  ListFilter,
  DigitalOceanInstance,
  DigitalOceanSnapshot,
} from "../../control/src/provider/types";
import { PROVIDER_CAPABILITIES } from "../../control/src/provider/types";
import {
  ProviderOperationError,
  type ProviderOperationErrorCode,
} from "../../control/src/provider/errors";

// =============================================================================
// Mock DO API (fetch dispatch)
// =============================================================================

interface MockState {
  droplets: Map<number, any>;
  snapshots: Map<number, any>;
  actions: Map<number, any>;
  nextDropletId: number;
  nextActionId: number;
  nextSnapshotId: number;
  lastCreateBody?: any;
  account: {
    email: string;
    droplet_limit: number;
    status: string;
  };
  // Error injection
  createError?: { status: number; body: any };
}

function createState(): MockState {
  return {
    droplets: new Map(),
    snapshots: new Map(),
    actions: new Map(),
    nextDropletId: 100000,
    nextActionId: 1,
    nextSnapshotId: 50000,
    account: {
      email: "test@skyrepl.dev",
      droplet_limit: 25,
      status: "active",
    },
  };
}

function createMockFetch(state: MockState): typeof fetch {
  return async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const url = typeof input === "string" ? input : input.toString();
    const method = init?.method ?? "GET";
    const body = init?.body ? JSON.parse(init.body as string) : undefined;

    // Route dispatch
    if (method === "POST" && url.endsWith("/v2/droplets")) {
      return handleCreateDroplet(state, body);
    }
    if (method === "DELETE" && url.match(/\/v2\/droplets\/\d+$/)) {
      return handleDeleteDroplet(state, url);
    }
    if (method === "GET" && url.match(/\/v2\/droplets\/\d+$/)) {
      return handleGetDroplet(state, url);
    }
    if (method === "GET" && url.includes("/v2/droplets?")) {
      return handleListDroplets(state, url);
    }
    if (method === "POST" && url.match(/\/v2\/droplets\/\d+\/actions$/)) {
      return handleDropletAction(state, url, body);
    }
    if (method === "GET" && url.match(/\/v2\/droplets\/\d+\/actions\/\d+$/)) {
      return handleGetAction(state, url);
    }
    if (method === "DELETE" && url.match(/\/v2\/snapshots\/\d+$/)) {
      return handleDeleteSnapshot(state, url);
    }
    if (method === "GET" && url.includes("/v2/snapshots")) {
      return handleListSnapshots(state, url);
    }
    if (method === "GET" && url.endsWith("/v2/account")) {
      return handleGetAccount(state);
    }

    return new Response(JSON.stringify({ id: "not_found", message: "Mock route not found" }), {
      status: 404,
      headers: { "Content-Type": "application/json" },
    });
  };
}

function handleCreateDroplet(state: MockState, body: any): Response {
  if (state.createError) {
    const { status, body: errBody } = state.createError;
    state.createError = undefined;
    return new Response(JSON.stringify(errBody), {
      status,
      headers: { "Content-Type": "application/json" },
    });
  }

  state.lastCreateBody = body;
  const id = state.nextDropletId++;
  const droplet = {
    id,
    name: body.name,
    status: "new",
    size_slug: body.size,
    image: { slug: typeof body.image === "string" ? body.image : undefined, id: body.image },
    region: { slug: body.region },
    vpc_uuid: body.vpc_uuid ?? null,
    tags: body.tags ?? [],
    networks: {
      v4: [],
      v6: [],
    },
    created_at: new Date().toISOString(),
  };
  state.droplets.set(id, droplet);
  return jsonResponse({ droplet }, 202);
}

function handleDeleteDroplet(state: MockState, url: string): Response {
  const id = extractId(url);
  if (!state.droplets.has(id)) {
    return jsonResponse({ id: "not_found", message: "Droplet not found" }, 404);
  }
  state.droplets.delete(id);
  return new Response(null, { status: 204 });
}

function handleGetDroplet(state: MockState, url: string): Response {
  const id = extractId(url);
  const droplet = state.droplets.get(id);
  if (!droplet) {
    return jsonResponse({ id: "not_found", message: "Droplet not found" }, 404);
  }
  return jsonResponse({ droplet });
}

function handleListDroplets(state: MockState, url: string): Response {
  const params = new URL(url).searchParams;
  const tagName = params.get("tag_name");
  const perPage = parseInt(params.get("per_page") ?? "100");
  const page = parseInt(params.get("page") ?? "1");

  let droplets = Array.from(state.droplets.values());

  // Filter by tag
  if (tagName) {
    droplets = droplets.filter((d: any) => d.tags?.includes(tagName));
  }

  const total = droplets.length;
  const start = (page - 1) * perPage;
  const slice = droplets.slice(start, start + perPage);

  const links: any = { pages: {} };
  if (start + perPage < total) {
    links.pages.next = `https://api.digitalocean.com/v2/droplets?page=${page + 1}&per_page=${perPage}`;
  }

  return jsonResponse({ droplets: slice, meta: { total }, links });
}

function handleDropletAction(state: MockState, url: string, body: any): Response {
  const dropletId = extractId(url);
  if (!state.droplets.has(dropletId)) {
    return jsonResponse({ id: "not_found", message: "Droplet not found" }, 404);
  }

  const actionId = state.nextActionId++;
  const action = {
    id: actionId,
    type: body.type,
    status: "in-progress",
    resource_id: dropletId,
    resource_type: "droplet",
    started_at: new Date().toISOString(),
  };

  // If it's a snapshot action, create a snapshot too
  if (body.type === "snapshot") {
    const snapId = state.nextSnapshotId++;
    state.snapshots.set(snapId, {
      id: snapId,
      name: body.name ?? "unnamed",
      regions: ["nyc3"],
      min_disk_size: 25,
      size_gigabytes: 2.5,
      created_at: new Date().toISOString(),
      resource_type: "droplet",
    });
    // Mark action completed with snapshot reference
    action.status = "completed";
  }

  state.actions.set(actionId, action);
  return jsonResponse({ action });
}

function handleGetAction(state: MockState, url: string): Response {
  // URL: /v2/droplets/:dropletId/actions/:actionId
  const parts = url.split("/");
  const actionId = parseInt(parts[parts.length - 1]!);
  const action = state.actions.get(actionId);
  if (!action) {
    return jsonResponse({ id: "not_found", message: "Action not found" }, 404);
  }
  return jsonResponse({ action });
}

function handleDeleteSnapshot(state: MockState, url: string): Response {
  const id = extractId(url);
  if (!state.snapshots.has(id)) {
    return jsonResponse({ id: "not_found", message: "Snapshot not found" }, 404);
  }
  state.snapshots.delete(id);
  return new Response(null, { status: 204 });
}

function handleListSnapshots(state: MockState, _url: string): Response {
  return jsonResponse({ snapshots: Array.from(state.snapshots.values()) });
}

function handleGetAccount(state: MockState): Response {
  return jsonResponse({ account: state.account });
}

function extractId(url: string): number {
  const parts = url.split("/");
  // Find the numeric part — handles /v2/droplets/123 and /v2/droplets/123/actions
  for (let i = parts.length - 1; i >= 0; i--) {
    const n = parseInt(parts[i]!);
    if (!isNaN(n)) return n;
  }
  return -1;
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
  overrides: Partial<DigitalOceanProviderConfig> = {},
): DigitalOceanProvider {
  return new DigitalOceanProvider({
    token: "test-token-abc",
    defaultRegion: "nyc3",
    defaultSizeSlug: "s-2vcpu-4gb",
    defaultSshKeyIds: [54322644],
    _fetchImpl: createMockFetch(state),
    ...overrides,
  });
}

const testBootstrap: BootstrapConfig = {
  agentUrl: "http://localhost:3000/v1/agent/download/agent.py",
  controlPlaneUrl: "http://localhost:3000",
  registrationToken: "test-token-abc",
};

const testIdentity = { controlId: "test01", manifestId: 1, instanceId: 1 };

// =============================================================================
// Tests: provider interface satisfaction
// =============================================================================

describe("PROV-054C: DigitalOcean provider type satisfaction", () => {
  test("satisfies Provider interface with correct capabilities", () => {
    const state = createState();
    const provider = createProvider(state);

    // Type-level: Provider assignment proves interface conformance
    const p: Provider = provider;
    expect(p.name).toBe("digitalocean");

    // Capabilities — toEqual checks every field at once
    expect(provider.capabilities).toEqual(PROVIDER_CAPABILITIES.digitalocean);
  });
});

// =============================================================================
// Tests: mapDOStatus
// =============================================================================

describe("PROV-054C: mapDOStatus", () => {
  test("maps all DO statuses to correct ProviderInstanceStatus", () => {
    const cases: [string, string][] = [
      ["new", "pending"],
      ["active", "running"],
      ["off", "stopped"],
      ["archive", "terminated"],
      ["unknown", "error"],
      ["", "error"],
    ];
    for (const [input, expected] of cases) {
      expect(mapDOStatus(input)).toBe(expected);
    }
  });
});

// =============================================================================
// Tests: mapDOError
// =============================================================================

describe("PROV-054C: mapDOError", () => {
  test("maps all HTTP error statuses to correct error code and retryable flag", () => {
    const cases: [number, string, boolean][] = [
      [401, "AUTH_ERROR", false],
      [403, "AUTH_ERROR", false],
      [404, "NOT_FOUND", false],
      [422, "INVALID_SPEC", false],
      [429, "RATE_LIMIT_ERROR", true],
      [500, "PROVIDER_INTERNAL", true],
      [503, "PROVIDER_INTERNAL", true],
      [418, "PROVIDER_INTERNAL", false],  // unknown status → default
    ];
    for (const [status, code, retryable] of cases) {
      const err = mapDOError(status, { id: "test", message: "test" });
      expect(err).toBeInstanceOf(ProviderOperationError);
      expect(err.code).toBe(code);
      expect(err.retryable).toBe(retryable);
    }
  });
});

// =============================================================================
// Tests: doRequest
// =============================================================================

describe("PROV-054C: doRequest", () => {
  test("parses 2xx JSON responses correctly", async () => {
    const mockFetch = async () => jsonResponse({ droplet: { id: 1 } });
    const result = await doRequest<{ droplet: any }>("token", "GET", "/v2/droplets/1", undefined, mockFetch as typeof fetch);
    expect(result.droplet.id).toBe(1);
  });

  test("returns null for 204 No Content", async () => {
    const mockFetch = async () => new Response(null, { status: 204 });
    const result = await doRequest<null>("token", "DELETE", "/v2/droplets/1", undefined, mockFetch as typeof fetch);
    expect(result).toBeNull();
  });

  test("throws mapped ProviderOperationError for non-2xx response", async () => {
    // Single test proves doRequest → mapDOError wiring; exhaustive status mapping tested in mapDOError
    const mockFetch = async () => jsonResponse({ id: "unauthorized", message: "Bad token" }, 401);
    await expect(
      doRequest("token", "GET", "/v2/account", undefined, mockFetch as typeof fetch),
    ).rejects.toMatchObject({ code: "AUTH_ERROR" });
  });
});

// =============================================================================
// Tests: spawn
// =============================================================================

describe("PROV-054C: spawn", () => {
  test("spawn creates droplet with correct POST body", async () => {
    const state = createState();
    const provider = createProvider(state);

    const result = await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
    });

    expect(result.id).toBeDefined();
    expect(result.status).toBe("pending"); // "new" maps to "pending"
    expect(result.isSpot).toBe(false);
    expect(result.dropletId).toBeGreaterThan(0);

    // Verify request body
    expect(state.lastCreateBody!.region).toBe("nyc3");
    expect(state.lastCreateBody!.size).toBe("s-2vcpu-4gb");
    expect(state.lastCreateBody!.image).toBe("ubuntu-24-04-x64");
    expect(state.lastCreateBody!.ssh_keys).toEqual([54322644]);
    expect(state.lastCreateBody!.user_data).toContain("#cloud-config");
  });

  test("spawn sets correct tags including skyrepl metadata", async () => {
    const state = createState();
    const provider = createProvider(state);

    await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
    });

    const tags: string[] = state.lastCreateBody!.tags;
    expect(tags).toContain("skyrepl");
    expect(tags.find((t: string) => t.startsWith("skyrepl-spec:"))).toBe("skyrepl-spec:ubuntu:noble:arm64");
    expect(tags.find((t: string) => t.startsWith("skyrepl-id:"))).toBe("skyrepl-id:1");
    expect(tags.find((t: string) => t.startsWith("skyrepl-key:"))).toBeDefined();
  });

  test("spawn uses naming convention for droplet name", async () => {
    const state = createState();
    const provider = createProvider(state);

    await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      controlId: "abc123",
      manifestId: 7,
      instanceId: 42,
    });

    expect(state.lastCreateBody!.name).toBe("repl-abc123-7-16"); // 42 in base36 = "16"
  });

  test("spawn with instanceType overrides default size slug", async () => {
    const state = createState();
    const provider = createProvider(state);

    await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
      instanceType: "g-32vcpu-128gb",
    });

    expect(state.lastCreateBody!.size).toBe("g-32vcpu-128gb");
  });

  test("spawn with snapshotId uses it as image (numeric)", async () => {
    const state = createState();
    const provider = createProvider(state);

    await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
      snapshotId: "12345678",
    });

    expect(state.lastCreateBody!.image).toBe(12345678);
  });

  test("spawn with snapshotId uses it as image (string slug)", async () => {
    const state = createState();
    const provider = createProvider(state);

    await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
      snapshotId: "ubuntu-24-04-x64",
    });

    expect(state.lastCreateBody!.image).toBe("ubuntu-24-04-x64");
  });

  test("spawn with VPC config sets vpc_uuid", async () => {
    const state = createState();
    const provider = createProvider(state);

    await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
      networkConfig: { vpcId: "vpc-uuid-123" },
    });

    expect(state.lastCreateBody!.vpc_uuid).toBe("vpc-uuid-123");
  });

  test("spawn with region override uses specified region", async () => {
    const state = createState();
    const provider = createProvider(state);

    await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
      region: "sfo3",
    });

    expect(state.lastCreateBody!.region).toBe("sfo3");
  });

  test("spawn cloud-init is plain UTF-8 (not base64)", async () => {
    const state = createState();
    const provider = createProvider(state);

    await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
    });

    const userData = state.lastCreateBody!.user_data as string;
    // Should be plain UTF-8, starting with #cloud-config
    expect(userData).toContain("#cloud-config");
    // Should NOT be base64 (base64 would not contain '#')
    expect(userData.startsWith("#")).toBe(true);
  });

  test("spawn with manifestId null uses 'none' in naming", async () => {
    const state = createState();
    const provider = createProvider(state);

    await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      controlId: "test01",
      manifestId: null,
      instanceId: 5,
    });

    expect(state.lastCreateBody!.name).toBe("repl-test01-none-5");
  });
});

// =============================================================================
// Tests: terminate
// =============================================================================

describe("PROV-054C: terminate", () => {
  test("terminate deletes droplet", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
    });

    await provider.terminate(inst.id);
    expect(state.droplets.has(inst.dropletId)).toBe(false);
  });

  test("terminate is idempotent (double-terminate does not throw)", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
    });

    await provider.terminate(inst.id);
    await expect(provider.terminate(inst.id)).resolves.toBeUndefined();
  });
});

// =============================================================================
// Tests: list and get
// =============================================================================

describe("PROV-054C: list and get", () => {
  test("list returns all skyrepl-tagged droplets", async () => {
    const state = createState();
    const provider = createProvider(state);

    await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
      instanceId: 1,
    });
    await provider.spawn({
      spec: "ubuntu:noble:amd64",
      bootstrap: testBootstrap,
      ...testIdentity,
      instanceId: 2,
    });

    const all = await provider.list();
    expect(all.length).toBe(2);
  });

  test("list with spec filter applies client-side filtering", async () => {
    const state = createState();
    const provider = createProvider(state);

    await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
      instanceId: 1,
    });
    await provider.spawn({
      spec: "ubuntu:jammy:amd64",
      bootstrap: testBootstrap,
      ...testIdentity,
      instanceId: 2,
    });

    const arm64Only = await provider.list({ spec: "ubuntu:noble:arm64" });
    expect(arm64Only.length).toBe(1);
    expect(arm64Only[0]!.spec).toBe("ubuntu:noble:arm64");
  });

  test("list excludes terminated droplets by default", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
    });

    // Mark as archived (terminated)
    state.droplets.get(inst.dropletId)!.status = "archive";

    const results = await provider.list();
    expect(results.length).toBe(0);
  });

  test("list includes terminated with includeTerminated flag", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
    });

    state.droplets.get(inst.dropletId)!.status = "archive";

    const results = await provider.list({ includeTerminated: true });
    expect(results.length).toBe(1);
  });

  test("list with status filter", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
    });

    // Droplets start as "new" → "pending"
    const pending = await provider.list({ status: ["pending"] });
    expect(pending.length).toBe(1);

    const running = await provider.list({ status: ["running"] });
    expect(running.length).toBe(0);
  });

  test("list with region filter", async () => {
    const state = createState();
    const provider = createProvider(state);

    await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
      instanceId: 1,
      region: "nyc3",
    });

    const nyc = await provider.list({ region: "nyc3" });
    expect(nyc.length).toBe(1);

    const sfo = await provider.list({ region: "sfo3" });
    expect(sfo.length).toBe(0);
  });

  test("list with limit", async () => {
    const state = createState();
    const provider = createProvider(state);

    for (let i = 1; i <= 5; i++) {
      await provider.spawn({
        spec: "ubuntu:noble:arm64",
        bootstrap: testBootstrap,
        ...testIdentity,
        instanceId: i,
      });
    }

    const limited = await provider.list({ limit: 3 });
    expect(limited.length).toBe(3);
  });

  test("get retrieves droplet by ID", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
    });

    const found = await provider.get(inst.id);
    expect(found).not.toBeNull();
    expect(found!.id).toBe(inst.id);
    expect(found!.sizeSlug).toBe("s-2vcpu-4gb");
  });

  test("get returns null for non-existent droplet", async () => {
    const state = createState();
    const provider = createProvider(state);

    const result = await provider.get("999999");
    expect(result).toBeNull();
  });

  test("get reads spec from skyrepl-spec tag", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
    });

    // get() doesn't pass originalSpec, so it reads from the tag
    const fetched = await provider.get(inst.id);
    expect(fetched!.spec).toBe("ubuntu:noble:arm64");
  });

  test("list paginates across multiple pages", async () => {
    // Provider hardcodes per_page=100 and breaks when length < per_page,
    // so we must return exactly 100 items on page 1 to force a second request.
    const makeDroplet = (id: number) => ({
      id, name: `test-${id}`, status: "active",
      size_slug: "s-1vcpu-1gb", image: { slug: "ubuntu-24-04-x64" },
      region: { slug: "nyc3" }, tags: ["skyrepl", `skyrepl-spec:gpu-small`],
      networks: { v4: [], v6: [] }, created_at: new Date().toISOString(),
    });
    const page1 = Array.from({ length: 100 }, (_, i) => makeDroplet(i + 1));
    const page2 = [makeDroplet(101), makeDroplet(102)];
    let requestCount = 0;

    const paginatingFetch: typeof fetch = async (input) => {
      const url = typeof input === "string" ? input : input.toString();
      if (url.includes("/v2/droplets?")) {
        requestCount++;
        const params = new URL(url).searchParams;
        const page = parseInt(params.get("page") ?? "1");
        if (page === 1) {
          return jsonResponse({
            droplets: page1, meta: { total: 102 },
            links: { pages: { next: "https://api.digitalocean.com/v2/droplets?page=2&per_page=100" } },
          });
        }
        return jsonResponse({ droplets: page2, meta: { total: 102 }, links: { pages: {} } });
      }
      return jsonResponse({ id: "not_found", message: "Mock route not found" }, 404);
    };

    const provider = new DigitalOceanProvider({
      token: "test-token-abc", defaultRegion: "nyc3",
      defaultSshKeyIds: [54322644], _fetchImpl: paginatingFetch as typeof fetch,
    });

    const all = await provider.list();
    expect(all.length).toBe(102);
    expect(requestCount).toBe(2); // Proves pagination loop iterated
  });
});

// =============================================================================
// Tests: generateBootstrap
// =============================================================================

describe("PROV-054C: generateBootstrap", () => {
  test("delegates to assembleCloudInit, returns cloud-init format", () => {
    const state = createState();
    const provider = createProvider(state);

    const result = provider.generateBootstrap(testBootstrap);

    expect(result.format).toBe("cloud-init");
    expect(result.content).toContain("#cloud-config");
    expect(result.checksum).toHaveLength(64);
  });

  test("cloud-init includes control plane URL and registration token", () => {
    const state = createState();
    const provider = createProvider(state);

    const result = provider.generateBootstrap(testBootstrap);

    expect(result.content).toContain("http://localhost:3000");
    expect(result.content).toContain("test-token-abc");
  });

  test("cloud-init content changes with different configs (checksum changes)", () => {
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
// Tests: snapshots
// =============================================================================

describe("PROV-054C: snapshots", () => {
  test("createSnapshot returns requestId in dropletId:actionId format", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
    });

    const req = await provider.createSnapshot!(inst.id, {
      name: "test-snapshot",
      description: "Test",
    });

    expect(req.requestId).toMatch(/^\d+:\d+$/);
    expect(req.status.status).toBe("creating");
  });

  test("getSnapshotStatus returns available for completed action", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
    });

    const req = await provider.createSnapshot!(inst.id, { name: "snap" });
    const status = await provider.getSnapshotStatus!(req.requestId);

    expect(status.status).toBe("available");
  });

  test("getSnapshotStatus returns creating for in-progress action", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
    });

    const req = await provider.createSnapshot!(inst.id, { name: "snap" });
    // Force the action back to in-progress
    const [, actionId] = req.requestId.split(":");
    state.actions.get(parseInt(actionId!))!.status = "in-progress";

    const status = await provider.getSnapshotStatus!(req.requestId);
    expect(status.status).toBe("creating");
  });

  test("getSnapshotStatus returns failed for errored action", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
    });

    const req = await provider.createSnapshot!(inst.id, { name: "snap" });
    // Force the action to errored
    const [, actionId] = req.requestId.split(":");
    state.actions.get(parseInt(actionId!))!.status = "errored";

    const status = await provider.getSnapshotStatus!(req.requestId);
    expect(status.status).toBe("failed");
    expect(status.error).toContain("Snapshot action failed");
  });

  test("getSnapshotStatus returns failed for invalid requestId format", async () => {
    const state = createState();
    const provider = createProvider(state);

    const status = await provider.getSnapshotStatus!("invalid-format");
    expect(status.status).toBe("failed");
    expect(status.error).toContain("Invalid requestId format");
  });

  test("deleteSnapshot is idempotent (second call does not throw)", async () => {
    const state = createState();
    const provider = createProvider(state);

    // Add a snapshot manually
    state.snapshots.set(12345, {
      id: 12345,
      name: "test",
      regions: ["nyc3"],
      min_disk_size: 25,
      size_gigabytes: 2.5,
      created_at: new Date().toISOString(),
      resource_type: "droplet",
    });

    await provider.deleteSnapshot!("12345");
    // Second call: snapshot is gone, should be silently ignored (404 swallowed)
    await expect(provider.deleteSnapshot!("12345")).resolves.toBeUndefined();
  });

  test("getSnapshotByName returns matching snapshot", async () => {
    const state = createState();
    const provider = createProvider(state);

    state.snapshots.set(99999, {
      id: 99999,
      name: "my-snapshot",
      regions: ["nyc3", "sfo3"],
      min_disk_size: 25,
      size_gigabytes: 5,
      created_at: new Date().toISOString(),
      resource_type: "droplet",
    });

    const snap = await provider.getSnapshotByName!("my-snapshot");
    expect(snap).not.toBeNull();
    expect(snap!.name).toBe("my-snapshot");
    expect(snap!.regions).toContain("nyc3");
  });

  test("getSnapshotByName returns null for non-existent snapshot", async () => {
    const state = createState();
    const provider = createProvider(state);

    const snap = await provider.getSnapshotByName!("nonexistent");
    expect(snap).toBeNull();
  });
});

// =============================================================================
// Tests: error injection
// =============================================================================

describe("PROV-054C: spawn error propagation", () => {
  test("spawn propagates non-retryable errors (AUTH_ERROR)", async () => {
    const state = createState();
    const provider = createProvider(state);

    state.createError = {
      status: 401,
      body: { id: "unauthorized", message: "Invalid token" },
    };

    await expect(
      provider.spawn({
        spec: "ubuntu:noble:arm64",
        bootstrap: testBootstrap,
        ...testIdentity,
      }),
    ).rejects.toMatchObject({ code: "AUTH_ERROR", retryable: false });
  });

  test("spawn propagates retryable errors with retryable=true (RATE_LIMIT_ERROR)", async () => {
    const state = createState();
    const provider = createProvider(state);

    state.createError = {
      status: 429,
      body: { id: "too_many_requests", message: "Rate limited" },
    };

    await expect(
      provider.spawn({
        spec: "ubuntu:noble:arm64",
        bootstrap: testBootstrap,
        ...testIdentity,
      }),
    ).rejects.toMatchObject({ code: "RATE_LIMIT_ERROR", retryable: true });
  });
});

// =============================================================================
// Tests: lifecycle hooks
// =============================================================================

describe("PROV-054C: lifecycle hooks", () => {
  test("onStartup calls verifyAccount", async () => {
    const state = createState();
    const provider = createProvider(state);

    let verified = false;
    (provider as any).verifyAccount = async () => {
      verified = true;
      return { email: "test@test.com", dropletLimit: 25, status: "active" };
    };

    const hooks = createDigitalOceanHooks(provider);
    await hooks.onStartup!();

    expect(verified).toBe(true);
  });

  test("onHeartbeat health_check returns completed receipt with latencyMs", async () => {
    const state = createState();
    const provider = createProvider(state);

    const hooks = createDigitalOceanHooks(provider);
    const result = await hooks.onHeartbeat!({
      tasks: [{ type: "health_check", priority: "high" }],
      deadline: Date.now() + 10_000,
    });

    expect(result.receipts).toHaveLength(1);
    expect(result.receipts[0]!.type).toBe("health_check");
    expect(result.receipts[0]!.status).toBe("completed");
    expect((result.receipts[0]!.result as any).latencyMs).toBeGreaterThanOrEqual(0);
  });

  test("onHeartbeat static task responses: reconcile skipped, cache_refresh completed, unknown skipped", async () => {
    const state = createState();
    const provider = createProvider(state);
    const hooks = createDigitalOceanHooks(provider);

    const cases: [string, string, string?][] = [
      ["reconcile", "skipped", "DB access"],
      ["cache_refresh", "completed"],
      ["some_unknown_task", "skipped", "Unknown task type"],
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

describe("PROV-054C: instance mapping", () => {
  test("maps droplet fields to DigitalOceanInstance correctly", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
    });

    // Simulate active status with networks populated
    const droplet = state.droplets.get(inst.dropletId)!;
    droplet.status = "active";
    droplet.networks = {
      v4: [
        { ip_address: "10.0.0.5", type: "private" },
        { ip_address: "165.232.44.100", type: "public" },
      ],
      v6: [],
    };

    const fetched = await provider.get(inst.id);
    expect(fetched).not.toBeNull();
    expect(fetched!.status).toBe("running");
    expect(fetched!.ip).toBe("165.232.44.100");
    expect(fetched!.privateIp).toBe("10.0.0.5");
    expect(fetched!.isSpot).toBe(false);
    expect(fetched!.region).toBe("nyc3");
    expect(fetched!.tags).toContain("skyrepl");
    expect(fetched!.dropletId).toBe(inst.dropletId);
  });

  test("numeric DO ID round-trips as string", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
    });

    // ID should be a string
    expect(typeof inst.id).toBe("string");
    expect(inst.id).toBe(String(inst.dropletId));

    // Round-trip: pass string ID back to get()
    const fetched = await provider.get(inst.id);
    expect(fetched!.id).toBe(inst.id);
  });
});

// =============================================================================
// Tests: registry
// =============================================================================

describe("PROV-054C: registry", () => {
  test("getProvider('digitalocean') resolves and returns provider with name 'digitalocean'", async () => {
    const { getProvider, clearProviderCache } = await import(
      "../../control/src/provider/registry"
    );
    clearProviderCache();

    const provider = await getProvider("digitalocean");

    expect(provider).toBeDefined();
    expect(provider.name).toBe("digitalocean");
    expect(typeof provider.spawn).toBe("function");
  });

});
