// tests/unit/provider-aws.test.ts - AWS Provider Validation Tests (#PROV-01)
//
// Validates the Provider interface against an AWS EC2 implementation.
// Uses a mock EC2 client — no real AWS account needed.

import { describe, test, expect, beforeEach } from "bun:test";
import {
  AWSProvider,
  mapEC2Error,
  type EC2Client,
  type AWSInstance,
  type AWSProviderConfig,
  type RunInstancesParams,
  type RunInstancesResult,
  type DescribeInstancesParams,
  type DescribeInstancesResult,
} from "../../control/src/provider/compute/aws";
import type {
  Provider,
  SpawnOptions,
  BootstrapConfig,
  ListFilter,
} from "../../control/src/provider/types";
import { PROVIDER_CAPABILITIES } from "../../control/src/provider/types";
import { ProviderError } from "../../control/src/provider/errors";

// =============================================================================
// Mock EC2 Client
// =============================================================================

function createMockEC2(): EC2Client & {
  _lastRunParams?: RunInstancesParams;
  _instances: Map<string, any>;
  _images: Map<string, any>;
  _nextInstanceId: number;
} {
  const instances = new Map<string, any>();
  const images = new Map<string, any>();
  let nextInstanceId = 1;

  const mock: ReturnType<typeof createMockEC2> = {
    _instances: instances,
    _images: images,
    _nextInstanceId: 1,

    async runInstances(params: RunInstancesParams): Promise<RunInstancesResult> {
      mock._lastRunParams = params;
      const id = `i-${String(nextInstanceId++).padStart(17, "0")}`;
      const inst = {
        instanceId: id,
        instanceType: params.instanceType,
        state: "pending",
        privateIpAddress: "10.0.1.100",
        publicIpAddress: null,
        availabilityZone: "us-east-1a",
        subnetId: params.subnetId,
        vpcId: "vpc-123",
        securityGroups: (params.securityGroupIds ?? []).map((g) => ({ groupId: g })),
        launchTime: Date.now(),
        spotInstanceRequestId: params.instanceMarketOptions ? `sir-${id}` : undefined,
      };
      instances.set(id, inst);
      return { instances: [inst] };
    },

    async terminateInstances(instanceIds: string[]): Promise<void> {
      for (const id of instanceIds) {
        if (!instances.has(id)) {
          throw Object.assign(new Error(`Instance ${id} not found`), { code: "InvalidInstanceID.NotFound" });
        }
        const inst = instances.get(id)!;
        inst.state = "shutting-down";
      }
    },

    async describeInstances(params: DescribeInstancesParams): Promise<DescribeInstancesResult> {
      let results = Array.from(instances.values());

      if (params.instanceIds) {
        results = results.filter((i) => params.instanceIds!.includes(i.instanceId));
        if (results.length === 0 && params.instanceIds.length > 0) {
          throw Object.assign(new Error("Not found"), { code: "InvalidInstanceID.NotFound" });
        }
      }

      if (params.filters) {
        for (const filter of params.filters) {
          if (filter.name === "instance-state-name") {
            results = results.filter((i) => filter.values.includes(i.state));
          }
        }
      }

      if (params.maxResults) {
        results = results.slice(0, params.maxResults);
      }

      return { reservations: [{ instances: results }] };
    },

    async createImage(params: any): Promise<{ imageId: string }> {
      const imageId = `ami-${Date.now()}`;
      images.set(imageId, {
        imageId,
        name: params.name,
        state: "available",
        architecture: "x86_64",
        blockDeviceMappings: [{ ebs: { snapshotId: `snap-${imageId}`, volumeSize: 8 } }],
      });
      return { imageId };
    },

    async describeImages(imageIds: string[]): Promise<any> {
      const found = imageIds.map((id) => images.get(id)).filter(Boolean);
      return { images: found };
    },

    async deregisterImage(imageId: string): Promise<void> {
      if (!images.has(imageId)) {
        throw Object.assign(new Error("Not found"), { code: "InvalidAMIID.NotFound" });
      }
      images.delete(imageId);
    },

    async deleteSnapshot(_snapshotId: string): Promise<void> {
      // No-op in mock
    },

    async requestSpotInstances(_params: any): Promise<{ requestId: string }> {
      return { requestId: `sir-${Date.now()}` };
    },

    async cancelSpotInstanceRequests(_requestIds: string[]): Promise<void> {
      // No-op in mock
    },

    async describeSpotPriceHistory(params: any): Promise<any> {
      return {
        spotPriceHistory: [
          {
            instanceType: params.instanceTypes[0],
            availabilityZone: "us-east-1a",
            spotPrice: "0.0035",
            timestamp: Date.now(),
            productDescription: "Linux/UNIX",
          },
        ],
      };
    },
  };

  return mock;
}

// =============================================================================
// Test Helpers
// =============================================================================

function createProvider(ec2?: EC2Client): AWSProvider {
  return new AWSProvider({
    ec2: ec2 ?? createMockEC2(),
    region: "us-east-1",
    defaultSubnetId: "subnet-abc",
    defaultSecurityGroupIds: ["sg-123"],
    defaultKeyName: "repl-key",
    defaultIamProfile: "repl-instance-role",
  });
}

const testBootstrap: BootstrapConfig = {
  agentUrl: "http://localhost:3000/v1/agent/download/agent.py",
  controlPlaneUrl: "http://localhost:3000",
  registrationToken: "test-token-abc",
};

// =============================================================================
// Tests
// =============================================================================

describe("PROV-01: AWS provider type satisfaction", () => {
  test("AWSProvider satisfies Provider interface", () => {
    const provider = createProvider();

    // Type-level check: AWSProvider implements Provider<AWSInstance, ...>
    const p: Provider = provider;
    expect(p.name).toBe("aws");
    expect(typeof p.spawn).toBe("function");
    expect(typeof p.terminate).toBe("function");
    expect(typeof p.list).toBe("function");
    expect(typeof p.get).toBe("function");
    expect(typeof p.generateBootstrap).toBe("function");
  });

  test("capabilities match PROVIDER_CAPABILITIES.aws", () => {
    const provider = createProvider();
    expect(provider.capabilities).toEqual(PROVIDER_CAPABILITIES.aws);
    expect(provider.capabilities.snapshots).toBe(true);
    expect(provider.capabilities.spot).toBe(true);
    expect(provider.capabilities.gpu).toBe(true);
    expect(provider.capabilities.multiRegion).toBe(true);
    expect(provider.capabilities.idempotentSpawn).toBe(true);
    expect(provider.capabilities.customNetworking).toBe(true);
  });

  test("optional methods present for declared capabilities", () => {
    const provider = createProvider();

    // snapshots: true → createSnapshot, getSnapshotStatus, deleteSnapshot
    expect(typeof provider.createSnapshot).toBe("function");
    expect(typeof provider.getSnapshotStatus).toBe("function");
    expect(typeof provider.deleteSnapshot).toBe("function");

    // spot: true → requestSpotInstance, cancelSpotRequest, getSpotPrices
    expect(typeof provider.requestSpotInstance).toBe("function");
    expect(typeof provider.cancelSpotRequest).toBe("function");
    expect(typeof provider.getSpotPrices).toBe("function");
  });
});

describe("PROV-01: spawn", () => {
  test("spawn creates instance with correct params", async () => {
    const ec2 = createMockEC2();
    const provider = createProvider(ec2);

    const result = await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      clientRequestId: "idem-key-123",
      tags: { environment: "test" },
    });

    expect(result.id).toMatch(/^i-/);
    expect(result.status).toBe("pending");
    expect(result.instanceType).toBe("t4g.micro");
    expect(result.isSpot).toBe(false);
    expect(result.availabilityZone).toBe("us-east-1a");

    // Verify EC2 params
    expect(ec2._lastRunParams!.clientToken).toBe("idem-key-123");
    expect(ec2._lastRunParams!.subnetId).toBe("subnet-abc");
    expect(ec2._lastRunParams!.securityGroupIds).toEqual(["sg-123"]);
    expect(ec2._lastRunParams!.userData).toBeTruthy();
    expect(ec2._lastRunParams!.tagSpecifications![0].tags["repl-managed"]).toBe("true");
    expect(ec2._lastRunParams!.tagSpecifications![0].tags["environment"]).toBe("test");
  });

  test("spawn with spot option sets market options", async () => {
    const ec2 = createMockEC2();
    const provider = createProvider(ec2);

    const result = await provider.spawn({
      spec: "ubuntu:noble:amd64",
      bootstrap: testBootstrap,
      spot: true,
      maxSpotPrice: 0.05,
    });

    expect(result.isSpot).toBe(true);
    expect(ec2._lastRunParams!.instanceMarketOptions).toEqual({
      marketType: "spot",
      spotOptions: { maxPrice: "0.05" },
    });
  });

  test("spawn with network config overrides defaults", async () => {
    const ec2 = createMockEC2();
    const provider = createProvider(ec2);

    await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      networkConfig: {
        subnetId: "subnet-custom",
        securityGroupIds: ["sg-custom-1", "sg-custom-2"],
      },
    });

    expect(ec2._lastRunParams!.subnetId).toBe("subnet-custom");
    expect(ec2._lastRunParams!.securityGroupIds).toEqual(["sg-custom-1", "sg-custom-2"]);
  });

  test("spawn with snapshot uses snapshotId as AMI", async () => {
    const ec2 = createMockEC2();
    const provider = createProvider(ec2);

    await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      snapshotId: "ami-custom-snapshot",
    });

    expect(ec2._lastRunParams!.imageId).toBe("ami-custom-snapshot");
  });
});

describe("PROV-01: terminate", () => {
  test("terminate calls EC2 terminateInstances", async () => {
    const ec2 = createMockEC2();
    const provider = createProvider(ec2);

    const inst = await provider.spawn({ spec: "ubuntu:noble:arm64", bootstrap: testBootstrap });
    await provider.terminate(inst.id);

    expect(ec2._instances.get(inst.id)!.state).toBe("shutting-down");
  });

  test("terminate is idempotent (not found → silent)", async () => {
    const provider = createProvider();
    // Should not throw
    await provider.terminate("i-nonexistent");
  });
});

describe("PROV-01: list and get", () => {
  test("list returns repl-managed instances", async () => {
    const ec2 = createMockEC2();
    const provider = createProvider(ec2);

    await provider.spawn({ spec: "ubuntu:noble:arm64", bootstrap: testBootstrap });
    await provider.spawn({ spec: "ubuntu:noble:amd64", bootstrap: testBootstrap });

    const all = await provider.list();
    expect(all.length).toBe(2);
    expect(all.every((i) => i.id.startsWith("i-"))).toBe(true);
  });

  test("get returns specific instance", async () => {
    const ec2 = createMockEC2();
    const provider = createProvider(ec2);

    const spawned = await provider.spawn({ spec: "ubuntu:noble:arm64", bootstrap: testBootstrap });
    const found = await provider.get(spawned.id);

    expect(found).toBeTruthy();
    expect(found!.id).toBe(spawned.id);
    expect(found!.instanceType).toBe("t4g.micro");
  });

  test("get returns null for nonexistent", async () => {
    const provider = createProvider();
    const found = await provider.get("i-nonexistent");
    expect(found).toBeNull();
  });
});

describe("PROV-01: generateBootstrap", () => {
  test("produces cloud-init format with required variables", () => {
    const provider = createProvider();
    const script = provider.generateBootstrap(testBootstrap);

    expect(script.format).toBe("cloud-init");
    expect(script.content).toContain("#!/bin/bash");
    expect(script.content).toContain("SKYREPL_CONTROL_PLANE_URL");
    expect(script.content).toContain("SKYREPL_REGISTRATION_TOKEN");
    expect(script.content).toContain("agent.py");
    expect(script.checksum).toHaveLength(64); // SHA-256 hex
  });

  test("includes environment variables", () => {
    const provider = createProvider();
    const script = provider.generateBootstrap({
      ...testBootstrap,
      environment: { MY_VAR: "hello", GPU_COUNT: "4" },
    });

    expect(script.content).toContain('export MY_VAR="hello"');
    expect(script.content).toContain('export GPU_COUNT="4"');
  });

  test("includes feature installations", () => {
    const provider = createProvider();
    const script = provider.generateBootstrap({
      ...testBootstrap,
      features: [{ name: "nvidia-driver" }, { name: "docker" }],
    });

    expect(script.content).toContain("nvidia-driver");
    expect(script.content).toContain("docker");
  });
});

describe("PROV-01: snapshots", () => {
  test("createSnapshot creates AMI", async () => {
    const ec2 = createMockEC2();
    const provider = createProvider(ec2);

    const inst = await provider.spawn({ spec: "ubuntu:noble:arm64", bootstrap: testBootstrap });
    const req = await provider.createSnapshot!(inst.id, {
      name: "test-snapshot",
      description: "Test",
    });

    expect(req.requestId).toMatch(/^ami-/);
    expect(req.status.status).toBe("creating");
  });

  test("getSnapshotStatus returns available for complete AMI", async () => {
    const ec2 = createMockEC2();
    const provider = createProvider(ec2);

    const inst = await provider.spawn({ spec: "ubuntu:noble:arm64", bootstrap: testBootstrap });
    const req = await provider.createSnapshot!(inst.id, { name: "snap-test" });

    const status = await provider.getSnapshotStatus!(req.requestId);
    expect(status.status).toBe("available");
    expect(status.providerSnapshotId).toBe(req.requestId);
    expect(status.sizeBytes).toBeGreaterThan(0);
  });

  test("deleteSnapshot deregisters AMI (idempotent)", async () => {
    const ec2 = createMockEC2();
    const provider = createProvider(ec2);

    const inst = await provider.spawn({ spec: "ubuntu:noble:arm64", bootstrap: testBootstrap });
    const req = await provider.createSnapshot!(inst.id, { name: "snap-del" });

    await provider.deleteSnapshot!(req.requestId);
    // Second delete should be idempotent
    await provider.deleteSnapshot!(req.requestId);
  });
});

describe("PROV-01: spot instances", () => {
  test("requestSpotInstance returns pending request", async () => {
    const provider = createProvider();

    const req = await provider.requestSpotInstance!({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      maxSpotPrice: 0.05,
      spot: true,
    });

    expect(req.requestId).toMatch(/^sir-/);
    expect(req.status).toBe("pending");
  });

  test("getSpotPrices returns price info", async () => {
    const provider = createProvider();
    const prices = await provider.getSpotPrices!("ubuntu:noble:arm64");

    expect(prices.length).toBeGreaterThan(0);
    expect(prices[0].price).toBeGreaterThan(0);
    expect(prices[0].region).toBe("us-east-1");
  });

  test("cancelSpotRequest is idempotent", async () => {
    const provider = createProvider();
    await provider.cancelSpotRequest!("sir-nonexistent");
  });
});

describe("PROV-01: error mapping", () => {
  test("InsufficientInstanceCapacity → CAPACITY_ERROR (retryable)", () => {
    const err = mapEC2Error("InsufficientInstanceCapacity", "No capacity");
    expect(err.code).toBe("CAPACITY_ERROR");
    expect(err.category).toBe("capacity");
    expect(err.retryable).toBe(true);
    expect(err.provider).toBe("aws");
  });

  test("AuthFailure → AUTH_ERROR (not retryable)", () => {
    const err = mapEC2Error("AuthFailure", "Bad creds");
    expect(err.code).toBe("AUTH_ERROR");
    expect(err.category).toBe("auth");
    expect(err.retryable).toBe(false);
  });

  test("RequestLimitExceeded → RATE_LIMIT_ERROR (retryable)", () => {
    const err = mapEC2Error("RequestLimitExceeded", "Throttled");
    expect(err.code).toBe("RATE_LIMIT_ERROR");
    expect(err.category).toBe("rate_limit");
    expect(err.retryable).toBe(true);
  });

  test("InvalidInstanceID.NotFound → NOT_FOUND", () => {
    const err = mapEC2Error("InvalidInstanceID.NotFound", "Not found");
    expect(err.code).toBe("NOT_FOUND");
    expect(err.category).toBe("not_found");
  });

  test("VcpuLimitExceeded → QUOTA_EXCEEDED", () => {
    const err = mapEC2Error("VcpuLimitExceeded", "vCPU limit");
    expect(err.code).toBe("QUOTA_EXCEEDED");
    expect(err.category).toBe("capacity");
  });

  test("unknown error → PROVIDER_INTERNAL", () => {
    const err = mapEC2Error("SomeUnknownCode", "Mystery error");
    expect(err.code).toBe("PROVIDER_INTERNAL");
    expect(err.category).toBe("internal");
  });

  test("all errors are ProviderError instances", () => {
    const codes = [
      "InsufficientInstanceCapacity",
      "AuthFailure",
      "RequestLimitExceeded",
      "Throttling",
      "InvalidParameterValue",
      "InvalidInstanceID.NotFound",
      "IdempotentParameterMismatch",
      "VcpuLimitExceeded",
      "Unsupported",
    ];
    for (const code of codes) {
      const err = mapEC2Error(code, "test");
      expect(err).toBeInstanceOf(ProviderError);
    }
  });
});
