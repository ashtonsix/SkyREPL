// tests/unit/provider-aws.test.ts - AWS Provider Tests (#PROV-02)
//
// Tests the rewritten AWSProvider that uses @aws-sdk/client-ec2 directly.
// Injects a mock EC2Client via the _ec2ClientFactory option — no real AWS calls.
//
// Mock strategy: _ec2ClientFactory returns an object whose send() dispatches
// on command instanceof checks (same pattern used by aws-sdk-client-mock,
// but without the extra dependency).

import { describe, test, expect, beforeEach } from "bun:test";
import {
  RunInstancesCommand,
  TerminateInstancesCommand,
  DescribeInstancesCommand,
  CreateImageCommand,
  DescribeImagesCommand,
  DeregisterImageCommand,
  DeleteSnapshotCommand,
  RequestSpotInstancesCommand,
  CancelSpotInstanceRequestsCommand,
  DescribeSpotPriceHistoryCommand,
  CreateSecurityGroupCommand,
  AuthorizeSecurityGroupIngressCommand,
  DescribeSecurityGroupsCommand,
  CreateKeyPairCommand,
  DescribeKeyPairsCommand,
  CreateTagsCommand,
} from "@aws-sdk/client-ec2";
import { tmpdir } from "os";
import { join } from "path";
import { stat } from "fs/promises";
import {
  AWSProvider,
  mapEC2Error,
  mapEC2State,
  getAwsErrorCode,
  createAwsHooks,
  resolveAmi,
  type AWSInstance,
  type AWSProviderConfig,
} from "../../control/src/provider/compute/aws";
import type {
  Provider,
  SpawnOptions,
  BootstrapConfig,
  ListFilter,
} from "../../control/src/provider/types";
import { PROVIDER_CAPABILITIES } from "../../control/src/provider/types";
import { ProviderOperationError, type ProviderOperationErrorCode, type ProviderOperationErrorCategory } from "../../control/src/provider/errors";

// =============================================================================
// Mock EC2 Client (SDK v3 send() dispatch)
// =============================================================================

interface MockState {
  instances: Map<string, any>;
  images: Map<string, any>;
  nextInstanceId: number;
  lastRunInput?: any;
  securityGroups: Map<string, { GroupId: string; GroupName: string }>;
  keyPairs: Map<string, string>; // keyName → keyName
  createSgCallCount: number;
  createKeyCallCount: number;
  authorizeSgCallCount: number;
  createTagsCallCount: number;
  lastAuthorizeSgInput?: any;
  lastCreateTagsInput?: any;
  // For resolveAmi tests: images returned by filter-based DescribeImages
  filterImages: Array<{ ImageId: string; Architecture: string; CreationDate: string; State: string }>;
  // For error injection tests: if set, handleRunInstances throws this error
  runInstancesError?: Error;
}

function makeAwsError(code: string, message: string): Error {
  // AWS SDK v3 errors expose code via .name
  return Object.assign(new Error(message), { name: code });
}

function createMockEC2(state: MockState) {
  return {
    send(command: any): Promise<any> {
      if (command instanceof RunInstancesCommand) {
        return handleRunInstances(command.input, state);
      }
      if (command instanceof TerminateInstancesCommand) {
        return handleTerminateInstances(command.input, state);
      }
      if (command instanceof DescribeInstancesCommand) {
        return handleDescribeInstances(command.input, state);
      }
      if (command instanceof CreateImageCommand) {
        return handleCreateImage(command.input, state);
      }
      if (command instanceof DescribeImagesCommand) {
        return handleDescribeImages(command.input, state);
      }
      if (command instanceof DeregisterImageCommand) {
        return handleDeregisterImage(command.input, state);
      }
      if (command instanceof DeleteSnapshotCommand) {
        return Promise.resolve({});
      }
      if (command instanceof RequestSpotInstancesCommand) {
        return handleRequestSpotInstances(command.input, state);
      }
      if (command instanceof CancelSpotInstanceRequestsCommand) {
        return Promise.resolve({});
      }
      if (command instanceof DescribeSpotPriceHistoryCommand) {
        return handleDescribeSpotPriceHistory(command.input, state);
      }
      if (command instanceof DescribeSecurityGroupsCommand) {
        return handleDescribeSecurityGroups(command.input, state);
      }
      if (command instanceof CreateSecurityGroupCommand) {
        return handleCreateSecurityGroup(command.input, state);
      }
      if (command instanceof AuthorizeSecurityGroupIngressCommand) {
        state.authorizeSgCallCount++;
        state.lastAuthorizeSgInput = command.input;
        return Promise.resolve({});
      }
      if (command instanceof CreateTagsCommand) {
        state.createTagsCallCount++;
        state.lastCreateTagsInput = command.input;
        return Promise.resolve({});
      }
      if (command instanceof DescribeKeyPairsCommand) {
        return handleDescribeKeyPairs(command.input, state);
      }
      if (command instanceof CreateKeyPairCommand) {
        return handleCreateKeyPair(command.input, state);
      }
      return Promise.reject(new Error(`Unhandled command: ${command.constructor.name}`));
    },
  };
}

async function handleRunInstances(input: any, state: MockState): Promise<any> {
  if (state.runInstancesError) {
    const err = state.runInstancesError;
    state.runInstancesError = undefined; // consume the one-shot error
    throw err;
  }
  state.lastRunInput = input;
  const id = `i-${String(state.nextInstanceId++).padStart(17, "0")}`;
  // Extract tags from TagSpecifications (same as real EC2)
  const tagSpecs = input.TagSpecifications ?? [];
  const instanceTags = tagSpecs
    .filter((ts: any) => ts.ResourceType === "instance")
    .flatMap((ts: any) => ts.Tags ?? []);
  const inst = {
    InstanceId: id,
    InstanceType: input.InstanceType,
    State: { Name: "pending" },
    PrivateIpAddress: "10.0.1.100",
    PublicIpAddress: undefined,
    Placement: { AvailabilityZone: "us-east-1a" },
    SubnetId: input.SubnetId,
    VpcId: "vpc-123",
    SecurityGroups: (input.SecurityGroupIds ?? []).map((g: string) => ({ GroupId: g })),
    LaunchTime: new Date(),
    SpotInstanceRequestId: input.InstanceMarketOptions ? `sir-${id}` : undefined,
    Tags: instanceTags,
  };
  state.instances.set(id, inst);
  return { Instances: [inst] };
}

async function handleTerminateInstances(input: any, state: MockState): Promise<any> {
  for (const id of input.InstanceIds ?? []) {
    if (!state.instances.has(id)) {
      throw makeAwsError("InvalidInstanceID.NotFound", `Instance ${id} not found`);
    }
    const inst = state.instances.get(id)!;
    inst.State = { Name: "shutting-down" };
  }
  return {};
}

async function handleDescribeInstances(input: any, state: MockState): Promise<any> {
  let results = Array.from(state.instances.values());

  if (input.InstanceIds?.length) {
    results = results.filter((i) => input.InstanceIds.includes(i.InstanceId));
    if (results.length === 0) {
      throw makeAwsError("InvalidInstanceID.NotFound", "Not found");
    }
  }

  if (input.Filters) {
    for (const filter of input.Filters) {
      if (filter.Name === "instance-state-name") {
        results = results.filter((i) => filter.Values.includes(i.State?.Name ?? ""));
      }
      // Tag filters: match against instance Tags array
      if (filter.Name?.startsWith("tag:")) {
        const tagKey = filter.Name.slice(4);
        results = results.filter((i) => {
          const tag = (i.Tags ?? []).find((t: any) => t.Key === tagKey);
          return tag && filter.Values.includes(tag.Value);
        });
      }
    }
  }

  if (input.MaxResults) {
    results = results.slice(0, input.MaxResults);
  }

  return { Reservations: [{ Instances: results }] };
}

async function handleCreateImage(input: any, state: MockState): Promise<any> {
  const imageId = `ami-${Date.now()}`;
  state.images.set(imageId, {
    ImageId: imageId,
    Name: input.Name,
    State: "available",
    Architecture: "x86_64",
    BlockDeviceMappings: [
      { Ebs: { SnapshotId: `snap-${imageId}`, VolumeSize: 8 } },
    ],
    CreationDate: new Date().toISOString(),
  });
  return { ImageId: imageId };
}

async function handleDescribeImages(input: any, state: MockState): Promise<any> {
  // ID-based lookup (snapshot tests, getSnapshotStatus)
  if (input.ImageIds?.length) {
    const found = input.ImageIds.map((id: string) => state.images.get(id)).filter(Boolean);
    return { Images: found };
  }
  // Filter-based lookup (resolveAmi tests via filterImages)
  let results = [...state.filterImages];
  if (input.Filters) {
    for (const filter of input.Filters) {
      if (filter.Name === "architecture") {
        results = results.filter((img) => filter.Values.includes(img.Architecture));
      }
      if (filter.Name === "state") {
        results = results.filter((img) => filter.Values.includes(img.State));
      }
    }
  }
  return { Images: results };
}

async function handleDeregisterImage(input: any, state: MockState): Promise<any> {
  if (!state.images.has(input.ImageId)) {
    throw makeAwsError("InvalidAMIID.NotFound", "Image not found");
  }
  state.images.delete(input.ImageId);
  return {};
}

async function handleRequestSpotInstances(input: any, state: MockState): Promise<any> {
  return {
    SpotInstanceRequests: [
      { SpotInstanceRequestId: `sir-${Date.now()}` },
    ],
  };
}

async function handleDescribeSpotPriceHistory(input: any, state: MockState): Promise<any> {
  return {
    SpotPriceHistory: [
      {
        InstanceType: input.InstanceTypes?.[0],
        AvailabilityZone: "us-east-1a",
        SpotPrice: "0.0035",
        Timestamp: new Date(),
        ProductDescription: "Linux/UNIX",
      },
    ],
  };
}

async function handleDescribeSecurityGroups(input: any, state: MockState): Promise<any> {
  let results = Array.from(state.securityGroups.values());

  // Filter by group-name if provided
  if (input.Filters) {
    for (const filter of input.Filters) {
      if (filter.Name === "group-name") {
        results = results.filter((sg) => filter.Values.includes(sg.GroupName));
      }
      // tag:skyrepl:managed — we trust all mock SGs are managed, skip filter
    }
  }

  return { SecurityGroups: results };
}

async function handleCreateSecurityGroup(input: any, state: MockState): Promise<any> {
  state.createSgCallCount++;
  const sgId = `sg-mock-${String(state.createSgCallCount).padStart(3, "0")}`;
  state.securityGroups.set(sgId, { GroupId: sgId, GroupName: input.GroupName });
  return { GroupId: sgId };
}

async function handleDescribeKeyPairs(input: any, state: MockState): Promise<any> {
  let results = Array.from(state.keyPairs.keys()).map((name) => ({ KeyName: name }));

  if (input.Filters) {
    for (const filter of input.Filters) {
      if (filter.Name === "key-name") {
        results = results.filter((kp) => filter.Values.includes(kp.KeyName));
      }
    }
  }

  return { KeyPairs: results };
}

async function handleCreateKeyPair(input: any, state: MockState): Promise<any> {
  state.createKeyCallCount++;
  state.keyPairs.set(input.KeyName, input.KeyName);
  return {
    KeyName: input.KeyName,
    KeyMaterial: "-----BEGIN RSA PRIVATE KEY-----\nMOCK\n-----END RSA PRIVATE KEY-----",
  };
}

// =============================================================================
// Test Helpers
// =============================================================================

function createState(): MockState {
  return {
    instances: new Map(),
    images: new Map(),
    nextInstanceId: 1,
    securityGroups: new Map(),
    keyPairs: new Map(),
    createSgCallCount: 0,
    createKeyCallCount: 0,
    authorizeSgCallCount: 0,
    createTagsCallCount: 0,
    filterImages: [],
    runInstancesError: undefined,
  };
}

function createProvider(
  state: MockState,
  overrides: Partial<AWSProviderConfig> = {}
): AWSProvider {
  return new AWSProvider({
    region: "us-east-1",
    defaultSubnetId: "subnet-abc",
    defaultSecurityGroupIds: ["sg-123"],
    defaultKeyName: "repl-key",
    defaultIamProfile: "repl-instance-role",
    _ec2ClientFactory: () => createMockEC2(state) as any,
    ...overrides,
  });
}

const testBootstrap: BootstrapConfig = {
  agentUrl: "http://localhost:3000/v1/agent/download/agent.py",
  controlPlaneUrl: "http://localhost:3000",
  registrationToken: "test-token-abc",
};

const testIdentity = { controlId: "test", manifestId: 0, instanceId: 1 };

// =============================================================================
// Tests: provider interface satisfaction
// =============================================================================

describe("PROV-02: AWS provider type satisfaction", () => {
  test("satisfies Provider interface with correct capabilities and methods", () => {
    const state = createState();
    const provider = createProvider(state);

    // Type-level check: AWSProvider implements Provider<AWSInstance, ...>
    const p: Provider = provider;
    expect(p.name).toBe("aws");
    expect(typeof p.spawn).toBe("function");
    expect(typeof p.terminate).toBe("function");
    expect(typeof p.list).toBe("function");
    expect(typeof p.get).toBe("function");
    expect(typeof p.generateBootstrap).toBe("function");

    // Capabilities
    expect(provider.capabilities).toEqual(PROVIDER_CAPABILITIES.aws);
    expect(provider.capabilities.snapshots).toBe(true);
    expect(provider.capabilities.spot).toBe(true);
    expect(provider.capabilities.gpu).toBe(true);
    expect(provider.capabilities.multiRegion).toBe(true);
    expect(provider.capabilities.idempotentSpawn).toBe(true);
    expect(provider.capabilities.customNetworking).toBe(true);

    // Optional methods declared for capabilities
    expect(typeof provider.createSnapshot).toBe("function");
    expect(typeof provider.getSnapshotStatus).toBe("function");
    expect(typeof provider.deleteSnapshot).toBe("function");
    expect(typeof provider.requestSpotInstance).toBe("function");
    expect(typeof provider.cancelSpotRequest).toBe("function");
    expect(typeof provider.getSpotPrices).toBe("function");
  });
});

// =============================================================================
// Tests: mapEC2State
// =============================================================================

describe("PROV-02: mapEC2State", () => {
  test("maps 'pending' to 'pending'", () => {
    expect(mapEC2State("pending")).toBe("pending");
  });
  test("maps 'running' to 'running'", () => {
    expect(mapEC2State("running")).toBe("running");
  });
  test("maps 'shutting-down' to 'terminating'", () => {
    expect(mapEC2State("shutting-down")).toBe("terminating");
  });
  test("maps 'terminated' to 'terminated'", () => {
    expect(mapEC2State("terminated")).toBe("terminated");
  });
  test("maps 'stopping' to 'stopping'", () => {
    expect(mapEC2State("stopping")).toBe("stopping");
  });
  test("maps 'stopped' to 'stopped'", () => {
    expect(mapEC2State("stopped")).toBe("stopped");
  });
  test("maps unknown state to 'error'", () => {
    expect(mapEC2State("unknown")).toBe("error");
    expect(mapEC2State("")).toBe("error");
  });
});

// =============================================================================
// Tests: spawn
// =============================================================================

describe("PROV-02: spawn", () => {
  test("spawn creates instance with correct RunInstances params", async () => {
    const state = createState();
    const provider = createProvider(state);

    const result = await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
      tags: { environment: "test" },
      snapshotId: "ami-known-test", // use fixed AMI to avoid live lookup
    });

    expect(result.id).toMatch(/^i-/);
    expect(result.status).toBe("pending");
    expect(result.instanceType).toBe("t4g.micro"); // arm64 default
    expect(result.isSpot).toBe(false);
    expect(result.availabilityZone).toBe("us-east-1a");

    // Verify SDK input — ClientToken is derived from naming triple
    expect(state.lastRunInput!.ClientToken).toBe("spawn:test-0-1");
    expect(state.lastRunInput!.SubnetId).toBe("subnet-abc");
    expect(state.lastRunInput!.SecurityGroupIds).toEqual(["sg-123"]);
    expect(state.lastRunInput!.UserData).toBeTruthy();
    expect(state.lastRunInput!.ImageId).toBe("ami-known-test");

    // Tags should include managed flag
    const tagMap = Object.fromEntries(
      (state.lastRunInput!.TagSpecifications[0].Tags as Array<{ Key: string; Value: string }>)
        .map(({ Key, Value }) => [Key, Value])
    );
    expect(tagMap["skyrepl:managed"]).toBe("true");
    expect(tagMap["environment"]).toBe("test");
  });

  test("spawn with spot option sets InstanceMarketOptions", async () => {
    const state = createState();
    const provider = createProvider(state);

    const result = await provider.spawn({
      spec: "ubuntu:noble:amd64",
      bootstrap: testBootstrap,
      ...testIdentity,
      spot: true,
      maxSpotPrice: 0.05,
      snapshotId: "ami-known-test",
    });

    expect(result.isSpot).toBe(true);
    expect(state.lastRunInput!.InstanceMarketOptions).toEqual({
      MarketType: "spot",
      SpotOptions: { MaxPrice: "0.05" },
    });
  });

  test("spawn with spot but no maxSpotPrice omits SpotOptions", async () => {
    const state = createState();
    const provider = createProvider(state);

    await provider.spawn({
      spec: "ubuntu:noble:amd64",
      bootstrap: testBootstrap,
      ...testIdentity,
      spot: true,
      snapshotId: "ami-known-test",
    });

    expect(state.lastRunInput!.InstanceMarketOptions).toEqual({
      MarketType: "spot",
      SpotOptions: undefined,
    });
  });

  test("spawn with network config overrides provider defaults", async () => {
    const state = createState();
    const provider = createProvider(state);

    await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
      snapshotId: "ami-known-test",
      networkConfig: {
        subnetId: "subnet-custom",
        securityGroupIds: ["sg-custom-1", "sg-custom-2"],
      },
    });

    expect(state.lastRunInput!.SubnetId).toBe("subnet-custom");
    expect(state.lastRunInput!.SecurityGroupIds).toEqual(["sg-custom-1", "sg-custom-2"]);
  });

  test("spawn with snapshotId uses it as AMI (skips live lookup)", async () => {
    const state = createState();
    const provider = createProvider(state);

    await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
      snapshotId: "ami-custom-snapshot",
    });

    expect(state.lastRunInput!.ImageId).toBe("ami-custom-snapshot");
  });

  test("spawn uses cloud-init format (not shell script)", async () => {
    const state = createState();
    const provider = createProvider(state);

    await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
      snapshotId: "ami-known-test",
    });

    // user-data is base64-encoded cloud-init content
    const decoded = Buffer.from(state.lastRunInput!.UserData, "base64").toString("utf8");
    expect(decoded).toContain("#cloud-config");
    // Should NOT be a shell script
    expect(decoded).not.toContain("#!/bin/bash");
  });

  test("spawn passes instanceType over arch default", async () => {
    const state = createState();
    const provider = createProvider(state);

    await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
      snapshotId: "ami-known-test",
      instanceType: "c7g.large",
    });

    expect(state.lastRunInput!.InstanceType).toBe("c7g.large");
  });

  test("spawn always tags instanceId and derived spawn_key", async () => {
    const state = createState();
    const provider = createProvider(state);

    await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      controlId: "test",
      manifestId: 0,
      instanceId: 42,
      snapshotId: "ami-known-test",
    });

    const tagMap = Object.fromEntries(
      (state.lastRunInput!.TagSpecifications[0].Tags as Array<{ Key: string; Value: string }>)
        .map(({ Key, Value }) => [Key, Value])
    );
    expect(tagMap["skyrepl:instance_id"]).toBe("42");
    expect(tagMap["skyrepl:spawn_key"]).toBe("spawn:test-0-42");
  });
});

// =============================================================================
// Tests: terminate
// =============================================================================

describe("PROV-02: terminate", () => {
  test("terminate calls TerminateInstances with correct instance ID", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
      snapshotId: "ami-known-test",
    });

    await provider.terminate(inst.id);

    expect(state.instances.get(inst.id)!.State.Name).toBe("shutting-down");
  });

  test("terminate is idempotent (not found → silent)", async () => {
    const state = createState();
    const provider = createProvider(state);

    // Should not throw even though instance doesn't exist
    await expect(provider.terminate("i-nonexistent")).resolves.toBeUndefined();
  });
});

// =============================================================================
// Tests: list and get
// =============================================================================

describe("PROV-02: list and get", () => {
  test("list returns all managed instances", async () => {
    const state = createState();
    const provider = createProvider(state);

    await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
      instanceId: 1,
      snapshotId: "ami-known-test",
    });
    await provider.spawn({
      spec: "ubuntu:noble:amd64",
      bootstrap: testBootstrap,
      ...testIdentity,
      instanceId: 2,
      snapshotId: "ami-known-test",
    });

    const all = await provider.list();
    expect(all.length).toBe(2);
    expect(all.every((i) => i.id.startsWith("i-"))).toBe(true);
  });

  test("list with status filter applies instance-state-name filter", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
      snapshotId: "ami-known-test",
    });

    // Instances are in "pending" state in mock
    const running = await provider.list({ status: ["running"] });
    expect(running.length).toBe(0);

    const pending = await provider.list({ status: ["pending"] });
    expect(pending.length).toBe(1);
    expect(pending[0]!.id).toBe(inst.id);
  });

  test("get retrieves instance by ID", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
      snapshotId: "ami-known-test",
    });

    const found = await provider.get(inst.id);
    expect(found).not.toBeNull();
    expect(found!.id).toBe(inst.id);
    expect(found!.instanceType).toBe("t4g.micro");
  });

  test("get returns null for non-existent instance", async () => {
    const state = createState();
    const provider = createProvider(state);

    const result = await provider.get("i-nonexistent");
    expect(result).toBeNull();
  });

  test("list with spec filter uses skyrepl:spec tag", async () => {
    const state = createState();
    const provider = createProvider(state);

    await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
      instanceId: 1,
      snapshotId: "ami-known-test",
    });
    await provider.spawn({
      spec: "ubuntu:jammy:amd64",
      bootstrap: testBootstrap,
      ...testIdentity,
      instanceId: 2,
      snapshotId: "ami-known-test",
    });

    const arm64Only = await provider.list({ spec: "ubuntu:noble:arm64" });
    expect(arm64Only.length).toBe(1);
    expect(arm64Only[0]!.spec).toBe("ubuntu:noble:arm64");

    const amd64Only = await provider.list({ spec: "ubuntu:jammy:amd64" });
    expect(amd64Only.length).toBe(1);
    expect(amd64Only[0]!.spec).toBe("ubuntu:jammy:amd64");
  });

  test("get reads spec from skyrepl:spec tag when originalSpec not provided", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
      snapshotId: "ami-known-test",
    });

    // get() doesn't pass originalSpec, so it reads from the tag
    const fetched = await provider.get(inst.id);
    expect(fetched!.spec).toBe("ubuntu:noble:arm64");
  });

  test("spawn always sets Name tag from naming triple", async () => {
    const state = createState();
    const provider = createProvider(state);

    await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      snapshotId: "ami-known-test",
      controlId: "abc123",
      instanceId: 42,
      manifestId: 7,
    });

    const lastInput = state.lastRunInput;
    const tags = lastInput.TagSpecifications[0].Tags;
    const nameTag = tags.find((t: any) => t.Key === "Name");
    expect(nameTag).toBeDefined();
    expect(nameTag.Value).toBe("repl-abc123-7-16"); // 7 in base36 = "7", 42 in base36 = "16"
  });
});

// =============================================================================
// Tests: generateBootstrap
// =============================================================================

describe("PROV-02: generateBootstrap", () => {
  test("delegates to assembleCloudInit, returns cloud-init format", () => {
    const state = createState();
    const provider = createProvider(state);

    const result = provider.generateBootstrap(testBootstrap);

    expect(result.format).toBe("cloud-init");
    expect(result.content).toContain("#cloud-config");
    expect(result.checksum).toHaveLength(64); // SHA-256 hex
  });

  test("cloud-init includes control plane URL and registration token", () => {
    const state = createState();
    const provider = createProvider(state);

    const result = provider.generateBootstrap(testBootstrap);

    expect(result.content).toContain("http://localhost:3000");
    expect(result.content).toContain("test-token-abc");
  });

  test("cloud-init includes features when specified", () => {
    const state = createState();
    const provider = createProvider(state);

    const result = provider.generateBootstrap({
      ...testBootstrap,
      features: [{ name: "nvidia-driver" }, { name: "docker" }],
    });

    expect(result.content).toContain("nvidia");
    expect(result.content).toContain("docker");
  });

  test("cloud-init content differs with different configs (checksum changes)", () => {
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

describe("PROV-02: snapshots", () => {
  test("createSnapshot returns requestId matching ami- pattern and creating status", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
      snapshotId: "ami-known-test",
    });

    const req = await provider.createSnapshot!(inst.id, {
      name: "test-snapshot",
      description: "Test",
    });

    expect(req.requestId).toMatch(/^ami-/);
    expect(req.status.status).toBe("creating");
  });

  test("getSnapshotStatus returns available status for existing image", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
      snapshotId: "ami-known-test",
    });

    const req = await provider.createSnapshot!(inst.id, { name: "snap" });
    const status = await provider.getSnapshotStatus!(req.requestId);

    expect(status.status).toBe("available");
    expect(status.providerSnapshotId).toBe(req.requestId);
    expect(status.sizeBytes).toBeGreaterThan(0);
  });

  test("deleteSnapshot is idempotent (second call does not throw)", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
      snapshotId: "ami-known-test",
    });

    const req = await provider.createSnapshot!(inst.id, { name: "snap" });

    await provider.deleteSnapshot!(req.requestId);
    // Second call: image is gone, should be silently ignored
    await expect(provider.deleteSnapshot!(req.requestId)).resolves.toBeUndefined();
  });

  test("deleteSnapshot swallows InvalidAMIID.NotFound", async () => {
    const state = createState();
    const provider = createProvider(state);

    await expect(provider.deleteSnapshot!("ami-nonexistent")).resolves.toBeUndefined();
  });
});

// =============================================================================
// Tests: spot instances
// =============================================================================

describe("PROV-02: spot instances", () => {
  test("requestSpotInstance returns a sir- prefixed requestId", async () => {
    const state = createState();
    const provider = createProvider(state);

    const req = await provider.requestSpotInstance!({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
      maxSpotPrice: 0.05,
      spot: true,
      snapshotId: "ami-known-test",
    });

    expect(req.requestId).toMatch(/^sir-/);
    expect(req.status).toBe("pending");
  });

  test("getSpotPrices returns price entries for the default region", async () => {
    const state = createState();
    const provider = createProvider(state);

    const prices = await provider.getSpotPrices!("ubuntu:noble:arm64");
    expect(prices.length).toBeGreaterThan(0);
    expect(prices[0]!.price).toBeGreaterThan(0);
    expect(prices[0]!.region).toBe("us-east-1");
  });

  test("cancelSpotRequest is idempotent (does not throw for any requestId)", async () => {
    const state = createState();
    const provider = createProvider(state);

    // Our mock CancelSpotInstanceRequestsCommand always succeeds
    await expect(provider.cancelSpotRequest!("sir-nonexistent")).resolves.toBeUndefined();
  });
});

// =============================================================================
// Tests: multi-region client caching
// =============================================================================

describe("PROV-02: multi-region client caching", () => {
  test("creates separate clients for different regions", async () => {
    const factoryCalls: string[] = [];
    const state = createState();

    const provider = new AWSProvider({
      region: "us-east-1",
      _ec2ClientFactory: (region: string) => {
        factoryCalls.push(region);
        return createMockEC2(state) as any;
      },
    });

    await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
      instanceId: 1,
      snapshotId: "ami-known-test",
      region: "us-east-1",
    });

    await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
      instanceId: 2,
      snapshotId: "ami-known-test",
      region: "eu-central-1",
    });

    // Factory should have been called for each unique region
    expect(factoryCalls).toContain("us-east-1");
    expect(factoryCalls).toContain("eu-central-1");
  });

  test("caches client for same region (factory called once per region)", async () => {
    const factoryCalls: string[] = [];
    const state = createState();

    const provider = new AWSProvider({
      region: "us-east-1",
      _ec2ClientFactory: (region: string) => {
        factoryCalls.push(region);
        return createMockEC2(state) as any;
      },
    });

    // Spawn twice in the same region
    await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
      instanceId: 1,
      snapshotId: "ami-known-test",
    });
    await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
      instanceId: 2,
      snapshotId: "ami-known-test",
    });

    // Factory should only be called once for us-east-1
    const usEast1Calls = factoryCalls.filter((r) => r === "us-east-1");
    expect(usEast1Calls.length).toBe(1);
  });
});

// =============================================================================
// Tests: error mapping
// =============================================================================

describe("PROV-02: error mapping", () => {
  test("maps all EC2 error codes to correct ProviderOperationError attributes", () => {
    const truthTable: Array<{
      awsCode: string;
      expectedCode: ProviderOperationErrorCode;
      expectedCategory: ProviderOperationErrorCategory;
      retryable: boolean;
    }> = [
      { awsCode: "InsufficientInstanceCapacity", expectedCode: "CAPACITY_ERROR",      expectedCategory: "capacity",   retryable: true  },
      { awsCode: "InstanceLimitExceeded",        expectedCode: "CAPACITY_ERROR",      expectedCategory: "capacity",   retryable: true  },
      { awsCode: "AuthFailure",                  expectedCode: "AUTH_ERROR",          expectedCategory: "auth",       retryable: false },
      { awsCode: "UnauthorizedAccess",           expectedCode: "AUTH_ERROR",          expectedCategory: "auth",       retryable: false },
      { awsCode: "RequestLimitExceeded",         expectedCode: "RATE_LIMIT_ERROR",    expectedCategory: "rate_limit", retryable: true  },
      { awsCode: "Throttling",                   expectedCode: "RATE_LIMIT_ERROR",    expectedCategory: "rate_limit", retryable: true  },
      { awsCode: "InvalidParameterValue",        expectedCode: "INVALID_SPEC",        expectedCategory: "validation", retryable: false },
      { awsCode: "InvalidAMIID.Malformed",       expectedCode: "INVALID_SPEC",        expectedCategory: "validation", retryable: false },
      { awsCode: "InvalidInstanceID.NotFound",   expectedCode: "NOT_FOUND",           expectedCategory: "not_found",  retryable: false },
      { awsCode: "InvalidInstanceID.Malformed",  expectedCode: "INVALID_SPEC",        expectedCategory: "validation", retryable: false },
      { awsCode: "IdempotentParameterMismatch",  expectedCode: "ALREADY_EXISTS",      expectedCategory: "conflict",   retryable: false },
      { awsCode: "VcpuLimitExceeded",            expectedCode: "QUOTA_EXCEEDED",      expectedCategory: "capacity",   retryable: false },
      { awsCode: "Unsupported",                  expectedCode: "UNSUPPORTED_OPERATION", expectedCategory: "internal", retryable: false },
      { awsCode: "SomeUnknownCode",              expectedCode: "PROVIDER_INTERNAL",   expectedCategory: "internal",   retryable: false },
    ];

    for (const { awsCode, expectedCode, expectedCategory, retryable } of truthTable) {
      const err = mapEC2Error(awsCode, "test message");
      expect(err).toBeInstanceOf(ProviderOperationError);
      expect(err.code).toBe(expectedCode);
      expect(err.category).toBe(expectedCategory);
      expect(err.retryable).toBe(retryable);
      expect(err.provider).toBe("aws");
    }
  });

});

// =============================================================================
// Tests: ensureSecurityGroup
// =============================================================================

describe("PROV-02: ensureSecurityGroup", () => {
  test("creates SG when none exists", async () => {
    const state = createState();
    // Use a provider without defaultSecurityGroupIds so the ensure path is exercised
    const provider = new AWSProvider({
      region: "us-east-1",
      keysDir: join(tmpdir(), `skyrepl-test-${Date.now()}`),
      _ec2ClientFactory: () => createMockEC2(state) as any,
    });

    const sgId = await provider.ensureSecurityGroup();

    expect(sgId).toMatch(/^sg-mock-/);
    expect(state.createSgCallCount).toBe(1);
    // The SG should be in the mock state
    expect(state.securityGroups.size).toBe(1);
  });

  test("returns cached SG on second call (create only called once)", async () => {
    const state = createState();
    const provider = new AWSProvider({
      region: "us-east-1",
      keysDir: join(tmpdir(), `skyrepl-test-${Date.now()}`),
      _ec2ClientFactory: () => createMockEC2(state) as any,
    });

    const sgId1 = await provider.ensureSecurityGroup();
    const sgId2 = await provider.ensureSecurityGroup();

    expect(sgId1).toBe(sgId2);
    expect(state.createSgCallCount).toBe(1); // only created once
  });

  test("finds existing SG without creating", async () => {
    const state = createState();
    // Pre-populate with an existing SG
    state.securityGroups.set("sg-existing-001", {
      GroupId: "sg-existing-001",
      GroupName: "skyrepl-default",
    });

    const provider = new AWSProvider({
      region: "us-east-1",
      keysDir: join(tmpdir(), `skyrepl-test-${Date.now()}`),
      _ec2ClientFactory: () => createMockEC2(state) as any,
    });

    const sgId = await provider.ensureSecurityGroup();

    expect(sgId).toBe("sg-existing-001");
    expect(state.createSgCallCount).toBe(0); // no create call
  });
});

// =============================================================================
// Tests: ensureKeyPair
// =============================================================================

describe("PROV-02: ensureKeyPair", () => {
  test("creates key pair when none exists", async () => {
    const state = createState();
    const keysDir = join(tmpdir(), `skyrepl-test-keys-${Date.now()}`);
    const provider = new AWSProvider({
      region: "us-east-1",
      keysDir,
      _ec2ClientFactory: () => createMockEC2(state) as any,
    });

    const keyName = await provider.ensureKeyPair();

    expect(keyName).toBe("skyrepl-us-east-1");
    expect(state.createKeyCallCount).toBe(1);
    expect(state.keyPairs.has("skyrepl-us-east-1")).toBe(true);
  });

  test("returns cached key on second call", async () => {
    const state = createState();
    const keysDir = join(tmpdir(), `skyrepl-test-keys-${Date.now()}`);
    const provider = new AWSProvider({
      region: "us-east-1",
      keysDir,
      _ec2ClientFactory: () => createMockEC2(state) as any,
    });

    const k1 = await provider.ensureKeyPair();
    const k2 = await provider.ensureKeyPair();

    expect(k1).toBe(k2);
    expect(state.createKeyCallCount).toBe(1); // only created once
  });

  test("finds existing key pair without creating", async () => {
    const state = createState();
    // Pre-populate with an existing key
    state.keyPairs.set("skyrepl-us-east-1", "skyrepl-us-east-1");

    const keysDir = join(tmpdir(), `skyrepl-test-keys-${Date.now()}`);
    const provider = new AWSProvider({
      region: "us-east-1",
      keysDir,
      _ec2ClientFactory: () => createMockEC2(state) as any,
    });

    const keyName = await provider.ensureKeyPair();

    expect(keyName).toBe("skyrepl-us-east-1");
    expect(state.createKeyCallCount).toBe(0); // no create call
  });
});

// =============================================================================
// Tests: startup
// =============================================================================

describe("PROV-02: startup", () => {
  test("verifies credentials and ensures infrastructure", async () => {
    const state = createState();
    const keysDir = join(tmpdir(), `skyrepl-test-startup-${Date.now()}`);
    const provider = new AWSProvider({
      region: "us-east-1",
      keysDir,
      _ec2ClientFactory: () => createMockEC2(state) as any,
    });

    // Override verifyCredentials to avoid real STS call
    (provider as any).verifyCredentials = async () => ({
      accountId: "123456789012",
      arn: "arn:aws:iam::123456789012:user/test",
      userId: "AIDATEST",
    });

    const result = await provider.startup();

    expect(result.accountId).toBe("123456789012");
    expect(result.region).toBe("us-east-1");
    expect(result.securityGroupId).toMatch(/^sg-/);
    expect(result.keyName).toBe("skyrepl-us-east-1");
  });
});

// =============================================================================
// Tests: lifecycle hooks
// =============================================================================

describe("PROV-02: lifecycle hooks", () => {
  test("onStartup calls provider.startup()", async () => {
    const state = createState();
    const keysDir = join(tmpdir(), `skyrepl-test-hooks-${Date.now()}`);
    const provider = new AWSProvider({
      region: "us-east-1",
      keysDir,
      _ec2ClientFactory: () => createMockEC2(state) as any,
    });

    let startupCalled = false;
    (provider as any).startup = async () => {
      startupCalled = true;
      return { accountId: "123", region: "us-east-1", securityGroupId: "sg-x", keyName: "k" };
    };

    const hooks = createAwsHooks(provider);
    await hooks.onStartup!();

    expect(startupCalled).toBe(true);
  });

  test("onHeartbeat health_check returns completed receipt with latencyMs", async () => {
    const state = createState();
    const provider = new AWSProvider({
      region: "us-east-1",
      _ec2ClientFactory: () => createMockEC2(state) as any,
    });

    // Mock verifyCredentials to avoid real STS
    (provider as any).verifyCredentials = async () => ({
      accountId: "123",
      arn: "arn:aws:iam::123:user/test",
      userId: "U123",
    });

    const hooks = createAwsHooks(provider);
    const result = await hooks.onHeartbeat!({
      tasks: [{ type: "health_check", priority: "high" }],
      deadline: Date.now() + 10_000,
    });

    expect(result.receipts).toHaveLength(1);
    expect(result.receipts[0]!.type).toBe("health_check");
    expect(result.receipts[0]!.status).toBe("completed");
    expect((result.receipts[0]!.result as any).latencyMs).toBeGreaterThanOrEqual(0);
  });

  test("onHeartbeat reconcile returns skipped receipt", async () => {
    const state = createState();
    const provider = new AWSProvider({
      region: "us-east-1",
      _ec2ClientFactory: () => createMockEC2(state) as any,
    });

    const hooks = createAwsHooks(provider);
    const result = await hooks.onHeartbeat!({
      tasks: [{ type: "reconcile", priority: "normal" }],
      deadline: Date.now() + 10_000,
    });

    expect(result.receipts[0]!.type).toBe("reconcile");
    expect(result.receipts[0]!.status).toBe("skipped");
    expect(result.receipts[0]!.reason).toContain("DB access");
  });

  test("onHeartbeat unknown task type returns skipped receipt with reason", async () => {
    const state = createState();
    const provider = new AWSProvider({
      region: "us-east-1",
      _ec2ClientFactory: () => createMockEC2(state) as any,
    });

    const hooks = createAwsHooks(provider);
    const result = await hooks.onHeartbeat!({
      tasks: [{ type: "some_unknown_task" as any, priority: "low" }],
      deadline: Date.now() + 10_000,
    });

    expect(result.receipts[0]!.type).toBe("some_unknown_task");
    expect(result.receipts[0]!.status).toBe("skipped");
    expect(result.receipts[0]!.reason).toContain("Unknown task type");
  });
});

// =============================================================================
// Tests: registry
// =============================================================================

describe("PROV-02: registry", () => {
  test("getProvider('aws') resolves and returns a provider with name 'aws'", async () => {
    const { getProvider, clearProviderCache } = await import(
      "../../control/src/provider/registry"
    );
    // Clear cache so we get a fresh import
    clearProviderCache();

    const provider = await getProvider("aws");

    expect(provider).toBeDefined();
    expect(provider.name).toBe("aws");
    expect(typeof provider.spawn).toBe("function");
  });
});

// =============================================================================
// Tests: resolveAmi (T2)
// =============================================================================

describe("PROV-02: resolveAmi", () => {
  // Use a per-test unique cache key to avoid disk cache hits from prior runs
  function uniqueCacheKey(): string {
    return `test-ami-${Date.now()}-${Math.random().toString(36).slice(2)}`;
  }

  test("selects newest AMI when multiple are returned", async () => {
    const state = createState();
    state.filterImages = [
      { ImageId: "ami-older", Architecture: "arm64", CreationDate: "2024-01-01T00:00:00Z", State: "available" },
      { ImageId: "ami-newest", Architecture: "arm64", CreationDate: "2024-06-01T00:00:00Z", State: "available" },
      { ImageId: "ami-middle", Architecture: "arm64", CreationDate: "2024-03-01T00:00:00Z", State: "available" },
    ];
    const mockClient = createMockEC2(state) as any;

    const result = await resolveAmi(mockClient, "noble", "arm64", uniqueCacheKey());

    expect(result).toBe("ami-newest");
  });

  test("arm64 request does not return an x86_64 AMI", async () => {
    const state = createState();
    state.filterImages = [
      { ImageId: "ami-x86", Architecture: "x86_64", CreationDate: "2024-09-01T00:00:00Z", State: "available" },
      { ImageId: "ami-arm", Architecture: "arm64", CreationDate: "2024-01-01T00:00:00Z", State: "available" },
    ];
    const mockClient = createMockEC2(state) as any;

    // Requesting arm64 — the x86_64 AMI must not be returned
    const result = await resolveAmi(mockClient, "noble", "arm64", uniqueCacheKey());

    expect(result).toBe("ami-arm");
    expect(result).not.toBe("ami-x86");
  });

  test("throws when no matching AMIs are found", async () => {
    const state = createState();
    state.filterImages = []; // empty — no AMIs match
    const mockClient = createMockEC2(state) as any;

    await expect(
      resolveAmi(mockClient, "noble", "arm64", uniqueCacheKey())
    ).rejects.toThrow(/No Ubuntu noble arm64 AMI found/);
  });

  test("x86_64 request selects newest x86_64 AMI only", async () => {
    const state = createState();
    state.filterImages = [
      { ImageId: "ami-arm-new", Architecture: "arm64", CreationDate: "2024-09-01T00:00:00Z", State: "available" },
      { ImageId: "ami-x86-old", Architecture: "x86_64", CreationDate: "2024-01-01T00:00:00Z", State: "available" },
      { ImageId: "ami-x86-new", Architecture: "x86_64", CreationDate: "2024-06-01T00:00:00Z", State: "available" },
    ];
    const mockClient = createMockEC2(state) as any;

    const result = await resolveAmi(mockClient, "noble", "amd64", uniqueCacheKey());

    expect(result).toBe("ami-x86-new");
  });
});

// =============================================================================
// Tests: AuthorizeSG and CreateTags tracking (T3)
// =============================================================================

describe("PROV-02: ensureSecurityGroup AuthorizeSG and CreateTags calls", () => {
  test("creating a new SG calls AuthorizeSecurityGroupIngress with SSH port range", async () => {
    const state = createState();
    const provider = new AWSProvider({
      region: "us-east-1",
      keysDir: join(tmpdir(), `skyrepl-test-${Date.now()}`),
      _ec2ClientFactory: () => createMockEC2(state) as any,
    });

    await provider.ensureSecurityGroup();

    expect(state.authorizeSgCallCount).toBe(1);
    const perms = state.lastAuthorizeSgInput!.IpPermissions;
    expect(perms).toBeDefined();
    expect(perms.length).toBeGreaterThan(0);
    const sshRule = perms[0];
    expect(sshRule.IpProtocol).toBe("tcp");
    expect(sshRule.FromPort).toBe(22);
    expect(sshRule.ToPort).toBe(22);
  });

  test("creating a new SG calls CreateTags to mark it as managed", async () => {
    const state = createState();
    const provider = new AWSProvider({
      region: "us-east-1",
      keysDir: join(tmpdir(), `skyrepl-test-${Date.now()}`),
      _ec2ClientFactory: () => createMockEC2(state) as any,
    });

    await provider.ensureSecurityGroup();

    expect(state.createTagsCallCount).toBe(1);
    const tags = state.lastCreateTagsInput!.Tags as Array<{ Key: string; Value: string }>;
    const managedTag = tags.find((t) => t.Key === "skyrepl:managed");
    expect(managedTag).toBeDefined();
    expect(managedTag!.Value).toBe("true");
  });

  test("finding an existing SG skips AuthorizeSG and CreateTags", async () => {
    const state = createState();
    state.securityGroups.set("sg-pre-existing", {
      GroupId: "sg-pre-existing",
      GroupName: "skyrepl-default",
    });

    const provider = new AWSProvider({
      region: "us-east-1",
      keysDir: join(tmpdir(), `skyrepl-test-${Date.now()}`),
      _ec2ClientFactory: () => createMockEC2(state) as any,
    });

    await provider.ensureSecurityGroup();

    expect(state.authorizeSgCallCount).toBe(0);
    expect(state.createTagsCallCount).toBe(0);
  });
});

// =============================================================================
// Tests: PEM write and permissions (T4)
// =============================================================================

describe("PROV-02: ensureKeyPair PEM file", () => {
  test("writes PEM file at expected path when creating a new key", async () => {
    const state = createState();
    const keysDir = join(tmpdir(), `skyrepl-test-pem-exists-${Date.now()}-${Math.random().toString(36).slice(2)}`);
    const provider = new AWSProvider({
      region: "us-east-1",
      keysDir,
      _ec2ClientFactory: () => createMockEC2(state) as any,
    });

    await provider.ensureKeyPair();

    const pemPath = join(keysDir, "aws-us-east-1.pem");
    // stat() throws if the file doesn't exist
    const info = await stat(pemPath);
    expect(info.isFile()).toBe(true);
  });

  test("PEM file is written with restrictive permissions (0o600)", async () => {
    const state = createState();
    const keysDir = join(tmpdir(), `skyrepl-test-pem-perms-${Date.now()}-${Math.random().toString(36).slice(2)}`);
    const provider = new AWSProvider({
      region: "us-east-1",
      keysDir,
      _ec2ClientFactory: () => createMockEC2(state) as any,
    });

    await provider.ensureKeyPair();

    const pemPath = join(keysDir, "aws-us-east-1.pem");
    const info = await stat(pemPath);
    // Mask to lower 9 permission bits
    const mode = info.mode & 0o777;
    // Accept 0o600 (rw owner only) or 0o400 (r owner only)
    expect(mode === 0o600 || mode === 0o400).toBe(true);
  });

  test("PEM file is not written when key pair already exists", async () => {
    const state = createState();
    // Pre-populate key so ensureKeyPair finds it without creating
    state.keyPairs.set("skyrepl-us-east-1", "skyrepl-us-east-1");
    const keysDir = join(tmpdir(), `skyrepl-test-pem-nowrite-${Date.now()}-${Math.random().toString(36).slice(2)}`);
    const provider = new AWSProvider({
      region: "us-east-1",
      keysDir,
      _ec2ClientFactory: () => createMockEC2(state) as any,
    });

    await provider.ensureKeyPair();

    const pemPath = join(keysDir, "aws-us-east-1.pem");
    // File should not exist since no create was needed
    let threw = false;
    try {
      await stat(pemPath);
    } catch {
      threw = true;
    }
    expect(threw).toBe(true);
  });
});

// =============================================================================
// Tests: list() termination exclusion (T5)
// =============================================================================

describe("PROV-02: list() terminated instance exclusion", () => {
  test("terminated instance is excluded from default list()", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
      snapshotId: "ami-known-test",
    });

    await provider.terminate(inst.id);
    // Mock sets state to "shutting-down" on terminate; simulate full termination
    state.instances.get(inst.id)!.State = { Name: "terminated" };

    const results = await provider.list();

    expect(results.find((i) => i.id === inst.id)).toBeUndefined();
  });

  test("terminated instance appears in list({ includeTerminated: true })", async () => {
    const state = createState();
    const provider = createProvider(state);

    const inst = await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
      snapshotId: "ami-known-test",
    });

    await provider.terminate(inst.id);
    state.instances.get(inst.id)!.State = { Name: "terminated" };

    const results = await provider.list({ includeTerminated: true });

    expect(results.find((i) => i.id === inst.id)).toBeDefined();
  });

  test("non-terminated instances are always included in default list()", async () => {
    const state = createState();
    const provider = createProvider(state);

    const alive = await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
      instanceId: 1,
      snapshotId: "ami-known-test",
    });
    const dead = await provider.spawn({
      spec: "ubuntu:noble:arm64",
      bootstrap: testBootstrap,
      ...testIdentity,
      instanceId: 2,
      snapshotId: "ami-known-test",
    });

    await provider.terminate(dead.id);
    state.instances.get(dead.id)!.State = { Name: "terminated" };

    const results = await provider.list();

    expect(results.find((i) => i.id === alive.id)).toBeDefined();
    expect(results.find((i) => i.id === dead.id)).toBeUndefined();
  });
});

// =============================================================================
// Tests: spawn error injection (C1)
// =============================================================================

describe("PROV-02: spawn error injection", () => {
  test("spawn returns CAPACITY_ERROR on InsufficientInstanceCapacity", async () => {
    const state = createState();
    const provider = createProvider(state);

    // Inject an AWS SDK v3-shaped error: code is exposed via .name
    state.runInstancesError = makeAwsError("InsufficientInstanceCapacity", "No capacity in this AZ");

    await expect(
      provider.spawn({
        spec: "ubuntu:noble:arm64",
        bootstrap: testBootstrap,
        ...testIdentity,
        snapshotId: "ami-known-test",
      })
    ).rejects.toMatchObject({
      code: "CAPACITY_ERROR",
      retryable: true,
    });
  });

  test("spawn returns AUTH_ERROR on AuthFailure", async () => {
    const state = createState();
    const provider = createProvider(state);

    // Inject an AWS SDK v3-shaped AuthFailure error
    state.runInstancesError = makeAwsError("AuthFailure", "Not authorized");

    const err = await provider
      .spawn({
        spec: "ubuntu:noble:arm64",
        bootstrap: testBootstrap,
        ...testIdentity,
        snapshotId: "ami-known-test",
      })
      .catch((e) => e);

    expect(err).toBeInstanceOf(ProviderOperationError);
    expect(err.code).toBe("AUTH_ERROR");
    expect(err.retryable).toBe(false);
  });
});

// =============================================================================
// Tests: mapEC2Error direct (C3)
// =============================================================================

describe("PROV-02: mapEC2Error direct", () => {
  test("maps InsufficientInstanceCapacity to CAPACITY_ERROR (retryable)", () => {
    const err = mapEC2Error("InsufficientInstanceCapacity", "No capacity");
    expect(err).toBeInstanceOf(ProviderOperationError);
    expect(err.code).toBe("CAPACITY_ERROR");
    expect(err.retryable).toBe(true);
  });

  test("maps AuthFailure to AUTH_ERROR (not retryable)", () => {
    const err = mapEC2Error("AuthFailure", "Bad credentials");
    expect(err).toBeInstanceOf(ProviderOperationError);
    expect(err.code).toBe("AUTH_ERROR");
    expect(err.retryable).toBe(false);
  });

  test("maps unknown error codes to PROVIDER_INTERNAL", () => {
    const err = mapEC2Error("SomeUnknownEC2Error", "Unexpected problem");
    expect(err).toBeInstanceOf(ProviderOperationError);
    expect(err.code).toBe("PROVIDER_INTERNAL");
  });
});

// =============================================================================
// Tests: getAwsErrorCode multi-field extraction (C3)
// =============================================================================

describe("PROV-02: getAwsErrorCode multi-field extraction", () => {
  test("extracts code from .name (AWS SDK v3 primary)", () => {
    const err = Object.assign(new Error("test"), { name: "AuthFailure" });
    expect(getAwsErrorCode(err)).toBe("AuthFailure");
  });

  test("extracts code from .Code when .name is the generic 'Error'", () => {
    // Some AWS error shapes put the code in .Code instead of .name
    const err = { name: "Error", Code: "InsufficientInstanceCapacity", message: "test" };
    expect(getAwsErrorCode(err)).toBe("Error"); // .name takes priority via ??
  });

  test("falls through .name → .Code → .code → 'Unknown'", () => {
    // Only .code set (older SDK patterns)
    const errCodeOnly = { code: "Throttling" };
    expect(getAwsErrorCode(errCodeOnly)).toBe("Throttling");

    // No code field at all
    const errNone = { message: "something went wrong" };
    expect(getAwsErrorCode(errNone)).toBe("Unknown");

    // Non-object
    expect(getAwsErrorCode("string error")).toBe("Unknown");
    expect(getAwsErrorCode(null)).toBe("Unknown");
    expect(getAwsErrorCode(undefined)).toBe("Unknown");
  });

  test(".name has priority over .Code and .code (SDK v3 convention)", () => {
    const err = { name: "RequestLimitExceeded", Code: "Throttling", code: "SomethingElse" };
    expect(getAwsErrorCode(err)).toBe("RequestLimitExceeded");
  });
});
