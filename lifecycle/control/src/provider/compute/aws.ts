// provider/compute/aws.ts - AWS EC2 Provider (#PROV-02)
//
// Production implementation of the Provider interface for AWS EC2.
// Uses the AWS SDK v3 directly — no EC2Client wrapper shim.
//
// Wave 2 deliverable: strips the PROV-01 skeleton shim, wires real SDK calls,
// multi-region client cache, AMI resolution with disk cache, cloud-init
// bootstrap via assembleCloudInit(), and proper naming-convention tags.

import {
  EC2Client,
  RunInstancesCommand,
  TerminateInstancesCommand,
  DescribeInstancesCommand,
  CreateImageCommand,
  DescribeImagesCommand,
  DeregisterImageCommand,
  DeleteSnapshotCommand,
  DescribeSpotPriceHistoryCommand,
  RequestSpotInstancesCommand,
  CancelSpotInstanceRequestsCommand,
  CreateSecurityGroupCommand,
  AuthorizeSecurityGroupIngressCommand,
  DescribeSecurityGroupsCommand,
  CreateKeyPairCommand,
  DescribeKeyPairsCommand,
  CreateTagsCommand,
  type RunInstancesCommandInput,
} from "@aws-sdk/client-ec2";
import { STSClient, GetCallerIdentityCommand } from "@aws-sdk/client-sts";
import type {
  Provider,
  ProviderInstance,
  ProviderSnapshot,
  ProviderCapabilities,
  ProviderInstanceStatus,
  SpawnOptions,
  BootstrapConfig,
  BootstrapScript,
  ListFilter,
  CreateSnapshotOptions,
  SnapshotRequest,
  SnapshotStatus,
  SpotOptions,
  SpotRequest,
  SpotPriceInfo,
} from "../types";
import { PROVIDER_CAPABILITIES } from "../types";
import {
  ProviderOperationError,
  ConcreteProviderError,
  withProviderErrorMapping,
} from "../errors";
import { parseSpec } from "../types";
import { assembleCloudInit } from "../bootstrap/cloud-init";
import { fetchWithDiskCache } from "../cache";
import { formatResourceName } from "../../material/control-id";
import { homedir } from "os";
import { join } from "path";
import { mkdir, writeFile, chmod } from "fs/promises";
import type { ProviderLifecycleHooks, TaskReceipt, HeartbeatExpectations } from "../extensions";

// =============================================================================
// AWS-Specific Types
// =============================================================================

export interface AWSInstance extends ProviderInstance {
  instanceType: string;
  availabilityZone: string;
  subnetId?: string;
  vpcId?: string;
  securityGroupIds: string[];
  iamInstanceProfile?: string;
  rootVolumeId?: string;
}

export interface AWSSnapshot extends ProviderSnapshot {
  amiId: string;
  region: string;
  architecture: string;
  rootSnapshotId?: string;
}

// =============================================================================
// AWS Error Handling
// =============================================================================

/**
 * Extract AWS error code from SDK v3 errors.
 *
 * AWS SDK v3 errors surface the code at different places depending on how
 * the error was constructed: .name (primary for SDK v3), .Code (some shapes),
 * or .code (older patterns). We check all three.
 */
export function getAwsErrorCode(err: unknown): string {
  if (err && typeof err === "object") {
    return (err as any).name ?? (err as any).Code ?? (err as any).code ?? "Unknown";
  }
  return "Unknown";
}

/**
 * Map AWS EC2 error codes to provider error taxonomy.
 *
 * All common EC2 errors map cleanly to ProviderOperationErrorCode.
 * No new codes needed.
 */
export function mapEC2Error(awsErrorCode: string, message: string): ConcreteProviderError {
  switch (awsErrorCode) {
    case "InsufficientInstanceCapacity":
    case "InstanceLimitExceeded":
      return new ConcreteProviderError("aws", "CAPACITY_ERROR", message, { retryable: true, retry_after_ms: 30000 });
    case "AuthFailure":
    case "UnauthorizedAccess":
      return new ConcreteProviderError("aws", "AUTH_ERROR", message);
    case "RequestLimitExceeded":
    case "Throttling":
      return new ConcreteProviderError("aws", "RATE_LIMIT_ERROR", message, { retryable: true, retry_after_ms: 5000 });
    case "InvalidParameterValue":
    case "InvalidAMIID.Malformed":
      return new ConcreteProviderError("aws", "INVALID_SPEC", message);
    case "InvalidInstanceID.NotFound":
      return new ConcreteProviderError("aws", "NOT_FOUND", message);
    case "InvalidInstanceID.Malformed":
      return new ConcreteProviderError("aws", "INVALID_SPEC", message);
    case "IdempotentParameterMismatch":
      return new ConcreteProviderError("aws", "ALREADY_EXISTS", message);
    case "VcpuLimitExceeded":
      return new ConcreteProviderError("aws", "QUOTA_EXCEEDED", message);
    case "Unsupported":
      return new ConcreteProviderError("aws", "UNSUPPORTED_OPERATION", message);
    default:
      return new ConcreteProviderError("aws", "PROVIDER_INTERNAL", message);
  }
}

// =============================================================================
// Spec Mapping
// =============================================================================

/**
 * Default instance types per architecture.
 *
 * Used when spec is an OS descriptor (e.g. "ubuntu:noble:arm64") rather than
 * an EC2 instance type. When spec IS an instance type (e.g. "c8g.large"),
 * it is used directly and arch is inferred from the instance family.
 */
const DEFAULT_INSTANCE_TYPES: Record<string, string> = {
  arm64: "t4g.micro",
  amd64: "t3.micro",
  x86_64: "t3.micro",
};

/**
 * Infer CPU architecture from an EC2 instance type string.
 *
 * Graviton (arm64) instances have families matching: letter(s) + digit(s) + 'g'
 * Examples: t4g, c7g, c8g, m7gd, r8g, hpc7g, g5g
 * Non-Graviton: p3, g4dn, c5, t3, m5 — all amd64/x86_64
 */
function inferArchFromInstanceType(instanceType: string): string {
  const family = instanceType.split(".")[0];
  return /^[a-z]\d+g/.test(family) ? "arm64" : "amd64";
}

/**
 * Ubuntu version codename → release number mapping for DescribeImages filter.
 */
const UBUNTU_VERSION_MAP: Record<string, string> = {
  noble: "24.04",
  jammy: "22.04",
  focal: "20.04",
};

/**
 * Resolve AMI for a given region, Ubuntu version codename, and architecture.
 *
 * Uses DescribeImages against Canonical's AWS account (099720109477) to find
 * the most recent published Ubuntu AMI matching the spec, then caches the
 * result for 24h using fetchWithDiskCache.
 *
 * Fallback: if DescribeImages fails (e.g. in tests), caller must handle.
 */
export async function resolveAmi(
  client: EC2Client,
  version: string,
  arch: string,
  cacheKey: string
): Promise<string> {
  return fetchWithDiskCache(cacheKey, 86_400_000, async () => {
    const ubuntuVersion = UBUNTU_VERSION_MAP[version] ?? "24.04";
    const ec2Arch = arch === "arm64" ? "arm64" : "x86_64";
    const namePattern =
      arch === "arm64"
        ? `ubuntu/images/hvm-ssd-gp3/ubuntu-${version}-${ubuntuVersion}-arm64-server-*`
        : `ubuntu/images/hvm-ssd-gp3/ubuntu-${version}-${ubuntuVersion}-amd64-server-*`;

    const result = await client.send(
      new DescribeImagesCommand({
        Owners: ["099720109477"], // Canonical's AWS account ID
        Filters: [
          { Name: "name", Values: [namePattern] },
          { Name: "state", Values: ["available"] },
          { Name: "architecture", Values: [ec2Arch] },
        ],
      })
    );

    const images = (result.Images ?? [])
      .filter((img) => img.ImageId && img.CreationDate)
      .sort((a, b) => (b.CreationDate ?? "").localeCompare(a.CreationDate ?? ""));

    if (images.length === 0) {
      throw new Error(`No Ubuntu ${version} ${arch} AMI found in region`);
    }

    return images[0].ImageId!;
  });
}

// =============================================================================
// EC2 State → Provider Status Mapping
// =============================================================================

export function mapEC2State(state: string): ProviderInstanceStatus {
  switch (state) {
    case "pending":      return "pending";
    case "running":      return "running";
    case "shutting-down": return "terminating";
    case "terminated":   return "terminated";
    case "stopping":     return "stopping";
    case "stopped":      return "stopped";
    default:             return "error";
  }
}

// =============================================================================
// AWS Provider Config
// =============================================================================

export interface AWSProviderConfig {
  region: string;
  credentials?: { accessKeyId: string; secretAccessKey: string };
  defaultSubnetId?: string;
  defaultSecurityGroupIds?: string[];
  defaultKeyName?: string;
  defaultIamProfile?: string;
  /** Override the directory where PEM key files are written. Useful in tests. */
  keysDir?: string;
  /** For tests only: inject a custom EC2Client factory. */
  _ec2ClientFactory?: (region: string) => EC2Client;
}

// =============================================================================
// AWS Provider
// =============================================================================

export class AWSProvider implements Provider<AWSInstance, AWSSnapshot> {
  readonly name = "aws" as const;
  readonly capabilities: ProviderCapabilities = PROVIDER_CAPABILITIES.aws;

  private defaultRegion: string;
  private credentials?: { accessKeyId: string; secretAccessKey: string };
  private clients = new Map<string, EC2Client>();
  private config: AWSProviderConfig;
  private ec2ClientFactory?: (region: string) => EC2Client;
  private securityGroups = new Map<string, string>(); // region → sgId
  private keyPairs = new Map<string, string>();        // region → keyName

  constructor(config: AWSProviderConfig) {
    this.defaultRegion = config.region;
    this.credentials = config.credentials;
    this.config = config;
    this.ec2ClientFactory = config._ec2ClientFactory;
  }

  /**
   * Get (or create and cache) an EC2Client for the given region.
   *
   * A separate client per region is required because EC2 is a regional
   * service — each client is bound to one endpoint.
   */
  private getClient(region?: string): EC2Client {
    const r = region ?? this.defaultRegion;
    if (!this.clients.has(r)) {
      if (this.ec2ClientFactory) {
        this.clients.set(r, this.ec2ClientFactory(r));
      } else {
        this.clients.set(
          r,
          new EC2Client({
            region: r,
            ...(this.credentials ? { credentials: this.credentials } : {}),
          })
        );
      }
    }
    return this.clients.get(r)!;
  }

  /**
   * Flush the cached EC2 clients so the next call creates fresh ones.
   * Called on auth failure to pick up refreshed credentials.
   */
  flushClientCache(): void {
    this.clients.clear();
  }

  // ─── Private: resolveInstanceParams ───────────────────────────────────────

  /**
   * Resolve AMI and instance type for a given spec in a region.
   * Shared by both spawn() and requestSpotInstance().
   *
   * spec can be either:
   *   - An EC2 instance type: "c8g.large", "p3.2xlarge" (contains '.', no ':')
   *   - An OS spec: "ubuntu:noble:arm64" (contains ':')
   *   - A bare distro name: "ubuntu" (no '.' or ':')
   */
  private async resolveInstanceParams(
    spec: string,
    region: string,
    snapshotId?: string,
  ): Promise<{ imageId: string; instanceType: string }> {
    let instanceType: string;
    let version: string;
    let arch: string;

    if (spec.includes(".") && !spec.includes(":")) {
      // spec IS the instance type (e.g., "c8g.large", "p3.2xlarge")
      instanceType = spec;
      arch = inferArchFromInstanceType(spec);
      version = "noble";
    } else {
      // spec is an OS spec (e.g., "ubuntu:noble:arm64")
      const parsed = parseSpec(spec);
      version = parsed.version;
      arch = parsed.arch;
      instanceType = DEFAULT_INSTANCE_TYPES[arch] ?? "t3.micro";
    }

    let imageId: string;
    if (snapshotId) {
      imageId = snapshotId;
    } else {
      const amiCacheKey = `ami-${region}-${version}-${arch}`;
      try {
        imageId = await resolveAmi(this.getClient(region), version, arch, amiCacheKey);
      } catch (err) {
        throw mapEC2Error(
          getAwsErrorCode(err),
          err instanceof Error ? err.message : `AMI resolution failed: ${String(err)}`
        );
      }
    }

    return { imageId, instanceType };
  }

  // ─── Core: spawn ──────────────────────────────────────────────────────────

  async spawn(options: SpawnOptions): Promise<AWSInstance> {
    const region = options.region ?? this.defaultRegion;
    const client = this.getClient(region);
    const { imageId, instanceType } = await this.resolveInstanceParams(
      options.spec, region, options.snapshotId
    );

    // Generate cloud-init user-data
    const bootstrap = this.generateBootstrap(options.bootstrap);
    const userData = Buffer.from(bootstrap.content).toString("base64");

    // Derive idempotency key from the naming triple
    const mPart = options.manifestId === null ? "none" : String(options.manifestId);
    const clientToken = `spawn:${options.controlId}-${mPart}-${options.instanceId}`;

    // Build tags — naming convention: skyrepl:managed + spec + name tag
    const tags: Record<string, string> = {
      "skyrepl:managed": "true",
      "skyrepl:spec": options.spec,
      "skyrepl:instance_id": String(options.instanceId),
      "skyrepl:spawn_key": clientToken,
      "Name": formatResourceName(options.controlId, options.manifestId, options.instanceId),
      ...(options.tags ?? {}),
    };

    // Build RunInstances command parameters
    const runParams: RunInstancesCommandInput = {
      ImageId: imageId,
      InstanceType: instanceType as RunInstancesCommandInput["InstanceType"],
      MinCount: 1,
      MaxCount: 1,
      UserData: userData,
      ClientToken: clientToken,
      TagSpecifications: [
        {
          ResourceType: "instance",
          Tags: Object.entries(tags).map(([Key, Value]) => ({ Key, Value })),
        },
      ],
      ...(this.config.defaultKeyName ? { KeyName: this.config.defaultKeyName } : {}),
      ...(this.config.defaultIamProfile
        ? { IamInstanceProfile: { Name: this.config.defaultIamProfile } }
        : {}),
    };

    // Network config: per-call overrides, then provider defaults, then startup-ensured cache
    let securityGroupIds =
      options.networkConfig?.securityGroupIds ?? this.config.defaultSecurityGroupIds;
    if (!securityGroupIds?.length) {
      const cachedSg = this.securityGroups.get(region);
      if (cachedSg) securityGroupIds = [cachedSg];
    }
    const subnetId = options.networkConfig?.subnetId ?? this.config.defaultSubnetId;
    if (securityGroupIds?.length) runParams.SecurityGroupIds = securityGroupIds;
    if (subnetId) runParams.SubnetId = subnetId;

    // Key name: from config default, then startup-ensured cache
    if (!this.config.defaultKeyName) {
      const cachedKey = this.keyPairs.get(region);
      if (cachedKey) runParams.KeyName = cachedKey;
    }

    // Spot support via RunInstances InstanceMarketOptions
    if (options.spot) {
      runParams.InstanceMarketOptions = {
        MarketType: "spot",
        SpotOptions: options.maxSpotPrice
          ? { MaxPrice: String(options.maxSpotPrice) }
          : undefined,
      };
    }

    try {
      const result = await client.send(new RunInstancesCommand(runParams));
      const inst = result.Instances?.[0];
      if (!inst) {
        throw new ConcreteProviderError("aws", "PROVIDER_INTERNAL", "RunInstances returned no instances");
      }
      return this.mapToAWSInstance(inst, options.spec, region);
    } catch (err) {
      if (err instanceof ProviderOperationError) throw err;
      throw mapEC2Error(
        getAwsErrorCode(err),
        err instanceof Error ? err.message : String(err)
      );
    }
  }

  // ─── Core: terminate ──────────────────────────────────────────────────────

  async terminate(providerId: string, region?: string): Promise<void> {
    const client = this.getClient(region);
    try {
      await client.send(
        new TerminateInstancesCommand({ InstanceIds: [providerId] })
      );
    } catch (err) {
      // Idempotent: swallow "not found" so terminate is safe to call twice
      const code = getAwsErrorCode(err);
      if (code === "InvalidInstanceID.NotFound") return;
      throw mapEC2Error(code, err instanceof Error ? err.message : String(err));
    }
  }

  // ─── Core: list ───────────────────────────────────────────────────────────

  async list(filter?: ListFilter): Promise<AWSInstance[]> {
    const region = filter?.region ?? this.defaultRegion;
    const client = this.getClient(region);

    const filters: Array<{ Name: string; Values: string[] }> = [
      { Name: "tag:skyrepl:managed", Values: ["true"] },
    ];

    if (filter?.spec) {
      filters.push({ Name: "tag:skyrepl:spec", Values: [filter.spec] });
    }

    // Pass arbitrary tag filters (e.g. skyrepl:spawn_key for reconciliation)
    if (filter?.tags) {
      for (const [key, value] of Object.entries(filter.tags)) {
        filters.push({ Name: `tag:${key}`, Values: [value] });
      }
    }

    if (filter?.status) {
      filters.push({
        Name: "instance-state-name",
        Values: filter.status.map((s) => {
          switch (s) {
            case "pending":     return "pending";
            case "running":     return "running";
            case "stopping":    return "stopping";
            case "stopped":     return "stopped";
            case "terminating": return "shutting-down";
            case "terminated":  return "terminated";
            default:            return s;
          }
        }),
      });
    } else if (!filter?.includeTerminated) {
      // Exclude terminated by default to avoid stale results
      filters.push({
        Name: "instance-state-name",
        Values: ["pending", "running", "stopping", "stopped"],
      });
    }

    return withProviderErrorMapping("aws", async () => {
      const result = await client.send(
        new DescribeInstancesCommand({
          Filters: filters,
          MaxResults: filter?.limit ?? 100,
        })
      );
      return (result.Reservations ?? []).flatMap((r) =>
        (r.Instances ?? []).map((inst) => this.mapToAWSInstance(inst, undefined, region))
      );
    }, (err) => mapEC2Error(getAwsErrorCode(err), err instanceof Error ? err.message : String(err)));
  }

  // ─── Core: get ────────────────────────────────────────────────────────────

  async get(providerId: string, region?: string): Promise<AWSInstance | null> {
    const r = region ?? this.defaultRegion;
    const client = this.getClient(r);
    try {
      const result = await client.send(
        new DescribeInstancesCommand({ InstanceIds: [providerId] })
      );
      const inst = result.Reservations?.[0]?.Instances?.[0];
      if (!inst) return null;
      return this.mapToAWSInstance(inst, undefined, r);
    } catch (err) {
      const code = getAwsErrorCode(err);
      if (code === "InvalidInstanceID.NotFound") return null;
      throw mapEC2Error(code, err instanceof Error ? err.message : String(err));
    }
  }

  // ─── Core: generateBootstrap ──────────────────────────────────────────────

  /**
   * Assemble a cloud-init user-data document from BootstrapConfig.
   *
   * Delegates entirely to assembleCloudInit() from the shared bootstrap
   * library. Returns { content, format: "cloud-init", checksum }.
   */
  generateBootstrap(config: BootstrapConfig): BootstrapScript {
    return assembleCloudInit(config);
  }

  // ─── Optional: createSnapshot ─────────────────────────────────────────────

  async createSnapshot(
    providerId: string,
    options: CreateSnapshotOptions,
    region?: string
  ): Promise<SnapshotRequest> {
    const client = this.getClient(region);
    const tags: Record<string, string> = {
      "skyrepl:managed": "true",
      ...(options.tags ?? {}),
    };

    return withProviderErrorMapping("aws", async () => {
      const result = await client.send(
        new CreateImageCommand({
          InstanceId: providerId,
          Name: options.name,
          Description: options.description,
          NoReboot: !options.includeMemory,
          TagSpecifications: [
            {
              ResourceType: "image",
              Tags: Object.entries(tags).map(([Key, Value]) => ({ Key, Value })),
            },
          ],
        })
      );

      return {
        requestId: result.ImageId!,
        status: { status: "creating" } as const,
      };
    }, (err) => mapEC2Error(getAwsErrorCode(err), err instanceof Error ? err.message : String(err)));
  }

  // ─── Optional: getSnapshotStatus ──────────────────────────────────────────

  async getSnapshotStatus(requestId: string, region?: string): Promise<SnapshotStatus> {
    const client = this.getClient(region);
    return withProviderErrorMapping("aws", async () => {
      const result = await client.send(
        new DescribeImagesCommand({ ImageIds: [requestId] })
      );
      const image = result.Images?.[0];
      if (!image) return { status: "failed" as const, error: "Image not found" };

      const statusMap: Record<string, SnapshotStatus["status"]> = {
        pending: "creating",
        available: "available",
        failed: "failed",
        deregistered: "failed",
      };

      const totalSize = (image.BlockDeviceMappings ?? []).reduce(
        (sum, bdm) => sum + (bdm.Ebs?.VolumeSize ?? 0) * 1024 * 1024 * 1024,
        0
      );

      return {
        status: statusMap[image.State ?? ""] ?? "failed",
        providerSnapshotId: image.ImageId,
        sizeBytes: totalSize,
      };
    }, (err) => mapEC2Error(getAwsErrorCode(err), err instanceof Error ? err.message : String(err)));
  }

  // ─── Optional: deleteSnapshot ─────────────────────────────────────────────

  async deleteSnapshot(providerSnapshotId: string, region?: string): Promise<void> {
    const client = this.getClient(region);
    try {
      // AMI deletion is two-step: deregister the AMI, then delete backing EBS snapshots
      const descResult = await client.send(
        new DescribeImagesCommand({ ImageIds: [providerSnapshotId] })
      );
      const image = descResult.Images?.[0];
      const backingSnapshotIds = (image?.BlockDeviceMappings ?? [])
        .map((bdm) => bdm.Ebs?.SnapshotId)
        .filter(Boolean) as string[];

      await client.send(new DeregisterImageCommand({ ImageId: providerSnapshotId }));

      for (const snapId of backingSnapshotIds) {
        try {
          await client.send(new DeleteSnapshotCommand({ SnapshotId: snapId }));
        } catch {
          // Best-effort: backing snapshot may be shared or already gone
        }
      }
    } catch (err) {
      const code = getAwsErrorCode(err);
      if (code === "InvalidAMIID.NotFound") return; // Idempotent
      throw mapEC2Error(code, err instanceof Error ? err.message : String(err));
    }
  }

  // ─── Optional: requestSpotInstance ────────────────────────────────────────

  async requestSpotInstance(options: SpotOptions): Promise<SpotRequest> {
    const region = options.region ?? this.defaultRegion;
    const client = this.getClient(region);
    const { imageId, instanceType } = await this.resolveInstanceParams(
      options.spec, region, options.snapshotId
    );

    const bootstrap = this.generateBootstrap(options.bootstrap);
    const userData = Buffer.from(bootstrap.content).toString("base64");

    try {
      const result = await client.send(
        new RequestSpotInstancesCommand({
          InstanceCount: 1,
          Type: options.requestType ?? "one-time",
          TagSpecifications: [
            {
              ResourceType: "spot-instances-request",
              Tags: [
                { Key: "skyrepl:managed", Value: "true" },
                { Key: "skyrepl:spec", Value: options.spec },
                { Key: "Name", Value: `spot-${options.spec}` },
              ],
            },
          ],
          LaunchSpecification: {
            ImageId: imageId,
            InstanceType: instanceType as RunInstancesCommandInput["InstanceType"],
            SecurityGroupIds:
              options.networkConfig?.securityGroupIds ?? this.config.defaultSecurityGroupIds,
            SubnetId: options.networkConfig?.subnetId ?? this.config.defaultSubnetId,
            UserData: userData,
          },
          SpotPrice: options.maxSpotPrice ? String(options.maxSpotPrice) : undefined,
        })
      );

      const requestId = result.SpotInstanceRequests?.[0]?.SpotInstanceRequestId;
      if (!requestId) {
        throw new ConcreteProviderError("aws", "PROVIDER_INTERNAL", "RequestSpotInstances returned no request ID");
      }
      return { requestId, status: "pending" };
    } catch (err) {
      if (err instanceof ProviderOperationError) throw err;
      throw mapEC2Error(
        getAwsErrorCode(err),
        err instanceof Error ? err.message : String(err)
      );
    }
  }

  // ─── Optional: cancelSpotRequest ──────────────────────────────────────────

  async cancelSpotRequest(requestId: string, region?: string): Promise<void> {
    const client = this.getClient(region);
    try {
      await client.send(
        new CancelSpotInstanceRequestsCommand({ SpotInstanceRequestIds: [requestId] })
      );
    } catch (err) {
      const code = getAwsErrorCode(err);
      if (code === "InvalidSpotInstanceRequestID.NotFound") return;
      throw mapEC2Error(code, err instanceof Error ? err.message : String(err));
    }
  }

  // ─── Optional: getSpotPrices ──────────────────────────────────────────────

  async getSpotPrices(spec: string, regions?: string[]): Promise<SpotPriceInfo[]> {
    let instanceType: string;
    if (spec.includes(".") && !spec.includes(":")) {
      instanceType = spec;
    } else {
      const { arch } = parseSpec(spec);
      instanceType = DEFAULT_INSTANCE_TYPES[arch] ?? "t3.micro";
    }
    const targetRegions = regions?.length ? regions : [this.defaultRegion];
    const results: SpotPriceInfo[] = [];

    for (const region of targetRegions) {
      const client = this.getClient(region);
      try {
        const result = await client.send(
          new DescribeSpotPriceHistoryCommand({
            InstanceTypes: [instanceType] as NonNullable<RunInstancesCommandInput["InstanceType"]>[],
            ProductDescriptions: ["Linux/UNIX"],
            MaxResults: 20,
          })
        );

        for (const entry of result.SpotPriceHistory ?? []) {
          results.push({
            spec: entry.InstanceType ?? instanceType,
            region,
            availabilityZone: entry.AvailabilityZone,
            price: parseFloat(entry.SpotPrice ?? "0"),
            timestamp: entry.Timestamp?.getTime() ?? Date.now(),
          });
        }
      } catch (err) {
        throw mapEC2Error(
          getAwsErrorCode(err),
          err instanceof Error ? err.message : String(err)
        );
      }
    }

    return results;
  }

  // ─── Private: credential verification (STS) ───────────────────────────────

  /**
   * Verify AWS credentials are valid by calling STS GetCallerIdentity.
   * Lightweight check: IAM is global so no per-region STS needed.
   */
  async verifyCredentials(): Promise<{ accountId: string; arn: string; userId: string }> {
    const sts = new STSClient({
      region: this.defaultRegion,
      ...(this.credentials ? { credentials: this.credentials } : {}),
    });
    const result = await sts.send(new GetCallerIdentityCommand({}));
    return {
      accountId: result.Account ?? "",
      arn: result.Arn ?? "",
      userId: result.UserId ?? "",
    };
  }

  // ─── Infrastructure ensure methods ────────────────────────────────────────

  /**
   * Ensure a SkyREPL-managed security group exists in the given region.
   * Creates one with SSH ingress if absent. Idempotent via describe-first.
   */
  async ensureSecurityGroup(region?: string): Promise<string> {
    const r = region ?? this.defaultRegion;

    // Cache hit
    const cached = this.securityGroups.get(r);
    if (cached) return cached;

    const client = this.getClient(r);
    try {
      // Check for existing SG
      const descResult = await client.send(
        new DescribeSecurityGroupsCommand({
          Filters: [
            { Name: "tag:skyrepl:managed", Values: ["true"] },
            { Name: "group-name", Values: ["skyrepl-default"] },
          ],
        })
      );

      const existing = descResult.SecurityGroups?.[0];
      if (existing?.GroupId) {
        this.securityGroups.set(r, existing.GroupId);
        return existing.GroupId;
      }

      // Create new SG
      const createResult = await client.send(
        new CreateSecurityGroupCommand({
          GroupName: "skyrepl-default",
          Description: "SkyREPL managed security group",
        })
      );
      const sgId = createResult.GroupId!;

      // Authorize SSH ingress (IPv4 + IPv6)
      await client.send(
        new AuthorizeSecurityGroupIngressCommand({
          GroupId: sgId,
          IpPermissions: [
            {
              IpProtocol: "tcp",
              FromPort: 22,
              ToPort: 22,
              IpRanges: [{ CidrIp: "0.0.0.0/0" }],
              Ipv6Ranges: [{ CidrIpv6: "::/0" }],
            },
          ],
        })
      );

      // Tag the security group
      await client.send(
        new CreateTagsCommand({
          Resources: [sgId],
          Tags: [{ Key: "skyrepl:managed", Value: "true" }],
        })
      );

      this.securityGroups.set(r, sgId);
      return sgId;
    } catch (err) {
      throw mapEC2Error(
        getAwsErrorCode(err),
        err instanceof Error ? err.message : String(err)
      );
    }
  }

  /**
   * Ensure a SkyREPL key pair exists in the given region.
   * Creates one and saves the PEM to ~/.repl/keys/aws-{region}.pem if absent.
   */
  async ensureKeyPair(region?: string): Promise<string> {
    const r = region ?? this.defaultRegion;

    // Cache hit
    const cached = this.keyPairs.get(r);
    if (cached) return cached;

    const client = this.getClient(r);
    const keyName = `skyrepl-${r}`;
    try {
      // Check for existing key pair
      const descResult = await client.send(
        new DescribeKeyPairsCommand({
          Filters: [{ Name: "key-name", Values: [keyName] }],
        })
      );

      if (descResult.KeyPairs?.length) {
        this.keyPairs.set(r, keyName);
        return keyName;
      }

      // Create new key pair
      const createResult = await client.send(
        new CreateKeyPairCommand({ KeyName: keyName })
      );

      // Save PEM to disk
      const keysDir = this.config.keysDir ?? join(homedir(), ".repl", "keys");
      await mkdir(keysDir, { recursive: true });
      const pemPath = join(keysDir, `aws-${r}.pem`);
      await writeFile(pemPath, createResult.KeyMaterial ?? "", { encoding: "utf8" });
      await chmod(pemPath, 0o600);

      this.keyPairs.set(r, keyName);
      return keyName;
    } catch (err) {
      throw mapEC2Error(
        getAwsErrorCode(err),
        err instanceof Error ? err.message : String(err)
      );
    }
  }

  /**
   * Run startup checks: verify credentials and ensure default SG + key pair exist.
   */
  async startup(): Promise<{ accountId: string; region: string; securityGroupId: string; keyName: string }> {
    const { accountId } = await this.verifyCredentials();
    const securityGroupId = await this.ensureSecurityGroup(this.defaultRegion);
    const keyName = await this.ensureKeyPair(this.defaultRegion);
    return { accountId, region: this.defaultRegion, securityGroupId, keyName };
  }

  // ─── Private: mapToAWSInstance ────────────────────────────────────────────

  private mapToAWSInstance(
    inst: {
      InstanceId?: string;
      InstanceType?: string;
      State?: { Name?: string };
      PrivateIpAddress?: string;
      PublicIpAddress?: string;
      Placement?: { AvailabilityZone?: string };
      SubnetId?: string;
      VpcId?: string;
      SecurityGroups?: Array<{ GroupId?: string }>;
      LaunchTime?: Date;
      SpotInstanceRequestId?: string;
      IamInstanceProfile?: { Arn?: string };
      BlockDeviceMappings?: Array<{ Ebs?: { VolumeId?: string } }>;
      Tags?: Array<{ Key?: string; Value?: string }>;
    },
    originalSpec?: string,
    region?: string
  ): AWSInstance {
    const r = region ?? this.defaultRegion;
    const specTag = inst.Tags?.find((t) => t.Key === "skyrepl:spec")?.Value;
    return {
      id: inst.InstanceId ?? "",
      status: mapEC2State(inst.State?.Name ?? ""),
      spec: originalSpec ?? specTag ?? inst.InstanceType ?? "",
      region: r,
      ip: inst.PublicIpAddress ?? null,
      privateIp: inst.PrivateIpAddress ?? null,
      createdAt: inst.LaunchTime?.getTime() ?? Date.now(),
      isSpot: !!inst.SpotInstanceRequestId,
      spotRequestId: inst.SpotInstanceRequestId,
      // AWS-specific fields
      instanceType: inst.InstanceType ?? "",
      availabilityZone: inst.Placement?.AvailabilityZone ?? `${r}a`,
      subnetId: inst.SubnetId,
      vpcId: inst.VpcId,
      securityGroupIds: (inst.SecurityGroups ?? []).map((sg) => sg.GroupId ?? ""),
      iamInstanceProfile: inst.IamInstanceProfile?.Arn,
      rootVolumeId: inst.BlockDeviceMappings?.[0]?.Ebs?.VolumeId,
    };
  }
}

// =============================================================================
// Lifecycle Hooks Factory
// =============================================================================

export function createAwsHooks(provider: AWSProvider): ProviderLifecycleHooks {
  return {
    async onStartup() {
      await provider.startup();
    },
    async onShutdown() {
      // No-op for now
    },
    async onHeartbeat(expectations: HeartbeatExpectations) {
      const receipts: TaskReceipt[] = [];

      for (const task of expectations.tasks) {
        switch (task.type) {
          case "health_check": {
            const start = Date.now();
            try {
              await provider.verifyCredentials();
              receipts.push({
                type: "health_check",
                status: "completed",
                result: { latencyMs: Date.now() - start },
              });
            } catch (err) {
              // Session expired or auth failure — flush cached clients so the
              // next API call picks up refreshed credentials from the ambient
              // credential chain (env vars, ~/.aws/credentials, SSO cache).
              provider.flushClientCache();
              receipts.push({
                type: "health_check",
                status: "failed",
                result: { error: err instanceof Error ? err.message : String(err) },
              });
            }
            break;
          }
          case "heartbeat_timeout_scan":
            // Provider acknowledges — control plane handles the DB scan
            receipts.push({ type: task.type, status: "completed" });
            break;

          case "reconcile_instances":
            // Provider returns list of live instances for comparison
            try {
              const instances = await provider.list({ tags: { "skyrepl:managed": "true" } });
              receipts.push({
                type: task.type,
                status: "completed",
                result: { instances: instances.map(i => ({ id: i.id, status: i.status, ip: i.ip })) },
              });
            } catch (err) {
              receipts.push({ type: task.type, status: "failed", reason: String(err) });
            }
            break;

          case "orphan_scan":
            // Provider returns full resource list for orphan detection
            try {
              const instances = await provider.list({ tags: { "skyrepl:managed": "true" } });
              receipts.push({
                type: task.type,
                status: "completed",
                result: { instances },
              });
            } catch (err) {
              receipts.push({ type: task.type, status: "failed", reason: String(err) });
            }
            break;

          default:
            receipts.push({
              type: task.type,
              status: "skipped",
              reason: `Unknown task type: ${task.type}`,
            });
        }
      }

      return { receipts };
    },
  };
}

// =============================================================================
// Default Export
// =============================================================================

// Reads region from env at module load time.
// Credentials come from the default credential chain (env, ~/.aws/credentials, IAM role).
const defaultRegion =
  process.env.AWS_DEFAULT_REGION ?? process.env.AWS_REGION ?? "us-east-1";

export default new AWSProvider({ region: defaultRegion });
