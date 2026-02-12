// provider/compute/aws.ts - AWS EC2 Provider (Skeleton for PROV-01 Validation)
//
// Dry-run implementation to validate the Provider interface against real AWS
// EC2 API patterns. NOT production-ready — see PROV-02 for full implementation.
//
// This skeleton:
// - Compiles against Provider<AWSInstance, AWSSnapshot>
// - Maps all required + optional methods to EC2 API calls
// - Uses constructor-injected EC2 client for testability (no real AWS SDK dep)
// - Documents interface gaps discovered during validation

import { createHash } from "crypto";
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
import { ProviderError, type ProviderErrorCode, type ProviderErrorCategory } from "../errors";

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
// EC2 Client Interface (for dependency injection / mocking)
// =============================================================================

/** Minimal EC2 client interface for PROV-01 validation. */
export interface EC2Client {
  runInstances(params: RunInstancesParams): Promise<RunInstancesResult>;
  terminateInstances(instanceIds: string[]): Promise<void>;
  describeInstances(params: DescribeInstancesParams): Promise<DescribeInstancesResult>;
  createImage(params: CreateImageParams): Promise<{ imageId: string }>;
  describeImages(imageIds: string[]): Promise<DescribeImagesResult>;
  deregisterImage(imageId: string): Promise<void>;
  deleteSnapshot(snapshotId: string): Promise<void>;
  requestSpotInstances(params: SpotInstanceParams): Promise<{ requestId: string }>;
  cancelSpotInstanceRequests(requestIds: string[]): Promise<void>;
  describeSpotPriceHistory(params: SpotPriceParams): Promise<SpotPriceResult>;
}

export interface RunInstancesParams {
  imageId: string;
  instanceType: string;
  minCount: number;
  maxCount: number;
  keyName?: string;
  securityGroupIds?: string[];
  subnetId?: string;
  userData?: string; // base64-encoded
  clientToken?: string;
  tagSpecifications?: Array<{
    resourceType: string;
    tags: Record<string, string>;
  }>;
  iamInstanceProfile?: { name: string };
  instanceMarketOptions?: { marketType: "spot"; spotOptions?: { maxPrice?: string } };
}

export interface RunInstancesResult {
  instances: Array<{
    instanceId: string;
    instanceType: string;
    state: string;
    privateIpAddress?: string;
    publicIpAddress?: string;
    availabilityZone?: string;
    subnetId?: string;
    vpcId?: string;
    securityGroups?: Array<{ groupId: string }>;
    launchTime?: number;
    spotInstanceRequestId?: string;
  }>;
}

export interface DescribeInstancesParams {
  instanceIds?: string[];
  filters?: Array<{ name: string; values: string[] }>;
  maxResults?: number;
}

export interface DescribeInstancesResult {
  reservations: Array<{
    instances: Array<{
      instanceId: string;
      instanceType: string;
      state: string;
      privateIpAddress?: string;
      publicIpAddress?: string;
      availabilityZone?: string;
      subnetId?: string;
      vpcId?: string;
      securityGroups?: Array<{ groupId: string }>;
      launchTime?: number;
      spotInstanceRequestId?: string;
      iamInstanceProfile?: { arn: string };
      blockDeviceMappings?: Array<{ ebs?: { volumeId: string } }>;
    }>;
  }>;
}

export interface CreateImageParams {
  instanceId: string;
  name: string;
  description?: string;
  noReboot?: boolean;
  tagSpecifications?: Array<{
    resourceType: string;
    tags: Record<string, string>;
  }>;
}

export interface DescribeImagesResult {
  images: Array<{
    imageId: string;
    name: string;
    state: "pending" | "available" | "failed" | "deregistered";
    architecture?: string;
    blockDeviceMappings?: Array<{
      ebs?: { snapshotId: string; volumeSize: number };
    }>;
    creationDate?: string;
  }>;
}

export interface SpotInstanceParams {
  instanceCount: number;
  type: "one-time" | "persistent";
  launchSpecification: {
    imageId: string;
    instanceType: string;
    securityGroupIds?: string[];
    subnetId?: string;
    userData?: string;
  };
  spotPrice?: string;
}

export interface SpotPriceParams {
  instanceTypes: string[];
  productDescriptions: string[];
  availabilityZone?: string;
  startTime?: Date;
  maxResults?: number;
}

export interface SpotPriceResult {
  spotPriceHistory: Array<{
    instanceType: string;
    availabilityZone: string;
    spotPrice: string;
    timestamp: number;
    productDescription: string;
  }>;
}

// =============================================================================
// AWS Error
// =============================================================================

class AWSError extends ProviderError {
  readonly code: ProviderErrorCode;
  readonly category: ProviderErrorCategory;
  readonly retryable: boolean;
  readonly retryAfterMs?: number;

  constructor(
    message: string,
    code: ProviderErrorCode = "PROVIDER_INTERNAL",
    options?: { retryable?: boolean; retryAfterMs?: number; details?: Record<string, unknown> }
  ) {
    super(message, "aws", options?.details);
    this.code = code;
    this.category = categorizeError(code);
    this.retryable = options?.retryable ?? false;
    this.retryAfterMs = options?.retryAfterMs;
  }
}

function categorizeError(code: ProviderErrorCode): ProviderErrorCategory {
  switch (code) {
    case "CAPACITY_ERROR":
    case "QUOTA_EXCEEDED":
    case "SPOT_INTERRUPTED":
      return "capacity";
    case "AUTH_ERROR":
      return "auth";
    case "RATE_LIMIT_ERROR":
      return "rate_limit";
    case "INVALID_SPEC":
    case "REGION_UNAVAILABLE":
      return "validation";
    case "NOT_FOUND":
      return "not_found";
    case "ALREADY_EXISTS":
    case "INVALID_STATE":
      return "conflict";
    default:
      return "internal";
  }
}

/**
 * Map AWS EC2 error codes to provider error taxonomy.
 *
 * Gap analysis: All common EC2 errors map cleanly. No new ProviderErrorCode needed.
 */
export function mapEC2Error(awsErrorCode: string, message: string): AWSError {
  switch (awsErrorCode) {
    case "InsufficientInstanceCapacity":
    case "InstanceLimitExceeded":
      return new AWSError(message, "CAPACITY_ERROR", { retryable: true, retryAfterMs: 30000 });
    case "AuthFailure":
    case "UnauthorizedAccess":
      return new AWSError(message, "AUTH_ERROR");
    case "RequestLimitExceeded":
    case "Throttling":
      return new AWSError(message, "RATE_LIMIT_ERROR", { retryable: true, retryAfterMs: 5000 });
    case "InvalidParameterValue":
    case "InvalidAMIID.Malformed":
      return new AWSError(message, "INVALID_SPEC");
    case "InvalidInstanceID.NotFound":
      return new AWSError(message, "NOT_FOUND");
    case "InvalidInstanceID.Malformed":
      return new AWSError(message, "INVALID_SPEC");
    case "IdempotentParameterMismatch":
      return new AWSError(message, "ALREADY_EXISTS");
    case "VcpuLimitExceeded":
      return new AWSError(message, "QUOTA_EXCEEDED");
    case "Unsupported":
      return new AWSError(message, "UNSUPPORTED_OPERATION");
    default:
      return new AWSError(message, "PROVIDER_INTERNAL");
  }
}

// =============================================================================
// Spec Mapping
// =============================================================================

/**
 * Map SkyREPL spec string to AWS instance type.
 *
 * Gap analysis: SkyREPL spec format ("ubuntu:noble:arm64") doesn't directly
 * encode instance size. AWS needs explicit instance type. Two approaches:
 *   1. Spec includes size hint (e.g., "ubuntu:noble:arm64:t4g.micro")
 *   2. Separate instanceType field in SpawnOptions
 *
 * For now: treat spec as AMI selector, default instance type per architecture.
 * Full PROV-02 should add instanceType to SpawnOptions or resolve via a spec catalog.
 */
const DEFAULT_INSTANCE_TYPES: Record<string, string> = {
  arm64: "t4g.micro",
  amd64: "t3.micro",
  x86_64: "t3.micro",
};

const DEFAULT_AMI: Record<string, string> = {
  "ubuntu:noble:arm64": "ami-0000000000000000a", // Placeholder
  "ubuntu:noble:amd64": "ami-0000000000000000b", // Placeholder
  "ubuntu:jammy:arm64": "ami-0000000000000000c", // Placeholder
  "ubuntu:jammy:amd64": "ami-0000000000000000d", // Placeholder
};

function resolveSpec(spec: string): { imageId: string; instanceType: string; arch: string } {
  const parts = spec.split(":");
  const arch = parts[2] ?? "arm64";
  const imageId = DEFAULT_AMI[spec] ?? DEFAULT_AMI[`ubuntu:noble:${arch}`] ?? "ami-0000000000000000a";
  const instanceType = DEFAULT_INSTANCE_TYPES[arch] ?? "t4g.micro";
  return { imageId, instanceType, arch };
}

// =============================================================================
// EC2 State → Provider Status Mapping
// =============================================================================

function mapEC2State(state: string): ProviderInstanceStatus {
  switch (state) {
    case "pending": return "pending";
    case "running": return "running";
    case "shutting-down": return "terminating";
    case "terminated": return "terminated";
    case "stopping": return "stopping";
    case "stopped": return "stopped";
    default: return "error";
  }
}

// =============================================================================
// AWS Provider
// =============================================================================

export interface AWSProviderConfig {
  ec2: EC2Client;
  region: string;
  defaultSubnetId?: string;
  defaultSecurityGroupIds?: string[];
  defaultKeyName?: string;
  defaultIamProfile?: string;
}

export class AWSProvider implements Provider<AWSInstance, AWSSnapshot> {
  readonly name = "aws" as const;
  readonly capabilities: ProviderCapabilities = PROVIDER_CAPABILITIES.aws;

  private ec2: EC2Client;
  private region: string;
  private config: AWSProviderConfig;

  constructor(config: AWSProviderConfig) {
    this.ec2 = config.ec2;
    this.region = config.region;
    this.config = config;
  }

  // ─── Core: spawn ──────────────────────────────────────────────────────

  async spawn(options: SpawnOptions): Promise<AWSInstance> {
    const { imageId, instanceType } = resolveSpec(options.spec);

    const tags: Record<string, string> = {
      "repl-managed": "true",
      ...(options.tags ?? {}),
    };
    if (options.instanceId != null) {
      tags["repl-instance-id"] = String(options.instanceId);
    }
    if (options.clientRequestId) {
      tags["skyrepl:spawn_key"] = options.clientRequestId;
    }

    const bootstrap = this.generateBootstrap(options.bootstrap);
    const userData = Buffer.from(bootstrap.content).toString("base64");

    const params: RunInstancesParams = {
      imageId: options.snapshotId ?? imageId,
      instanceType,
      minCount: 1,
      maxCount: 1,
      keyName: this.config.defaultKeyName,
      securityGroupIds: options.networkConfig?.securityGroupIds ?? this.config.defaultSecurityGroupIds,
      subnetId: options.networkConfig?.subnetId ?? this.config.defaultSubnetId,
      userData,
      clientToken: options.clientRequestId,
      tagSpecifications: [{
        resourceType: "instance",
        tags,
      }],
      iamInstanceProfile: this.config.defaultIamProfile
        ? { name: this.config.defaultIamProfile }
        : undefined,
    };

    // Spot instance support
    if (options.spot) {
      params.instanceMarketOptions = {
        marketType: "spot",
        spotOptions: options.maxSpotPrice
          ? { maxPrice: String(options.maxSpotPrice) }
          : undefined,
      };
    }

    try {
      const result = await this.ec2.runInstances(params);
      const inst = result.instances[0];
      if (!inst) throw new AWSError("RunInstances returned no instances", "PROVIDER_INTERNAL");

      return this.mapToAWSInstance(inst, options.spec);
    } catch (err) {
      if (err instanceof AWSError) throw err;
      throw mapEC2Error(
        (err as any)?.code ?? "Unknown",
        err instanceof Error ? err.message : String(err)
      );
    }
  }

  // ─── Core: terminate ─────────────────────────────────────────────────

  async terminate(providerId: string): Promise<void> {
    try {
      await this.ec2.terminateInstances([providerId]);
    } catch (err) {
      // Idempotent: ignore already-terminated
      const code = (err as any)?.code;
      if (code === "InvalidInstanceID.NotFound") return;
      throw mapEC2Error(code ?? "Unknown", err instanceof Error ? err.message : String(err));
    }
  }

  // ─── Core: list ──────────────────────────────────────────────────────

  async list(filter?: ListFilter): Promise<AWSInstance[]> {
    const params: DescribeInstancesParams = {
      filters: [{ name: "tag:repl-managed", values: ["true"] }],
      maxResults: filter?.limit ?? 100,
    };

    if (filter?.spec) {
      params.filters!.push({ name: "instance-type", values: [filter.spec] });
    }
    if (filter?.status) {
      params.filters!.push({
        name: "instance-state-name",
        values: filter.status.map((s) => {
          // Map provider status back to EC2 state names
          switch (s) {
            case "pending": return "pending";
            case "running": return "running";
            case "stopping": return "stopping";
            case "stopped": return "stopped";
            case "terminating": return "shutting-down";
            case "terminated": return "terminated";
            default: return s;
          }
        }),
      });
    }
    if (!filter?.includeTerminated) {
      params.filters!.push({
        name: "instance-state-name",
        values: ["pending", "running", "stopping", "stopped"],
      });
    }

    try {
      const result = await this.ec2.describeInstances(params);
      return result.reservations.flatMap((r) =>
        r.instances.map((inst) => this.mapToAWSInstance(inst))
      );
    } catch (err) {
      throw mapEC2Error(
        (err as any)?.code ?? "Unknown",
        err instanceof Error ? err.message : String(err)
      );
    }
  }

  // ─── Core: get ───────────────────────────────────────────────────────

  async get(providerId: string): Promise<AWSInstance | null> {
    try {
      const result = await this.ec2.describeInstances({ instanceIds: [providerId] });
      const inst = result.reservations[0]?.instances[0];
      if (!inst) return null;
      return this.mapToAWSInstance(inst);
    } catch (err) {
      const code = (err as any)?.code;
      if (code === "InvalidInstanceID.NotFound") return null;
      throw mapEC2Error(code ?? "Unknown", err instanceof Error ? err.message : String(err));
    }
  }

  // ─── Core: generateBootstrap ─────────────────────────────────────────

  generateBootstrap(config: BootstrapConfig): BootstrapScript {
    // Generate cloud-init user-data script
    const lines = [
      "#!/bin/bash",
      "set -euo pipefail",
      "",
      "# SkyREPL agent bootstrap (AWS)",
      `export SKYREPL_CONTROL_PLANE_URL="${config.controlPlaneUrl}"`,
      `export SKYREPL_REGISTRATION_TOKEN="${config.registrationToken}"`,
      `export SKYREPL_AGENT_URL="${config.agentUrl}"`,
    ];

    // Environment variables
    if (config.environment) {
      for (const [key, value] of Object.entries(config.environment)) {
        lines.push(`export ${key}="${value}"`);
      }
    }

    // Feature installations
    if (config.features) {
      for (const feature of config.features) {
        switch (feature.name) {
          case "nvidia-driver":
            lines.push("", "# Install NVIDIA driver", "apt-get update -qq", "apt-get install -y nvidia-driver-535");
            break;
          case "cuda":
            lines.push("", "# Install CUDA", "apt-get update -qq", "apt-get install -y cuda-toolkit-12-2");
            break;
          case "docker":
            lines.push("", "# Install Docker", "curl -fsSL https://get.docker.com | sh");
            break;
        }
      }
    }

    // Init script
    if (config.initScript) {
      lines.push("", "# User init script", config.initScript);
    }

    // Agent download and start
    lines.push(
      "",
      "# Download and start agent",
      "mkdir -p /opt/skyrepl",
      `curl -fsSL "$SKYREPL_AGENT_URL" -o /opt/skyrepl/agent.py`,
      "cd /opt/skyrepl",
      `python3 agent.py &`,
    );

    const content = lines.join("\n") + "\n";
    const checksum = createHash("sha256").update(content).digest("hex");

    return { content, format: "cloud-init", checksum };
  }

  // ─── Optional: createSnapshot ────────────────────────────────────────

  async createSnapshot(providerId: string, options: CreateSnapshotOptions): Promise<SnapshotRequest> {
    const tags: Record<string, string> = {
      "repl-managed": "true",
      ...(options.tags ?? {}),
    };

    try {
      const result = await this.ec2.createImage({
        instanceId: providerId,
        name: options.name,
        description: options.description,
        noReboot: !options.includeMemory,
        tagSpecifications: [{ resourceType: "image", tags }],
      });

      return {
        requestId: result.imageId,
        status: { status: "creating" },
      };
    } catch (err) {
      throw mapEC2Error(
        (err as any)?.code ?? "Unknown",
        err instanceof Error ? err.message : String(err)
      );
    }
  }

  // ─── Optional: getSnapshotStatus ─────────────────────────────────────

  async getSnapshotStatus(requestId: string): Promise<SnapshotStatus> {
    try {
      const result = await this.ec2.describeImages([requestId]);
      const image = result.images[0];
      if (!image) return { status: "failed", error: "Image not found" };

      const statusMap: Record<string, SnapshotStatus["status"]> = {
        pending: "creating",
        available: "available",
        failed: "failed",
        deregistered: "failed",
      };

      const totalSize = (image.blockDeviceMappings ?? []).reduce(
        (sum, bdm) => sum + (bdm.ebs?.volumeSize ?? 0) * 1024 * 1024 * 1024,
        0
      );

      return {
        status: statusMap[image.state] ?? "failed",
        providerSnapshotId: image.imageId,
        sizeBytes: totalSize,
      };
    } catch (err) {
      throw mapEC2Error(
        (err as any)?.code ?? "Unknown",
        err instanceof Error ? err.message : String(err)
      );
    }
  }

  // ─── Optional: deleteSnapshot ────────────────────────────────────────

  async deleteSnapshot(providerSnapshotId: string): Promise<void> {
    try {
      // AMI deletion requires: deregister image + delete backing EBS snapshot
      const result = await this.ec2.describeImages([providerSnapshotId]);
      const image = result.images[0];
      const backingSnapshotIds = (image?.blockDeviceMappings ?? [])
        .map((bdm) => bdm.ebs?.snapshotId)
        .filter(Boolean) as string[];

      await this.ec2.deregisterImage(providerSnapshotId);

      for (const snapId of backingSnapshotIds) {
        try {
          await this.ec2.deleteSnapshot(snapId);
        } catch {
          // Best effort — backing snapshot may be shared
        }
      }
    } catch (err) {
      const code = (err as any)?.code;
      if (code === "InvalidAMIID.NotFound") return; // Idempotent
      throw mapEC2Error(code ?? "Unknown", err instanceof Error ? err.message : String(err));
    }
  }

  // ─── Optional: requestSpotInstance ───────────────────────────────────

  async requestSpotInstance(options: SpotOptions): Promise<SpotRequest> {
    const { imageId, instanceType } = resolveSpec(options.spec);

    try {
      const result = await this.ec2.requestSpotInstances({
        instanceCount: 1,
        type: options.requestType ?? "one-time",
        launchSpecification: {
          imageId: options.snapshotId ?? imageId,
          instanceType,
          securityGroupIds: options.networkConfig?.securityGroupIds ?? this.config.defaultSecurityGroupIds,
          subnetId: options.networkConfig?.subnetId ?? this.config.defaultSubnetId,
          userData: Buffer.from(this.generateBootstrap(options.bootstrap).content).toString("base64"),
        },
        spotPrice: options.maxSpotPrice ? String(options.maxSpotPrice) : undefined,
      });

      return {
        requestId: result.requestId,
        status: "pending",
      };
    } catch (err) {
      throw mapEC2Error(
        (err as any)?.code ?? "Unknown",
        err instanceof Error ? err.message : String(err)
      );
    }
  }

  // ─── Optional: cancelSpotRequest ─────────────────────────────────────

  async cancelSpotRequest(requestId: string): Promise<void> {
    try {
      await this.ec2.cancelSpotInstanceRequests([requestId]);
    } catch (err) {
      const code = (err as any)?.code;
      if (code === "InvalidSpotInstanceRequestID.NotFound") return;
      throw mapEC2Error(code ?? "Unknown", err instanceof Error ? err.message : String(err));
    }
  }

  // ─── Optional: getSpotPrices ─────────────────────────────────────────

  async getSpotPrices(spec: string, regions?: string[]): Promise<SpotPriceInfo[]> {
    const { instanceType } = resolveSpec(spec);

    try {
      const result = await this.ec2.describeSpotPriceHistory({
        instanceTypes: [instanceType],
        productDescriptions: ["Linux/UNIX"],
        maxResults: 20,
      });

      return result.spotPriceHistory.map((entry) => ({
        spec: entry.instanceType,
        region: this.region,
        availabilityZone: entry.availabilityZone,
        price: parseFloat(entry.spotPrice),
        timestamp: entry.timestamp,
      }));
    } catch (err) {
      throw mapEC2Error(
        (err as any)?.code ?? "Unknown",
        err instanceof Error ? err.message : String(err)
      );
    }
  }

  // ─── Private: mapToAWSInstance ────────────────────────────────────────

  private mapToAWSInstance(inst: {
    instanceId: string;
    instanceType: string;
    state: string;
    privateIpAddress?: string;
    publicIpAddress?: string;
    availabilityZone?: string;
    subnetId?: string;
    vpcId?: string;
    securityGroups?: Array<{ groupId: string }>;
    launchTime?: number;
    spotInstanceRequestId?: string;
    iamInstanceProfile?: { arn: string };
    blockDeviceMappings?: Array<{ ebs?: { volumeId: string } }>;
  }, originalSpec?: string): AWSInstance {
    return {
      id: inst.instanceId,
      status: mapEC2State(inst.state),
      spec: originalSpec ?? inst.instanceType,
      region: this.region,
      ip: inst.publicIpAddress ?? null,
      privateIp: inst.privateIpAddress ?? null,
      createdAt: inst.launchTime ?? Date.now(),
      isSpot: !!inst.spotInstanceRequestId,
      spotRequestId: inst.spotInstanceRequestId,
      // AWS-specific fields
      instanceType: inst.instanceType,
      availabilityZone: inst.availabilityZone ?? `${this.region}a`,
      subnetId: inst.subnetId,
      vpcId: inst.vpcId,
      securityGroupIds: (inst.securityGroups ?? []).map((sg) => sg.groupId),
      iamInstanceProfile: inst.iamInstanceProfile?.arn,
      rootVolumeId: inst.blockDeviceMappings?.[0]?.ebs?.volumeId,
    };
  }
}

// =============================================================================
// Interface Gap Analysis (PROV-01 Findings)
// =============================================================================
//
// 1. SPEC RESOLUTION GAP: SpawnOptions.spec is a distro:version:arch string
//    but AWS needs an AMI ID and instance type. Need either:
//    a) A spec catalog that maps "gpu-small" → {ami, instanceType}
//    b) Extended SpawnOptions with instanceType field
//    Recommendation: Add optional `instanceType?: string` to SpawnOptions.
//
// 2. IAM PROFILE GAP: No SpawnOptions field for IAM role/instance profile.
//    Currently handled via config default. For multi-tenant or per-run IAM,
//    need `iamProfile?: string` in SpawnOptions.
//    Recommendation: Add to SpawnOptions (optional, provider-specific).
//
// 3. KEY PAIR GAP: SSH key injection is implicit via config.defaultKeyName.
//    For SkyREPL's bootstrap-based model this is fine (no SSH expected), but
//    cloud providers typically require key pairs for emergency access.
//    Recommendation: Document as provider config concern, not interface concern.
//
// 4. VPC/SUBNET: NetworkConfig already covers this well. No gap.
//
// 5. SECURITY GROUP: NetworkConfig.securityGroupIds handles this. No gap.
//
// 6. MULTI-REGION: Provider interface doesn't support per-call region override.
//    AWSProvider binds to a single region in constructor. Multi-region needs
//    multiple AWSProvider instances (one per region).
//    Recommendation: Document pattern; no interface change needed.
//
// 7. ERROR MAPPING: All common EC2 errors map cleanly to ProviderErrorCode.
//    No new error codes needed.
//
// 8. IDEMPOTENT SPAWN: AWS ClientToken (24h dedup) maps to
//    SpawnOptions.clientRequestId. Works perfectly.
//
// 9. SNAPSHOT TWO-STEP: AMI deletion requires deregistering the AMI AND
//    deleting the backing EBS snapshot. deleteSnapshot() handles this
//    internally. The interface's single-method design works.
//
// 10. BOOTSTRAP FORMAT: cloud-init user-data is the right format for AWS.
//     generateBootstrap produces shell script which EC2 executes as user-data.
//     No gap.

// Default export for registry (NOT wired yet — see registry.ts comment)
// export default new AWSProvider({ ... });
