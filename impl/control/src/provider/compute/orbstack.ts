// provider/compute/orbstack.ts - OrbStack Provider Implementation
// Local VM provider using OrbStack for development and end-to-end testing.

import { createHash } from "crypto";
import type {
  Provider,
  OrbStackInstance,
  SpawnOptions,
  BootstrapConfig,
  BootstrapScript,
  ListFilter,
  CreateSnapshotOptions,
  SnapshotRequest,
  SnapshotStatus,
  ProviderSnapshot,
  ProviderCapabilities,
  ProviderInstanceStatus,
} from "../types";
import { getProviderTiming } from "@skyrepl/shared";
import { ProviderError, type ProviderErrorCode, type ProviderErrorCategory } from "../errors";

// =============================================================================
// OrbStack-specific Error
// =============================================================================

/**
 * Concrete error class for OrbStack provider operations.
 * Extends ProviderError from the provider error taxonomy.
 */
class OrbStackError extends ProviderError {
  readonly code: ProviderErrorCode;
  readonly category: ProviderErrorCategory;
  readonly retryable: boolean;
  readonly retryAfterMs?: number;

  constructor(
    message: string,
    code: ProviderErrorCode = "PROVIDER_INTERNAL",
    options?: { retryable?: boolean; retryAfterMs?: number; details?: Record<string, unknown> }
  ) {
    super(message, "orbstack", options?.details);
    this.code = code;
    this.category = this.categorizeError(code);
    this.retryable = options?.retryable ?? false;
    this.retryAfterMs = options?.retryAfterMs;
  }

  private categorizeError(code: ProviderErrorCode): ProviderErrorCategory {
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
}

// =============================================================================
// OrbStack CLI Types
// =============================================================================

interface OrbctlListItem {
  id: string;
  name: string;
  image?: { distro?: string; version?: string; arch?: string };
  state: string;
}

interface OrbctlInfoOutput {
  record: {
    id: string;
    name: string;
    image?: { distro?: string; version?: string; arch?: string };
    state: string;
  };
  disk_size?: number;
  ip4?: string;
  ip6?: string;
}

// =============================================================================
// Helpers
// =============================================================================

const VM_PREFIX = "repl-";

function isReplVM(name: string): boolean {
  return name.startsWith(VM_PREFIX) || /^repl-i\d+-\d+$/.test(name);
}

function generateVmName(instanceId?: number): string {
  return instanceId ? `repl-i${instanceId}-${Date.now()}` : `${VM_PREFIX}${Date.now()}`;
}

function mapOrbStackStatus(state: string): ProviderInstanceStatus {
  switch (state) {
    case "running":
      return "running";
    case "stopped":
      return "stopped";
    case "starting":
      return "starting";
    case "error":
      return "error";
    default:
      return "pending";
  }
}

/**
 * Parse OrbStack spec string into distro, version, and arch.
 * Format: "distro:version:arch" with defaults "ubuntu:noble:arm64"
 */
function parseSpec(spec: string): {
  distro: string;
  version: string;
  arch: string;
} {
  const parts = spec.split(":");
  return {
    distro: parts[0] || "ubuntu",
    version: parts[1] || "noble",
    arch: parts[2] || "arm64",
  };
}

/**
 * Execute a command using Bun.spawn with optional timeout.
 */
async function exec(
  command: string[],
  options?: { timeout?: number }
): Promise<{ stdout: string; stderr: string; exitCode: number }> {
  const timeout = options?.timeout ?? 30_000;

  const proc = Bun.spawn(command, {
    stdout: "pipe",
    stderr: "pipe",
  });

  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    proc.kill();
  }, timeout);

  try {
    const [stdout, stderr] = await Promise.all([
      new Response(proc.stdout).text(),
      new Response(proc.stderr).text(),
    ]);
    const exitCode = await proc.exited;

    if (timedOut) {
      throw new OrbStackError(
        `Command timed out after ${timeout}ms: ${command.join(" ")}`,
        "TIMEOUT_ERROR",
        { retryable: true, details: { command, timeout } }
      );
    }

    return { stdout, stderr, exitCode };
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Run orbctl command, falling back to `mac orbctl` if orbctl is not found
 * directly (for running inside an OrbStack VM).
 */
async function orbctl(
  args: string[],
  options?: { timeout?: number }
): Promise<{ stdout: string; stderr: string; exitCode: number }> {
  try {
    return await exec(["orbctl", ...args], options);
  } catch (err) {
    const msg = String(err);
    if (msg.includes("not found") || msg.includes("ENOENT")) {
      return exec(["mac", "orbctl", ...args], options);
    }
    throw err;
  }
}

/**
 * Map OrbctlListItem or OrbctlInfoOutput record to OrbStackInstance.
 */
function toOrbStackInstance(
  record: {
    id: string;
    name: string;
    image?: { distro?: string; version?: string; arch?: string };
    state: string;
  },
  ip?: string | null,
  diskGb?: number
): OrbStackInstance {
  const distro = record.image?.distro ?? "ubuntu";
  const version = record.image?.version ?? "noble";
  const arch = (record.image?.arch as "amd64" | "arm64") ?? "arm64";

  return {
    id: record.name,
    vmName: record.name,
    status: mapOrbStackStatus(record.state),
    spec: `${distro}:${version}:${arch}`,
    ip: ip ?? null,
    isSpot: false,
    createdAt: extractTimestamp(record.name),
    arch,
    distro,
    distroVersion: version,
    cpuCores: 0, // OrbStack shares host resources
    memoryMb: 0,
    diskGb: diskGb ?? 0,
  };
}

/**
 * Extract timestamp from VM name (repl-{timestamp} or repl-i{instanceId}-{timestamp}).
 */
function extractTimestamp(name: string): number {
  const match = name.match(/^repl-(?:i\d+-)?(\d+)$/);
  return match ? parseInt(match[1]!, 10) : Date.now();
}

/**
 * Discover IP address for an OrbStack VM using a 3-method fallback:
 * 1. orbctl info JSON (ip4 field)
 * 2. DNS resolution via getent hosts
 * 3. orbctl run hostname -I
 */
async function discoverIp(name: string): Promise<string | null> {
  // Method 1: orbctl info
  try {
    const result = await orbctl(["info", name, "--format", "json"], {
      timeout: 10_000,
    });
    if (result.exitCode === 0 && result.stdout.trim()) {
      const info: OrbctlInfoOutput = JSON.parse(result.stdout);
      if (info.ip4) return info.ip4;
    }
  } catch {
    // Fall through to next method
  }

  // Method 2: DNS resolution
  try {
    const result = await exec(
      ["getent", "hosts", `${name}.orb.local`],
      { timeout: 5_000 }
    );
    if (result.exitCode === 0 && result.stdout.trim()) {
      const ip = result.stdout.trim().split(/\s+/)[0];
      if (ip) return ip;
    }
  } catch {
    // Fall through to next method
  }

  // Method 3: orbctl run hostname -I
  try {
    const result = await orbctl(
      ["run", "-m", name, "hostname", "-I"],
      { timeout: 10_000 }
    );
    if (result.exitCode === 0 && result.stdout.trim()) {
      const ip = result.stdout.trim().split(/\s+/)[0];
      if (ip) return ip;
    }
  } catch {
    // All methods failed
  }

  return null;
}

// =============================================================================
// OrbStack Provider
// =============================================================================

export class OrbStackProvider implements Provider<OrbStackInstance> {
  readonly name = "orbstack" as const;

  readonly capabilities: ProviderCapabilities = {
    snapshots: true,
    spot: false,
    gpu: false,
    multiRegion: false,
    persistentVolumes: false,
    warmVolumes: false,
    hibernation: false,
    costExplorer: false,
    tailscaleNative: true,
    idempotentSpawn: true,
    customNetworking: false,
  };

  async spawn(options: SpawnOptions): Promise<OrbStackInstance> {
    const { distro, version, arch } = parseSpec(options.spec);
    const name = generateVmName(options.instanceId);

    // Create the VM
    const createTimeout = getProviderTiming("SPAWN_TIMEOUT_MS", "orbstack");
    const createResult = await orbctl(
      ["create", `${distro}:${version}`, name, "-a", arch],
      { timeout: createTimeout }
    );

    if (createResult.exitCode !== 0) {
      throw new OrbStackError(
        `Failed to create VM ${name}: ${createResult.stderr.trim()}`,
        "PROVIDER_INTERNAL",
        { retryable: true, details: { name, spec: options.spec, exitCode: createResult.exitCode } }
      );
    }

    // Poll for running state
    const pollInterval = getProviderTiming(
      "SPAWN_POLL_INTERVAL_MS",
      "orbstack"
    );
    const pollTimeout = 60_000;
    const pollStart = Date.now();

    while (Date.now() - pollStart < pollTimeout) {
      try {
        const infoResult = await orbctl(
          ["info", name, "--format", "json"],
          { timeout: 10_000 }
        );
        if (infoResult.exitCode === 0 && infoResult.stdout.trim()) {
          const info: OrbctlInfoOutput = JSON.parse(infoResult.stdout);
          if (info.record.state === "running") {
            break;
          }
        }
      } catch {
        // Polling failure is not fatal, will retry
      }

      if (Date.now() - pollStart >= pollTimeout) {
        throw new OrbStackError(
          `VM ${name} did not reach running state within ${pollTimeout}ms`,
          "TIMEOUT_ERROR",
          { retryable: true, details: { name, timeout: pollTimeout } }
        );
      }

      await Bun.sleep(pollInterval);
    }

    // Discover IP
    const ip = await discoverIp(name);

    // Execute bootstrap if provided
    if (options.bootstrap) {
      const bootstrapScript = this.generateBootstrap(options.bootstrap);
      const encoded = Buffer.from(bootstrapScript.content).toString("base64");

      // Two-step bootstrap: write script first, then start it detached.
      // Single-command nohup via orbctl run is unreliable — the backgrounded
      // process gets killed when orbctl's session exits.
      const writeCmd = `echo '${encoded}' | base64 -d > /tmp/bootstrap.sh && chmod +x /tmp/bootstrap.sh`;
      let writeOk = false;
      try {
        const writeResult = await orbctl(["run", "-m", name, "-u", "root", "sh", "-c", writeCmd], {
          timeout: 30_000,
        });
        writeOk = writeResult.exitCode === 0;
      } catch (err) {
        console.warn(
          `[orbstack] Bootstrap write failed for ${name} (non-fatal):`,
          err instanceof Error ? err.message : String(err)
        );
      }

      // Start bootstrap detached with setsid so it survives orbctl exit.
      // The trailing sleep ensures the shell stays alive long enough for
      // the setsid'd process to fully start before orbctl's session ends.
      if (writeOk) {
        const startCmd = `setsid /tmp/bootstrap.sh </dev/null >/tmp/bootstrap.log 2>&1 & sleep 0.5`;
        try {
          await orbctl(["run", "-m", name, "-u", "root", "sh", "-c", startCmd], {
            timeout: 30_000,
          });
        } catch (err) {
          // Bootstrap failure is NON-FATAL
          console.warn(
            `[orbstack] Bootstrap start failed for ${name} (non-fatal):`,
            err instanceof Error ? err.message : String(err)
          );
        }
      }
    }

    return toOrbStackInstance(
      {
        id: name,
        name,
        image: { distro, version, arch },
        state: "running",
      },
      ip
    );
  }

  async terminate(providerId: string): Promise<void> {
    const timeout = getProviderTiming("TERMINATE_TIMEOUT_MS", "orbstack");

    // Graceful stop before delete: gives OrbStack's DHCP allocator time to
    // reclaim the IP address. Without this, rapid create/delete cycles can
    // exhaust the IP pool (leases expire after ~24h).
    try {
      await orbctl(["stop", providerId], { timeout: 15_000 });
      await Bun.sleep(1_000);
    } catch {
      // Stop failure is non-fatal — VM may already be stopped or deleted
    }

    const result = await orbctl(["delete", "-f", providerId], { timeout });

    if (result.exitCode !== 0) {
      const stderr = result.stderr.trim().toLowerCase();
      // Idempotent: if already deleted, succeed silently
      if (stderr.includes("not found") || stderr.includes("does not exist")) {
        return;
      }
      throw new OrbStackError(
        `Failed to terminate VM ${providerId}: ${result.stderr.trim()}`,
        "PROVIDER_INTERNAL",
        { retryable: true, details: { providerId, exitCode: result.exitCode } }
      );
    }
  }

  async list(filter?: ListFilter): Promise<OrbStackInstance[]> {
    const result = await orbctl(["list", "--format", "json"], {
      timeout: 10_000,
    });

    if (result.exitCode !== 0) {
      throw new OrbStackError(
        `Failed to list VMs: ${result.stderr.trim()}`,
        "PROVIDER_INTERNAL",
        { retryable: true, details: { exitCode: result.exitCode } }
      );
    }

    const raw = result.stdout.trim();
    if (!raw || raw === "[]") {
      return [];
    }

    let items: OrbctlListItem[];
    try {
      items = JSON.parse(raw);
    } catch {
      throw new OrbStackError(
        `Failed to parse orbctl list output: ${raw.slice(0, 200)}`,
        "PROVIDER_INTERNAL",
        { retryable: false }
      );
    }

    // Filter to repl- VMs only
    const replItems = items.filter((item) => isReplVM(item.name));

    // Map to OrbStackInstance
    const instances: OrbStackInstance[] = replItems.map((item) =>
      toOrbStackInstance(item, null)
    );

    // Apply status filter if provided
    if (filter?.status && filter.status.length > 0) {
      return instances.filter((inst) => filter.status!.includes(inst.status));
    }

    return instances;
  }

  async get(providerId: string): Promise<OrbStackInstance | null> {
    const result = await orbctl(
      ["info", providerId, "--format", "json"],
      { timeout: 10_000 }
    );

    if (result.exitCode !== 0) {
      const stderr = result.stderr.trim().toLowerCase();
      if (stderr.includes("not found") || stderr.includes("does not exist")) {
        return null;
      }
      throw new OrbStackError(
        `Failed to get VM ${providerId}: ${result.stderr.trim()}`,
        "PROVIDER_INTERNAL",
        { retryable: true, details: { providerId, exitCode: result.exitCode } }
      );
    }

    const raw = result.stdout.trim();
    if (!raw) return null;

    let info: OrbctlInfoOutput;
    try {
      info = JSON.parse(raw);
    } catch {
      throw new OrbStackError(
        `Failed to parse orbctl info output for ${providerId}`,
        "PROVIDER_INTERNAL",
        { retryable: false, details: { providerId } }
      );
    }

    const diskGb = info.disk_size
      ? Math.round(info.disk_size / (1024 * 1024 * 1024))
      : undefined;

    return toOrbStackInstance(info.record, info.ip4 ?? null, diskGb);
  }

  generateBootstrap(config: BootstrapConfig): BootstrapScript {
    const envVars: Record<string, string> = {
      SKYREPL_CONTROL_PLANE_URL: config.controlPlaneUrl,
      SKYREPL_REGISTRATION_TOKEN: config.registrationToken,
      ...(config.environment ?? {}),
    };

    const envLines = Object.entries(envVars)
      .map(([key, value]) => `export ${key}="${value}"`)
      .join("\n");

    const script = `#!/bin/bash
set -eu

exec > /tmp/bootstrap.log 2>&1

# Disable apt-daily to prevent lock contention
systemctl disable --now apt-daily.timer apt-daily-upgrade.timer 2>/dev/null || true

# Write agent config
mkdir -p /etc/skyrepl
cat > /etc/skyrepl/config.json << 'SKYREPL_CONFIG_EOF'
${JSON.stringify(
  {
    controlPlaneUrl: config.controlPlaneUrl,
    registrationToken: config.registrationToken,
  },
  null,
  2
)}
SKYREPL_CONFIG_EOF

${config.initScript ? `# User init script\n${config.initScript}\n` : ""}# Download agent files
mkdir -p /opt/skyrepl-agent
cd /opt/skyrepl-agent
AGENT_BASE="${config.controlPlaneUrl}/v1/agent/download"
for f in agent.py executor.py heartbeat.py logs.py sse.py http_client.py; do
  curl -fsSL "$AGENT_BASE/$f" -o "$f"
done

# Set environment variables
${envLines}

# Start agent
cd /opt/skyrepl-agent
exec python3 agent.py
`;

    const checksum = createHash("sha256").update(script).digest("hex");

    return {
      content: script,
      format: "shell",
      checksum,
    };
  }

  async createSnapshot(
    providerId: string,
    options: CreateSnapshotOptions
  ): Promise<SnapshotRequest> {
    const snapshotName = options.name;

    // Clone the VM
    const cloneResult = await orbctl(["clone", providerId, snapshotName], {
      timeout: 120_000,
    });

    if (cloneResult.exitCode !== 0) {
      throw new OrbStackError(
        `Failed to clone VM ${providerId} as ${snapshotName}: ${cloneResult.stderr.trim()}`,
        "PROVIDER_INTERNAL",
        { retryable: true, details: { providerId, snapshotName, exitCode: cloneResult.exitCode } }
      );
    }

    // Stop the cloned VM to freeze it as a snapshot
    try {
      await orbctl(["stop", snapshotName], { timeout: 30_000 });
    } catch (err) {
      console.warn(
        `[orbstack] Failed to stop snapshot VM ${snapshotName} (non-fatal):`,
        err instanceof Error ? err.message : String(err)
      );
    }

    return {
      requestId: snapshotName,
      status: {
        status: "available",
        providerSnapshotId: snapshotName,
      },
    };
  }

  async getSnapshotStatus(requestId: string): Promise<SnapshotStatus> {
    const result = await orbctl(
      ["info", requestId, "--format", "json"],
      { timeout: 10_000 }
    );

    if (result.exitCode !== 0) {
      const stderr = result.stderr.trim().toLowerCase();
      if (stderr.includes("not found") || stderr.includes("does not exist")) {
        return { status: "failed", error: `Snapshot ${requestId} not found` };
      }
      return {
        status: "failed",
        error: `Failed to query snapshot status: ${result.stderr.trim()}`,
      };
    }

    let info: OrbctlInfoOutput;
    try {
      info = JSON.parse(result.stdout);
    } catch {
      return {
        status: "failed",
        error: `Failed to parse snapshot info for ${requestId}`,
      };
    }

    // A stopped clone is a "ready" snapshot
    const state = info.record.state;
    if (state === "stopped") {
      return {
        status: "available",
        providerSnapshotId: requestId,
        sizeBytes: info.disk_size,
      };
    }
    if (state === "running" || state === "starting") {
      return {
        status: "creating",
        providerSnapshotId: requestId,
      };
    }
    if (state === "error") {
      return {
        status: "failed",
        providerSnapshotId: requestId,
        error: "Snapshot VM in error state",
      };
    }

    return {
      status: "pending",
      providerSnapshotId: requestId,
    };
  }

  async deleteSnapshot(providerSnapshotId: string): Promise<void> {
    // Graceful stop before delete to help DHCP allocator reclaim the IP
    try {
      await orbctl(["stop", providerSnapshotId], { timeout: 15_000 });
      await Bun.sleep(1_000);
    } catch {
      // Stop failure is non-fatal — snapshot VM may already be stopped
    }

    const result = await orbctl(["delete", "-f", providerSnapshotId], {
      timeout: 30_000,
    });

    if (result.exitCode !== 0) {
      const stderr = result.stderr.trim().toLowerCase();
      // Idempotent: if already deleted, succeed silently
      if (stderr.includes("not found") || stderr.includes("does not exist")) {
        return;
      }
      throw new OrbStackError(
        `Failed to delete snapshot ${providerSnapshotId}: ${result.stderr.trim()}`,
        "PROVIDER_INTERNAL",
        { retryable: true, details: { providerSnapshotId, exitCode: result.exitCode } }
      );
    }
  }

  async getSnapshotByName(name: string): Promise<ProviderSnapshot | null> {
    const result = await orbctl(["info", name, "--format", "json"], {
      timeout: 10_000,
    });

    if (result.exitCode !== 0) {
      const stderr = result.stderr.trim().toLowerCase();
      if (stderr.includes("not found") || stderr.includes("does not exist")) {
        return null;
      }
      throw new OrbStackError(
        `Failed to query snapshot ${name}: ${result.stderr.trim()}`,
        "PROVIDER_INTERNAL",
        { retryable: true, details: { name, exitCode: result.exitCode } }
      );
    }

    const raw = result.stdout.trim();
    if (!raw) return null;

    let info: OrbctlInfoOutput;
    try {
      info = JSON.parse(raw);
    } catch {
      return null;
    }

    const state = info.record.state;
    let snapshotStatus: "available" | "pending" | "failed";
    if (state === "stopped") {
      snapshotStatus = "available";
    } else if (state === "error") {
      snapshotStatus = "failed";
    } else {
      snapshotStatus = "pending";
    }

    return {
      id: info.record.name,
      name: info.record.name,
      status: snapshotStatus,
      createdAt: extractTimestamp(info.record.name),
      sizeBytes: info.disk_size,
      spec: info.record.image
        ? `${info.record.image.distro ?? "ubuntu"}:${info.record.image.version ?? "noble"}:${info.record.image.arch ?? "arm64"}`
        : undefined,
    };
  }
}

// Export both named and default
export { OrbStackError, mapOrbStackStatus, generateVmName, parseSpec, VM_PREFIX };
export default new OrbStackProvider();
