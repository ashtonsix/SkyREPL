// provider/compute/orbstack.ts - OrbStack Provider Implementation
// Local VM provider using OrbStack for development and end-to-end testing.

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
import { getProviderTiming } from "@skyrepl/contracts";
import { ConcreteProviderError } from "../errors";
import { parseSpec } from "../types";
import { formatResourceName, isReplResource } from "../../material/control-id";
import { assembleShellBootstrap } from "../bootstrap/shell";
import type {
  ProviderLifecycleHooks,
  HeartbeatExpectations,
  TaskReceipt,
} from "../extensions";

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
// Exec Injection Types
// =============================================================================

/**
 * Signature for the low-level command executor.
 * Matches the module-level `exec` implementation and can be overridden in
 * tests to avoid spawning real processes.
 */
export type ExecFunction = (
  command: string[],
  options?: { timeout?: number }
) => Promise<{ stdout: string; stderr: string; exitCode: number }>;

/** Constructor config for OrbStackProvider. */
export interface OrbStackProviderConfig {
  /**
   * For tests only: inject a custom exec function.
   * When provided, ALL shell calls (both direct exec() and orbctl() paths)
   * use this function instead of Bun.spawn.
   */
  _execFactory?: ExecFunction;
  /** For tests only: override the spawn poll timeout (default 60_000ms). */
  _spawnPollTimeoutMs?: number;
}

// =============================================================================
// Helpers
// =============================================================================

const VM_PREFIX = "repl-";

function isReplVM(name: string): boolean {
  // New format: repl-{cid}-{mid}-{rid}
  if (isReplResource(name)) return true;
  // Legacy format: repl-{timestamp} or repl-i{id}-{timestamp}
  return name.startsWith("repl-");
}

function generateVmName(options: SpawnOptions): string {
  return formatResourceName(options.controlId, options.manifestId, options.instanceId);
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
 * Execute a command using Bun.spawn with optional timeout.
 * This is the real implementation used when no _execFactory is injected.
 */
async function realExec(
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
      throw new ConcreteProviderError("orbstack", "TIMEOUT_ERROR",
        `Command timed out after ${timeout}ms: ${command.join(" ")}`,
        { retryable: true, details: { command, timeout } }
      );
    }

    return { stdout, stderr, exitCode };
  } finally {
    clearTimeout(timer);
  }
}

/**
 * Run orbctl command via the provided exec function, falling back to
 * `mac orbctl` if orbctl is not found directly (for running inside an
 * OrbStack VM).
 */
async function orbctlVia(
  execFn: ExecFunction,
  args: string[],
  options?: { timeout?: number }
): Promise<{ stdout: string; stderr: string; exitCode: number }> {
  try {
    return await execFn(["orbctl", ...args], options);
  } catch (err) {
    const msg = String(err);
    if (msg.includes("not found") || msg.includes("ENOENT")) {
      return execFn(["mac", "orbctl", ...args], options);
    }
    throw err;
  }
}

/**
 * Discover IP address for an OrbStack VM using a 3-method fallback.
 * Accepts the exec function so both real and injected paths work.
 */
async function discoverIpVia(execFn: ExecFunction, name: string): Promise<string | null> {
  // Method 1: orbctl info
  try {
    const result = await orbctlVia(execFn, ["info", name, "--format", "json"], {
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
    const result = await execFn(
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
    const result = await orbctlVia(
      execFn,
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
 * Extract timestamp from VM name.
 * New format (repl-{cid}-{mid}-{rid}): no timestamp embedded, return Date.now().
 * Legacy format (repl-{timestamp} or repl-i{id}-{timestamp}): parse the suffix.
 */
function extractTimestamp(name: string): number {
  // New format: no timestamp embedded, use Date.now()
  if (isReplResource(name)) return Date.now();
  // Legacy format
  const match = name.match(/^repl-(?:i\d+-)?(\d+)$/);
  return match ? parseInt(match[1]!, 10) : Date.now();
}

// =============================================================================
// OrbStack Provider
// =============================================================================

export class OrbStackProvider implements Provider<OrbStackInstance> {
  readonly name = "orbstack" as const;

  private readonly execFn: ExecFunction;
  private readonly isMocked: boolean;
  private readonly spawnPollTimeoutMs: number;

  constructor(config?: OrbStackProviderConfig) {
    this.execFn = config?._execFactory ?? realExec;
    this.isMocked = !!config?._execFactory;
    this.spawnPollTimeoutMs = config?._spawnPollTimeoutMs ?? 60_000;
  }

  /** Run orbctl via the instance exec function. */
  private orbctl(
    args: string[],
    options?: { timeout?: number }
  ): Promise<{ stdout: string; stderr: string; exitCode: number }> {
    return orbctlVia(this.execFn, args, options);
  }

  /** Discover IP for a named VM via the instance exec function. */
  private discoverIp(name: string): Promise<string | null> {
    return discoverIpVia(this.execFn, name);
  }

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
    const name = generateVmName(options);

    // Create the VM
    const createTimeout = getProviderTiming("SPAWN_TIMEOUT_MS", "orbstack");
    const createResult = await this.orbctl(
      ["create", `${distro}:${version}`, name, "-a", arch],
      { timeout: createTimeout }
    );

    if (createResult.exitCode !== 0) {
      const stderr = createResult.stderr.trim().toLowerCase();
      // Idempotent spawn: if VM already exists, return existing instance
      if (stderr.includes("already exists") || stderr.includes("machine already exists")) {
        // VM exists — fetch and return it
        const existing = await this.get(name);
        if (existing) return existing;
      }
      throw new ConcreteProviderError("orbstack", "PROVIDER_INTERNAL",
        `Failed to create VM ${name}: ${createResult.stderr.trim()}`,
        { retryable: true, details: { name, spec: options.spec, exitCode: createResult.exitCode } }
      );
    }

    // Poll for running state
    const pollInterval = getProviderTiming(
      "SPAWN_POLL_INTERVAL_MS",
      "orbstack"
    );
    const pollTimeout = this.spawnPollTimeoutMs;
    const pollStart = Date.now();

    while (Date.now() - pollStart < pollTimeout) {
      try {
        const infoResult = await this.orbctl(
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
        throw new ConcreteProviderError("orbstack", "TIMEOUT_ERROR",
          `VM ${name} did not reach running state within ${pollTimeout}ms`,
          { retryable: true, details: { name, timeout: pollTimeout } }
        );
      }

      await Bun.sleep(pollInterval);
    }

    // Discover IP
    const ip = await this.discoverIp(name);

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
        const writeResult = await this.orbctl(["run", "-m", name, "-u", "root", "sh", "-c", writeCmd], {
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
          await this.orbctl(["run", "-m", name, "-u", "root", "sh", "-c", startCmd], {
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

  async terminate(providerId: string, region?: string): Promise<void> {
    const timeout = getProviderTiming("TERMINATE_TIMEOUT_MS", "orbstack");

    // Graceful stop before delete: gives OrbStack's DHCP allocator time to
    // reclaim the IP address. Without this, rapid create/delete cycles can
    // exhaust the IP pool (leases expire after ~24h).
    try {
      await this.orbctl(["stop", providerId], { timeout: 15_000 });
      if (!this.isMocked) await Bun.sleep(1_000); // DHCP grace period — skip in tests
    } catch {
      // Stop failure is non-fatal — VM may already be stopped or deleted
    }

    const result = await this.orbctl(["delete", "-f", providerId], { timeout });

    if (result.exitCode !== 0) {
      const stderr = result.stderr.trim().toLowerCase();
      // Idempotent: if already deleted, succeed silently
      if (stderr.includes("not found") || stderr.includes("does not exist")) {
        return;
      }
      throw new ConcreteProviderError("orbstack", "PROVIDER_INTERNAL",
        `Failed to terminate VM ${providerId}: ${result.stderr.trim()}`,
        { retryable: true, details: { providerId, exitCode: result.exitCode } }
      );
    }
  }

  async list(filter?: ListFilter): Promise<OrbStackInstance[]> {
    const result = await this.orbctl(["list", "--format", "json"], {
      timeout: 10_000,
    });

    if (result.exitCode !== 0) {
      throw new ConcreteProviderError("orbstack", "PROVIDER_INTERNAL",
        `Failed to list VMs: ${result.stderr.trim()}`,
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
      throw new ConcreteProviderError("orbstack", "PROVIDER_INTERNAL",
        `Failed to parse orbctl list output: ${raw.slice(0, 200)}`,
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

  async get(providerId: string, region?: string): Promise<OrbStackInstance | null> {
    const result = await this.orbctl(
      ["info", providerId, "--format", "json"],
      { timeout: 10_000 }
    );

    if (result.exitCode !== 0) {
      const stderr = result.stderr.trim().toLowerCase();
      if (stderr.includes("not found") || stderr.includes("does not exist")) {
        return null;
      }
      throw new ConcreteProviderError("orbstack", "PROVIDER_INTERNAL",
        `Failed to get VM ${providerId}: ${result.stderr.trim()}`,
        { retryable: true, details: { providerId, exitCode: result.exitCode } }
      );
    }

    const raw = result.stdout.trim();
    if (!raw) return null;

    let info: OrbctlInfoOutput;
    try {
      info = JSON.parse(raw);
    } catch {
      throw new ConcreteProviderError("orbstack", "PROVIDER_INTERNAL",
        `Failed to parse orbctl info output for ${providerId}`,
        { retryable: false, details: { providerId } }
      );
    }

    const diskGb = info.disk_size
      ? Math.round(info.disk_size / (1024 * 1024 * 1024))
      : undefined;

    return toOrbStackInstance(info.record, info.ip4 ?? null, diskGb);
  }

  generateBootstrap(config: BootstrapConfig): BootstrapScript {
    // OrbStack VMs can't reach host localhost — rewrite to host.internal
    const rewritten: BootstrapConfig = {
      ...config,
      controlPlaneUrl: config.controlPlaneUrl
        .replace("localhost", "host.internal")
        .replace("127.0.0.1", "host.internal"),
      agentUrl: config.agentUrl
        ?.replace("localhost", "host.internal")
        ?.replace("127.0.0.1", "host.internal"),
    };
    return assembleShellBootstrap(rewritten);
  }

  async createSnapshot(
    providerId: string,
    options: CreateSnapshotOptions
  ): Promise<SnapshotRequest> {
    const snapshotName = options.name;

    // Clone the VM
    const cloneResult = await this.orbctl(["clone", providerId, snapshotName], {
      timeout: 120_000,
    });

    if (cloneResult.exitCode !== 0) {
      throw new ConcreteProviderError("orbstack", "PROVIDER_INTERNAL",
        `Failed to clone VM ${providerId} as ${snapshotName}: ${cloneResult.stderr.trim()}`,
        { retryable: true, details: { providerId, snapshotName, exitCode: cloneResult.exitCode } }
      );
    }

    // Stop the cloned VM to freeze it as a snapshot
    try {
      await this.orbctl(["stop", snapshotName], { timeout: 30_000 });
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
    const result = await this.orbctl(
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
      await this.orbctl(["stop", providerSnapshotId], { timeout: 15_000 });
      if (!this.isMocked) await Bun.sleep(1_000); // DHCP grace period — skip in tests
    } catch {
      // Stop failure is non-fatal — snapshot VM may already be stopped
    }

    const result = await this.orbctl(["delete", "-f", providerSnapshotId], {
      timeout: 30_000,
    });

    if (result.exitCode !== 0) {
      const stderr = result.stderr.trim().toLowerCase();
      // Idempotent: if already deleted, succeed silently
      if (stderr.includes("not found") || stderr.includes("does not exist")) {
        return;
      }
      throw new ConcreteProviderError("orbstack", "PROVIDER_INTERNAL",
        `Failed to delete snapshot ${providerSnapshotId}: ${result.stderr.trim()}`,
        { retryable: true, details: { providerSnapshotId, exitCode: result.exitCode } }
      );
    }
  }

  async getSnapshotByName(name: string): Promise<ProviderSnapshot | null> {
    const result = await this.orbctl(["info", name, "--format", "json"], {
      timeout: 10_000,
    });

    if (result.exitCode !== 0) {
      const stderr = result.stderr.trim().toLowerCase();
      if (stderr.includes("not found") || stderr.includes("does not exist")) {
        return null;
      }
      throw new ConcreteProviderError("orbstack", "PROVIDER_INTERNAL",
        `Failed to query snapshot ${name}: ${result.stderr.trim()}`,
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

// =============================================================================
// Lifecycle Hooks Factory
// =============================================================================

/**
 * Create lifecycle hooks for the OrbStack provider.
 *
 * @param execFn - Optional exec override for testing. Defaults to the
 *   module-level realExec (Bun.spawn-based) implementation.
 */
export function createOrbStackHooks(execFn?: ExecFunction): ProviderLifecycleHooks {
  const run = execFn ?? realExec;

  return {
    async onStartup() {
      let result: { stdout: string; stderr: string; exitCode: number };
      try {
        result = await run(["orbctl", "version"]);
      } catch (err) {
        throw new ConcreteProviderError("orbstack", "PROVIDER_INTERNAL",
          `orbctl not found — install OrbStack to use the OrbStack provider: ${err instanceof Error ? err.message : String(err)}`,
          { retryable: false }
        );
      }

      if (result.exitCode !== 0) {
        throw new ConcreteProviderError("orbstack", "PROVIDER_INTERNAL",
          `orbctl version check failed (exit ${result.exitCode}): ${result.stderr.trim()}`,
          { retryable: false }
        );
      }

      const version = result.stdout.trim();
      console.log(`[orbstack] orbctl available: ${version}`);
    },

    async onHeartbeat(expectations: HeartbeatExpectations) {
      const receipts: TaskReceipt[] = [];

      for (const task of expectations.tasks) {
        if (task.type === "health_check") {
          try {
            const result = await run(["orbctl", "list", "--format", "json"]);
            if (result.exitCode !== 0) {
              receipts.push({
                type: "health_check",
                status: "failed",
                result: {
                  error: `orbctl list failed (exit ${result.exitCode}): ${result.stderr.trim()}`,
                },
              });
              continue;
            }

            const raw = result.stdout.trim();
            let items: Array<{ name: string }> = [];
            if (raw && raw !== "[]") {
              items = JSON.parse(raw);
            }

            const replCount = items.filter((item) =>
              item.name?.startsWith("repl-")
            ).length;

            receipts.push({
              type: "health_check",
              status: "completed",
              result: { replVmCount: replCount },
            });
          } catch (err) {
            receipts.push({
              type: "health_check",
              status: "failed",
              result: { error: err instanceof Error ? err.message : String(err) },
            });
          }
        } else {
          receipts.push({
            type: task.type,
            status: "skipped",
            reason: `Task type '${task.type}' not handled by OrbStack provider`,
          });
        }
      }

      return { receipts };
    },
  };
}

// Export both named and default
export {
  mapOrbStackStatus,
  generateVmName,
  VM_PREFIX,
  isReplVM,
  extractTimestamp,
};
export { parseSpec } from "../types";
export default new OrbStackProvider();
