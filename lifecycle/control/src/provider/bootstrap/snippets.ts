// provider/bootstrap/snippets.ts - Cloud-init snippet generators
//
// Each function returns a partial cloud-init structure. Snippets are combined
// by assembleCloudInit() in cloud-init.ts into a full #cloud-config document.

// =============================================================================
// Types
// =============================================================================

export interface WriteFileEntry {
  path: string;
  content: string;
  permissions?: string;
  owner?: string;
}

export interface CloudInitSnippet {
  writeFiles?: WriteFileEntry[];
  runcmd?: string[][];
  packages?: string[];
}

// Agent file list — must match what the control plane serves at /v1/agent/download/
const AGENT_FILES = [
  "agent.py",
  "executor.py",
  "heartbeat.py",
  "logs.py",
  "sse.py",
  "http_client.py",
  "tailscale.py",
  "artifacts.py",
  "spot.py",
];

// =============================================================================
// Snippet generators
// =============================================================================

/**
 * Disable apt-daily and apt-daily-upgrade timers to prevent lock contention
 * on ephemeral instances. These timers fire shortly after boot and will grab
 * the dpkg lock, blocking any subsequent apt-get calls for minutes.
 */
export function disableAptDaily(): CloudInitSnippet {
  return {
    runcmd: [
      [
        "systemctl",
        "disable",
        "--now",
        "apt-daily.timer",
        "apt-daily-upgrade.timer",
      ],
    ],
  };
}

/**
 * Write /etc/skyrepl/config.json with control plane URL and registration token.
 * The agent reads this file on startup to know where to register.
 */
export function writeAgentConfig(
  controlPlaneUrl: string,
  registrationToken: string,
  extra?: Record<string, unknown>
): CloudInitSnippet {
  const config: Record<string, unknown> = {
    controlPlaneUrl,
    registrationToken,
    ...extra,
  };

  return {
    writeFiles: [
      {
        path: "/etc/skyrepl/config.json",
        content: JSON.stringify(config, null, 2),
        permissions: "0644",
        owner: "root:root",
      },
    ],
    runcmd: [["mkdir", "-p", "/etc/skyrepl"]],
  };
}

/**
 * Download agent files from the control plane's /v1/agent/download/ endpoint
 * and make them executable. Creates /opt/skyrepl-agent/ first.
 */
export function downloadAgent(controlPlaneUrl: string): CloudInitSnippet {
  const baseUrl = `${controlPlaneUrl}/v1/agent/download`;

  const cmds: string[][] = [["mkdir", "-p", "/opt/skyrepl-agent"]];

  for (const filename of AGENT_FILES) {
    cmds.push([
      "curl",
      "-fsSL",
      `${baseUrl}/${filename}`,
      "-o",
      `/opt/skyrepl-agent/${filename}`,
    ]);
  }

  cmds.push(["chmod", "+x", "/opt/skyrepl-agent/agent.py"]);

  return { runcmd: cmds };
}

/**
 * Install a named bootstrap feature.
 *
 * Supported features:
 *   - nvidia-driver  apt install nvidia-driver-535
 *   - cuda           apt install cuda-toolkit-12-2
 *   - docker         curl get.docker.com | sh
 *
 * Returns an empty snippet for unknown feature names so callers can pass
 * any feature list without crashing; unrecognised names are silently skipped.
 */
export function installFeature(
  name: string,
  _config?: Record<string, string>
): CloudInitSnippet {
  switch (name) {
    case "nvidia-driver":
      return {
        runcmd: [
          ["apt-get", "update", "-qq"],
          ["apt-get", "install", "-y", "nvidia-driver-535"],
        ],
      };

    case "cuda":
      return {
        runcmd: [
          ["apt-get", "update", "-qq"],
          ["apt-get", "install", "-y", "cuda-toolkit-12-2"],
        ],
      };

    case "docker":
      return {
        runcmd: [
          ["bash", "-c", "curl -fsSL https://get.docker.com | sh"],
        ],
      };

    default:
      // Unknown feature — return empty snippet; caller is responsible for
      // deciding whether to warn or error.
      return {};
  }
}

// Pinned Tailscale static binary version. Update here when upgrading.
const TAILSCALE_VERSION = "1.80.2";

/**
 * Install Tailscale at boot time (eager path) for instances that will need
 * SSH or private networking.
 *
 * Downloads the official static binary tarball from pkgs.tailscale.com,
 * installs tailscale and tailscaled to /usr/local/bin, then starts the
 * daemon and authenticates using the TAILSCALE_AUTH_KEY environment variable
 * injected by the control plane at spawn time.
 *
 * This is the eager path — Tailscale is installed regardless of whether SSH
 * will be used, eliminating the ~15-30s JIT delay on first connection.
 * Only include this snippet when the run config sets use_tailscale=true.
 *
 * The instance hostname in the tailnet follows the naming convention:
 *   skyrepl-<instance_id>
 * which is stable across reboots and survives DB loss (just the numeric ID).
 */
export function installTailscale(instanceId: string | number): CloudInitSnippet {
  const arch = "amd64"; // cloud-init runs on amd64; extend if arm64 support added
  const tarball = `tailscale_${TAILSCALE_VERSION}_${arch}.tgz`;
  const downloadUrl = `https://pkgs.tailscale.com/stable/${tarball}`;
  const extractDir = `/tmp/tailscale_${TAILSCALE_VERSION}_${arch}`;

  return {
    runcmd: [
      // Download static tarball to /tmp
      ["curl", "-fsSL", downloadUrl, "-o", `/tmp/${tarball}`],
      // Extract
      ["tar", "-C", "/tmp", "-xzf", `/tmp/${tarball}`],
      // Install binaries
      ["cp", `${extractDir}/tailscale`, "/usr/local/bin/tailscale"],
      ["cp", `${extractDir}/tailscaled`, "/usr/local/bin/tailscaled"],
      ["chmod", "+x", "/usr/local/bin/tailscale", "/usr/local/bin/tailscaled"],
      // Start daemon in background (no systemd dependency)
      ["bash", "-c", "tailscaled --state=/var/lib/tailscale/tailscaled.state &"],
      // Allow daemon a moment to start accepting connections
      ["sleep", "2"],
      // Authenticate; TAILSCALE_AUTH_KEY is written to env by cloud-init
      [
        "bash",
        "-c",
        `tailscale up --authkey=\${TAILSCALE_AUTH_KEY} --hostname=skyrepl-${instanceId} --accept-routes`,
      ],
      // Clean up tarball
      ["rm", "-f", `/tmp/${tarball}`],
    ],
  };
}

/**
 * Run a user-supplied init script via bash. The script string is executed
 * as-is inside `bash -c`, so it can be multi-line or a single command.
 */
export function userInitScript(script: string): CloudInitSnippet {
  return {
    runcmd: [["bash", "-c", script]],
  };
}

/**
 * Start the SkyREPL agent process. This should always be the last snippet
 * in the assembled cloud-init document so that all prior setup (config write,
 * downloads, feature installs) completes first.
 */
export function startAgent(): CloudInitSnippet {
  return {
    runcmd: [
      ["bash", "-c", "cd /opt/skyrepl-agent && exec python3 -u agent.py"],
    ],
  };
}
