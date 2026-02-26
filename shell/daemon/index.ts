// daemon/index.ts — Local daemon entry point
//
// Long-running background process that:
//   1. Subscribes to the control plane SSE event stream
//   2. Regenerates SSH config on allocation state changes
//   3. Keeps tunnel processes alive (30s keepalive check)
//   4. Sends desktop notifications for key workflow events
//   5. Listens on Unix socket ~/.repl/daemon.sock for CLI messages
//
// Start via: bun run shell/daemon/index.ts
// Managed by:  shell/daemon/lifecycle.ts

import { existsSync, readFileSync, writeFileSync, mkdirSync, unlinkSync } from 'fs';
import { join } from 'path';
import { homedir } from 'os';
import { notify } from './notify';
import { triggerCompletionCacheUpdate } from './completion-cache';
import { writeRunStatus, appendRunLog, cleanupOldRuns } from './run-artifacts';
import type { Catalog } from '../../scaffold/src/catalog';

// Lazy import: regenerateSSHConfig needs ApiClient which reads process.env at
// call time, so we import these after env is loaded.
const HOME = homedir();
const REPL_DIR = join(HOME, '.repl');
const CONTROL_ENV_FILE = join(REPL_DIR, 'control.env');
const SOCK_PATH = join(REPL_DIR, 'daemon.sock');
const TUNNELS_FILE = join(REPL_DIR, 'tunnels.json');

// =============================================================================
// Env loading (minimal, no side effects)
// =============================================================================

function loadEnvFile(filePath: string): void {
  if (!existsSync(filePath)) return;
  const content = readFileSync(filePath, 'utf-8');
  for (const line of content.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const eqIdx = trimmed.indexOf('=');
    if (eqIdx === -1) continue;
    const key = trimmed.slice(0, eqIdx).trim();
    let value = trimmed.slice(eqIdx + 1).trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    if (process.env[key] === undefined) {
      process.env[key] = value;
    }
  }
}

function getControlPlaneUrl(): string {
  return process.env.SKYREPL_CONTROL_PLANE_URL ?? 'http://localhost:3000';
}

function getApiKey(): string | null {
  const keyFile = join(REPL_DIR, 'api-key');
  if (!existsSync(keyFile)) return null;
  return readFileSync(keyFile, 'utf-8').trim().split('\n')[0].trim() || null;
}

// =============================================================================
// SSH Config Regeneration
// =============================================================================

async function triggerSSHRegen(): Promise<void> {
  try {
    // Dynamic import so we pick up env after it's loaded
    const { regenerateSSHConfig } = await import('../cli/src/ssh');
    const { ApiClient } = await import('../cli/src/client');
    const client = new ApiClient(getControlPlaneUrl(), getApiKey());
    await regenerateSSHConfig(client);
  } catch {
    // Control plane may be temporarily unreachable — not fatal
  }
}

// =============================================================================
// SSE Subscription
// =============================================================================

// Reconnect backoff state
let sseReconnectDelay = 1000; // ms, capped at 30s
const SSE_MAX_DELAY = 30_000;

/**
 * Connect to the control plane event stream and process events.
 * Automatically reconnects on disconnect.
 */
function connectEventStream(): void {
  const url = `${getControlPlaneUrl()}/v1/workflows/events`;
  const apiKey = getApiKey();
  const headers: Record<string, string> = {};
  if (apiKey) headers['Authorization'] = `Bearer ${apiKey}`;

  fetch(url, { headers })
    .then(async (response) => {
      if (!response.ok || !response.body) {
        scheduleReconnect();
        return;
      }

      // Successful connection — reset backoff
      sseReconnectDelay = 1000;

      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';
      let currentEvent = '';

      const processLine = (line: string) => {
        if (line.startsWith('event:')) {
          currentEvent = line.slice(6).trim();
        } else if (line.startsWith('data:')) {
          const dataStr = line.slice(5).trim();
          if (!dataStr) return;
          handleSSEEvent(currentEvent, dataStr);
          currentEvent = '';
        }
      };

      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split('\n');
          buffer = lines.pop() ?? '';
          for (const line of lines) {
            processLine(line);
          }
        }
      } catch {
        // Stream broken
      }

      scheduleReconnect();
    })
    .catch(() => {
      scheduleReconnect();
    });
}

function scheduleReconnect(): void {
  setTimeout(() => {
    connectEventStream();
  }, sseReconnectDelay);

  sseReconnectDelay = Math.min(sseReconnectDelay * 2, SSE_MAX_DELAY);
}

/**
 * Handle a single SSE event from the control plane.
 *
 * The events stream is the same stream used by the CLI for workflow progress
 * (WorkflowStreamEvent). We trigger SSH regen on workflow_completed and
 * node_completed events (allocation state may have changed), and send desktop
 * notifications for terminal workflow states.
 */
export function handleSSEEvent(eventType: string, dataStr: string): void {
  let data: Record<string, unknown>;
  try {
    data = JSON.parse(dataStr);
  } catch {
    return;
  }

  if (eventType === 'workflow_completed') {
    // An allocation may have transitioned to COMPLETE — regenerate SSH config
    triggerSSHRegen();
    triggerCompletionCacheUpdate();
    notify('SkyREPL', 'Run completed');
    // Update run status artifact if we have a run slug
    const runSlug = typeof data.run_slug === 'string' ? data.run_slug : null;
    if (runSlug) {
      writeRunStatus(runSlug, { state: 'completed', ...data });
    }
  } else if (eventType === 'workflow_failed') {
    triggerSSHRegen();
    triggerCompletionCacheUpdate();
    const errMsg = typeof data.error === 'string' ? data.error : 'Workflow failed';
    notify('SkyREPL', errMsg);
    const runSlug = typeof data.run_slug === 'string' ? data.run_slug : null;
    if (runSlug) {
      writeRunStatus(runSlug, { state: 'failed', error: errMsg, ...data });
    }
  } else if (eventType === 'node_started') {
    // Update run status so agents see early progress events (E4)
    const runSlug = typeof data.run_slug === 'string' ? data.run_slug : null;
    const nodeId = typeof data.node_id === 'string' ? data.node_id : '';
    if (runSlug) {
      writeRunStatus(runSlug, { state: 'running', current_node: nodeId, ...data });
    }
  } else if (eventType === 'workflow_started') {
    // Update run status on workflow start (E4)
    const runSlug = typeof data.run_slug === 'string' ? data.run_slug : null;
    if (runSlug) {
      writeRunStatus(runSlug, { state: 'running', ...data });
    }
  } else if (eventType === 'node_completed') {
    // spawn-instance or similar nodes completing means a new allocation may
    // have become SSH-accessible
    const nodeId = typeof data.node_id === 'string' ? data.node_id : '';
    if (nodeId.includes('spawn') || nodeId.includes('allocation')) {
      triggerSSHRegen();
      triggerCompletionCacheUpdate();
    }
    // Update run status artifact on node progress
    const runSlug = typeof data.run_slug === 'string' ? data.run_slug : null;
    if (runSlug) {
      writeRunStatus(runSlug, { state: 'running', current_node: nodeId, ...data });
    }
  } else if (eventType === 'log') {
    // Persist log data to output.log (E2)
    const runSlug = typeof data.run_slug === 'string' ? data.run_slug : null;
    const logData = typeof data.data === 'string' ? data.data : null;
    if (runSlug && logData) {
      appendRunLog(runSlug, logData);
    }
  }
}

// =============================================================================
// Tunnel Keepalive
// =============================================================================

interface TunnelConfig {
  type: 'tailscale' | 'cloudflared' | 'ngrok' | 'manual';
  url: string;
  pid?: number;
}

function loadTunnels(): TunnelConfig[] {
  if (!existsSync(TUNNELS_FILE)) return [];
  try {
    return JSON.parse(readFileSync(TUNNELS_FILE, 'utf-8'));
  } catch {
    return [];
  }
}

/**
 * Check whether a process is alive using signal 0.
 */
function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

/**
 * Check tunnel processes and restart any that have died.
 * Tailscale Funnel is managed by the tailscale daemon — we only check
 * cloudflared and ngrok which we spawned ourselves.
 */
function checkTunnels(): void {
  const tunnels = loadTunnels();
  for (const tunnel of tunnels) {
    if (!tunnel.pid) continue;
    if (tunnel.type === 'tailscale') continue; // managed externally
    if (isProcessAlive(tunnel.pid)) continue;

    // Tunnel process is dead — restart it
    restartTunnel(tunnel);
  }
}

function restartTunnel(tunnel: TunnelConfig): void {
  const port = parseInt(process.env.SKYREPL_PORT ?? process.env.PORT ?? '3000', 10);

  let args: string[];
  if (tunnel.type === 'cloudflared') {
    args = ['cloudflared', 'tunnel', '--url', `http://localhost:${port}`];
  } else if (tunnel.type === 'ngrok') {
    args = ['ngrok', 'http', String(port), '--log=stdout', '--log-format=json'];
  } else {
    return;
  }

  try {
    const proc = Bun.spawn(args, {
      stdio: ['ignore', 'ignore', 'ignore'],
      detached: true,
    });
    proc.unref();

    // Update PID in tunnels file
    const tunnels = loadTunnels();
    const idx = tunnels.findIndex(t => t.url === tunnel.url && t.type === tunnel.type);
    if (idx >= 0) {
      tunnels[idx].pid = proc.pid;
      writeFileSync(TUNNELS_FILE, JSON.stringify(tunnels, null, 2) + '\n', { mode: 0o600 });
    }
  } catch {
    // Binary not available — ignore
  }
}

// =============================================================================
// IPC Unix Socket Server
// =============================================================================

interface IPCMessage {
  type: 'regenerate-ssh' | 'regenerate-completions' | 'status';
}

interface IPCResponse {
  ok: boolean;
  pid?: number;
  uptime_ms?: number;
  error?: string;
}

const daemonStartedAt = Date.now();

function startIPCServer(): void {
  // Remove stale socket file if it exists
  if (existsSync(SOCK_PATH)) {
    try { unlinkSync(SOCK_PATH); } catch { /* ignore */ }
  }

  Bun.serve({
    unix: SOCK_PATH,
    fetch(req) {
      return handleIPCRequest(req);
    },
  });
}

async function handleIPCRequest(req: Request): Promise<Response> {
  let msg: IPCMessage;
  try {
    msg = (await req.json()) as IPCMessage;
  } catch {
    const res: IPCResponse = { ok: false, error: 'invalid JSON' };
    return new Response(JSON.stringify(res), { status: 400 });
  }

  if (msg.type === 'regenerate-ssh') {
    // Fire-and-forget; client gets an immediate ack
    triggerSSHRegen();
    const res: IPCResponse = { ok: true };
    return new Response(JSON.stringify(res));
  }

  if (msg.type === 'regenerate-completions') {
    // Fire-and-forget; client gets an immediate ack
    triggerCompletionCacheUpdate();
    const res: IPCResponse = { ok: true };
    return new Response(JSON.stringify(res));
  }

  if (msg.type === 'status') {
    const res: IPCResponse = {
      ok: true,
      pid: process.pid,
      uptime_ms: Date.now() - daemonStartedAt,
    };
    return new Response(JSON.stringify(res));
  }

  const res: IPCResponse = { ok: false, error: `unknown message type: ${(msg as any).type}` };
  return new Response(JSON.stringify(res), { status: 400 });
}

// =============================================================================
// Graceful Shutdown
// =============================================================================

function shutdown(): void {
  if (existsSync(SOCK_PATH)) {
    try { unlinkSync(SOCK_PATH); } catch { /* ignore */ }
  }
  process.exit(0);
}

// =============================================================================
// initDaemon — programmatic entry point for service fabric
// =============================================================================

export async function initDaemon(catalog: Catalog): Promise<{ shutdown: () => Promise<void> }> {
  // Start IPC server first so CLI commands can reach us immediately
  startIPCServer();

  // Start listening for control plane events
  connectEventStream();

  // Warm the completion cache on boot so the first tab-completion is fast
  triggerCompletionCacheUpdate();

  // Check tunnel processes every 30s
  const tunnelInterval = setInterval(checkTunnels, 30_000);

  // Clean up old run artifact directories daily
  cleanupOldRuns();
  const cleanupInterval = setInterval(cleanupOldRuns, 24 * 60 * 60 * 1000);

  // Register ourselves in the catalog so other services can discover us
  catalog.registerService('daemon', {
    mode: 'local',
    version: catalog.getConfig().version,
    ref: { ipcSockPath: SOCK_PATH },
  });

  return {
    async shutdown() {
      clearInterval(tunnelInterval);
      clearInterval(cleanupInterval);
      if (existsSync(SOCK_PATH)) {
        try { unlinkSync(SOCK_PATH); } catch { /* ignore */ }
      }
    },
  };
}

// =============================================================================
// Main — only runs when this file is the entry point, not when imported
// =============================================================================

if (import.meta.main) {
  // Load env before anything else
  loadEnvFile(join(REPL_DIR, 'cli.env'));
  loadEnvFile(CONTROL_ENV_FILE);

  mkdirSync(REPL_DIR, { recursive: true });

  // Signal handlers — only for standalone mode; under scaffold, scaffold owns signals
  process.on('SIGTERM', shutdown);
  process.on('SIGINT', shutdown);

  // Start IPC server first so CLI commands can reach us immediately
  startIPCServer();

  // Start listening for control plane events
  connectEventStream();

  // Warm the completion cache on boot so the first tab-completion is fast
  triggerCompletionCacheUpdate();

  // Check tunnel processes every 30s
  setInterval(checkTunnels, 30_000);

  // Clean up old run artifact directories daily
  cleanupOldRuns();
  setInterval(cleanupOldRuns, 24 * 60 * 60 * 1000);
}
