import { readFileSync, writeFileSync, unlinkSync, existsSync, readdirSync, rmSync } from 'fs';
import { homedir } from 'os';
import { join, resolve } from 'path';
import { getControlPlaneUrl, REPL_DIR } from '../config';
import { spawn } from 'child_process';

const PID_FILE = join(homedir(), '.repl', 'control.pid');
const DB_FILE = join(homedir(), '.repl', 'skyrepl-control.db');

function readPid(): number | null {
  try {
    const pid = parseInt(readFileSync(PID_FILE, 'utf-8').trim(), 10);
    if (isNaN(pid)) return null;
    // Check if process is alive
    try {
      process.kill(pid, 0);
      return pid;
    } catch {
      // Process not alive, clean up stale PID file
      unlinkSync(PID_FILE);
      return null;
    }
  } catch {
    return null;
  }
}

// ─── Internal helpers for auto-management ────────────────────────────────────

async function checkHealth(): Promise<boolean> {
  try {
    const res = await fetch(`${getControlPlaneUrl()}/v1/health`, { signal: AbortSignal.timeout(2000) });
    return res.ok;
  } catch {
    return false;
  }
}

function isLocalControlPlane(): boolean {
  const url = getControlPlaneUrl();
  try {
    const parsed = new URL(url);
    return parsed.hostname === 'localhost' || parsed.hostname === '127.0.0.1' || parsed.hostname === '::1';
  } catch {
    return true;
  }
}

async function spawnControlPlane(): Promise<number | null> {
  const repoRoot = resolve(__dirname, '../../../..');
  const controlMain = resolve(repoRoot, 'lifecycle/control/src/main.ts');
  if (!existsSync(controlMain)) return null;

  const { openSync } = await import('fs');
  const logFd = openSync(join(REPL_DIR, 'control.log'), 'a');
  const child = spawn('bun', ['run', controlMain], {
    detached: true,
    stdio: ['ignore', logFd, logFd],
    env: { ...process.env },
  });
  child.unref();

  if (child.pid) {
    writeFileSync(PID_FILE, String(child.pid));
    return child.pid;
  }
  return null;
}

// ─── Exported: auto-start / diagnose control plane ───────────────────────────

/**
 * Ensure the control plane is reachable before running a command.
 *
 * Local mode (localhost):  auto-starts if not running, waits for health.
 * Remote mode:             prints diagnostic advice and exits if unreachable.
 */
export async function ensureControlPlane(): Promise<void> {
  // Fast path: already healthy
  if (await checkHealth()) return;

  if (!isLocalControlPlane()) {
    const url = getControlPlaneUrl();
    console.error(`Control plane at ${url} is not responding.`);
    console.error('');
    console.error('  - Check if the remote server is running');
    console.error('  - Check your network / VPN / tunnel');
    console.error(`  - Test: curl ${url}/v1/health`);
    console.error('  - Config: ~/.repl/control.env');
    process.exit(2);
  }

  // Local mode — auto-start
  const isFirstRun = !existsSync(DB_FILE);

  // Clean up stale PID file if process is dead
  readPid();

  console.log('Starting control plane...');
  const pid = await spawnControlPlane();
  if (!pid) {
    console.error('Failed to start control plane. Is the repo intact?');
    process.exit(2);
  }

  // Poll for health (up to 10s)
  for (let i = 0; i < 20; i++) {
    await new Promise(r => setTimeout(r, 500));
    if (await checkHealth()) {
      const port = process.env.SKYREPL_PORT || process.env.PORT || '3000';
      console.log(`Control plane ready on port ${port}.`);
      if (isFirstRun) {
        console.log('');
        console.log('For remote deployment (needed when VMs must reach the control plane):');
        console.log('  repl setup');
      }
      return;
    }
  }

  console.error('Control plane started but not responding after 10s.');
  console.error('Check logs: ~/.repl/control.log');
  process.exit(2);
}

export async function controlCommand(args: string[]): Promise<void> {
  const subcommand = args[0];

  switch (subcommand) {
    case 'start':
      await controlStart();
      break;
    case 'stop':
      await controlStop();
      break;
    case 'restart':
      await controlStop();
      await controlStart();
      break;
    case 'status':
      await controlStatus();
      break;
    case 'reset':
      await controlReset();
      break;
    default:
      console.error('Usage: repl control <start|stop|restart|status|reset>');
      process.exit(2);
  }
}

async function controlStart(): Promise<void> {
  const existingPid = readPid();
  if (existingPid) {
    console.log(`Control plane already running (PID ${existingPid}).`);
    return;
  }

  const pid = await spawnControlPlane();
  if (pid) {
    const port = process.env.SKYREPL_PORT || process.env.PORT || '3000';
    console.log(`Control plane started (PID ${pid}) on port ${port}.`);
  } else {
    console.error('Failed to start control plane.');
    process.exit(1);
  }
}

async function controlStop(): Promise<void> {
  const pid = readPid();
  if (!pid) {
    console.log('Control plane is not running.');
    return;
  }

  try {
    process.kill(pid, 'SIGTERM');
    // Wait briefly for process to exit
    for (let i = 0; i < 20; i++) {
      await new Promise(r => setTimeout(r, 100));
      try {
        process.kill(pid, 0);
      } catch {
        // Process exited
        break;
      }
    }
  } catch {
    // Already dead
  }

  try {
    unlinkSync(PID_FILE);
  } catch {
    // Already cleaned up
  }

  console.log(`Control plane stopped (PID ${pid}).`);
}

async function controlStatus(): Promise<void> {
  const pid = readPid();
  if (!pid) {
    console.log('Control plane is not running.');
    process.exit(1);
  }

  // Hit health endpoint
  const url = `${getControlPlaneUrl()}/v1/health`;
  try {
    const res = await fetch(url);
    if (res.ok) {
      const body = await res.json() as any;
      console.log(`Control plane running (PID ${pid}), status: ${body.status}`);
    } else {
      console.log(`Control plane running (PID ${pid}) but health check failed: HTTP ${res.status}`);
    }
  } catch {
    console.log(`Control plane running (PID ${pid}) but not responding at ${url}.`);
  }
}

async function controlReset(): Promise<void> {
  // Stop if running
  await controlStop();

  // Terminate all repl- OrbStack VMs
  try {
    const { execSync } = await import('child_process');
    const list = execSync('orbctl list 2>/dev/null', { encoding: 'utf-8' });
    const vms = list.split('\n').filter(l => l.startsWith('repl-')).map(l => l.split(/\s+/)[0]);
    for (const vm of vms) {
      try {
        execSync(`orbctl delete -f ${vm} 2>/dev/null`);
        console.log(`Terminated VM: ${vm}`);
      } catch { /* already gone */ }
    }
  } catch { /* orbctl not available */ }

  // Remove everything in ~/.repl/ except *.env files
  const kept: string[] = [];
  const removed: string[] = [];
  try {
    for (const entry of readdirSync(REPL_DIR)) {
      if (entry.endsWith('.env')) {
        kept.push(entry);
        continue;
      }
      rmSync(join(REPL_DIR, entry), { recursive: true, force: true });
      removed.push(entry);
    }
  } catch { /* directory might not exist */ }

  // Remove database files from CWD
  for (const dbFile of ['skyrepl-control.db', 'skyrepl-control.db-shm', 'skyrepl-control.db-wal']) {
    try {
      rmSync(dbFile, { force: true });
      removed.push(dbFile);
    } catch { /* not present */ }
  }

  if (removed.length > 0) {
    console.log(`Cleaned: ${removed.join(', ')}`);
  }
  if (kept.length > 0) {
    console.log(`Kept: ${kept.join(', ')}`);
  }
  console.log('Reset complete. Start fresh with `repl control start`.');
}
