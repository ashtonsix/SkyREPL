import { readFileSync, writeFileSync, unlinkSync, existsSync, readdirSync, rmSync } from 'fs';
import { homedir } from 'os';
import { join, resolve } from 'path';
import { getControlPlaneUrl, REPL_DIR } from '../config';
import { spawn } from 'child_process';

const PID_FILE = join(homedir(), '.repl', 'control.pid');
const DB_FILE = join(homedir(), '.repl', 'skyrepl-control.db');
const MINIO_PID_FILE = join(homedir(), '.repl', 'minio.pid');
const MINIO_DIR = join(homedir(), '.repl', 'minio');
const MINIO_BINARY = join(MINIO_DIR, 'minio');
const MINIO_DATA = join(MINIO_DIR, 'data');

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

function readMinioPid(): number | null {
  try {
    const pid = parseInt(readFileSync(MINIO_PID_FILE, 'utf-8').trim(), 10);
    if (isNaN(pid)) return null;
    try {
      process.kill(pid, 0);
      return pid;
    } catch {
      unlinkSync(MINIO_PID_FILE);
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

async function stopControlPlane(pid: number): Promise<void> {
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

async function downloadMinio(): Promise<boolean> {
  const platform = process.platform;
  const arch = process.arch;

  let target: string;
  if (platform === 'darwin' && arch === 'arm64') target = 'darwin-arm64';
  else if (platform === 'linux' && arch === 'x64') target = 'linux-amd64';
  else if (platform === 'linux' && arch === 'arm64') target = 'linux-arm64';
  else {
    console.warn(`[minio] Unsupported platform ${platform}/${arch}, skipping MinIO`);
    return false;
  }

  const url = `https://dl.min.io/server/minio/release/${target}/minio`;
  console.log(`Downloading MinIO for ${target}...`);

  const { mkdirSync, writeFileSync: writeFile, chmodSync } = await import('fs');
  mkdirSync(MINIO_DIR, { recursive: true });

  const response = await fetch(url);
  if (!response.ok) {
    console.warn(`[minio] Download failed: HTTP ${response.status}`);
    return false;
  }

  const data = Buffer.from(await response.arrayBuffer());
  writeFile(MINIO_BINARY, data);
  chmodSync(MINIO_BINARY, 0o755);
  console.log(`MinIO downloaded to ${MINIO_BINARY}`);
  return true;
}

async function ensureDefaultBucket(): Promise<void> {
  try {
    const { S3Client, CreateBucketCommand } = await import('@aws-sdk/client-s3');
    const client = new S3Client({
      endpoint: 'http://127.0.0.1:9000',
      region: 'us-east-1',
      credentials: { accessKeyId: 'minioadmin', secretAccessKey: 'minioadmin' },
      forcePathStyle: true,
    });
    await client.send(new CreateBucketCommand({ Bucket: 'skyrepl-blobs' }));
  } catch (err: any) {
    if (err.name !== 'BucketAlreadyOwnedByYou' && err.name !== 'BucketAlreadyExists') {
      console.warn(`[minio] Bucket creation: ${err.message}`);
    }
  }
}

async function spawnMinio(): Promise<number | null> {
  // Skip if already running
  const existing = readMinioPid();
  if (existing) return existing;

  // Download if not present
  if (!existsSync(MINIO_BINARY)) {
    const ok = await downloadMinio();
    if (!ok) return null;
  }

  // Ensure data dir
  const { mkdirSync, openSync } = await import('fs');
  mkdirSync(MINIO_DATA, { recursive: true });

  const logFd = openSync(join(REPL_DIR, 'minio.log'), 'a');
  const child = spawn(MINIO_BINARY, ['server', MINIO_DATA, '--address', ':9000', '--console-address', ':9001'], {
    detached: true,
    stdio: ['ignore', logFd, logFd],
    env: {
      ...process.env,
      MINIO_ROOT_USER: 'minioadmin',
      MINIO_ROOT_PASSWORD: 'minioadmin',
    },
  });
  child.unref();

  if (!child.pid) return null;
  writeFileSync(MINIO_PID_FILE, String(child.pid));

  // Poll health (up to 15s)
  for (let i = 0; i < 30; i++) {
    await new Promise(r => setTimeout(r, 500));
    try {
      const res = await fetch('http://127.0.0.1:9000/minio/health/live', { signal: AbortSignal.timeout(2000) });
      if (res.ok) {
        // Create default bucket
        await ensureDefaultBucket();
        return child.pid;
      }
    } catch {}
  }

  console.warn('[minio] Started but health check not responding after 15s');
  return child.pid;
}

async function stopMinio(): Promise<void> {
  const pid = readMinioPid();
  if (!pid) return;

  try {
    process.kill(pid, 'SIGTERM');
    for (let i = 0; i < 20; i++) {
      await new Promise(r => setTimeout(r, 100));
      try { process.kill(pid, 0); } catch { break; }
    }
  } catch {}

  try { unlinkSync(MINIO_PID_FILE); } catch {}

  console.log(`MinIO stopped (PID ${pid}).`);
}

function resetMinioData(): void {
  if (existsSync(MINIO_DATA)) {
    rmSync(MINIO_DATA, { recursive: true, force: true });
  }
}

async function cleanControlState(): Promise<void> {
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

  // Remove everything in ~/.repl/ except *.env files and the minio binary
  const kept: string[] = [];
  const removed: string[] = [];
  try {
    for (const entry of readdirSync(REPL_DIR)) {
      if (entry.endsWith('.env')) {
        kept.push(entry);
        continue;
      }
      // Preserve the minio binary (only remove data, not the binary itself)
      if (entry === 'minio') {
        // resetMinioData() handles minio/data; skip the directory here
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
  const [pid, minioPid] = await Promise.all([
    spawnControlPlane(),
    spawnMinio(),
  ]);

  if (!pid) {
    console.error('Failed to start control plane. Is the repo intact?');
    process.exit(2);
  }

  if (minioPid) {
    console.log(`MinIO started (PID ${minioPid}) on port 9000.`);
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
  const existingMinioPid = readMinioPid();

  if (existingPid && existingMinioPid) {
    console.log(`Control plane (PID ${existingPid}) and MinIO (PID ${existingMinioPid}) already running.`);
    return;
  }

  const results = await Promise.allSettled([
    existingPid ? Promise.resolve(existingPid) : spawnControlPlane(),
    spawnMinio(),
  ]);

  const cpPid = results[0].status === 'fulfilled' ? results[0].value : null;
  const minioPid = results[1].status === 'fulfilled' ? results[1].value : null;

  const port = process.env.SKYREPL_PORT || process.env.PORT || '3000';
  if (cpPid) console.log(`Control plane started (PID ${cpPid}) on port ${port}.`);
  else console.error('Failed to start control plane.');

  if (minioPid) console.log(`MinIO started (PID ${minioPid}) on port 9000.`);
  else console.log('MinIO not started (optional for local dev).');

  if (!cpPid) process.exit(1);
}

async function controlStop(): Promise<void> {
  const cpPid = readPid();
  const minioPid = readMinioPid();

  if (!cpPid && !minioPid) {
    console.log('Nothing is running.');
    return;
  }

  await Promise.all([
    cpPid ? stopControlPlane(cpPid) : Promise.resolve(),
    stopMinio(),
  ]);
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

  const minioPid = readMinioPid();
  if (minioPid) {
    try {
      const res = await fetch('http://127.0.0.1:9000/minio/health/live', { signal: AbortSignal.timeout(2000) });
      if (res.ok) console.log(`MinIO running (PID ${minioPid}), healthy.`);
      else console.log(`MinIO running (PID ${minioPid}) but health check failed.`);
    } catch {
      console.log(`MinIO running (PID ${minioPid}) but not responding.`);
    }
  } else {
    console.log('MinIO is not running.');
  }
}

async function controlReset(): Promise<void> {
  await controlStop();

  // Clean concurrently
  await Promise.all([
    cleanControlState(),
    Promise.resolve(resetMinioData()),
  ]);

  console.log('Reset complete. Start fresh with `repl control start`.');
}
