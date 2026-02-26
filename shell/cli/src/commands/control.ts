// commands/control.ts — Service management (start/stop/status/reset/restart)
//
// Manages the SkyREPL service fabric:
//   scaffold  — unified process: control + orbital + proxy + daemon
//   daemon    — standalone (when control plane is remote)
//   minio     — blob storage server
//
// Usage: repl control <start|stop|restart|status|reset> [service]
// When no service specified: defaults to all enabled services.

import { readFileSync, writeFileSync, unlinkSync, existsSync, readdirSync, rmSync } from 'fs';
import { join, resolve } from 'path';
import { getControlPlaneUrl, REPL_DIR } from '../config';
import { spawn } from 'child_process';
import { isDaemonRunning, startDaemon, stopDaemon } from '../../../daemon/lifecycle';

// =============================================================================
// Constants
// =============================================================================

const SCAFFOLD_PID_FILE = join(REPL_DIR, 'scaffold.pid');
const DB_FILE = join(REPL_DIR, 'skyrepl-control.db');
const MINIO_PID_FILE = join(REPL_DIR, 'minio.pid');
const MINIO_DIR = join(REPL_DIR, 'minio');
const MINIO_BINARY = join(MINIO_DIR, 'minio');
const MINIO_DATA = join(MINIO_DIR, 'data');

type ServiceTarget = 'scaffold' | 'daemon' | 'minio';
const VALID_SERVICES: ServiceTarget[] = ['scaffold', 'daemon', 'minio'];

// =============================================================================
// PID File Helpers
// =============================================================================

function readPidFile(pidFile: string): number | null {
  try {
    const pid = parseInt(readFileSync(pidFile, 'utf-8').trim(), 10);
    if (isNaN(pid)) return null;
    try {
      process.kill(pid, 0);
      return pid;
    } catch {
      try { unlinkSync(pidFile); } catch {}
      return null;
    }
  } catch {
    return null;
  }
}

function readScaffoldPid(): number | null {
  return readPidFile(SCAFFOLD_PID_FILE);
}

function readMinioPid(): number | null {
  return readPidFile(MINIO_PID_FILE);
}

// =============================================================================
// Health Checks
// =============================================================================

async function checkHealth(url: string, timeoutMs = 2000): Promise<boolean> {
  try {
    const res = await fetch(url, { signal: AbortSignal.timeout(timeoutMs) });
    return res.ok;
  } catch {
    return false;
  }
}

async function checkControlHealth(): Promise<boolean> {
  return checkHealth(`${getControlPlaneUrl()}/v1/health`);
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

// =============================================================================
// Scaffold Process Management
// =============================================================================

async function spawnScaffold(): Promise<number | null> {
  await killExistingScaffold();
  await new Promise(r => setTimeout(r, 200));

  const repoRoot = resolve(__dirname, '../../../..');
  const scaffoldMain = resolve(repoRoot, 'scaffold/src/main.ts');

  if (!existsSync(scaffoldMain)) {
    console.error('scaffold/src/main.ts not found. Is the repo intact?');
    return null;
  }

  const { openSync } = await import('fs');
  const logFd = openSync(join(REPL_DIR, 'scaffold.log'), 'a');
  const child = spawn('bun', [scaffoldMain], {
    detached: true,
    stdio: ['ignore', logFd, logFd],
    env: { ...process.env, SKYREPL_SCAFFOLD: '1' },
  });
  child.unref();

  if (child.pid) {
    writeFileSync(SCAFFOLD_PID_FILE, String(child.pid));
    return child.pid;
  }
  return null;
}

async function stopScaffold(): Promise<void> {
  const pid = readScaffoldPid();
  if (!pid) {
    await killExistingScaffold();
    return;
  }

  try {
    process.kill(pid, 'SIGTERM');
    for (let i = 0; i < 30; i++) {
      await new Promise(r => setTimeout(r, 100));
      try { process.kill(pid, 0); } catch { break; }
    }
  } catch {}

  try { unlinkSync(SCAFFOLD_PID_FILE); } catch {}

  await killExistingScaffold();
}

async function killExistingScaffold(): Promise<void> {
  const myPid = process.pid;
  const killed = new Set<number>();

  if (process.platform === 'linux') {
    try {
      for (const entry of readdirSync('/proc')) {
        const pid = parseInt(entry, 10);
        if (isNaN(pid) || pid === myPid) continue;
        try {
          const environ = readFileSync(`/proc/${pid}/environ`, 'utf-8');
          if (environ.includes('SKYREPL_SCAFFOLD=1') || environ.includes('SKYREPL_CONTROL_PLANE=1')) {
            process.kill(pid, 'SIGTERM');
            killed.add(pid);
          }
        } catch {}
      }
    } catch {}
  }

  for (const pidFile of [SCAFFOLD_PID_FILE]) {
    try {
      const pid = parseInt(readFileSync(pidFile, 'utf-8').trim(), 10);
      if (!isNaN(pid) && pid !== myPid && !killed.has(pid)) {
        try { process.kill(pid, 'SIGTERM'); } catch {}
      }
    } catch {}
  }
}

// =============================================================================
// MinIO Management
// =============================================================================

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
    await client.send(new CreateBucketCommand({ Bucket: 'skyrepl' }));
  } catch (err: any) {
    if (err.name !== 'BucketAlreadyOwnedByYou' && err.name !== 'BucketAlreadyExists') {
      console.warn(`[minio] Bucket creation: ${err.message}`);
    }
  }
}

async function spawnMinio(): Promise<number | null> {
  const existing = readMinioPid();
  if (existing) return existing;

  if (!existsSync(MINIO_BINARY)) {
    const ok = await downloadMinio();
    if (!ok) return null;
  }

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

  for (let i = 0; i < 30; i++) {
    await new Promise(r => setTimeout(r, 500));
    try {
      const res = await fetch('http://127.0.0.1:9000/minio/health/live', { signal: AbortSignal.timeout(2000) });
      if (res.ok) {
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
}

function resetMinioData(): void {
  if (existsSync(MINIO_DATA)) {
    rmSync(MINIO_DATA, { recursive: true, force: true });
  }
}

// =============================================================================
// State Cleanup
// =============================================================================

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
      if (entry === 'minio') {
        kept.push(entry);
        continue;
      }
      rmSync(join(REPL_DIR, entry), { recursive: true, force: true });
      removed.push(entry);
    }
  } catch { /* directory might not exist */ }

  for (const dbSuffix of ['skyrepl-control.db', 'skyrepl-control.db-shm', 'skyrepl-control.db-wal']) {
    const dbPath = join(REPL_DIR, dbSuffix);
    try {
      rmSync(dbPath, { force: true });
      removed.push(dbSuffix);
    } catch { /* not present */ }
  }

  if (removed.length > 0) {
    console.log(`Cleaned: ${removed.join(', ')}`);
  }
  if (kept.length > 0) {
    console.log(`Kept: ${kept.join(', ')}`);
  }
}

// =============================================================================
// Service Status
// =============================================================================

async function serviceStatus(): Promise<void> {
  const scaffoldPid = readScaffoldPid();
  const minioPid = readMinioPid();
  const daemonStandalone = !scaffoldPid && isDaemonRunning();

  console.log('SkyREPL Services\n');

  // Scaffold
  if (scaffoldPid) {
    const controlPort = parseInt(process.env.SKYREPL_PORT || process.env.PORT || '3000', 10);
    const orbitalPort = parseInt(process.env.ORBITAL_PORT || '3002', 10);
    const proxyPort = parseInt(process.env.SHELL_PORT || '3001', 10);

    const [controlOk, orbitalOk, proxyOk] = await Promise.all([
      checkHealth(`http://localhost:${controlPort}/v1/health`),
      checkHealth(`http://localhost:${orbitalPort}/v1/advisory/health`),
      checkHealth(`http://localhost:${proxyPort}/v1/health`),
    ]);

    console.log(`  scaffold    running   PID ${scaffoldPid}`);
    console.log(`    control   :${controlPort}     ${controlOk ? 'healthy' : 'unhealthy'}`);
    console.log(`    orbital   :${orbitalPort}     ${orbitalOk ? 'healthy' : 'unhealthy'}`);
    console.log(`    proxy     :${proxyPort}     ${proxyOk ? 'healthy' : 'unhealthy'}`);
    console.log(`    daemon    in-process`);
  } else {
    console.log('  scaffold    stopped');
  }

  // Standalone daemon (only shown when scaffold isn't running)
  if (daemonStandalone) {
    console.log('  daemon      running   (standalone)');
  }

  // MinIO
  if (minioPid) {
    const minioOk = await checkHealth('http://127.0.0.1:9000/minio/health/live');
    console.log(`  minio       running   PID ${minioPid}  :9000  ${minioOk ? 'healthy' : 'unhealthy'}`);
  } else {
    console.log('  minio       stopped');
  }
}

// =============================================================================
// Service Start / Stop / Reset
// =============================================================================

function parseService(s: string | undefined): ServiceTarget | undefined {
  if (!s) return undefined;
  // "control", "orbital", "proxy" all map to "scaffold"
  if (s === 'control' || s === 'orbital' || s === 'proxy') return 'scaffold';
  if (VALID_SERVICES.includes(s as ServiceTarget)) return s as ServiceTarget;
  console.error(`Unknown service: ${s}`);
  console.error('Services: scaffold (or control/orbital/proxy), daemon, minio');
  process.exit(2);
}

async function serviceStart(service?: ServiceTarget): Promise<void> {
  if (!service) {
    // Start all: scaffold + minio
    const scaffoldPid = readScaffoldPid();
    const [scaffoldResult, minioResult] = await Promise.allSettled([
      scaffoldPid ? Promise.resolve(scaffoldPid) : spawnScaffold(),
      spawnMinio(),
    ]);

    const pid = scaffoldResult.status === 'fulfilled' ? scaffoldResult.value : null;
    const minioPid = minioResult.status === 'fulfilled' ? minioResult.value : null;

    if (pid) {
      for (let i = 0; i < 20; i++) {
        await new Promise(r => setTimeout(r, 500));
        if (await checkControlHealth()) {
          console.log(`Scaffold started (PID ${pid}). Services: control, orbital, proxy, daemon.`);
          break;
        }
        if (i === 19) {
          console.log(`Scaffold started (PID ${pid}) but health check not passing.`);
          console.log('Check logs: ~/.repl/scaffold.log');
        }
      }
    } else {
      console.error('Failed to start scaffold.');
    }

    if (minioPid) console.log(`MinIO running (PID ${minioPid}) on port 9000.`);
    else console.log('MinIO not started (optional for local dev).');

    if (!pid) process.exit(1);
    return;
  }

  switch (service) {
    case 'scaffold': {
      const existing = readScaffoldPid();
      if (existing) {
        console.log(`Scaffold already running (PID ${existing}).`);
        return;
      }
      const pid = await spawnScaffold();
      if (pid) {
        for (let i = 0; i < 20; i++) {
          await new Promise(r => setTimeout(r, 500));
          if (await checkControlHealth()) break;
        }
        console.log(`Scaffold started (PID ${pid}). Services: control, orbital, proxy, daemon.`);
      } else {
        console.error('Failed to start scaffold.');
        process.exit(1);
      }
      break;
    }
    case 'daemon': {
      if (readScaffoldPid()) {
        console.log('Daemon is running in-process under scaffold.');
        return;
      }
      if (isDaemonRunning()) {
        console.log('Standalone daemon already running.');
        return;
      }
      const pid = startDaemon();
      console.log(`Standalone daemon started (PID ${pid}).`);
      break;
    }
    case 'minio': {
      const pid = await spawnMinio();
      if (pid) console.log(`MinIO running (PID ${pid}) on port 9000.`);
      else console.error('Failed to start MinIO.');
      break;
    }
  }
}

async function serviceStop(service?: ServiceTarget): Promise<void> {
  if (!service) {
    // Stop everything
    const scaffoldPid = readScaffoldPid();
    const minioPid = readMinioPid();
    const daemonUp = !scaffoldPid && isDaemonRunning();

    await Promise.all([
      stopScaffold(),
      stopMinio(),
      daemonUp ? Promise.resolve(stopDaemon()) : Promise.resolve(),
    ]);

    if (scaffoldPid) console.log('Scaffold stopped.');
    if (minioPid) console.log('MinIO stopped.');
    if (daemonUp) console.log('Standalone daemon stopped.');
    if (!scaffoldPid && !minioPid && !daemonUp) {
      console.log('Nothing was running (cleaned up any orphans).');
    }
    return;
  }

  switch (service) {
    case 'scaffold':
      await stopScaffold();
      console.log('Scaffold stopped.');
      break;
    case 'daemon':
      if (readScaffoldPid()) {
        console.log('Daemon runs in-process under scaffold. Stop scaffold to stop all services.');
        return;
      }
      if (stopDaemon()) console.log('Standalone daemon stopped.');
      else console.log('Standalone daemon was not running.');
      break;
    case 'minio':
      await stopMinio();
      console.log('MinIO stopped.');
      break;
  }
}

async function serviceReset(service?: ServiceTarget): Promise<void> {
  if (service === 'minio') {
    await stopMinio();
    resetMinioData();
    console.log('MinIO data reset.');
    return;
  }

  // Default: reset everything
  await serviceStop();

  await Promise.all([
    cleanControlState(),
    Promise.resolve(resetMinioData()),
  ]);

  console.log('Reset complete. Start fresh with `repl control start`.');
}

// =============================================================================
// Exported: Command Dispatch
// =============================================================================

export async function controlCommand(args: string[]): Promise<void> {
  const subcommand = args[0];
  const service = parseService(args[1]);

  switch (subcommand) {
    case 'start':
      await serviceStart(service);
      break;
    case 'stop':
      await serviceStop(service);
      break;
    case 'restart':
      await serviceStop(service);
      await serviceStart(service);
      break;
    case 'status':
      await serviceStatus();
      break;
    case 'reset':
      await serviceReset(service);
      break;
    default:
      console.error('Usage: repl control <start|stop|restart|status|reset> [service]');
      console.error('');
      console.error('Services: scaffold (or control/orbital/proxy), daemon, minio');
      console.error('No service specified: all enabled services.');
      process.exit(2);
  }
}

// =============================================================================
// Exported: Auto-Start (called before user-facing commands)
// =============================================================================

/**
 * Ensure services are reachable before running a command.
 *
 * Local mode (localhost):  auto-starts scaffold + minio, waits for health.
 * Remote mode:             prints diagnostic advice and exits if unreachable.
 */
export async function ensureControlPlane(): Promise<void> {
  if (await checkControlHealth()) return;

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

  // Local mode — auto-start scaffold
  const isFirstRun = !existsSync(DB_FILE);

  console.log('Starting services...');
  const [pid, minioPid] = await Promise.all([
    spawnScaffold(),
    spawnMinio(),
  ]);

  if (!pid) {
    console.error('Failed to start services. Is the repo intact?');
    process.exit(2);
  }

  if (minioPid) {
    console.log(`MinIO started (PID ${minioPid}) on port 9000.`);
  }

  for (let i = 0; i < 20; i++) {
    await new Promise(r => setTimeout(r, 500));
    if (await checkControlHealth()) {
      const port = process.env.SKYREPL_PORT || process.env.PORT || '3000';
      console.log(`Services ready (control :${port}).`);
      if (isFirstRun) {
        console.log('');
        console.log('For remote deployment (needed when VMs must reach the control plane):');
        console.log('  repl setup');
      }
      return;
    }
  }

  console.error('Services started but not responding after 10s.');
  console.error('Check logs: ~/.repl/scaffold.log');
  process.exit(2);
}
