import { readFileSync, writeFileSync, unlinkSync, existsSync, readdirSync, rmSync } from 'fs';
import { homedir } from 'os';
import { join, resolve } from 'path';
import { getControlPlaneUrl, REPL_DIR } from '../config';
import { spawn } from 'child_process';

const PID_FILE = join(homedir(), '.repl', 'control.pid');

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

  // Find the control plane entry point relative to CLI
  // This file is at impl/cli/src/commands/, control is at impl/control/src/main.ts
  const controlMain = resolve(__dirname, '../../../control/src/main.ts');
  if (!existsSync(controlMain)) {
    console.error(`Control plane not found at ${controlMain}`);
    process.exit(2);
  }

  const child = spawn('bun', ['run', controlMain], {
    detached: true,
    stdio: 'ignore',
    env: { ...process.env },
  });

  child.unref();

  if (child.pid) {
    writeFileSync(PID_FILE, String(child.pid));
    const port = process.env.SKYREPL_PORT || process.env.PORT || '3000';
    console.log(`Control plane started (PID ${child.pid}) on port ${port}.`);
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
