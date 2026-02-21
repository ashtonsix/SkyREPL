// daemon/lifecycle.ts — Daemon process management
//
// Start/stop/check the local daemon process. Called by CLI commands.
// The daemon itself lives at shell/daemon/index.ts.

import { existsSync, readFileSync, writeFileSync, mkdirSync } from 'fs';
import { join } from 'path';
import { homedir } from 'os';

const HOME = homedir();
const REPL_DIR = join(HOME, '.repl');
const PID_FILE = join(REPL_DIR, 'daemon.pid');

// Path to this file's directory so we can resolve index.ts
const DAEMON_ENTRY = join(import.meta.dir, 'index.ts');

/**
 * Start the daemon as a detached background process.
 * Writes the PID to ~/.repl/daemon.pid.
 * Returns the PID.
 */
export function startDaemon(): number {
  mkdirSync(REPL_DIR, { recursive: true });

  const proc = Bun.spawn(['bun', 'run', DAEMON_ENTRY], {
    stdio: ['ignore', 'ignore', 'ignore'],
    detached: true,
  });

  proc.unref();

  const pid = proc.pid;
  writeFileSync(PID_FILE, String(pid), { encoding: 'utf-8' });
  return pid;
}

/**
 * Stop the daemon by reading the PID file and sending SIGTERM.
 * Returns true if a process was signalled, false if no daemon was running.
 */
export function stopDaemon(): boolean {
  if (!existsSync(PID_FILE)) return false;

  const pid = readPid();
  if (pid === null) return false;

  try {
    process.kill(pid, 'SIGTERM');
    return true;
  } catch {
    // Process already gone
    return false;
  }
}

/**
 * Check whether the daemon process is alive.
 * Uses signal 0 (no-op) to probe the process without affecting it.
 */
export function isDaemonRunning(): boolean {
  if (!existsSync(PID_FILE)) return false;

  const pid = readPid();
  if (pid === null) return false;

  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

/**
 * Start the daemon if it is not already running.
 * Non-blocking — returns immediately after spawning.
 * Safe to call from any CLI command; idempotent.
 */
export function ensureDaemon(): void {
  if (!isDaemonRunning()) {
    startDaemon();
  }
}

// =============================================================================
// Internal helpers
// =============================================================================

function readPid(): number | null {
  try {
    const raw = readFileSync(PID_FILE, 'utf-8').trim();
    const pid = parseInt(raw, 10);
    return isNaN(pid) ? null : pid;
  } catch {
    return null;
  }
}
